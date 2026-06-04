use anyhow::Result;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

use db_con::backtest::domain::engine::{BacktestConfig, BacktestEngine};
use db_con::backtest::domain::fill_collector::BacktestFillCollector;
use db_con::backtest::domain::loader::ScyllaHistoricalLoader;
use db_con::backtest::domain::report::BacktestReport;
use db_con::data_ingestion::adapters::kafka_producer::KafkaProducerAdapter;
use db_con::data_ingestion::ports::MessageProducerPort;
use db_con::database::adapters::scylladb::ScyllaRepository;
use db_con::database::domain::models::{HistoricalCandle, StockTick, Timeframe};
use db_con::database::ports::repository::Repository;
use db_con::features::domain::indicators::{
    BidAskSpreadProxyFeature, BollingerBandsFeature, EmaFeature, LiquidityImbalanceProxyFeature,
    MacdFeature, MeanReversionFeature, MomentumFeature, OrderFlowProxyFeature, RsiFeature,
    VolatilityClusteringFeature, VolatilityFeature, VolumeSmaFeature,
};
use db_con::features::domain::registry::FeatureRegistry;
use db_con::message_broker::adapters::kafka_admin::ensure_topics;
use db_con::message_broker::adapters::kafka_consumer::KafkaConsumerAdapter;
use db_con::message_broker::domain::services::MessageBrokerService;
use db_con::message_broker::ports::MessageHandler;
use db_con::model::domain::candle_linear::CandleLinearModel;
use db_con::model::domain::ensemble::WeightedEnsembleModel;
use db_con::model::domain::models::PredictionModel;
use db_con::trading::adapters::broker_handler::BrokerKafkaHandler;
use db_con::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use db_con::trading::adapters::execution_handler::SimpleExecutionHandler;
use db_con::trading::adapters::execution_handler_kafka::ExecutionKafkaHandler;
use db_con::trading::adapters::momentum_strategy::MomentumStrategy;
use db_con::trading::adapters::portfolio_manager::PortfolioManager;
use db_con::trading::adapters::strategy_handler::StrategyHandler;
use db_con::trading::domain::events::{FillEvent, OrderEvent, OrderSide, OrderType};
use db_con::trading::ports::{
    BrokerSimulatorPort, ExecutionHandlerPort, PortfolioPort, StrategyPort,
};
use db_con::shared::config::{kafka_brokers, scylla_nodes};

#[derive(Clone, Debug)]
struct BacktestArgs {
    symbol: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    capital: Decimal,
    fetch_missing: bool,
    min_confidence: Decimal,
    train_split: Decimal,
    trade_start: Option<DateTime<Utc>>,
    walk_forward: bool,
    wf_windows: usize,
    checkpoint_dir: Option<String>,
    export_dataset: Option<String>,
    mode: String,
    tune_ensemble: bool,
    min_signal_gap_secs: i64,
    max_position_pct: Decimal,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal,
    reserve_cash_pct: Decimal,
}

fn parse_date(s: &str) -> Result<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap()))
}

fn parse_args() -> Result<BacktestArgs> {
    let args: Vec<String> = env::args().collect();
    let mut symbol = "AAPL".to_string();
    let mut start_str = "2022-01-01".to_string();
    let mut end_str = "2024-12-31".to_string();
    let mut capital = dec!(100000);
    let mut fetch_missing = false;
    let mut min_confidence = dec!(0.01);
    let mut train_split = dec!(0.70);
    let mut trade_start: Option<DateTime<Utc>> = None;
    let mut walk_forward = false;
    let mut wf_windows = 4usize;
    let mut checkpoint_dir: Option<String> = None;
    let mut export_dataset: Option<String> = None;
    let mut mode = "direct".to_string();
    let mut tune_ensemble = false;
    let mut min_signal_gap_secs = 900i64;
    let mut max_position_pct = dec!(0.80);
    let mut stop_loss_pct = dec!(0.01);
    let mut take_profit_pct = dec!(0.025);
    let mut reserve_cash_pct = dec!(0.02);

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                symbol = args.get(i + 1).cloned().unwrap_or(symbol);
                i += 2;
            }
            "--start" => {
                start_str = args.get(i + 1).cloned().unwrap_or(start_str);
                i += 2;
            }
            "--end" => {
                end_str = args.get(i + 1).cloned().unwrap_or(end_str);
                i += 2;
            }
            "--capital" => {
                if let Some(c) = args.get(i + 1) {
                    capital = c.parse().unwrap_or(capital);
                }
                i += 2;
            }
            "--fetch-missing" => {
                fetch_missing = true;
                i += 1;
            }
            "--min-confidence" => {
                if let Some(c) = args.get(i + 1) {
                    min_confidence = c.parse().unwrap_or(min_confidence);
                }
                i += 2;
            }
            "--train-split" => {
                if let Some(v) = args.get(i + 1) {
                    train_split = v.parse().unwrap_or(train_split);
                }
                i += 2;
            }
            "--trade-start" => {
                if let Some(v) = args.get(i + 1) {
                    trade_start = Some(parse_date(v)?);
                }
                i += 2;
            }
            "--walk-forward" => {
                walk_forward = true;
                i += 1;
            }
            "--wf-windows" => {
                if let Some(v) = args.get(i + 1) {
                    wf_windows = v.parse().unwrap_or(wf_windows);
                }
                i += 2;
            }
            "--checkpoint-dir" => {
                checkpoint_dir = args.get(i + 1).cloned();
                i += 2;
            }
            "--export-dataset" => {
                export_dataset = args.get(i + 1).cloned();
                i += 2;
            }
            "--mode" => {
                mode = args
                    .get(i + 1)
                    .cloned()
                    .unwrap_or_else(|| "direct".to_string());
                i += 2;
            }
            "--tune-ensemble" => {
                tune_ensemble = true;
                i += 1;
            }
            "--min-signal-gap-secs" => {
                if let Some(v) = args.get(i + 1) {
                    min_signal_gap_secs = v.parse().unwrap_or(min_signal_gap_secs);
                }
                i += 2;
            }
            "--max-position-pct" => {
                if let Some(v) = args.get(i + 1) {
                    max_position_pct = v.parse().unwrap_or(max_position_pct);
                }
                i += 2;
            }
            "--stop-loss-pct" => {
                if let Some(v) = args.get(i + 1) {
                    stop_loss_pct = v.parse().unwrap_or(stop_loss_pct);
                }
                i += 2;
            }
            "--take-profit-pct" => {
                if let Some(v) = args.get(i + 1) {
                    take_profit_pct = v.parse().unwrap_or(take_profit_pct);
                }
                i += 2;
            }
            "--reserve-cash-pct" => {
                if let Some(v) = args.get(i + 1) {
                    reserve_cash_pct = v.parse().unwrap_or(reserve_cash_pct);
                }
                i += 2;
            }
            _ => i += 1,
        }
    }

    Ok(BacktestArgs {
        symbol,
        start: parse_date(&start_str)?,
        end: parse_date(&end_str)?,
        capital,
        fetch_missing,
        min_confidence,
        train_split: train_split.clamp(dec!(0.50), dec!(0.95)),
        trade_start,
        walk_forward,
        wf_windows: wf_windows.clamp(2, 20),
        checkpoint_dir,
        export_dataset,
        mode,
        tune_ensemble,
        min_signal_gap_secs: min_signal_gap_secs.clamp(10, 3600),
        max_position_pct: max_position_pct.clamp(dec!(0.05), dec!(1.00)),
        stop_loss_pct: stop_loss_pct.clamp(dec!(0.002), dec!(0.20)),
        take_profit_pct: take_profit_pct.clamp(dec!(0.002), dec!(0.20)),
        reserve_cash_pct: reserve_cash_pct.clamp(dec!(0.00), dec!(0.50)),
    })
}

fn build_feature_registry() -> Arc<FeatureRegistry> {
    let mut feature_registry = FeatureRegistry::new();
    feature_registry.register("rsi_14".into(), Arc::new(RsiFeature::new(14)));
    feature_registry.register("ema_12".into(), Arc::new(EmaFeature::new(12)));
    feature_registry.register("ema_26".into(), Arc::new(EmaFeature::new(26)));
    feature_registry.register("macd".into(), Arc::new(MacdFeature::new()));
    feature_registry.register(
        "bbands_20".into(),
        Arc::new(BollingerBandsFeature::new(20, dec!(2))),
    );
    feature_registry.register("volume_sma_20".into(), Arc::new(VolumeSmaFeature::new(20)));
    feature_registry.register("momentum_5".into(), Arc::new(MomentumFeature::new(5)));
    feature_registry.register("momentum_20".into(), Arc::new(MomentumFeature::new(20)));
    feature_registry.register(
        "mean_reversion_20".into(),
        Arc::new(MeanReversionFeature::new(20)),
    );
    feature_registry.register("volatility_20".into(), Arc::new(VolatilityFeature::new(20)));
    feature_registry.register(
        "vol_cluster_5_20".into(),
        Arc::new(VolatilityClusteringFeature::new(5, 20)),
    );
    feature_registry.register(
        "bid_ask_spread_proxy".into(),
        Arc::new(BidAskSpreadProxyFeature::new()),
    );
    feature_registry.register(
        "liquidity_imbalance_proxy_20".into(),
        Arc::new(LiquidityImbalanceProxyFeature::new(20)),
    );
    feature_registry.register(
        "order_flow_proxy_20".into(),
        Arc::new(OrderFlowProxyFeature::new(20)),
    );
    Arc::new(feature_registry)
}

fn model_feature_keys() -> Vec<String> {
    vec![
        "ema_12".into(),
        "ema_26".into(),
        "rsi_14".into(),
        "macd".into(),
        "momentum_5".into(),
        "momentum_20".into(),
        "mean_reversion_20".into(),
        "volatility_20".into(),
        "vol_cluster_5_20".into(),
        "bid_ask_spread_proxy".into(),
        "liquidity_imbalance_proxy_20".into(),
        "order_flow_proxy_20".into(),
        "volume_sma_20".into(),
    ]
}

fn build_model() -> Arc<WeightedEnsembleModel> {
    let fast = Arc::new(CandleLinearModel::new(model_feature_keys(), 0.0020, 0.0))
        as Arc<dyn PredictionModel>;
    let medium = Arc::new(CandleLinearModel::new(model_feature_keys(), 0.0010, 0.0))
        as Arc<dyn PredictionModel>;
    let slow = Arc::new(CandleLinearModel::new(model_feature_keys(), 0.0005, 0.0))
        as Arc<dyn PredictionModel>;

    Arc::new(WeightedEnsembleModel::new_with_signal_deadzone(
        vec![(fast, dec!(0.40)), (medium, dec!(0.35)), (slow, dec!(0.25))],
        dec!(0.01),
        dec!(0.0),
    ))
}

fn candidate_weight_sets() -> Vec<Vec<Decimal>> {
    vec![
        vec![dec!(0.60), dec!(0.25), dec!(0.15)],
        vec![dec!(0.45), dec!(0.35), dec!(0.20)],
        vec![dec!(0.35), dec!(0.40), dec!(0.25)],
        vec![dec!(0.30), dec!(0.30), dec!(0.40)],
        vec![dec!(0.20), dec!(0.50), dec!(0.30)],
    ]
}

fn candles_to_ticks(candles: &[HistoricalCandle]) -> Vec<StockTick> {
    candles
        .iter()
        .map(|c| StockTick {
            symbol: c.symbol.clone(),
            timestamp: c.timestamp,
            price: c.close,
            volume: c.volume,
            bid: None,
            ask: None,
            source: c.source.clone(),
        })
        .collect()
}

async fn run_direct_train_test(
    args: &BacktestArgs,
    model: Arc<dyn PredictionModel>,
    feature_registry: Arc<FeatureRegistry>,
    train_ticks: &[StockTick],
    test_ticks: &[StockTick],
) -> Result<BacktestReport> {
    let strategy_impl = Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry,
        model,
        60,
        args.min_confidence,
        args.min_signal_gap_secs,
    )));
    {
        let mut s = strategy_impl.write().await;
        s.set_emit_signals(false);
    }

    for tick in train_ticks {
        let mut s = strategy_impl.write().await;
        let _ = s.on_market_event(tick).await?;
    }

    {
        let mut s = strategy_impl.write().await;
        s.reset_state();
        s.set_emit_signals(true);
        s.set_learning_enabled(false);
    }

    let portfolio_manager = Arc::new(PortfolioManager::new(args.capital));
    let portfolio_port = portfolio_manager.clone() as Arc<dyn PortfolioPort>;
    let execution_handler = SimpleExecutionHandler {
        default_quantity: dec!(0),
        max_position_pct: args.max_position_pct,
        min_trade_quantity: dec!(1),
        stop_loss_pct: args.stop_loss_pct,
        take_profit_pct: args.take_profit_pct,
        reserve_cash_pct: args.reserve_cash_pct,
        portfolio: Some(portfolio_port.clone()),
    };
    let broker = SimpleBrokerSimulator {
        slippage_pct: dec!(0.0002),
        commission: dec!(0.10),
    };

    let mut fills: Vec<FillEvent> = Vec::new();
    for tick in test_ticks {
        let signal_opt = {
            let mut s = strategy_impl.write().await;
            s.on_market_event(tick).await?
        };
        if let Some(signal) = signal_opt {
            if let Some(order) = execution_handler.on_signal(&signal).await? {
                let fill = broker.execute_order(&order).await?;
                portfolio_port.update_on_fill(&fill).await?;
                fills.push(fill);
            }
        }
    }

    // Force-close any open position at the end of the test window so PnL/trades are realized.
    if let Some(last_tick) = test_ticks.last() {
        if let Some(position) = portfolio_port.get_position(&last_tick.symbol).await? {
            let qty = position.quantity.max(Decimal::ZERO);
            if qty > Decimal::ZERO {
                let final_exit_order = OrderEvent {
                    id: uuid::Uuid::new_v4(),
                    signal_id: uuid::Uuid::new_v4(),
                    timestamp: Utc::now(),
                    symbol: last_tick.symbol.clone(),
                    side: OrderSide::Sell,
                    quantity: qty,
                    order_type: OrderType::Market,
                    limit_price: Some(last_tick.price),
                    stop_price: None,
                };
                let fill = broker.execute_order(&final_exit_order).await?;
                portfolio_port.update_on_fill(&fill).await?;
                fills.push(fill);
            }
        }
    }

    let portfolio = portfolio_port.get_portfolio().await?;
    Ok(BacktestReport::from_fills_and_portfolio(
        &fills,
        &portfolio,
        args.capital,
    ))
}

async fn tune_ensemble_weights(
    args: &BacktestArgs,
    feature_registry: Arc<FeatureRegistry>,
    train_ticks: &[StockTick],
) -> Result<(Vec<Decimal>, Decimal, BacktestReport)> {
    if train_ticks.len() < 700 {
        let default_weights = vec![dec!(0.45), dec!(0.35), dec!(0.20)];
        let model = build_model();
        model.set_weights(default_weights.clone()).await;
        model.set_adaptive_learning_enabled(false).await;
        let fit_cut = (train_ticks.len() * 3) / 4;
        let fit_cut = fit_cut.clamp(100, train_ticks.len().saturating_sub(1));
        let fit = &train_ticks[..fit_cut];
        let val = &train_ticks[fit_cut..];
        let report = run_direct_train_test(
            args,
            model as Arc<dyn PredictionModel>,
            feature_registry,
            fit,
            val,
        )
        .await?;
        return Ok((default_weights, args.min_confidence, report));
    }

    let fit_cut = ((train_ticks.len() as f64) * 0.75) as usize;
    let fit_cut = fit_cut.clamp(300, train_ticks.len().saturating_sub(200));
    let fit_ticks = &train_ticks[..fit_cut];
    let val_ticks = &train_ticks[fit_cut..];

    let mut best: Option<(Vec<Decimal>, Decimal, Decimal, BacktestReport)> = None;
    let mut best_profitable: Option<(Vec<Decimal>, Decimal, Decimal, BacktestReport)> = None;
    let mut confidence_grid = vec![
        dec!(0.0005),
        dec!(0.001),
        dec!(0.002),
        dec!(0.003),
        dec!(0.005),
        dec!(0.01),
        dec!(0.02),
        dec!(0.03),
        dec!(0.05),
    ];
    confidence_grid.push(args.min_confidence);
    confidence_grid.sort();
    confidence_grid.dedup();
    for w in candidate_weight_sets() {
        for &conf in &confidence_grid {
            let model = build_model();
            model.set_weights(w.clone()).await;
            model.set_adaptive_learning_enabled(false).await;

            let mut tuned_args = args.clone();
            tuned_args.min_confidence = conf;

            let report = run_direct_train_test(
                &tuned_args,
                model.clone() as Arc<dyn PredictionModel>,
                feature_registry.clone(),
                fit_ticks,
                val_ticks,
            )
            .await?;

            let no_trade_penalty = if report.total_trades == 0 {
                dec!(5.0)
            } else {
                Decimal::ZERO
            };
            let low_trade_penalty = if report.total_trades < 8 {
                Decimal::from((8 - report.total_trades) as u64) * dec!(0.20)
            } else {
                Decimal::ZERO
            };
            let pf_component = if report.profit_factor > Decimal::ZERO {
                report.profit_factor.min(dec!(3.0)) - Decimal::ONE
            } else {
                dec!(-1.0)
            };
            let score = report.total_return_pct - (report.max_drawdown_pct * dec!(0.5))
                + (pf_component * dec!(0.2))
                - no_trade_penalty
                - low_trade_penalty;
            println!(
                "Tune weights {:?}, conf {} => ret {}%, dd {}%, trades {}, score {}",
                w,
                conf,
                report.total_return_pct,
                report.max_drawdown_pct,
                report.total_trades,
                score
            );

            let candidate = (w.clone(), conf, score, report.clone());
            if best
                .as_ref()
                .map(|(_, _, best_score, _)| score > *best_score)
                .unwrap_or(true)
            {
                best = Some(candidate.clone());
            }

            let profitable = report.total_trades >= 5
                && report.total_return_pct > Decimal::ZERO
                && report.profit_factor >= Decimal::ONE;
            if profitable
                && best_profitable
                    .as_ref()
                    .map(|(_, _, best_score, _)| score > *best_score)
                    .unwrap_or(true)
            {
                best_profitable = Some(candidate);
            }
        }
    }
    let (best, best_conf, best_score, best_report) = best_profitable
        .or(best)
        .ok_or_else(|| anyhow::anyhow!("No tuning candidates evaluated"))?;
    if best_score <= Decimal::ZERO {
        println!(
            "Tuning warning: no profitable setup found (best score {}). Using best available.",
            best_score
        );
    }
    println!(
        "Selected ensemble weights: {:?} with min_confidence {} (score {})",
        best, best_conf, best_score
    );
    Ok((best, best_conf, best_report))
}

async fn build_pipeline(
    args: &BacktestArgs,
    market_topic: String,
    signal_topic: String,
    order_topic: String,
    fill_topic: String,
    model: Arc<dyn PredictionModel>,
    feature_registry: Arc<FeatureRegistry>,
    producer: Arc<dyn MessageProducerPort>,
    loader: Arc<ScyllaHistoricalLoader>,
) -> Result<(
    Arc<PortfolioManager>,
    Arc<RwLock<MomentumStrategy>>,
    Arc<RwLock<Vec<db_con::trading::domain::events::FillEvent>>>,
)> {
    let brokers = kafka_brokers();

    ensure_topics(
        &brokers,
        &[
            market_topic.clone(),
            signal_topic.clone(),
            order_topic.clone(),
            fill_topic.clone(),
        ],
    )
    .await?;

    let strategy_impl = Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry,
        model,
        60,
        args.min_confidence,
        args.min_signal_gap_secs,
    )));
    let strategy = strategy_impl.clone() as Arc<RwLock<dyn StrategyPort>>;
    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.0002),
        commission: dec!(0.10),
    }) as Arc<dyn BrokerSimulatorPort>;
    let portfolio_manager = Arc::new(PortfolioManager::new(args.capital));
    let portfolio_port = portfolio_manager.clone() as Arc<dyn PortfolioPort>;

    let execution_handler = Arc::new(SimpleExecutionHandler {
        default_quantity: dec!(0),
        max_position_pct: args.max_position_pct,
        min_trade_quantity: dec!(1),
        stop_loss_pct: args.stop_loss_pct,
        take_profit_pct: args.take_profit_pct,
        reserve_cash_pct: args.reserve_cash_pct,
        portfolio: Some(portfolio_port.clone()),
    }) as Arc<dyn ExecutionHandlerPort>;

    let consumer_strategy = Arc::new(KafkaConsumerAdapter::new(
        &brokers,
        &format!("bt-strategy-{}", Utc::now().timestamp_millis()),
        &[market_topic.clone()],
    )?);
    let consumer_execution = Arc::new(KafkaConsumerAdapter::new(
        &brokers,
        &format!("bt-execution-{}", Utc::now().timestamp_millis()),
        &[signal_topic.clone()],
    )?);
    let consumer_broker = Arc::new(KafkaConsumerAdapter::new(
        &brokers,
        &format!("bt-broker-{}", Utc::now().timestamp_millis()),
        &[order_topic.clone()],
    )?);
    let consumer_portfolio = Arc::new(KafkaConsumerAdapter::new(
        &brokers,
        &format!("bt-portfolio-{}", Utc::now().timestamp_millis()),
        &[fill_topic.clone()],
    )?);

    let strategy_handler = Arc::new(StrategyHandler::new(
        strategy,
        producer.clone(),
        market_topic.clone(),
        signal_topic.clone(),
    )) as Arc<dyn MessageHandler>;
    let execution_handler = Arc::new(ExecutionKafkaHandler::new(
        execution_handler,
        producer.clone(),
        signal_topic.clone(),
        order_topic.clone(),
    )) as Arc<dyn MessageHandler>;
    let broker_handler = Arc::new(BrokerKafkaHandler::new(
        broker,
        producer.clone(),
        order_topic.clone(),
        fill_topic.clone(),
    )) as Arc<dyn MessageHandler>;

    let fills = Arc::new(RwLock::new(Vec::new()));
    let fill_handler = Arc::new(BacktestFillCollector::new(
        portfolio_port,
        fills.clone(),
        fill_topic.clone(),
    )) as Arc<dyn MessageHandler>;

    let mut svc_strategy = MessageBrokerService::new(consumer_strategy);
    svc_strategy.register_handler(strategy_handler);
    let mut svc_execution = MessageBrokerService::new(consumer_execution);
    svc_execution.register_handler(execution_handler);
    let mut svc_broker = MessageBrokerService::new(consumer_broker);
    svc_broker.register_handler(broker_handler);
    let mut svc_portfolio = MessageBrokerService::new(consumer_portfolio);
    svc_portfolio.register_handler(fill_handler);

    tokio::spawn(async move {
        let _ = Arc::new(svc_strategy).start_consuming().await;
    });
    tokio::spawn(async move {
        let _ = Arc::new(svc_execution).start_consuming().await;
    });
    tokio::spawn(async move {
        let _ = Arc::new(svc_broker).start_consuming().await;
    });
    tokio::spawn(async move {
        let _ = Arc::new(svc_portfolio).start_consuming().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
    let _ = loader;
    Ok((portfolio_manager, strategy_impl, fills))
}

async fn export_dataset(
    path: &str,
    candles: &[HistoricalCandle],
    feature_registry: Arc<FeatureRegistry>,
) -> Result<()> {
    if candles.len() < 200 {
        return Ok(());
    }
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let ticks: Vec<StockTick> = candles
        .iter()
        .map(|c| StockTick {
            symbol: c.symbol.clone(),
            timestamp: c.timestamp,
            price: c.close,
            volume: c.volume,
            bid: None,
            ask: None,
            source: c.source.clone(),
        })
        .collect();
    let lookback = 60usize;

    let mut timestamps: Vec<String> = Vec::new();
    let mut symbols: Vec<String> = Vec::new();
    let mut ema_12: Vec<f64> = Vec::new();
    let mut ema_26: Vec<f64> = Vec::new();
    let mut rsi_14: Vec<f64> = Vec::new();
    let mut macd: Vec<f64> = Vec::new();
    let mut momentum_5: Vec<f64> = Vec::new();
    let mut momentum_20: Vec<f64> = Vec::new();
    let mut mean_reversion_20: Vec<f64> = Vec::new();
    let mut volatility_20: Vec<f64> = Vec::new();
    let mut vol_cluster_5_20: Vec<f64> = Vec::new();
    let mut bid_ask_spread_proxy: Vec<f64> = Vec::new();
    let mut liquidity_imbalance_proxy_20: Vec<f64> = Vec::new();
    let mut order_flow_proxy_20: Vec<f64> = Vec::new();
    let mut label_return_5m: Vec<f64> = Vec::new();

    for i in lookback..(ticks.len().saturating_sub(6)) {
        let data = ticks[i - lookback..=i]
            .iter()
            .map(|t| db_con::database::domain::models::Candle {
                symbol: t.symbol.clone(),
                timestamp: t.timestamp,
                timeframe: Timeframe::Tick,
                open: t.price,
                high: t.price,
                low: t.price,
                close: t.price,
                volume: t.volume,
                trades_count: Some(1),
                vwap: None,
            })
            .collect::<Vec<_>>();
        let f = feature_registry.calculate_all(&ticks[i].symbol, &data)?;
        let p0 = ticks[i].price;
        let p5 = ticks[i + 5].price;
        let y = if p0 > Decimal::ZERO {
            (p5 - p0) / p0
        } else {
            Decimal::ZERO
        };

        let g = |k: &str| {
            f.get_scalar(k)
                .unwrap_or(Decimal::ZERO)
                .to_f64()
                .unwrap_or(0.0)
        };
        timestamps.push(ticks[i].timestamp.to_rfc3339());
        symbols.push(ticks[i].symbol.clone());
        ema_12.push(g("ema_12"));
        ema_26.push(g("ema_26"));
        rsi_14.push(g("rsi_14"));
        macd.push(g("macd"));
        momentum_5.push(g("momentum_5"));
        momentum_20.push(g("momentum_20"));
        mean_reversion_20.push(g("mean_reversion_20"));
        volatility_20.push(g("volatility_20"));
        vol_cluster_5_20.push(g("vol_cluster_5_20"));
        bid_ask_spread_proxy.push(g("bid_ask_spread_proxy"));
        liquidity_imbalance_proxy_20.push(g("liquidity_imbalance_proxy_20"));
        order_flow_proxy_20.push(g("order_flow_proxy_20"));
        label_return_5m.push(y.to_f64().unwrap_or(0.0));
    }

    #[cfg(feature = "polars-df")]
    {
        use polars::prelude::*;

        let mut df = DataFrame::new(vec![
            Series::new("timestamp".into(), timestamps).into(),
            Series::new("symbol".into(), symbols).into(),
            Series::new("ema_12".into(), ema_12).into(),
            Series::new("ema_26".into(), ema_26).into(),
            Series::new("rsi_14".into(), rsi_14).into(),
            Series::new("macd".into(), macd).into(),
            Series::new("momentum_5".into(), momentum_5).into(),
            Series::new("momentum_20".into(), momentum_20).into(),
            Series::new("mean_reversion_20".into(), mean_reversion_20).into(),
            Series::new("volatility_20".into(), volatility_20).into(),
            Series::new("vol_cluster_5_20".into(), vol_cluster_5_20).into(),
            Series::new("bid_ask_spread_proxy".into(), bid_ask_spread_proxy).into(),
            Series::new(
                "liquidity_imbalance_proxy_20".into(),
                liquidity_imbalance_proxy_20,
            )
            .into(),
            Series::new("order_flow_proxy_20".into(), order_flow_proxy_20).into(),
            Series::new("label_return_5m".into(), label_return_5m).into(),
        ])?;
        let mut file = std::fs::File::create(path)?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .finish(&mut df)?;
    }

    #[cfg(not(feature = "polars-df"))]
    {
        use std::io::Write;

        let mut w = std::fs::File::create(path)?;
        writeln!(
            w,
            "timestamp,symbol,ema_12,ema_26,rsi_14,macd,momentum_5,momentum_20,mean_reversion_20,volatility_20,vol_cluster_5_20,bid_ask_spread_proxy,liquidity_imbalance_proxy_20,order_flow_proxy_20,label_return_5m"
        )?;
        for i in 0..timestamps.len() {
            writeln!(
                w,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                timestamps[i],
                symbols[i],
                ema_12[i],
                ema_26[i],
                rsi_14[i],
                macd[i],
                momentum_5[i],
                momentum_20[i],
                mean_reversion_20[i],
                volatility_20[i],
                vol_cluster_5_20[i],
                bid_ask_spread_proxy[i],
                liquidity_imbalance_proxy_20[i],
                order_flow_proxy_20[i],
                label_return_5m[i]
            )?;
        }
    }

    println!("Exported ML dataset to {}", path);
    Ok(())
}

async fn run_single_split(
    args: &BacktestArgs,
    model: Arc<dyn PredictionModel>,
    feature_registry: Arc<FeatureRegistry>,
    loader: Arc<ScyllaHistoricalLoader>,
    producer: Arc<dyn MessageProducerPort>,
    train_start: DateTime<Utc>,
    train_end: DateTime<Utc>,
    test_start: DateTime<Utc>,
    test_end: DateTime<Utc>,
) -> Result<db_con::backtest::domain::report::BacktestReport> {
    let ts = Utc::now().timestamp_millis();
    let market_topic = format!("backtest-market-data-raw-{}", ts);
    let signal_topic = format!("backtest-trading-signals-{}", ts);
    let order_topic = format!("backtest-trading-orders-{}", ts);
    let fill_topic = format!("backtest-trading-fills-{}", ts);

    let (portfolio_manager, strategy_impl, fills) = build_pipeline(
        args,
        market_topic.clone(),
        signal_topic,
        order_topic,
        fill_topic,
        model,
        feature_registry,
        producer,
        loader.clone(),
    )
    .await?;

    {
        let mut s = strategy_impl.write().await;
        s.set_emit_signals(false);
    }
    let train_cfg = BacktestConfig {
        symbol: args.symbol.clone(),
        start: train_start,
        end: train_end,
        initial_capital: args.capital,
        topic: market_topic.clone(),
    };
    let train_engine = BacktestEngine::new(
        loader.clone(),
        portfolio_manager.clone(),
        fills.clone(),
        train_cfg,
    );
    let _ = train_engine.run().await?;

    {
        let mut s = strategy_impl.write().await;
        s.reset_state();
        s.set_emit_signals(true);
    }
    fills.write().await.clear();
    portfolio_manager.reset(args.capital).await?;

    let test_cfg = BacktestConfig {
        symbol: args.symbol.clone(),
        start: test_start,
        end: test_end,
        initial_capital: args.capital,
        topic: market_topic,
    };
    let test_engine = BacktestEngine::new(loader, portfolio_manager.clone(), fills, test_cfg);
    test_engine.run().await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let mut args = parse_args()?;
    let now = Utc::now();
    if args.end > now {
        println!(
            "Requested end {} is in the future. Clamping to {}.",
            args.end, now
        );
        args.end = now;
    }
    if args.start >= args.end {
        anyhow::bail!(
            "Invalid range: start {} must be before end {}",
            args.start,
            args.end
        );
    }
    if let Some(ts) = args.trade_start {
        if ts >= args.end {
            println!(
                "trade-start {} is not before end {}; ignoring trade-start.",
                ts, args.end
            );
            args.trade_start = None;
        }
    }

    println!("=== Backtest Configuration ===");
    println!("Symbol: {}", args.symbol);
    println!("Period: {} to {}", args.start, args.end);
    println!("Initial Capital: {}", args.capital);
    println!("Min Confidence: {}", args.min_confidence);
    println!("Train Split: {}", args.train_split);
    println!(
        "Trade Start: {}",
        args.trade_start
            .map(|d| d.to_string())
            .unwrap_or_else(|| "None".to_string())
    );
    println!("Walk Forward: {}", args.walk_forward);
    println!("Mode: {}", args.mode);
    println!("Tune Ensemble: {}", args.tune_ensemble);
    println!(
        "Risk Params: gap={}s, max_pos={}, stop_loss={}, take_profit={}, reserve_cash={}",
        args.min_signal_gap_secs,
        args.max_position_pct,
        args.stop_loss_pct,
        args.take_profit_pct,
        args.reserve_cash_pct
    );
    println!();

    let repository = Arc::new(ScyllaRepository::new(scylla_nodes(), "market_data").await?)
        as Arc<dyn Repository>;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let producer = Arc::new(KafkaProducerAdapter::new(&kafka_brokers())?) as Arc<dyn MessageProducerPort>;
    let loader = Arc::new(ScyllaHistoricalLoader::new(
        repository,
        producer.clone(),
        args.fetch_missing,
    ));
    let feature_registry = build_feature_registry();

    let all_candles = loader
        .load_candles(&args.symbol, args.start, args.end)
        .await?;
    if all_candles.is_empty() {
        println!("No data in requested range.");
        return Ok(());
    }
    if let Some(path) = &args.export_dataset {
        export_dataset(path, &all_candles, feature_registry.clone()).await?;
    }

    let model = build_model();
    if let Some(dir) = &args.checkpoint_dir {
        let _ = (model.clone() as Arc<dyn PredictionModel>)
            .load_checkpoint(dir)
            .await;
    }

    if !args.walk_forward {
        let (train_start, train_end, test_start, test_end, train_len, test_len) =
            if let Some(ts) = args.trade_start {
                let split_idx = all_candles
                    .iter()
                    .position(|c| c.timestamp >= ts)
                    .unwrap_or(all_candles.len().saturating_sub(1))
                    .clamp(1, all_candles.len().saturating_sub(1));
                (
                    all_candles.first().unwrap().timestamp,
                    all_candles[split_idx - 1].timestamp,
                    all_candles[split_idx].timestamp,
                    all_candles.last().unwrap().timestamp,
                    split_idx,
                    all_candles.len() - split_idx,
                )
            } else {
                let split = args.train_split.to_string().parse::<f64>().unwrap_or(0.7);
                let split_idx = ((all_candles.len() as f64) * split) as usize;
                let split_idx = split_idx.clamp(1, all_candles.len().saturating_sub(1));
                (
                    all_candles.first().unwrap().timestamp,
                    all_candles[split_idx - 1].timestamp,
                    all_candles[split_idx].timestamp,
                    all_candles.last().unwrap().timestamp,
                    split_idx,
                    all_candles.len() - split_idx,
                )
            };
        println!(
            "Training window: {} .. {} ({} candles)",
            train_start, train_end, train_len
        );
        println!(
            "Test window: {} .. {} ({} candles)",
            test_start, test_end, test_len
        );

        let train_candles: Vec<HistoricalCandle> = all_candles
            .iter()
            .filter(|c| c.timestamp >= train_start && c.timestamp <= train_end)
            .cloned()
            .collect();
        let test_candles: Vec<HistoricalCandle> = all_candles
            .iter()
            .filter(|c| c.timestamp >= test_start && c.timestamp <= test_end)
            .cloned()
            .collect();
        let train_ticks = candles_to_ticks(&train_candles);
        let test_ticks = candles_to_ticks(&test_candles);

        let mut run_args = args.clone();
        if args.tune_ensemble {
            let (best, best_conf, val_report) =
                tune_ensemble_weights(&args, feature_registry.clone(), &train_ticks).await?;
            println!(
                "Validation after tuning => ret {}%, dd {}%, trades {}, pf {}",
                val_report.total_return_pct,
                val_report.max_drawdown_pct,
                val_report.total_trades,
                val_report.profit_factor
            );
            model.set_weights(best).await;
            model.set_adaptive_learning_enabled(false).await;
            run_args.min_confidence = best_conf;
        }

        let report = if args.mode.eq_ignore_ascii_case("direct") {
            run_direct_train_test(
                &run_args,
                model.clone() as Arc<dyn PredictionModel>,
                feature_registry.clone(),
                &train_ticks,
                &test_ticks,
            )
            .await?
        } else {
            run_single_split(
                &args,
                model.clone() as Arc<dyn PredictionModel>,
                feature_registry.clone(),
                loader.clone(),
                producer.clone(),
                train_start,
                train_end,
                test_start,
                test_end,
            )
            .await?
        };
        println!("\n=== Backtest Report ===");
        println!("Final Portfolio Value: {}", report.final_portfolio_value);
        println!(
            "Total Return: {} ({}%)",
            report.total_return, report.total_return_pct
        );
        println!("Sharpe Ratio (ann.): {}", report.sharpe_ratio);
        match report.sortino_ratio {
            Some(s) => println!("Sortino Ratio (ann.): {}", s),
            None if report.total_trades < 10 =>
                println!("Sortino Ratio (ann.): N/A (менше 10 угод)"),
            None =>
                println!("Sortino Ratio (ann.): N/A (σ_down = 0, усі equity-кроки невід'ємні)"),
        }
        println!(
            "Max Drawdown: {} ({}%)",
            report.max_drawdown, report.max_drawdown_pct
        );
        println!("Win Rate: {}%", report.win_rate);
        println!("Total Trades: {}", report.total_trades);
        println!(
            "Winning: {} | Losing: {}",
            report.winning_trades, report.losing_trades
        );
        println!(
            "Avg Win: {} | Avg Loss: {}",
            report.avg_win, report.avg_loss
        );
        if report.losing_trades == 0 {
            println!("Profit Factor: N/A (no losing trades)");
        } else {
            println!("Profit Factor: {}", report.profit_factor);
        }
        if report.total_trades < 5 {
            println!(
                "Report warning: лише {} угод(и) — метрики статистично нестабільні.",
                report.total_trades
            );
        } else if report.total_trades < 30 {
            println!(
                "Report warning: {} угод — для надійної інтерпретації win rate і Sharpe \
                 бажано 30+. Розгляньте довший інтервал або ширший min_confidence.",
                report.total_trades
            );
        }
    } else {
        let n = all_candles.len();
        let window = n / args.wf_windows.max(2);
        let mut reports = Vec::new();
        for i in 1..args.wf_windows {
            let train_end_idx = window * i;
            let test_end_idx = (window * (i + 1)).min(n - 1);
            if train_end_idx + 1 >= test_end_idx {
                continue;
            }
            let train_start = all_candles[0].timestamp;
            let train_end = all_candles[train_end_idx - 1].timestamp;
            let test_start = all_candles[train_end_idx].timestamp;
            let test_end = all_candles[test_end_idx].timestamp;
            println!(
                "\nWF Fold {}: train {}..{} | test {}..{}",
                i, train_start, train_end, test_start, test_end
            );
            let train_candles: Vec<HistoricalCandle> = all_candles
                .iter()
                .filter(|c| c.timestamp >= train_start && c.timestamp <= train_end)
                .cloned()
                .collect();
            let test_candles: Vec<HistoricalCandle> = all_candles
                .iter()
                .filter(|c| c.timestamp >= test_start && c.timestamp <= test_end)
                .cloned()
                .collect();
            let train_ticks = candles_to_ticks(&train_candles);
            let test_ticks = candles_to_ticks(&test_candles);
            let mut run_args = args.clone();
            if args.tune_ensemble {
                let (best, best_conf, val_report) =
                    tune_ensemble_weights(&args, feature_registry.clone(), &train_ticks).await?;
                println!(
                    "Fold {} validation after tuning => ret {}%, dd {}%, trades {}, pf {}",
                    i,
                    val_report.total_return_pct,
                    val_report.max_drawdown_pct,
                    val_report.total_trades,
                    val_report.profit_factor
                );
                model.set_weights(best).await;
                model.set_adaptive_learning_enabled(false).await;
                run_args.min_confidence = best_conf;
            }
            let rep = if args.mode.eq_ignore_ascii_case("direct") {
                run_direct_train_test(
                    &run_args,
                    model.clone() as Arc<dyn PredictionModel>,
                    feature_registry.clone(),
                    &train_ticks,
                    &test_ticks,
                )
                .await?
            } else {
                run_single_split(
                    &args,
                    model.clone() as Arc<dyn PredictionModel>,
                    feature_registry.clone(),
                    loader.clone(),
                    producer.clone(),
                    train_start,
                    train_end,
                    test_start,
                    test_end,
                )
                .await?
            };
            println!(
                "Fold {} return: {}% | trades: {}",
                i, rep.total_return_pct, rep.total_trades
            );
            reports.push(rep);
        }
        if !reports.is_empty() {
            let n = Decimal::from(reports.len() as u64);
            let avg_ret: Decimal =
                reports.iter().map(|r| r.total_return_pct).sum::<Decimal>() / n;
            let avg_sharpe: Decimal =
                reports.iter().map(|r| r.sharpe_ratio).sum::<Decimal>() / n;
            let total_trades: usize = reports.iter().map(|r| r.total_trades).sum();

            let sortino_vals: Vec<Decimal> =
                reports.iter().filter_map(|r| r.sortino_ratio).collect();
            let avg_sortino_str = if sortino_vals.is_empty() {
                "N/A".to_string()
            } else {
                let s = sortino_vals.iter().copied().sum::<Decimal>()
                    / Decimal::from(sortino_vals.len() as u64);
                s.to_string()
            };

            println!("\n=== Walk-Forward Summary ===");
            println!("Folds: {}", reports.len());
            println!("Average Return: {}%", avg_ret);
            println!("Average Sharpe: {}", avg_sharpe);
            println!("Average Sortino: {}", avg_sortino_str);
            println!("Total Trades: {}", total_trades);
        }
    }

    if let Some(dir) = &args.checkpoint_dir {
        (model as Arc<dyn PredictionModel>)
            .save_checkpoint(dir)
            .await?;
        println!("Saved model checkpoint to {}", dir);
    }

    Ok(())
}
