//! Чесний матч ML-ансамблю проти TSMOM і бенчмарків.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

use super::{log_run, QuantArgs};
use crate::backtest::application::benchmark_runner::BenchmarkRunner;
use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use crate::trading::adapters::portfolio_manager::PortfolioManager;
use crate::trading::domain::strategies::tsmom_strategy::TsmomStrategy;
use crate::trading::domain::allocation::{AlignedMarketData, UniverseView};
use crate::trading::domain::costs::cost_model_from_config;
use crate::trading::ports::{BrokerSimulatorPort, PortfolioPort};

/// Чесний матч ML-ансамблю проти TSMOM і бенчмарків (той самий період,
/// ті самі витрати, той самий execution-пайплайн).
///
/// Протокол як у дипломному бектесті: навчання на перших train_ratio даних
/// (сигнали вимкнені) → скидання стану → торгівля на решті БЕЗ донавчання.
/// TSMOM і бенчмарки ганяються на ТОМУ Ж тестовому вікні.
pub async fn run_ml_match(args: &QuantArgs, data: Arc<AlignedMarketData>) -> Result<()> {
    use crate::backtest::domain::metrics as qmetrics;
    use crate::backtest::domain::multi_report::{ComparativeReport, StrategySummaryRow};
    use crate::backtest::domain::report::BacktestReport;
    use crate::database::domain::models::StockTick;
    use crate::model::domain::adaptive_linear::AdaptiveLinearModel;
    use crate::model::domain::ensemble::WeightedEnsembleModel;
    use crate::model::domain::models::PredictionModel;
    use crate::model::domain::random_forest_like::RandomForestLikeModel;
    use crate::trading::adapters::execution_handler::SimpleExecutionHandler;
    use crate::trading::domain::strategies::momentum_strategy::MomentumStrategy;
    use crate::trading::domain::events::{OrderEvent, OrderSide, OrderType};
    use crate::trading::domain::sizing::PositionSizer;
    use crate::trading::ports::{ExecutionHandlerPort, StrategyPort};
    use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

    let n = data.len();
    let train_ratio = args
        .config
        .walk_forward
        .train_ratio
        .to_f64()
        .unwrap_or(0.7);
    let split = ((n as f64) * train_ratio) as usize;
    let split = split.clamp(100, n - 30);

    // Бари → потік тіків у хронологічному порядку (символи відсортовані —
    // детермінізм). Ціна = close, обсяг = обсяг бару.
    let symbols: Vec<String> = data.symbols().cloned().collect();
    let ticks_for = |from: usize, to: usize| -> Vec<StockTick> {
        let mut out = Vec::with_capacity((to - from) * symbols.len());
        for t in from..to {
            let view = UniverseView::new(data.clone(), t);
            for sym in &symbols {
                let Some(price) = view.price(sym) else { continue };
                let volume = view
                    .avg_volume(sym, 1)
                    .and_then(|v| v.to_i64())
                    .unwrap_or(0);
                out.push(StockTick {
                    symbol: sym.clone(),
                    timestamp: view.timestamp(),
                    price,
                    volume,
                    bid: None,
                    ask: None,
                    source: "csv".into(),
                });
            }
        }
        out
    };
    let train_ticks = ticks_for(0, split);
    let test_ticks = ticks_for(split, n);
    println!(
        "ML-матч: {} символів; train {} барів, test {} барів (з бару {})",
        symbols.len(),
        split,
        n - split,
        split
    );

    // Ансамбль — ТІЛЬКИ з конфіга (M0.4): ваги, learning rates, deadzone, сід.
    let ens = &args.config.ensemble;
    let feature_keys: Vec<String> = vec![
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
    ];
    let build_ensemble = |forest: bool| -> Arc<WeightedEnsembleModel> {
        let lr = |i: usize, d: f64| ens.learning_rates.get(i).copied().unwrap_or(d);
        let w = |i: usize, d: Decimal| ens.weights.get(i).copied().unwrap_or(d);
        let submodels: Vec<(Arc<dyn PredictionModel>, Decimal)> = if forest {
            // Той самий склад RF-ансамблю, що в live http-handler.
            vec![
                (
                    Arc::new(RandomForestLikeModel::new_with_params_and_seed(
                        feature_keys.clone(),
                        500,
                        100,
                        256,
                        8,
                        4,
                        0.004,
                        ens.seed,
                    )) as Arc<dyn PredictionModel>,
                    w(0, dec!(0.40)),
                ),
                (
                    Arc::new(RandomForestLikeModel::new_with_params_and_seed(
                        feature_keys.clone(),
                        1200,
                        240,
                        512,
                        14,
                        6,
                        0.006,
                        ens.seed + 1,
                    )) as Arc<dyn PredictionModel>,
                    w(1, dec!(0.35)),
                ),
                (
                    Arc::new(RandomForestLikeModel::new_with_params_and_seed(
                        feature_keys.clone(),
                        2600,
                        520,
                        1024,
                        24,
                        8,
                        0.009,
                        ens.seed + 2,
                    )) as Arc<dyn PredictionModel>,
                    w(2, dec!(0.25)),
                ),
            ]
        } else {
            // Лінійний адаптивний ансамбль fast/medium/slow.
            vec![
                (
                    Arc::new(AdaptiveLinearModel::new(
                        feature_keys.clone(),
                        Decimal::from_f64(lr(0, 0.0020)).unwrap_or(dec!(0.002)),
                    )) as Arc<dyn PredictionModel>,
                    w(0, dec!(0.40)),
                ),
                (
                    Arc::new(AdaptiveLinearModel::new(
                        feature_keys.clone(),
                        Decimal::from_f64(lr(1, 0.0010)).unwrap_or(dec!(0.001)),
                    )) as Arc<dyn PredictionModel>,
                    w(1, dec!(0.35)),
                ),
                (
                    Arc::new(AdaptiveLinearModel::new(
                        feature_keys.clone(),
                        Decimal::from_f64(lr(2, 0.0005)).unwrap_or(dec!(0.0005)),
                    )) as Arc<dyn PredictionModel>,
                    w(2, dec!(0.25)),
                ),
            ]
        };
        Arc::new(WeightedEnsembleModel::new_with_signal_deadzone(
            submodels,
            ens.signal_deadzone,
            dec!(0.0),
        ))
    };

    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.0002),
        cost_model: cost_model_from_config(&args.config.costs),
    }) as Arc<dyn BrokerSimulatorPort>;

    // Прогін одного варіанта ансамблю через повний пайплайн.
    let run_variant = |name: &'static str, forest: bool| {
        let broker = broker.clone();
        let data = data.clone();
        let symbols = symbols.clone();
        let train_ticks = train_ticks.clone();
        let test_ticks = test_ticks.clone();
        let build = build_ensemble;
        async move {
            let model = build(forest);
            model
                .set_adaptive_learning_enabled(args.config.ensemble.adaptive_learning)
                .await;
            let registry = crate::bootstrap::features::init_features();
            let mut strategy = MomentumStrategy::new(
                registry,
                model as Arc<dyn PredictionModel>,
                args.config.strategy.lookback_size,
                args.config.strategy.min_confidence,
                0, // денні бари: часовий гейт не потрібен
            );

            // Фаза 1: навчання (сигнали вимкнені).
            strategy.set_emit_signals(false);
            for tick in &train_ticks {
                let _ = strategy.on_market_event(tick).await?;
            }
            // Фаза 2: торгівля без донавчання (протокол диплому).
            strategy.reset_state();
            strategy.set_emit_signals(true);
            strategy.set_learning_enabled(false);

            let last_symbol = symbols.last().cloned().context("порожній юніверс")?;
            let portfolio: Arc<dyn PortfolioPort> =
                Arc::new(PortfolioManager::new(args.capital));
            let execution = SimpleExecutionHandler {
                sizer: PositionSizer::from_config(args.config.sizing.clone()),
                portfolio: Some(portfolio.clone()),
            };

            let mut fills = Vec::new();
            let mut equity_curve = vec![args.capital];
            let mut current_bar_ts = None;
            for tick in &test_ticks {
                // Закриття бару: mark-to-market перед першим тіком нового бару.
                if current_bar_ts.is_some() && current_bar_ts != Some(tick.timestamp) {
                    // (еквіті фіксується нижче після обробки всіх тіків бару)
                }
                let signal_opt = strategy.on_market_event(tick).await?;
                if let Some(signal) = signal_opt {
                    if let Some(order) = execution.on_signal(&signal).await? {
                        let fill = broker.execute_order(&order).await?;
                        portfolio.update_on_fill(&fill).await?;
                        fills.push(fill);
                    }
                }
                current_bar_ts = Some(tick.timestamp);
                // Останній символ бару → зафіксувати еквіті бару.
                if tick.symbol == last_symbol {
                    let snapshot = portfolio.get_portfolio().await?;
                    let mut equity = snapshot.cash;
                    for (sym, pos) in &snapshot.positions {
                        // Ціна закриття цього бару.
                        let bar_price = test_ticks
                            .iter()
                            .rev()
                            .find(|t| t.timestamp == tick.timestamp && t.symbol == *sym)
                            .map(|t| t.price)
                            .unwrap_or(pos.current_price);
                        equity += pos.quantity * bar_price;
                    }
                    equity_curve.push(equity);
                }
            }

            // Форс-закриття залишкових позицій на останньому барі.
            let last_view = UniverseView::new(data.clone(), data.len() - 1);
            let snapshot = portfolio.get_portfolio().await?;
            for (sym, pos) in snapshot.positions {
                if pos.quantity <= Decimal::ZERO {
                    continue;
                }
                let Some(price) = last_view.price(&sym) else { continue };
                let seed = format!("ml-final-exit|{name}|{sym}");
                let order = OrderEvent {
                    id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()),
                    signal_id: uuid::Uuid::nil(),
                    timestamp: last_view.timestamp(),
                    symbol: sym.clone(),
                    side: OrderSide::Sell,
                    quantity: pos.quantity,
                    order_type: OrderType::Market,
                    limit_price: Some(price),
                    stop_price: None,
                    market_context: None,
                };
                let fill = broker.execute_order(&order).await?;
                portfolio.update_on_fill(&fill).await?;
                fills.push(fill);
            }
            let final_portfolio = portfolio.get_portfolio().await?;
            if let Some(last) = equity_curve.last_mut() {
                *last = final_portfolio.get_total_value();
            }

            // Звіт: trade-статистика з філів, ризикові метрики — з bar-level
            // кривої (та сама шкала, що в TSMOM/бенчмарків).
            let mut report =
                BacktestReport::from_fills_and_portfolio(&fills, &final_portfolio, args.capital);
            let (mdd, mdd_pct) = qmetrics::max_drawdown(&equity_curve);
            report.sharpe_ratio = qmetrics::sharpe_annualized(&equity_curve);
            report.max_drawdown = mdd;
            report.max_drawdown_pct = mdd_pct;
            report.calmar = qmetrics::calmar(report.total_return_pct, mdd_pct);
            let avg_equity = equity_curve.iter().copied().sum::<Decimal>()
                / Decimal::from(equity_curve.len().max(1) as u64);
            report.turnover = qmetrics::turnover(&fills, avg_equity);

            Ok::<StrategySummaryRow, anyhow::Error>(StrategySummaryRow {
                name: name.to_string(),
                report,
                median_sharpe: None,
                instruments: Vec::new(),
            })
        }
    };

    println!("Ганяю лінійний ансамбль (fast/medium/slow adaptive linear)...");
    let ml_linear = run_variant("ml_linear_ensemble", false).await?;
    println!("Ганяю random-forest ансамбль (як у live)...");
    let ml_forest = run_variant("ml_forest_ensemble", true).await?;

    // TSMOM + бенчмарки на ТОМУ Ж тестовому вікні, тим самим брокером.
    let runner = BenchmarkRunner::new(data.clone(), broker.clone(), args.capital);
    let mut tsmom = TsmomStrategy::new(
        args.config.strategy.momentum_lookback,
        20,
        args.config.strategy.vol_target_annual,
        21,
        false,
    );
    let mut comparative = runner
        .run_with_benchmarks(&mut tsmom, false, split, n)
        .await?;

    // Зведена таблиця: обидва ML-варіанти + tsmom + пасивні бенчмарки.
    let mut benchmarks = vec![ml_forest, comparative.strategy.clone()];
    benchmarks.append(&mut comparative.benchmarks);
    let final_report = ComparativeReport {
        strategy: ml_linear,
        benchmarks,
    };
    println!(
        "\n=== ML-МАТЧ: той самий OOS-період (останні {} барів), ті самі витрати ===",
        n - split
    );
    print!("{}", final_report.render());

    log_run(args, serde_json::json!({
        "strategy": "ml_match",
        "test_window_bars": n - split,
        "comparative": serde_json::to_value(&final_report)?,
    }))
    .await;
    Ok(())
}
