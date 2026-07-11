use axum::{extract::State, http::StatusCode, Json};
use chrono::{NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backtest::domain::engine::{BacktestConfig, BacktestEngine};
use crate::backtest::domain::fill_collector::BacktestFillCollector;
use crate::backtest::domain::loader::ScyllaHistoricalLoader;
use crate::features::domain::indicators::{
    BidAskSpreadProxyFeature, BollingerBandsFeature, EmaFeature, LiquidityImbalanceProxyFeature,
    MacdFeature, MeanReversionFeature, MomentumFeature, OrderFlowProxyFeature, RsiFeature,
    VolatilityClusteringFeature, VolatilityFeature, VolumeSmaFeature,
};
use crate::features::domain::registry::FeatureRegistry;
use crate::message_broker::adapters::kafka_admin::ensure_topics;
use crate::message_broker::adapters::kafka_consumer::KafkaConsumerAdapter;
use crate::message_broker::domain::services::MessageBrokerService;
use crate::message_broker::ports::MessageHandler;
use crate::model::domain::ensemble::WeightedEnsembleModel;
use crate::model::domain::models::PredictionModel;
use crate::model::domain::random_forest_like::RandomForestLikeModel;
use crate::trading::adapters::broker_handler::BrokerKafkaHandler;
use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use crate::trading::adapters::execution_handler::SimpleExecutionHandler;
use crate::trading::adapters::execution_handler_kafka::ExecutionKafkaHandler;
use crate::trading::adapters::momentum_strategy::MomentumStrategy;
use crate::trading::adapters::portfolio_manager::PortfolioManager;
use crate::trading::adapters::strategy_handler::StrategyHandler;
use crate::trading::ports::{
    BrokerSimulatorPort, ExecutionHandlerPort, PortfolioPort, StrategyPort,
};
use crate::shared::config::kafka_brokers;

use crate::http::models::BacktestRequest;
use crate::http::middlewares::auth::Claims;
use super::AppState;

#[utoipa::path(
    post,
    path = "/api/backtest",
    tag = "Backtest",
    request_body = BacktestRequest,
    responses(
        (status = 200, description = "Backtest report", body = Object),
        (status = 400, description = "Invalid date format", body = String),
        (status = 500, description = "Internal error", body = String),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn run_backtest(
    State(state): State<AppState>,
    _claims: Claims,
    Json(payload): Json<BacktestRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let start = match NaiveDate::parse_from_str(&payload.start, "%Y-%m-%d") {
        Ok(d) => Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("Invalid start date: {}", e) })),
            );
        }
    };
    let end = match NaiveDate::parse_from_str(&payload.end, "%Y-%m-%d") {
        Ok(d) => Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("Invalid end date: {}", e) })),
            );
        }
    };

    let initial_capital = Decimal::try_from(payload.initial_capital).unwrap_or(dec!(100000));
    let fetch_missing = std::env::var("BACKTEST_FETCH_MISSING")
        .unwrap_or_else(|_| "false".to_string())
        .to_lowercase();
    let fetch_missing = fetch_missing == "true" || fetch_missing == "1" || fetch_missing == "yes";
    let min_confidence = std::env::var("BACKTEST_MIN_CONFIDENCE")
        .ok()
        .and_then(|v| v.parse::<Decimal>().ok())
        .unwrap_or(dec!(0.01));
    let brokers = kafka_brokers();

    // --- Feature Registry ---
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
    let feature_registry = Arc::new(feature_registry);

    // --- Model ---
    let feature_keys = vec![
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
    let fast_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys.clone(),
        500,
        100,
        256,
        8,
        4,
        0.004,
    )) as Arc<dyn PredictionModel>;
    let medium_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys.clone(),
        1200,
        240,
        512,
        14,
        6,
        0.006,
    )) as Arc<dyn PredictionModel>;
    let slow_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys,
        2600,
        520,
        1024,
        24,
        8,
        0.009,
    )) as Arc<dyn PredictionModel>;
    let model = Arc::new(WeightedEnsembleModel::new(
        vec![
            (fast_rf, dec!(0.40)),
            (medium_rf, dec!(0.35)),
            (slow_rf, dec!(0.25)),
        ],
        dec!(0.01),
    )) as Arc<dyn PredictionModel>;

    // --- Strategy ---
    let strategy = Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry,
        model,
        60,
        min_confidence,
        60,
    ))) as Arc<RwLock<dyn StrategyPort>>;

    // --- Broker (модель витрат і сайзинг — з єдиного RunConfig, M0.4) ---
    let run_config = crate::shared::run_config::RunConfig::default();
    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.001),
        cost_model: crate::trading::domain::costs::cost_model_from_config(&run_config.costs),
    }) as Arc<dyn BrokerSimulatorPort>;

    // --- Portfolio ---
    let portfolio_manager =
        Arc::new(PortfolioManager::new(initial_capital)) as Arc<dyn PortfolioPort>;

    // --- Execution ---
    let execution_handler = Arc::new(SimpleExecutionHandler {
        sizer: crate::trading::domain::sizing::PositionSizer::from_config(
            run_config.sizing.clone(),
        ),
        portfolio: Some(portfolio_manager.clone()),
    }) as Arc<dyn ExecutionHandlerPort>;

    // --- Kafka consumers for this backtest run ---
    let ts = chrono::Utc::now().timestamp_millis();
    let market_topic = format!("backtest-market-data-raw-{}", ts);
    let signal_topic = format!("backtest-trading-signals-{}", ts);
    let order_topic = format!("backtest-trading-orders-{}", ts);
    let fill_topic = format!("backtest-trading-fills-{}", ts);

    if let Err(e) = ensure_topics(
        &brokers,
        &[
            market_topic.clone(),
            signal_topic.clone(),
            order_topic.clone(),
            fill_topic.clone(),
        ],
    )
    .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Kafka topic init error: {}", e) })),
        );
    }

    let consumer_strategy = match KafkaConsumerAdapter::new(
        &brokers,
        &format!("http-bt-strategy-{}", ts),
        std::slice::from_ref(&market_topic),
    ) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Kafka consumer error: {}", e) })),
            );
        }
    };

    let consumer_execution = match KafkaConsumerAdapter::new(
        &brokers,
        &format!("http-bt-execution-{}", ts),
        std::slice::from_ref(&signal_topic),
    ) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Kafka consumer error: {}", e) })),
            );
        }
    };

    let consumer_broker = match KafkaConsumerAdapter::new(
        &brokers,
        &format!("http-bt-broker-{}", ts),
        std::slice::from_ref(&order_topic),
    ) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Kafka consumer error: {}", e) })),
            );
        }
    };

    let consumer_portfolio = match KafkaConsumerAdapter::new(
        &brokers,
        &format!("http-bt-portfolio-{}", ts),
        std::slice::from_ref(&fill_topic),
    ) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Kafka consumer error: {}", e) })),
            );
        }
    };

    // --- Wire handlers ---
    let strategy_handler = Arc::new(StrategyHandler::new(
        strategy,
        state.producer.clone(),
        market_topic.clone(),
        signal_topic.clone(),
    )) as Arc<dyn MessageHandler>;
    let execution_kafka_handler = Arc::new(ExecutionKafkaHandler::new(
        execution_handler,
        state.producer.clone(),
        signal_topic.clone(),
        order_topic.clone(),
    )) as Arc<dyn MessageHandler>;
    let broker_kafka_handler = Arc::new(BrokerKafkaHandler::new(
        broker,
        state.producer.clone(),
        order_topic.clone(),
        fill_topic.clone(),
    )) as Arc<dyn MessageHandler>;
    let fills = Arc::new(RwLock::new(Vec::new()));
    let fill_kafka_handler = Arc::new(BacktestFillCollector::new(
        portfolio_manager.clone(),
        fills.clone(),
        fill_topic.clone(),
    )) as Arc<dyn MessageHandler>;

    let mut svc_strategy = MessageBrokerService::new(consumer_strategy);
    svc_strategy.register_handler(strategy_handler);

    let mut svc_execution = MessageBrokerService::new(consumer_execution);
    svc_execution.register_handler(execution_kafka_handler);

    let mut svc_broker = MessageBrokerService::new(consumer_broker);
    svc_broker.register_handler(broker_kafka_handler);

    let mut svc_portfolio = MessageBrokerService::new(consumer_portfolio);
    svc_portfolio.register_handler(fill_kafka_handler);

    // --- Spawn pipeline consumers ---
    let svc = Arc::new(svc_strategy);
    tokio::spawn(async move {
        let _ = svc.start_consuming().await;
    });

    let svc = Arc::new(svc_execution);
    tokio::spawn(async move {
        let _ = svc.start_consuming().await;
    });

    let svc = Arc::new(svc_broker);
    tokio::spawn(async move {
        let _ = svc.start_consuming().await;
    });

    let svc = Arc::new(svc_portfolio);
    tokio::spawn(async move {
        let _ = svc.start_consuming().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;

    // --- Run backtest ---
    let loader = Arc::new(ScyllaHistoricalLoader::new(
        state.repository.clone(),
        state.producer.clone(),
        fetch_missing,
    ));

    let config = BacktestConfig {
        symbol: payload.symbol.clone(),
        start,
        end,
        initial_capital,
        topic: market_topic,
    };

    let engine = BacktestEngine::new(loader, portfolio_manager.clone(), fills, config);

    match engine.run().await {
        Ok(report) => {
            let json = serde_json::to_value(&report).unwrap();
            (StatusCode::OK, Json(json))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Backtest failed: {}", e) })),
        ),
    }
}
