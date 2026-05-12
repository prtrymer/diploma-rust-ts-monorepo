use anyhow::Result;
use db_con::data_ingestion;
use db_con::database;
use db_con::features;
use db_con::http;
use db_con::message_broker;
use db_con::model;
use db_con::trading;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

use data_ingestion::adapters::{
    kafka_producer::KafkaProducerAdapter, yahoo_finance::YahooFinanceAdapter,
};
use data_ingestion::domain::services::DataIngestionService;
use data_ingestion::ports::{DataSourcePort, MessageProducerPort};
use database::adapters::postgres::PostgresRepository;
use database::adapters::scylladb::ScyllaRepository;
use database::ports::repository::{Repository, UserRepository};
use features::domain::indicators::{
    BidAskSpreadProxyFeature, EmaFeature, LiquidityImbalanceProxyFeature, MacdFeature,
    MeanReversionFeature, MomentumFeature, OrderFlowProxyFeature, RsiFeature,
    VolatilityClusteringFeature, VolatilityFeature,
};
use features::domain::registry::FeatureRegistry;
use message_broker::adapters::kafka_consumer::KafkaConsumerAdapter;
use message_broker::adapters::market_data_handler::MarketDataHandler;
use message_broker::domain::services::MessageBrokerService;
use message_broker::ports::MessageHandler;
use model::domain::ensemble::WeightedEnsembleModel;
use model::domain::models::PredictionModel;
use model::domain::random_forest_like::RandomForestLikeModel;
use trading::adapters::broker_handler::BrokerKafkaHandler;
use trading::adapters::broker_simulator::SimpleBrokerSimulator;
use trading::adapters::execution_handler::SimpleExecutionHandler;
use trading::adapters::execution_handler_kafka::ExecutionKafkaHandler;
use trading::adapters::fill_handler::FillKafkaHandler;
use trading::adapters::momentum_strategy::MomentumStrategy;
use trading::adapters::portfolio_manager::PortfolioManager;
use trading::adapters::strategy_handler::StrategyHandler;
use trading::ports::{BrokerSimulatorPort, ExecutionHandlerPort, PortfolioPort, StrategyPort};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("Starting Market Data & Trading Engine");

    // --- PostgreSQL (users / auth) ---
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://trading:trading@localhost:5432/trading".to_string());
    println!("Connecting to PostgreSQL...");
    let user_repo = Arc::new(
        PostgresRepository::new(&database_url).await?
    ) as Arc<dyn UserRepository>;

    // --- ScyllaDB (market / candle / tick / ML data) ---
    println!("Connecting to ScyllaDB...");
    let repository =
        Arc::new(ScyllaRepository::new(vec!["127.0.0.1:9042".to_string()], "market_data").await?)
            as Arc<dyn Repository>;

    println!("Waiting for Kafka...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    let producer =
        Arc::new(KafkaProducerAdapter::new("localhost:9092")?) as Arc<dyn MessageProducerPort>;

    let is_simulated = std::env::var("USE_SIMULATION").unwrap_or_else(|_| "false".to_string()) == "true";
    let data_source = if is_simulated {
        println!("Using Simulated Data Source (Random Walk)...");
        Arc::new(data_ingestion::adapters::simulated::SimulatedDataSource::new(0.005)) as Arc<dyn DataSourcePort>
    } else {
        Arc::new(YahooFinanceAdapter::new()) as Arc<dyn DataSourcePort>
    };

    // --- Shared symbols ---
    let mut db_symbols = user_repo.get_active_symbols().await?;
    if db_symbols.is_empty() {
        println!("No active symbols found in database, inserting defaults...");
        let defaults = vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()];
        for sym in &defaults {
            user_repo.add_active_symbol(sym).await?;
        }
        db_symbols = defaults;
    }
    let symbols_vec = db_symbols;
    let symbols = Arc::new(RwLock::new(symbols_vec.clone()));

    // Optional startup historical sync for backtest/RL dataset
    let auto_sync = std::env::var("HIST_SYNC_ON_START")
        .unwrap_or_else(|_| "false".to_string())
        .to_lowercase();
    if auto_sync == "true" || auto_sync == "1" || auto_sync == "yes" {
        let days: i64 = std::env::var("HIST_SYNC_DAYS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(30);
        let interval = std::env::var("HIST_SYNC_INTERVAL").unwrap_or_else(|_| "1m".to_string());
        println!(
            "Startup historical sync enabled (days={}, interval={})",
            days, interval
        );
        println!("Note: backtest now auto-syncs missing historical ranges on demand.");
    }

    // --- Feature Registry ---
    let mut feature_registry = FeatureRegistry::new();
    feature_registry.register("rsi_14".into(), Arc::new(RsiFeature::new(14)));
    feature_registry.register("ema_12".into(), Arc::new(EmaFeature::new(12)));
    feature_registry.register("ema_26".into(), Arc::new(EmaFeature::new(26)));
    feature_registry.register("macd".into(), Arc::new(MacdFeature::new()));
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
        100,
        if is_simulated { 5 } else { 20 },
        128,
        8,
        4,
        0.005,
    )) as Arc<dyn PredictionModel>;
    let medium_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys.clone(),
        500,
        if is_simulated { 10 } else { 100 },
        256,
        12,
        6,
        0.006,
    )) as Arc<dyn PredictionModel>;
    let slow_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys,
        2600,
        if is_simulated { 20 } else { 520 },
        1024,
        24,
        8,
        0.009,
    )) as Arc<dyn PredictionModel>;
    
    let heuristic_model = Arc::new(model::domain::ensemble::EnsembleFeatureModel) as Arc<dyn PredictionModel>;

    let mut model_weights = vec![
        (fast_rf, dec!(0.30)),
        (medium_rf, dec!(0.25)),
        (slow_rf, dec!(0.20)),
    ];
    
    if is_simulated {
        model_weights.push((heuristic_model, dec!(0.25)));
    } else {
        model_weights.push((heuristic_model, dec!(0.05)));
    }

    let model = Arc::new(WeightedEnsembleModel::new(
        model_weights,
        dec!(0.01),
    )) as Arc<dyn PredictionModel>;

    // --- Strategy ---
    let lookback_size = if is_simulated { 10 } else { 30 };
    let strategy = Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry.clone(),
        model.clone(),
        lookback_size,
        dec!(0.001),
        if is_simulated { 2 } else { 10 },
    ))) as Arc<RwLock<dyn StrategyPort>>;

    // --- Strategy Warmup ---
    println!("Warming up strategy with historical data (lookback_size={})...", lookback_size);
    for symbol in &symbols_vec {
        match repository.get_latest_candles(symbol, "1min", lookback_size).await {
            Ok(candles) => {
                if !candles.is_empty() {
                    println!("  Warming up {} with {} candles", symbol, candles.len());
                    let mut strat = strategy.write().await;
                    if let Err(e) = strat.warmup(candles).await {
                        eprintln!("  Failed to warm up {}: {}", symbol, e);
                    }
                } else {
                    println!("  No historical candles found for {} in ScyllaDB", symbol);
                }
            }
            Err(e) => eprintln!("  Error fetching candles for {}: {}", symbol, e),
        }
    }

    // --- Broker Simulator ---
    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.001),
        commission: dec!(1.00),
    }) as Arc<dyn BrokerSimulatorPort>;

    // --- Portfolio ---
    let portfolio_manager = Arc::new(PortfolioManager::new(dec!(100000))) as Arc<dyn PortfolioPort>;

    // --- Execution Handler ---
    let execution_handler = Arc::new(SimpleExecutionHandler {
        default_quantity: dec!(0),
        max_position_pct: dec!(0.65),
        min_trade_quantity: dec!(1),
        stop_loss_pct: dec!(0.03),
        take_profit_pct: dec!(0.02),
        reserve_cash_pct: dec!(0.02),
        portfolio: Some(portfolio_manager.clone()),
    }) as Arc<dyn ExecutionHandlerPort>;

    // --- Kafka Consumers (each with unique consumer group) ---
    let consumer_market = Arc::new(KafkaConsumerAdapter::new(
        "localhost:9092",
        "market-data-persist-group",
        &["market-data-raw".to_string()],
    )?);

    let consumer_strategy = Arc::new(KafkaConsumerAdapter::new(
        "localhost:9092",
        "strategy-group",
        &["market-data-raw".to_string()],
    )?);

    let consumer_execution = Arc::new(KafkaConsumerAdapter::new(
        "localhost:9092",
        "execution-group",
        &["trading-signals".to_string()],
    )?);

    let consumer_broker = Arc::new(KafkaConsumerAdapter::new(
        "localhost:9092",
        "broker-group",
        &["trading-orders".to_string()],
    )?);

    let consumer_portfolio = Arc::new(KafkaConsumerAdapter::new(
        "localhost:9092",
        "portfolio-group",
        &["trading-fills".to_string()],
    )?);

    let consumer_http_signals = Arc::new(KafkaConsumerAdapter::new(
        "localhost:9092",
        "http-signals-group",
        &["trading-signals".to_string()],
    )?);

    // --- Message Handlers ---
    let market_data_handler =
        Arc::new(MarketDataHandler::new(repository.clone())) as Arc<dyn MessageHandler>;

    let strategy_handler = Arc::new(StrategyHandler::new(
        strategy.clone(),
        producer.clone(),
        "market-data-raw".to_string(),
        "trading-signals".to_string(),
    )) as Arc<dyn MessageHandler>;

    let execution_kafka_handler = Arc::new(ExecutionKafkaHandler::new(
        execution_handler,
        producer.clone(),
        "trading-signals".to_string(),
        "trading-orders".to_string(),
    )) as Arc<dyn MessageHandler>;

    let broker_kafka_handler = Arc::new(BrokerKafkaHandler::new(
        broker,
        producer.clone(),
        "trading-orders".to_string(),
        "trading-fills".to_string(),
    )) as Arc<dyn MessageHandler>;

    let fill_kafka_handler = Arc::new(FillKafkaHandler::new(
        portfolio_manager.clone(),
        "trading-fills".to_string(),
    )) as Arc<dyn MessageHandler>;

    let (signals_tx, _) = broadcast::channel(100);
    let recent_signals = Arc::new(RwLock::new(std::collections::VecDeque::new()));

    let http_signal_handler = Arc::new(http::handlers::signals::HttpSignalHandler {
        tx: signals_tx.clone(),
        recent_signals: recent_signals.clone(),
        topic: "trading-signals".to_string(),
    }) as Arc<dyn MessageHandler>;

    // --- Wire Broker Services ---
    let mut svc_market = MessageBrokerService::new(consumer_market);
    svc_market.register_handler(market_data_handler);

    let mut svc_strategy = MessageBrokerService::new(consumer_strategy);
    svc_strategy.register_handler(strategy_handler);

    let mut svc_execution = MessageBrokerService::new(consumer_execution);
    svc_execution.register_handler(execution_kafka_handler);

    let mut svc_broker = MessageBrokerService::new(consumer_broker);
    svc_broker.register_handler(broker_kafka_handler);

    let mut svc_portfolio = MessageBrokerService::new(consumer_portfolio);
    svc_portfolio.register_handler(fill_kafka_handler);

    let mut svc_http_signals = MessageBrokerService::new(consumer_http_signals);
    svc_http_signals.register_handler(http_signal_handler);

    // --- Ingestion Service ---
    let ingestion_service = Arc::new(DataIngestionService::new(
        data_source,
        producer.clone(),
        symbols.clone(),
        "market-data-raw".to_string(),
    ));

    // --- Spawn all background tasks ---
    let ingestion_clone = ingestion_service.clone();
    let interval_secs = if is_simulated { 10 } else { 60 };
    tokio::spawn(async move {
        println!("Starting data ingestion service (interval: {}s)...", interval_secs);
        if let Err(e) = ingestion_clone.start_streaming(interval_secs).await {
            eprintln!("Data ingestion error: {}", e);
        }
    });

    let svc = Arc::new(svc_market);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Market data handler error: {}", e);
        }
    });

    let svc = Arc::new(svc_strategy);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Strategy handler error: {}", e);
        }
    });

    let svc = Arc::new(svc_execution);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Execution handler error: {}", e);
        }
    });

    let svc = Arc::new(svc_broker);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Broker handler error: {}", e);
        }
    });

    let svc = Arc::new(svc_portfolio);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Portfolio handler error: {}", e);
        }
    });

    let svc = Arc::new(svc_http_signals);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("HTTP signals handler error: {}", e);
        }
    });

    // --- HTTP API server ---
    println!("Starting HTTP API server on http://localhost:3000");
    println!("Swagger UI: http://localhost:3000/swagger-ui/");
    let app_state = http::handlers::AppState {
        symbols,
        repository: repository.clone(),
        user_repo,
        producer: producer.clone(),
        signals_tx,
        recent_signals,
    };
    let app = http::routes::create_router(app_state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    println!("Application is running. Press Ctrl+C to stop.");
    println!("API Endpoints:");
    println!("   GET    http://localhost:3000/http/symbols");
    println!("   POST   http://localhost:3000/http/symbols");
    println!("   DELETE http://localhost:3000/http/symbols");
    println!();
    println!("Trading Pipeline:");
    println!("   market-data-raw -> Strategy -> trading-signals -> Execution -> trading-orders -> Broker -> trading-fills -> Portfolio");

    axum::serve(listener, app).await?;

    Ok(())
}
