pub mod database;
pub mod features;
pub mod models;
pub mod trading;
pub mod ingestion;
pub mod message_broker;
pub mod http;

use std::sync::Arc;
use anyhow::Result;

pub async fn run() -> Result<()> {
    dotenv::dotenv().ok();
    println!("Starting Market Data & Trading Engine");

    // 1. Initialize databases
    let (user_repo, repository, symbols, symbols_vec) = database::init_database().await?;

    let is_simulated = std::env::var("USE_SIMULATION").unwrap_or_else(|_| "false".to_string()) == "true";

    // 2. Initialize Ingestion engine (Kafka Producer / Data Source / Ingestion Service)
    let (producer, data_source, ingestion_service) = ingestion::init_ingestion(is_simulated, symbols.clone()).await?;

    // 3. Initialize feature registry
    let feature_registry = features::init_features();

    // 4. Initialize predictive ML models
    let model = models::init_models(is_simulated);

    // 5. Initialize Strategy and warm it up
    let (strategy, broker, portfolio_manager, execution_handler, _lookback_size) = trading::init_trading_engine(
        feature_registry,
        model,
        &repository,
        &symbols_vec,
        is_simulated,
        data_source.clone(),
    ).await;

    // 6. Wire Message Broker & Message Handlers
    let wiring = message_broker::wire_message_broker(
        repository.clone(),
        producer.clone(),
        strategy,
        execution_handler,
        portfolio_manager,
        broker,
    )?;

    // 7. Spawn Background Tasks
    let ingestion_clone = ingestion_service.clone();
    let interval_secs = if is_simulated { 10 } else { 60 };
    tokio::spawn(async move {
        println!("Starting data ingestion service (interval: {}s)...", interval_secs);
        if let Err(e) = ingestion_clone.start_streaming(interval_secs).await {
            eprintln!("Data ingestion error: {}", e);
        }
    });

    let svc = Arc::new(wiring.svc_market);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Market data handler error: {}", e);
        }
    });

    let svc = Arc::new(wiring.svc_strategy);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Strategy handler error: {}", e);
        }
    });

    let svc = Arc::new(wiring.svc_execution);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Execution handler error: {}", e);
        }
    });

    let svc = Arc::new(wiring.svc_broker);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Broker handler error: {}", e);
        }
    });

    let svc = Arc::new(wiring.svc_portfolio);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("Portfolio handler error: {}", e);
        }
    });

    let svc = Arc::new(wiring.svc_http_signals);
    tokio::spawn(async move {
        if let Err(e) = svc.start_consuming().await {
            eprintln!("HTTP signals handler error: {}", e);
        }
    });

    // 8. Start HTTP API Server
    http::start_http_server(
        symbols,
        repository,
        user_repo,
        producer,
        wiring.signals_tx,
        wiring.recent_signals,
    ).await?;

    Ok(())
}
