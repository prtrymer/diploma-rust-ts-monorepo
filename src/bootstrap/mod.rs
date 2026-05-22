pub mod database;
pub mod features;
pub mod models;
pub mod trading;
pub mod ingestion;
pub mod message_broker;
pub mod http;

use std::sync::Arc;
use anyhow::Result;
use crate::data_ingestion::domain::models::StreamMessage;
use crate::database::domain::models::StockTick;
use rust_decimal::Decimal;
use chrono::Utc;

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

    // Startup Warmup: Fetch all portfolio symbols from PostgreSQL (including any added after boot),
    // train models on historical data, then send one "now" tick to anchor signal timestamps.
    let producer_warmup = producer.clone();
    let data_source_warmup = data_source.clone();
    let user_repo_warmup = user_repo.clone();
    tokio::spawn(async move {
        println!("⏳ Waiting 3 seconds for Kafka consumers to settle before starting startup warmup...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Re-fetch fresh from postgres to catch ALL portfolio symbols (not just the boot snapshot)
        let all_symbols = match user_repo_warmup.get_active_symbols().await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("❌ Failed to load portfolio symbols for warmup: {}", e);
                return;
            }
        };
        println!("🚀 Starting startup warmup for all portfolio symbols: {:?}", all_symbols);

        for symbol in all_symbols {
            let symbol_clone = symbol.clone();
            let producer_inner = producer_warmup.clone();
            let data_source_inner = data_source_warmup.clone();

            tokio::spawn(async move {
                let end = Utc::now();
                // 120 hours = 5 days to ensure enough trading minutes across weekends
                let start = end - chrono::Duration::hours(120);

                match data_source_inner.fetch_historical_quotes(&symbol_clone, start, end, "1m").await {
                    Ok(quotes) => {
                        let recent: Vec<_> = quotes.into_iter().rev().take(1000).collect::<Vec<_>>().into_iter().rev().collect();
                        println!("Fetched {} historical 1m candles for {} startup warmup", recent.len(), symbol_clone);

                        let mut last_price: Option<Decimal> = None;

                        // Send historical ticks for ML model training (with real historical timestamps)
                        for quote in recent {
                            if let Some(price) = Decimal::from_f64_retain(quote.close) {
                                last_price = Some(price);
                                let tick = StockTick {
                                    symbol: quote.symbol.clone(),
                                    timestamp: quote.timestamp,
                                    price,
                                    volume: quote.volume as i64,
                                    bid: None,
                                    ask: None,
                                    source: "warmup".to_string(),
                                };
                                if let Ok(json) = serde_json::to_string(&tick) {
                                    let _ = producer_inner.send_message(StreamMessage {
                                        topic: "market-data-raw".to_string(),
                                        key: Some(quote.symbol),
                                        value: json,
                                    }).await;
                                }
                            }
                        }

                        // Send one final "now" tick to anchor the generated signal to the current time.
                        // Without this, the signal timestamp would be the last historical candle's time (days ago).
                        if let Some(price) = last_price {
                            let now_tick = StockTick {
                                symbol: symbol_clone.clone(),
                                timestamp: Utc::now(),
                                price,
                                volume: 0,
                                bid: None,
                                ask: None,
                                source: "warmup_anchor".to_string(),
                            };
                            if let Ok(json) = serde_json::to_string(&now_tick) {
                                let _ = producer_inner.send_message(StreamMessage {
                                    topic: "market-data-raw".to_string(),
                                    key: Some(symbol_clone.clone()),
                                    value: json,
                                }).await;
                            }
                        }

                        println!("✅ Warmup complete for {} — signal timestamps anchored to now", symbol_clone);
                    }
                    Err(e) => eprintln!("❌ Failed to fetch startup warmup data for {}: {}", symbol_clone, e),
                }
            });
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
