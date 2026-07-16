use std::sync::Arc;
use std::collections::VecDeque;
use tokio::sync::{RwLock, broadcast};
use anyhow::Result;
use crate::database::ports::repository::UserRepository;
use crate::database::ports::repository::Repository;
use crate::data_ingestion::ports::MessageProducerPort;
use crate::http::handlers::AppState;
use crate::http::routes::create_router;

pub async fn start_http_server(
    symbols: Arc<RwLock<Vec<String>>>,
    repository: Arc<dyn Repository>,
    user_repo: Arc<dyn UserRepository>,
    producer: Arc<dyn MessageProducerPort>,
    signals_tx: broadcast::Sender<String>,
    recent_signals: Arc<RwLock<VecDeque<String>>>,
) -> Result<()> {
    tracing::info!("starting HTTP API server on http://localhost:3000 (Swagger UI: /swagger-ui/)");
    let app_state = AppState {
        symbols,
        repository,
        user_repo,
        producer,
        signals_tx,
        recent_signals,
    };
    let app = create_router(app_state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    tracing::info!("application is running (Ctrl+C to stop)");
    tracing::info!("pipeline: market-data-raw -> Strategy -> trading-signals -> Execution -> trading-orders -> Broker -> trading-fills -> Portfolio");

    axum::serve(listener, app).await?;
    Ok(())
}
