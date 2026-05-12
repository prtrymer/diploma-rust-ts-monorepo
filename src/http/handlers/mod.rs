use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

use crate::database::ports::repository::{Repository, UserRepository};
use crate::data_ingestion::ports::MessageProducerPort;

pub mod auth;
pub mod backtest;
pub mod signals;
pub mod symbols;

#[derive(Clone)]
pub struct AppState {
    pub symbols: Arc<RwLock<Vec<String>>>,
    /// ScyllaDB — market/candle/tick data (ML training)
    pub repository: Arc<dyn Repository>,
    /// PostgreSQL — user auth data
    pub user_repo: Arc<dyn UserRepository>,
    pub producer: Arc<dyn MessageProducerPort>,
    pub signals_tx: broadcast::Sender<String>,
    pub recent_signals: Arc<RwLock<std::collections::VecDeque<String>>>,
}
