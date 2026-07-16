use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use crate::database::adapters::postgres::PostgresRepository;
use crate::database::adapters::scylladb::ScyllaRepository;
use crate::database::ports::repository::{Repository, UserRepository};
use crate::shared::config::{database_url, scylla_nodes};

pub async fn init_database() -> Result<(
    Arc<dyn UserRepository>,
    Arc<dyn Repository>,
    Arc<RwLock<Vec<String>>>,
    Vec<String>,
)> {
    tracing::info!("connecting to PostgreSQL");
    let user_repo = Arc::new(PostgresRepository::new(&database_url()).await?) as Arc<dyn UserRepository>;

    tracing::info!("connecting to ScyllaDB");
    let repository = Arc::new(ScyllaRepository::new(scylla_nodes(), "market_data").await?)
        as Arc<dyn Repository>;

    let mut db_symbols = user_repo.get_active_symbols().await?;
    if db_symbols.is_empty() {
        tracing::info!("no active symbols in database, inserting defaults");
        let defaults = vec!["AAPL".to_string(), "GOOGL".to_string(), "MSFT".to_string()];
        for sym in &defaults {
            user_repo.add_active_symbol(sym).await?;
        }
        db_symbols = defaults;
    }
    let symbols_vec = db_symbols;
    let symbols = Arc::new(RwLock::new(symbols_vec.clone()));

    Ok((user_repo, repository, symbols, symbols_vec))
}
