use crate::data_ingestion::domain::models::{HistoricalQuote, StockQuote};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Port for fetching data from external sources
#[async_trait]
pub trait DataSourcePort: Send + Sync {
    async fn fetch_quote(&self, symbol: &str) -> Result<StockQuote>;

    async fn fetch_historical_quotes(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<HistoricalQuote>>;
}
