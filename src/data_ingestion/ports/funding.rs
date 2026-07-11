//! Порт funding-rate даних (M3.1) — нові порти, та сама гексагональна
//! архітектура: стратегія не знає, звідки ставки (Binance REST чи CSV).

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::data_ingestion::domain::funding::FundingRatePoint;

#[async_trait]
pub trait FundingDataPort: Send + Sync {
    /// Історія funding-ставок символу за період, відсортована за часом.
    async fn funding_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<FundingRatePoint>>;
}
