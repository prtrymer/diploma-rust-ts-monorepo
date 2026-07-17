//! Порти бектест-модуля.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Завантаження історії і її replay у пайплайн (Kafka-топік).
#[async_trait]
pub trait HistoricalDataLoader: Send + Sync {
    async fn load_and_replay(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        topic: &str,
    ) -> Result<usize>;
}
