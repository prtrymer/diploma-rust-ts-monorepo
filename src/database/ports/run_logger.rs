//! Порт логування прогонів (M0.4, інваріант 5: Provenance).
//!
//! Кожен прогін бектесту записується з хешем ПОВНОГО конфіга — питання
//! «а з якими параметрами це рахувалось» більше не існує. Лічильник прогонів
//! також живить N trials у deflated Sharpe (M1.3).

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: Uuid,
    pub config_hash: String,
    pub config_json: String,
    pub metrics: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl RunRecord {
    pub fn new(config_hash: String, config_json: String, metrics: serde_json::Value) -> Self {
        Self {
            run_id: Uuid::new_v4(),
            config_hash,
            config_json,
            metrics,
            created_at: Utc::now(),
        }
    }
}

#[async_trait]
pub trait RunLogger: Send + Sync {
    async fn log_run(&self, record: &RunRecord) -> Result<()>;
    /// Кількість залогованих прогонів — N trials для deflated Sharpe (M1.3).
    async fn count_runs(&self) -> Result<usize>;
    /// Кількість прогонів З УНІКАЛЬНИМ конфігом (чесніший N trials).
    async fn count_distinct_configs(&self) -> Result<usize>;
}
