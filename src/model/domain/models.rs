use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::features::domain::models::FeatureSet;
use crate::trading::domain::events::SignalDirection;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prediction {
    pub direction: SignalDirection,
    pub confidence: Decimal,
    pub model_name: String,
}

#[async_trait]
pub trait PredictionModel: Send + Sync {
    fn name(&self) -> &str;
    async fn predict(&self, features: &FeatureSet) -> Result<Prediction>;
    async fn learn(&self, _features: &FeatureSet, _realized_return: Decimal) -> Result<()> {
        Ok(())
    }
    async fn save_checkpoint(&self, _path: &str) -> Result<()> {
        Ok(())
    }
    async fn load_checkpoint(&self, _path: &str) -> Result<()> {
        Ok(())
    }
}
