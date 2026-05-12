use anyhow::Result;
use async_trait::async_trait;

use crate::trading::domain::events::{FillEvent, OrderEvent};

#[async_trait]
pub trait BrokerSimulatorPort: Send + Sync {
    async fn execute_order(&self, order: &OrderEvent) -> Result<FillEvent>;
}
