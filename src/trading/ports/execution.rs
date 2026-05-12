use anyhow::Result;
use async_trait::async_trait;

use crate::trading::domain::events::{OrderEvent, SignalEvent};

#[async_trait]
pub trait ExecutionHandlerPort: Send + Sync {
    async fn on_signal(&self, signal: &SignalEvent) -> Result<Option<OrderEvent>>;
}
