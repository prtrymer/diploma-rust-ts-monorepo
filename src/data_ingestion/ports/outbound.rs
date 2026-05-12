use crate::data_ingestion::domain::models::StreamMessage;
use anyhow::Result;
use async_trait::async_trait;

/// Port for producing messages to stream (Kafka)
#[async_trait]
pub trait MessageProducerPort: Send + Sync {
    async fn send_message(&self, message: StreamMessage) -> Result<()>;
}
