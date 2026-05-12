use anyhow::Result;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::MessageProducerPort;

pub struct KafkaProducerAdapter {
    producer: FutureProducer,
}

impl KafkaProducerAdapter {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer })
    }
}

#[async_trait]
impl MessageProducerPort for KafkaProducerAdapter {
    async fn send_message(&self, message: StreamMessage) -> Result<()> {
        let mut record = FutureRecord::to(&message.topic).payload(&message.value);

        if let Some(key) = &message.key {
            record = record.key(key);
        }

        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to send message: {}", e))?;

        Ok(())
    }
}
