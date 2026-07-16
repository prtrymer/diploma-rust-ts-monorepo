use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;

use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::MessageProducerPort;
use crate::message_broker::ports::{Message, MessageHandler};
use crate::trading::domain::events::OrderEvent;
use crate::trading::ports::BrokerSimulatorPort;

pub struct BrokerKafkaHandler {
    broker: Arc<dyn BrokerSimulatorPort>,
    producer: Arc<dyn MessageProducerPort>,
    input_topic: String,
    output_topic: String,
}

impl BrokerKafkaHandler {
    pub fn new(
        broker: Arc<dyn BrokerSimulatorPort>,
        producer: Arc<dyn MessageProducerPort>,
        input_topic: String,
        output_topic: String,
    ) -> Self {
        Self {
            broker,
            producer,
            input_topic,
            output_topic,
        }
    }
}

#[async_trait]
impl MessageHandler for BrokerKafkaHandler {
    fn topics(&self) -> Vec<String> {
        vec![self.input_topic.clone()]
    }

    async fn handle(&self, message: &Message) -> Result<()> {
        let order: OrderEvent =
            serde_json::from_str(&message.payload).context("Failed to parse OrderEvent")?;

        let fill = self.broker.execute_order(&order).await?;
        let json = serde_json::to_string(&fill)?;
        self.producer
            .send_message(StreamMessage {
                topic: self.output_topic.clone(),
                key: Some(fill.symbol.clone()),
                value: json,
            })
            .await?;
        tracing::info!(
            symbol = %fill.symbol,
            quantity = %fill.quantity,
            price = %fill.fill_price,
            "published fill"
        );

        Ok(())
    }
}
