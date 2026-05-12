use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;

use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::MessageProducerPort;
use crate::message_broker::ports::{Message, MessageHandler};
use crate::trading::domain::events::SignalEvent;
use crate::trading::ports::ExecutionHandlerPort;

pub struct ExecutionKafkaHandler {
    execution_handler: Arc<dyn ExecutionHandlerPort>,
    producer: Arc<dyn MessageProducerPort>,
    input_topic: String,
    output_topic: String,
}

impl ExecutionKafkaHandler {
    pub fn new(
        execution_handler: Arc<dyn ExecutionHandlerPort>,
        producer: Arc<dyn MessageProducerPort>,
        input_topic: String,
        output_topic: String,
    ) -> Self {
        Self {
            execution_handler,
            producer,
            input_topic,
            output_topic,
        }
    }
}

#[async_trait]
impl MessageHandler for ExecutionKafkaHandler {
    fn topics(&self) -> Vec<String> {
        vec![self.input_topic.clone()]
    }

    async fn handle(&self, message: &Message) -> Result<()> {
        let signal: SignalEvent =
            serde_json::from_str(&message.payload).context("Failed to parse SignalEvent")?;

        if let Some(order) = self.execution_handler.on_signal(&signal).await? {
            let json = serde_json::to_string(&order)?;
            self.producer
                .send_message(StreamMessage {
                    topic: self.output_topic.clone(),
                    key: Some(order.symbol.clone()),
                    value: json,
                })
                .await?;
            println!(
                "Published order for {}: {:?} qty={}",
                order.symbol, order.side, order.quantity
            );
        }

        Ok(())
    }
}
