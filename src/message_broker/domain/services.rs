use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::message_broker::ports::{Consumer, MessageHandler};

pub struct MessageBrokerService {
    consumer: Arc<dyn Consumer>,
    handlers: HashMap<String, Arc<dyn MessageHandler>>,
}

impl MessageBrokerService {
    pub fn new(consumer: Arc<dyn Consumer>) -> Self {
        Self {
            consumer,
            handlers: HashMap::new(),
        }
    }

    pub fn register_handler(&mut self, handler: Arc<dyn MessageHandler>) {
        for topic in handler.topics() {
            self.handlers.insert(topic, handler.clone());
        }
    }

    pub async fn start_consuming(&self) -> Result<()> {
        tracing::info!(topics = ?self.handlers.keys().collect::<Vec<_>>(), "starting to consume messages");
        loop {
            if let Some(message) = self.consumer.consume().await? {
                if let Some(handler) = self.handlers.get(&message.topic) {
                    if let Err(e) = handler.handle(&message).await {
                        tracing::error!(topic = %message.topic, error = ?e, "message handler failed");
                    }
                } else {
                    tracing::warn!(topic = %message.topic, "no handler registered for topic");
                }
            }
        }
    }
}
