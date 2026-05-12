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
        println!("Starting to consume messages...");
        loop {
            if let Some(message) = self.consumer.consume().await? {
                if let Some(handler) = self.handlers.get(&message.topic) {
                    if let Err(e) = handler.handle(&message).await {
                        eprintln!(
                            "Error processing message on topic '{}': {:?}",
                            message.topic, e
                        );
                    }
                } else {
                    eprintln!("No handler registered for topic: {}", message.topic);
                }
            }
        }
    }
}
