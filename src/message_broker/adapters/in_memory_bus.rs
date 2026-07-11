//! In-memory event bus (M0.5): адаптер `MessageProducerPort`, що доставляє
//! повідомлення зареєстрованим `MessageHandler`-ам синхронно, у порядку
//! публікації.
//!
//! Це ДОКАЗ головної переваги гексагона: live-пайплайн
//! (StrategyHandler → ExecutionKafkaHandler → BrokerKafkaHandler →
//! FillCollector) працює без жодної зміни domain-логіки і на Kafka, і на
//! цьому адаптері — property-тест детермінізму порівнює його з прямим
//! бектест-циклом побайтово.

use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::MessageProducerPort;
use crate::message_broker::ports::{Message, MessageHandler};

#[derive(Default)]
pub struct InMemoryBus {
    handlers: RwLock<Vec<Arc<dyn MessageHandler>>>,
    offset: AtomicI64,
}

impl InMemoryBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_handler(&self, handler: Arc<dyn MessageHandler>) {
        self.handlers.write().await.push(handler);
    }

    async fn dispatch(&self, message: Message) -> Result<()> {
        // Клон списку, щоб не тримати лок під час обробки (хендлер може
        // публікувати наступну стадію через цей же bus).
        let handlers: Vec<Arc<dyn MessageHandler>> =
            self.handlers.read().await.iter().cloned().collect();
        for handler in handlers {
            if handler.topics().contains(&message.topic) {
                handler.handle(&message).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl MessageProducerPort for InMemoryBus {
    async fn send_message(&self, message: StreamMessage) -> Result<()> {
        let msg = Message {
            topic: message.topic,
            partition: 0,
            offset: self.offset.fetch_add(1, Ordering::SeqCst),
            payload: message.value,
            key: message.key,
        };
        self.dispatch(msg).await
    }
}
