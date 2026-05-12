pub mod handler;

use anyhow::Result;
use async_trait::async_trait;

pub use handler::MessageHandler;

#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub payload: String,
    pub key: Option<String>,
}

#[async_trait]
pub trait Consumer: Send + Sync {
    async fn consume(&self) -> Result<Option<Message>>;
    async fn commit(&self) -> Result<()>;
}
