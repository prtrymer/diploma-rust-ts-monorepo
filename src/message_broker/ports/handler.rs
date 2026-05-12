use anyhow::Result;
use async_trait::async_trait;

use super::Message;

#[async_trait]
pub trait MessageHandler: Send + Sync {
    fn topics(&self) -> Vec<String>;
    async fn handle(&self, message: &Message) -> Result<()>;
}
