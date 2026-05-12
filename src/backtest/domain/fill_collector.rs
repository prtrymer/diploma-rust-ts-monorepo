use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::message_broker::ports::{Message, MessageHandler};
use crate::trading::domain::events::FillEvent;
use crate::trading::ports::PortfolioPort;

pub struct BacktestFillCollector {
    portfolio: Arc<dyn PortfolioPort>,
    fills: Arc<RwLock<Vec<FillEvent>>>,
    input_topic: String,
}

impl BacktestFillCollector {
    pub fn new(
        portfolio: Arc<dyn PortfolioPort>,
        fills: Arc<RwLock<Vec<FillEvent>>>,
        input_topic: String,
    ) -> Self {
        Self {
            portfolio,
            fills,
            input_topic,
        }
    }
}

#[async_trait]
impl MessageHandler for BacktestFillCollector {
    fn topics(&self) -> Vec<String> {
        vec![self.input_topic.clone()]
    }

    async fn handle(&self, message: &Message) -> Result<()> {
        let fill: FillEvent =
            serde_json::from_str(&message.payload).context("Failed to parse FillEvent")?;

        self.portfolio.update_on_fill(&fill).await?;
        self.fills.write().await.push(fill);

        Ok(())
    }
}
