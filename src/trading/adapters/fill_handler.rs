use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;

use crate::message_broker::ports::{Message, MessageHandler};
use crate::trading::domain::events::FillEvent;
use crate::trading::ports::PortfolioPort;

pub struct FillKafkaHandler {
    portfolio: Arc<dyn PortfolioPort>,
    input_topic: String,
}

impl FillKafkaHandler {
    pub fn new(portfolio: Arc<dyn PortfolioPort>, input_topic: String) -> Self {
        Self {
            portfolio,
            input_topic,
        }
    }
}

#[async_trait]
impl MessageHandler for FillKafkaHandler {
    fn topics(&self) -> Vec<String> {
        vec![self.input_topic.clone()]
    }

    async fn handle(&self, message: &Message) -> Result<()> {
        let fill: FillEvent =
            serde_json::from_str(&message.payload).context("Failed to parse FillEvent")?;

        self.portfolio.update_on_fill(&fill).await?;
        println!(
            "Portfolio updated after fill: {} {} @ {}",
            fill.symbol, fill.quantity, fill.fill_price
        );

        Ok(())
    }
}
