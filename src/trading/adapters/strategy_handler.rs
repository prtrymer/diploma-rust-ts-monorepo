use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::MessageProducerPort;
use crate::database::domain::models::StockTick;
use crate::message_broker::ports::{Message, MessageHandler};
use crate::trading::ports::StrategyPort;

pub struct StrategyHandler {
    strategy: Arc<RwLock<dyn StrategyPort>>,
    producer: Arc<dyn MessageProducerPort>,
    input_topic: String,
    output_topic: String,
}

impl StrategyHandler {
    pub fn new(
        strategy: Arc<RwLock<dyn StrategyPort>>,
        producer: Arc<dyn MessageProducerPort>,
        input_topic: String,
        output_topic: String,
    ) -> Self {
        Self {
            strategy,
            producer,
            input_topic,
            output_topic,
        }
    }
}

#[async_trait]
impl MessageHandler for StrategyHandler {
    fn topics(&self) -> Vec<String> {
        vec![self.input_topic.clone()]
    }

    async fn handle(&self, message: &Message) -> Result<()> {
        let tick: StockTick =
            serde_json::from_str(&message.payload).context("Failed to parse StockTick")?;

        let signal = {
            let mut strategy = self.strategy.write().await;
            strategy.on_market_event(&tick).await?
        };

        if let Some(signal) = signal {
            let json = serde_json::to_string(&signal)?;
            self.producer
                .send_message(StreamMessage {
                    topic: self.output_topic.clone(),
                    key: Some(signal.symbol.clone()),
                    value: json,
                })
                .await?;
            println!(
                "🚀 Published signal for {}: {:?} (strength={:.4})",
                signal.symbol, signal.direction, signal.strength
            );
        } else {
            let strategy = self.strategy.read().await;
            if strategy.is_warmed_up(&tick.symbol).await {
                println!(
                    "🔍 {} — analyzing tick (no signal generated)",
                    tick.symbol
                );
            } else {
                println!(
                    "📊 {} — buffering tick (waiting for lookback to fill)",
                    tick.symbol
                );
            }
        }

        Ok(())
    }
}
