use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;

use crate::aggregation::domain::services::CandleAggregator;
use crate::database::domain::models::StockTick;
use crate::database::ports::repository::Repository;
use crate::message_broker::ports::{Message, MessageHandler};

pub struct MarketDataHandler {
    repository: Arc<dyn Repository>,
    aggregator: Arc<CandleAggregator>,
}

impl MarketDataHandler {
    pub fn new(repository: Arc<dyn Repository>) -> Self {
        let aggregator = Arc::new(CandleAggregator::new(repository.clone()));
        Self {
            repository,
            aggregator,
        }
    }
}

#[async_trait]
impl MessageHandler for MarketDataHandler {
    fn topics(&self) -> Vec<String> {
        vec!["market-data-raw".to_string()]
    }

    async fn handle(&self, message: &Message) -> Result<()> {
        let tick: StockTick = serde_json::from_str(&message.payload)
            .context("Failed to parse StockTick from message payload")?;

        self.repository
            .insert_stock_tick(&tick)
            .await
            .context("Failed to insert stock tick")?;

        if let Err(e) = self.aggregator.process_tick(&tick).await {
            tracing::warn!(symbol = %tick.symbol, error = %e, "candle aggregation failed");
        }

        tracing::debug!(symbol = %tick.symbol, price = %tick.price, volume = tick.volume, "saved tick");
        Ok(())
    }
}
