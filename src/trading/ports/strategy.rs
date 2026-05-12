use anyhow::Result;
use async_trait::async_trait;

use crate::database::domain::models::{StockTick, Candle};
use crate::trading::domain::events::SignalEvent;

#[async_trait]
pub trait StrategyPort: Send + Sync {
    async fn on_market_event(&mut self, tick: &StockTick) -> Result<Option<SignalEvent>>;
    async fn warmup(&mut self, candles: Vec<Candle>) -> Result<()>;
    async fn is_warmed_up(&self, symbol: &str) -> bool;
    fn name(&self) -> &str;
}
