use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::trading::domain::events::FillEvent;
use crate::trading::domain::models::{Portfolio, Position};

#[async_trait]
pub trait PortfolioPort: Send + Sync {
    async fn update_on_fill(&self, fill: &FillEvent) -> Result<()>;
    async fn get_portfolio(&self) -> Result<Portfolio>;
    async fn get_position(&self, symbol: &str) -> Result<Option<Position>>;
    async fn get_total_value(&self) -> Result<Decimal>;
    async fn reset(&self, _initial_capital: Decimal) -> Result<()> {
        Ok(())
    }
}
