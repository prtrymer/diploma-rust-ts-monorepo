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
    /// Чи веде цей портфель шорти. Звіт реконструює трейд-книгу з філів
    /// (`BacktestReport::from_fills_and_portfolio`) тими самими правилами, що
    /// й портфель, а правила залежать від режиму: у long-only Sell при
    /// нульовій позиції — no-op, у режимі шортів — вхід у шорт. Режим питаємо
    /// в самого портфеля, а не носимо окремим прапорцем поруч: прапорець може
    /// розійтися з тим, як облік справді працює, і звіт почне описувати не той
    /// портфель, який його підписує.
    fn allows_short(&self) -> bool;
}
