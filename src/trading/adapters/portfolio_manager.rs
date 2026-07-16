use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::trading::domain::events::{FillEvent, OrderSide};
use crate::trading::domain::models::{Portfolio, Position};
use crate::trading::ports::PortfolioPort;

/// Обліковець портфеля. За замовчуванням long-only (поведінка live незмінна);
/// `new_allowing_short` вмикає шорти для market-neutral стратегій (M3.2).
///
/// Інваріант руху через нуль: один філ або закриває позицію (до нуля), або
/// відкриває/нарощує — двобічні переходи двигун зобов'язаний різати на два
/// ордери. Це тримає облік середньої ціни входу чесним.
pub struct PortfolioManager {
    portfolio: Arc<RwLock<Portfolio>>,
    allow_short: bool,
}

impl PortfolioManager {
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            portfolio: Arc::new(RwLock::new(Portfolio::new(initial_capital))),
            allow_short: false,
        }
    }

    pub fn new_allowing_short(initial_capital: Decimal) -> Self {
        Self {
            portfolio: Arc::new(RwLock::new(Portfolio::new(initial_capital))),
            allow_short: true,
        }
    }
}

#[async_trait]
impl PortfolioPort for PortfolioManager {
    async fn update_on_fill(&self, fill: &FillEvent) -> Result<()> {
        let mut portfolio = self.portfolio.write().await;
        if fill.quantity <= Decimal::ZERO {
            return Ok(());
        }

        if !portfolio.positions.contains_key(&fill.symbol) {
            portfolio.positions.insert(
                fill.symbol.clone(),
                Position {
                    symbol: fill.symbol.clone(),
                    quantity: Decimal::ZERO,
                    avg_entry_price: Decimal::ZERO,
                    current_price: fill.fill_price,
                    unrealized_pnl: Decimal::ZERO,
                    realized_pnl: Decimal::ZERO,
                    last_updated: Utc::now(),
                },
            );
        }

        match fill.side {
            OrderSide::Buy => {
                let position = portfolio
                    .positions
                    .get(&fill.symbol)
                    .context("position upserted above")?;
                if position.quantity >= Decimal::ZERO {
                    // Відкриття/нарощення лонга: комісія входить у середню ціну.
                    let cost = fill.fill_price * fill.quantity + fill.commission;
                    let total_cost = position.avg_entry_price * position.quantity + cost;
                    let new_qty = position.quantity + fill.quantity;
                    let new_avg = if new_qty > Decimal::ZERO {
                        total_cost / new_qty
                    } else {
                        Decimal::ZERO
                    };
                    let position = portfolio
                        .positions
                        .get_mut(&fill.symbol)
                        .context("position upserted above")?;
                    position.quantity = new_qty;
                    position.avg_entry_price = new_avg;
                    portfolio.cash -= cost;
                } else {
                    // Покриття шорта.
                    anyhow::ensure!(
                        self.allow_short,
                        "short position exists in long-only portfolio: {}",
                        fill.symbol
                    );
                    let short_qty = -position.quantity;
                    anyhow::ensure!(
                        fill.quantity <= short_qty,
                        "fill crosses zero (cover {} > short {}): engine must split orders",
                        fill.quantity,
                        short_qty
                    );
                    let avg_entry = position.avg_entry_price;
                    let pnl =
                        (avg_entry - fill.fill_price) * fill.quantity - fill.commission;
                    let position = portfolio
                        .positions
                        .get_mut(&fill.symbol)
                        .context("position upserted above")?;
                    position.realized_pnl += pnl;
                    position.quantity += fill.quantity;
                    if position.quantity == Decimal::ZERO {
                        position.avg_entry_price = Decimal::ZERO;
                    }
                    portfolio.cash -= fill.fill_price * fill.quantity + fill.commission;
                }
            }
            OrderSide::Sell => {
                let position = portfolio
                    .positions
                    .get(&fill.symbol)
                    .context("position upserted above")?;
                if position.quantity > Decimal::ZERO || !self.allow_short {
                    // Закриття лонга (стара семантика: клемп до наявної кількості).
                    let position = portfolio
                        .positions
                        .get_mut(&fill.symbol)
                        .context("position upserted above")?;
                    let sell_qty = fill.quantity.min(position.quantity.max(Decimal::ZERO));
                    if sell_qty <= Decimal::ZERO {
                        return Ok(());
                    }
                    anyhow::ensure!(
                        !self.allow_short || fill.quantity <= position.quantity,
                        "fill crosses zero (sell {} > long {}): engine must split orders",
                        fill.quantity,
                        position.quantity
                    );
                    let pnl =
                        (fill.fill_price - position.avg_entry_price) * sell_qty - fill.commission;
                    position.realized_pnl += pnl;
                    position.quantity -= sell_qty;
                    portfolio.cash += fill.fill_price * sell_qty - fill.commission;
                } else {
                    // Відкриття/нарощення шорта: комісія зменшує ефективну ціну входу.
                    let short_qty = -position.quantity;
                    let gross_proceeds = fill.fill_price * fill.quantity;
                    let total_entry = position.avg_entry_price * short_qty + gross_proceeds
                        - fill.commission;
                    let new_short_qty = short_qty + fill.quantity;
                    let new_avg = if new_short_qty > Decimal::ZERO {
                        total_entry / new_short_qty
                    } else {
                        Decimal::ZERO
                    };
                    let position = portfolio
                        .positions
                        .get_mut(&fill.symbol)
                        .context("position upserted above")?;
                    position.quantity = -new_short_qty;
                    position.avg_entry_price = new_avg;
                    portfolio.cash += gross_proceeds - fill.commission;
                }
            }
        }

        let position = portfolio
            .positions
            .get_mut(&fill.symbol)
            .context("position upserted above")?;
        position.current_price = fill.fill_price;
        position.unrealized_pnl =
            (position.current_price - position.avg_entry_price) * position.quantity;
        position.last_updated = Utc::now();
        let flat = position.quantity == Decimal::ZERO;
        portfolio.last_updated = Utc::now();

        if flat {
            portfolio.positions.remove(&fill.symbol);
        }

        Ok(())
    }

    async fn get_portfolio(&self) -> Result<Portfolio> {
        Ok(self.portfolio.read().await.clone())
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<Position>> {
        Ok(self.portfolio.read().await.positions.get(symbol).cloned())
    }

    async fn get_total_value(&self) -> Result<Decimal> {
        Ok(self.portfolio.read().await.get_total_value())
    }

    async fn reset(&self, initial_capital: Decimal) -> Result<()> {
        let mut p = self.portfolio.write().await;
        p.positions.clear();
        p.cash = initial_capital;
        p.initial_capital = initial_capital;
        p.last_updated = Utc::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn fill(side: OrderSide, qty: Decimal, price: Decimal, commission: Decimal) -> FillEvent {
        FillEvent {
            id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            symbol: "T".into(),
            side,
            quantity: qty,
            fill_price: price,
            commission,
            slippage: Decimal::ZERO,
        }
    }

    #[tokio::test]
    async fn long_roundtrip_realizes_pnl() {
        let pm = PortfolioManager::new(dec!(10000));
        pm.update_on_fill(&fill(OrderSide::Buy, dec!(10), dec!(100), dec!(1)))
            .await
            .unwrap();
        pm.update_on_fill(&fill(OrderSide::Sell, dec!(10), dec!(110), dec!(1)))
            .await
            .unwrap();
        let p = pm.get_portfolio().await.unwrap();
        // 10000 − 1001 + 1099 = 10098
        assert_eq!(p.cash, dec!(10098));
        assert!(p.positions.is_empty());
    }

    #[tokio::test]
    async fn short_roundtrip_realizes_pnl() {
        let pm = PortfolioManager::new_allowing_short(dec!(10000));
        // Шорт 10 по 100 (комісія 1): кеш +999, позиція −10.
        pm.update_on_fill(&fill(OrderSide::Sell, dec!(10), dec!(100), dec!(1)))
            .await
            .unwrap();
        let p = pm.get_portfolio().await.unwrap();
        assert_eq!(p.positions.get("T").unwrap().quantity, dec!(-10));
        assert_eq!(p.cash, dec!(10999));

        // Покриття по 90 (комісія 1): прибуток ≈ (99.9 − 90)·10 − 1 = 98.
        pm.update_on_fill(&fill(OrderSide::Buy, dec!(10), dec!(90), dec!(1)))
            .await
            .unwrap();
        let p = pm.get_portfolio().await.unwrap();
        assert!(p.positions.is_empty());
        assert_eq!(p.cash, dec!(10098));
    }

    #[tokio::test]
    async fn long_only_portfolio_clamps_oversell() {
        let pm = PortfolioManager::new(dec!(10000));
        pm.update_on_fill(&fill(OrderSide::Buy, dec!(5), dec!(100), Decimal::ZERO))
            .await
            .unwrap();
        // Продаж 10 при позиції 5 — клемп до 5, шорт не відкривається.
        pm.update_on_fill(&fill(OrderSide::Sell, dec!(10), dec!(100), Decimal::ZERO))
            .await
            .unwrap();
        let p = pm.get_portfolio().await.unwrap();
        assert!(p.positions.is_empty());
        assert_eq!(p.cash, dec!(10000));
    }

    #[tokio::test]
    async fn cross_zero_fill_rejected_in_short_mode() {
        let pm = PortfolioManager::new_allowing_short(dec!(10000));
        pm.update_on_fill(&fill(OrderSide::Buy, dec!(5), dec!(100), Decimal::ZERO))
            .await
            .unwrap();
        let res = pm
            .update_on_fill(&fill(OrderSide::Sell, dec!(10), dec!(100), Decimal::ZERO))
            .await;
        assert!(res.is_err(), "рух через нуль одним філом заборонений");
    }
}
