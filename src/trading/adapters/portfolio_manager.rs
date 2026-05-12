use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::trading::domain::events::{FillEvent, OrderSide};
use crate::trading::domain::models::{Portfolio, Position};
use crate::trading::ports::PortfolioPort;

pub struct PortfolioManager {
    portfolio: Arc<RwLock<Portfolio>>,
}

impl PortfolioManager {
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            portfolio: Arc::new(RwLock::new(Portfolio::new(initial_capital))),
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
        let cost = fill.fill_price * fill.quantity + fill.commission;

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
                let position = portfolio.positions.get(&fill.symbol).unwrap();
                let total_cost = position.avg_entry_price * position.quantity + cost;
                let new_qty = position.quantity + fill.quantity;
                let new_avg = if new_qty > Decimal::ZERO {
                    total_cost / new_qty
                } else {
                    Decimal::ZERO
                };
                let position = portfolio.positions.get_mut(&fill.symbol).unwrap();
                position.quantity = new_qty;
                position.avg_entry_price = new_avg;
                portfolio.cash -= cost;
            }
            OrderSide::Sell => {
                let position = portfolio.positions.get_mut(&fill.symbol).unwrap();
                let sell_qty = fill.quantity.min(position.quantity.max(Decimal::ZERO));
                if sell_qty <= Decimal::ZERO {
                    return Ok(());
                }
                let pnl = (fill.fill_price - position.avg_entry_price) * sell_qty - fill.commission;
                position.realized_pnl += pnl;
                position.quantity -= sell_qty;
                portfolio.cash += fill.fill_price * sell_qty - fill.commission;
            }
        }

        let position = portfolio.positions.get_mut(&fill.symbol).unwrap();
        position.current_price = fill.fill_price;
        position.unrealized_pnl =
            (position.current_price - position.avg_entry_price) * position.quantity;
        position.last_updated = Utc::now();
        portfolio.last_updated = Utc::now();

        if portfolio.positions.get(&fill.symbol).unwrap().quantity == Decimal::ZERO {
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
