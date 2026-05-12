use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use uuid::Uuid;

use crate::trading::domain::events::*;
use crate::trading::ports::ExecutionHandlerPort;
use crate::trading::ports::PortfolioPort;

pub struct SimpleExecutionHandler {
    pub default_quantity: Decimal,
    pub max_position_pct: Decimal,
    pub min_trade_quantity: Decimal,
    pub stop_loss_pct: Decimal,
    pub take_profit_pct: Decimal,
    pub reserve_cash_pct: Decimal,
    pub portfolio: Option<Arc<dyn PortfolioPort>>,
}

#[async_trait]
impl ExecutionHandlerPort for SimpleExecutionHandler {
    async fn on_signal(&self, signal: &SignalEvent) -> Result<Option<OrderEvent>> {
        // Extract last price from signal metadata for limit_price.
        let limit_price = signal
            .metadata
            .as_ref()
            .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
            .and_then(|v| v["last_price"].as_str().map(String::from))
            .and_then(|s| s.parse::<Decimal>().ok());

        let Some(price) = limit_price else {
            return Ok(None);
        };
        if price <= Decimal::ZERO {
            return Ok(None);
        }

        let mk_order = |side: OrderSide, quantity: Decimal| -> OrderEvent {
            OrderEvent {
                id: Uuid::new_v4(),
                signal_id: signal.id,
                timestamp: Utc::now(),
                symbol: signal.symbol.clone(),
                side,
                quantity,
                order_type: OrderType::Market,
                limit_price,
                stop_price: None,
            }
        };

        if let Some(portfolio_port) = &self.portfolio {
            let portfolio = portfolio_port.get_portfolio().await?;
            let current_pos = portfolio.positions.get(&signal.symbol).cloned();
            let current_qty = current_pos
                .as_ref()
                .map(|p| p.quantity.max(Decimal::ZERO))
                .unwrap_or(Decimal::ZERO);

            if current_qty > Decimal::ZERO {
                if let Some(position) = current_pos.as_ref() {
                    let stop_price = position.avg_entry_price * (Decimal::ONE - self.stop_loss_pct);
                    if price <= stop_price {
                        return Ok(Some(mk_order(OrderSide::Sell, current_qty)));
                    }
                    let take_profit_price =
                        position.avg_entry_price * (Decimal::ONE + self.take_profit_pct);
                    if price >= take_profit_price {
                        return Ok(Some(mk_order(OrderSide::Sell, current_qty)));
                    }
                }
            }

            let quantity = match signal.direction {
                SignalDirection::Long => {
                    let total_value = portfolio.get_total_value();
                    let max_notional = (total_value * self.max_position_pct).max(Decimal::ZERO);
                    let current_notional = current_qty * price;
                    let strength = signal.strength.clamp(dec!(0.10), Decimal::ONE);
                    let target_notional = max_notional * strength;
                    let additional_cap = (target_notional - current_notional).max(Decimal::ZERO);

                    let cash_reserve = (total_value * self.reserve_cash_pct).max(Decimal::ZERO);
                    let available_cash = (portfolio.cash - cash_reserve).max(Decimal::ZERO);
                    let cash_cap = available_cash / price;
                    let position_cap = additional_cap / price;

                    cash_cap.min(position_cap)
                }
                SignalDirection::Short => current_qty,
                SignalDirection::Exit => current_qty,
            };

            if quantity < self.min_trade_quantity {
                return Ok(None);
            }

            let side = match signal.direction {
                SignalDirection::Long => OrderSide::Buy,
                SignalDirection::Short | SignalDirection::Exit => OrderSide::Sell,
            };
            return Ok(Some(mk_order(side, quantity)));
        }

        let side = match signal.direction {
            SignalDirection::Long => OrderSide::Buy,
            SignalDirection::Short => OrderSide::Sell,
            SignalDirection::Exit => return Ok(None),
        };
        Ok(Some(mk_order(side, self.default_quantity.max(dec!(1)))))
    }
}
