use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

use crate::trading::domain::events::*;
use crate::trading::domain::sizing::{PositionSizer, SizingRequest};
use crate::trading::ports::ExecutionHandlerPort;
use crate::trading::ports::PortfolioPort;

/// Перетворює сигнали на ордери. Розмір позиції та SL/TP делеговані єдиному
/// domain-компоненту `PositionSizer` (одна конфігурація — одна правда).
///
/// Детермінізм: час ордера = час сигналу (ринковий), id виводиться з id
/// сигналу+сторони, а не з ГВЧ.
pub struct SimpleExecutionHandler {
    pub sizer: PositionSizer,
    pub portfolio: Option<Arc<dyn PortfolioPort>>,
}

fn deterministic_order_id(signal_id: Uuid, side: OrderSide) -> Uuid {
    let mut seed = signal_id.as_bytes().to_vec();
    seed.push(match side {
        OrderSide::Buy => 0x01,
        OrderSide::Sell => 0x02,
    });
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &seed)
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
                id: deterministic_order_id(signal.id, side),
                signal_id: signal.id,
                timestamp: signal.timestamp,
                symbol: signal.symbol.clone(),
                side,
                quantity,
                order_type: OrderType::Market,
                limit_price,
                stop_price: None,
                market_context: signal.market_context.clone(),
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
                    if price <= self.sizer.stop_loss_price(position.avg_entry_price) {
                        return Ok(Some(mk_order(OrderSide::Sell, current_qty)));
                    }
                    if price >= self.sizer.take_profit_price(position.avg_entry_price) {
                        return Ok(Some(mk_order(OrderSide::Sell, current_qty)));
                    }
                }
            }

            let quantity = match signal.direction {
                SignalDirection::Long => self.sizer.buy_quantity(&SizingRequest {
                    price,
                    total_value: portfolio.get_total_value(),
                    cash: portfolio.cash,
                    current_qty,
                    strength: signal.strength,
                }),
                SignalDirection::Short => current_qty,
                SignalDirection::Exit => current_qty,
            };

            if quantity < self.sizer.config().min_trade_quantity || quantity <= Decimal::ZERO {
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
        Ok(Some(mk_order(
            side,
            self.sizer.config().min_trade_quantity.max(Decimal::ONE),
        )))
    }
}
