use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::trading::domain::events::*;
use crate::trading::ports::BrokerSimulatorPort;

pub struct SimpleBrokerSimulator {
    pub slippage_pct: Decimal,
    pub commission: Decimal,
}

#[async_trait]
impl BrokerSimulatorPort for SimpleBrokerSimulator {
    async fn execute_order(&self, order: &OrderEvent) -> Result<FillEvent> {
        let base_price = order.limit_price.unwrap_or(Decimal::ZERO);
        let slippage_amount = base_price * self.slippage_pct;

        let fill_price = match order.side {
            OrderSide::Buy => base_price + slippage_amount,
            OrderSide::Sell => base_price - slippage_amount,
        };

        Ok(FillEvent {
            id: Uuid::new_v4(),
            order_id: order.id,
            timestamp: Utc::now(),
            symbol: order.symbol.clone(),
            side: order.side,
            quantity: order.quantity,
            fill_price,
            commission: self.commission,
            slippage: slippage_amount,
        })
    }
}
