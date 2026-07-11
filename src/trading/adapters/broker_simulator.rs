use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

use crate::trading::domain::costs::CostModel;
use crate::trading::domain::events::*;
use crate::trading::ports::BrokerSimulatorPort;

/// Симулятор виконання. Уся вартість угоди (комісія + спред + impact)
/// обчислюється CostModel (M0.1) і записується у `fill.commission` — далі
/// portfolio/report бачать тільки net-of-cost грошові потоки (інваріант 2).
///
/// Детермінізм (інваріант 4): час філа — час ордера (ринковий, не wall-clock),
/// id філа виводиться з id ордера, а не з ГВЧ.
pub struct SimpleBrokerSimulator {
    pub slippage_pct: Decimal,
    pub cost_model: Arc<dyn CostModel>,
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

        let mut fill = FillEvent {
            id: Uuid::new_v5(&Uuid::NAMESPACE_OID, order.id.as_bytes()),
            order_id: order.id,
            timestamp: order.timestamp,
            symbol: order.symbol.clone(),
            side: order.side,
            quantity: order.quantity,
            fill_price,
            commission: Decimal::ZERO,
            slippage: slippage_amount,
        };

        let ctx = order.market_context.clone().unwrap_or_default();
        fill.commission = self.cost_model.cost(&fill, &ctx);
        Ok(fill)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::run_config::CostConfig;
    use crate::trading::domain::costs::{cost_model_from_config, MarketContext, ZeroCost};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn order(qty: Decimal, price: Decimal) -> OrderEvent {
        OrderEvent {
            id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            timestamp: Utc.with_ymd_and_hms(2024, 1, 2, 15, 30, 0).unwrap(),
            symbol: "TEST".into(),
            side: OrderSide::Buy,
            quantity: qty,
            order_type: OrderType::Market,
            limit_price: Some(price),
            stop_price: None,
            market_context: Some(MarketContext {
                avg_volume: Some(dec!(1000000)),
                volatility: Some(dec!(0.02)),
                spread_pct: None,
            }),
        }
    }

    #[tokio::test]
    async fn fill_carries_cost_from_model() {
        let broker = SimpleBrokerSimulator {
            slippage_pct: Decimal::ZERO,
            cost_model: cost_model_from_config(&CostConfig::default()),
        };
        let fill = broker.execute_order(&order(dec!(100), dec!(50))).await.unwrap();
        assert!(fill.commission > Decimal::ZERO, "витрати завжди увімкнені");
    }

    #[tokio::test]
    async fn fill_is_deterministic_for_same_order() {
        let broker = SimpleBrokerSimulator {
            slippage_pct: dec!(0.0002),
            cost_model: Arc::new(ZeroCost),
        };
        let o = order(dec!(10), dec!(100));
        let f1 = broker.execute_order(&o).await.unwrap();
        let f2 = broker.execute_order(&o).await.unwrap();
        assert_eq!(serde_json::to_string(&f1).unwrap(), serde_json::to_string(&f2).unwrap());
        assert_eq!(f1.timestamp, o.timestamp, "час філа — ринковий час ордера");
    }
}
