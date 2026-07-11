//! Модель транзакційних витрат — first-class domain concept (M0.1).
//!
//! Інваріант 2: жоден прогін не рахує PnL без витрат. Уся вартість виконання
//! (комісія + спред + market impact) обчислюється тут і записується у
//! `FillEvent.commission`, тому net-of-cost PnL виходить автоматично в усіх
//! споживачах філів — і в бектесті, і в live (той самий broker-адаптер).

use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::shared::run_config::CostConfig;
use crate::trading::domain::events::FillEvent;

/// Ринковий контекст на момент виконання. Постачається стратегією/адаптером
/// даних разом з ордером; відсутні поля моделі заповнюють дефолтами з конфіга.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MarketContext {
    /// Середній обсяг за бар (штук) — знаменник participation rate.
    pub avg_volume: Option<Decimal>,
    /// Волатильність бару (частка ціни).
    pub volatility: Option<Decimal>,
    /// Спостережений відносний спред (повний).
    pub spread_pct: Option<Decimal>,
}

/// Порт domain-логіки витрат. Повертає ГРОШОВУ вартість виконання філа
/// (Decimal — інваріант 6), яку виконання записує в `fill.commission`.
pub trait CostModel: Send + Sync {
    fn name(&self) -> &str;
    fn cost(&self, fill: &FillEvent, ctx: &MarketContext) -> Decimal;
}

/// Нульові витрати. Використовується ТІЛЬКИ для діагностичного порівняння
/// gross==net; бойові конфігурації завжди мають ненульову модель.
pub struct ZeroCost;

impl CostModel for ZeroCost {
    fn name(&self) -> &str {
        "zero"
    }
    fn cost(&self, _fill: &FillEvent, _ctx: &MarketContext) -> Decimal {
        Decimal::ZERO
    }
}

/// Комісія (фіксована + відсоткова) + половина спреду за сторону.
pub struct SimpleCommissionSpread {
    pub commission_fixed: Decimal,
    pub commission_pct: Decimal,
    /// Повний відносний спред; taker платить половину за кожну сторону.
    pub spread_pct: Decimal,
}

impl CostModel for SimpleCommissionSpread {
    fn name(&self) -> &str {
        "commission_spread"
    }

    fn cost(&self, fill: &FillEvent, ctx: &MarketContext) -> Decimal {
        let notional = (fill.fill_price * fill.quantity).abs();
        let spread = ctx.spread_pct.unwrap_or(self.spread_pct);
        self.commission_fixed + notional * self.commission_pct + notional * spread / Decimal::TWO
    }
}

/// Market impact за Almgren–Chriss (розділ 3 диплома, застосований реально).
///
/// Тимчасовий impact: `η · σ · sqrt(q / V)` (square-root law, β = 1/2),
/// постійний: `γ · (q / V)`. Обидва — частки ціни; грошова вартість =
/// notional × (temporary + permanent / 2): половина постійного зсуву
/// в середньому реалізується проти виконуваного ордера.
pub struct AlmgrenChrissImpact {
    pub eta: Decimal,
    pub gamma: Decimal,
    pub default_volatility: Decimal,
    pub default_avg_volume: Decimal,
}

impl CostModel for AlmgrenChrissImpact {
    fn name(&self) -> &str {
        "almgren_chriss"
    }

    fn cost(&self, fill: &FillEvent, ctx: &MarketContext) -> Decimal {
        let qty = fill.quantity.abs();
        if qty <= Decimal::ZERO || fill.fill_price <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let volume = ctx
            .avg_volume
            .filter(|v| *v > Decimal::ZERO)
            .unwrap_or(self.default_avg_volume)
            .max(Decimal::ONE);
        let sigma = ctx
            .volatility
            .filter(|v| *v > Decimal::ZERO)
            .unwrap_or(self.default_volatility);

        let participation = qty / volume;
        let sqrt_participation = participation.sqrt().unwrap_or(Decimal::ZERO);

        let temporary = self.eta * sigma * sqrt_participation;
        let permanent = self.gamma * participation;
        let impact_fraction = temporary + permanent / Decimal::TWO;

        let notional = fill.fill_price * qty;
        notional * impact_fraction
    }
}

/// Сума кількох моделей (комісія+спред плюс impact тощо).
pub struct CompositeCost {
    pub components: Vec<Arc<dyn CostModel>>,
}

impl CostModel for CompositeCost {
    fn name(&self) -> &str {
        "composite"
    }

    fn cost(&self, fill: &FillEvent, ctx: &MarketContext) -> Decimal {
        self.components.iter().map(|c| c.cost(fill, ctx)).sum()
    }
}

/// Єдина фабрика: модель витрат будується ВИКЛЮЧНО з конфіга (інваріант 5 —
/// жодних магічних параметрів у коді).
pub fn cost_model_from_config(cfg: &CostConfig) -> Arc<dyn CostModel> {
    let mut components: Vec<Arc<dyn CostModel>> = vec![Arc::new(SimpleCommissionSpread {
        commission_fixed: cfg.commission_fixed,
        commission_pct: cfg.commission_pct,
        spread_pct: cfg.spread_pct,
    })];
    if let Some(impact) = &cfg.impact {
        components.push(Arc::new(AlmgrenChrissImpact {
            eta: impact.eta,
            gamma: impact.gamma,
            default_volatility: impact.default_volatility,
            default_avg_volume: impact.default_avg_volume,
        }));
    }
    Arc::new(CompositeCost { components })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use crate::trading::domain::events::OrderSide;

    fn fill(qty: Decimal, price: Decimal) -> FillEvent {
        FillEvent {
            id: Uuid::nil(),
            order_id: Uuid::nil(),
            timestamp: Utc::now(),
            symbol: "TEST".into(),
            side: OrderSide::Buy,
            quantity: qty,
            fill_price: price,
            commission: Decimal::ZERO,
            slippage: Decimal::ZERO,
        }
    }

    // M0.1: збільшення розміру ордера монотонно збільшує impact-складову.
    #[test]
    fn impact_monotonically_increases_with_order_size() {
        let model = AlmgrenChrissImpact {
            eta: dec!(0.1),
            gamma: dec!(0.05),
            default_volatility: dec!(0.02),
            default_avg_volume: dec!(1000000),
        };
        let ctx = MarketContext::default();
        let mut prev = Decimal::ZERO;
        for qty in [10u32, 100, 1000, 10000, 100000] {
            let c = model.cost(&fill(Decimal::from(qty), dec!(100)), &ctx);
            assert!(
                c > prev,
                "cost must strictly grow with size: qty={qty}, cost={c}, prev={prev}"
            );
            prev = c;
        }
    }

    // Impact на одиницю нотіоналу теж має рости (суперлінійність у грошах).
    #[test]
    fn impact_per_notional_increases_with_participation() {
        let model = AlmgrenChrissImpact {
            eta: dec!(0.1),
            gamma: dec!(0.05),
            default_volatility: dec!(0.02),
            default_avg_volume: dec!(1000000),
        };
        let ctx = MarketContext::default();
        let small = model.cost(&fill(dec!(1000), dec!(100)), &ctx) / dec!(100000);
        let large = model.cost(&fill(dec!(100000), dec!(100)), &ctx) / dec!(10000000);
        assert!(large > small);
    }

    // M0.1: нульова конфігурація витрат → cost == 0 (net == gross).
    #[test]
    fn zero_cost_config_gives_zero_cost() {
        let model = cost_model_from_config(&CostConfig::zero());
        let ctx = MarketContext {
            avg_volume: Some(dec!(1000000)),
            volatility: Some(dec!(0.02)),
            spread_pct: Some(Decimal::ZERO),
        };
        assert_eq!(model.cost(&fill(dec!(500), dec!(123.45)), &ctx), Decimal::ZERO);
    }

    // M0.1: параметри читаються з конфіга — зміна конфіга змінює вартість.
    #[test]
    fn cost_follows_config_parameters() {
        let base = CostConfig::default();
        let mut doubled = base.clone();
        doubled.commission_pct = base.commission_pct * Decimal::TWO;

        let f = fill(dec!(100), dec!(50));
        let ctx = MarketContext::default();
        let c_base = cost_model_from_config(&base).cost(&f, &ctx);
        let c_doubled = cost_model_from_config(&doubled).cost(&f, &ctx);
        assert!(c_doubled > c_base);

        let expected_delta = dec!(5000) * base.commission_pct;
        assert_eq!(c_doubled - c_base, expected_delta);
    }

    #[test]
    fn composite_is_sum_of_components() {
        let a = Arc::new(SimpleCommissionSpread {
            commission_fixed: dec!(1),
            commission_pct: Decimal::ZERO,
            spread_pct: Decimal::ZERO,
        }) as Arc<dyn CostModel>;
        let b = Arc::new(SimpleCommissionSpread {
            commission_fixed: dec!(2),
            commission_pct: Decimal::ZERO,
            spread_pct: Decimal::ZERO,
        }) as Arc<dyn CostModel>;
        let composite = CompositeCost {
            components: vec![a, b],
        };
        let ctx = MarketContext::default();
        assert_eq!(composite.cost(&fill(dec!(1), dec!(1)), &ctx), dec!(3));
    }

    #[test]
    fn spread_component_charges_half_spread() {
        let model = SimpleCommissionSpread {
            commission_fixed: Decimal::ZERO,
            commission_pct: Decimal::ZERO,
            spread_pct: dec!(0.001),
        };
        // notional = 100 * 10 = 1000; half-spread = 1000 * 0.001 / 2 = 0.5
        let c = model.cost(&fill(dec!(10), dec!(100)), &MarketContext::default());
        assert_eq!(c, dec!(0.5));
    }
}
