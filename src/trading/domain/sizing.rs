//! Єдиний domain-компонент position sizing (наскрізна задача роадмапу).
//!
//! Лікує розбіжність параметрів між розділами (65% vs 80%): УСІ рішення про
//! розмір позиції проходять через `PositionSizer`, який конструюється тільки
//! з `SizingConfig` (одне джерело правди, M0.4).

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::shared::run_config::SizingConfig;

/// Запит на розрахунок розміру: скільки купити на сигнал заданої сили.
#[derive(Debug, Clone)]
pub struct SizingRequest {
    /// Поточна ціна інструмента.
    pub price: Decimal,
    /// Повна вартість портфеля (кеш + позиції).
    pub total_value: Decimal,
    /// Доступний кеш.
    pub cash: Decimal,
    /// Поточна кількість у позиції (штук, ≥ 0).
    pub current_qty: Decimal,
    /// Сила сигналу [0..1].
    pub strength: Decimal,
}

#[derive(Debug, Clone)]
pub struct PositionSizer {
    config: SizingConfig,
}

impl PositionSizer {
    pub fn from_config(config: SizingConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &SizingConfig {
        &self.config
    }

    /// Кількість до купівлі (0 — якщо угода менша за мінімальну).
    ///
    /// target_notional = total_value × max_position_pct × strength (clamped),
    /// обмежений доступним кешем мінус резерв. Kelly-кап (якщо увімкнений)
    /// додатково обмежує частку капіталу.
    pub fn buy_quantity(&self, req: &SizingRequest) -> Decimal {
        if req.price <= Decimal::ZERO || req.total_value <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let mut max_fraction = self.config.max_position_pct;
        if self.config.kelly_fraction_cap > Decimal::ZERO {
            max_fraction = max_fraction.min(self.config.kelly_fraction_cap);
        }

        let max_notional = (req.total_value * max_fraction).max(Decimal::ZERO);
        let current_notional = req.current_qty.max(Decimal::ZERO) * req.price;
        let strength = req.strength.clamp(dec!(0.10), Decimal::ONE);
        let target_notional = max_notional * strength;
        let additional_cap = (target_notional - current_notional).max(Decimal::ZERO);

        let cash_reserve = (req.total_value * self.config.reserve_cash_pct).max(Decimal::ZERO);
        let available_cash = (req.cash - cash_reserve).max(Decimal::ZERO);

        let cash_cap = available_cash / req.price;
        let position_cap = additional_cap / req.price;
        let qty = cash_cap.min(position_cap);

        if qty < self.config.min_trade_quantity {
            Decimal::ZERO
        } else {
            qty
        }
    }

    /// Ціна stop-loss відносно середньої ціни входу.
    pub fn stop_loss_price(&self, avg_entry: Decimal) -> Decimal {
        avg_entry * (Decimal::ONE - self.config.stop_loss_pct)
    }

    /// Ціна take-profit відносно середньої ціни входу.
    pub fn take_profit_price(&self, avg_entry: Decimal) -> Decimal {
        avg_entry * (Decimal::ONE + self.config.take_profit_pct)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> SizingRequest {
        SizingRequest {
            price: dec!(100),
            total_value: dec!(100000),
            cash: dec!(100000),
            current_qty: Decimal::ZERO,
            strength: Decimal::ONE,
        }
    }

    #[test]
    fn respects_max_position_pct_from_config() {
        let sizer = PositionSizer::from_config(SizingConfig {
            max_position_pct: dec!(0.50),
            reserve_cash_pct: Decimal::ZERO,
            ..Default::default()
        });
        // 50% від 100k при ціні 100 → 500 штук.
        assert_eq!(sizer.buy_quantity(&req()), dec!(500));
    }

    #[test]
    fn respects_cash_reserve() {
        let sizer = PositionSizer::from_config(SizingConfig {
            max_position_pct: Decimal::ONE,
            reserve_cash_pct: dec!(0.10),
            ..Default::default()
        });
        // Доступно 90k → 900 штук, хоча позиційний кап дозволяє 1000.
        assert_eq!(sizer.buy_quantity(&req()), dec!(900));
    }

    #[test]
    fn kelly_cap_tightens_position() {
        let sizer = PositionSizer::from_config(SizingConfig {
            max_position_pct: dec!(0.80),
            kelly_fraction_cap: dec!(0.25),
            reserve_cash_pct: Decimal::ZERO,
            ..Default::default()
        });
        assert_eq!(sizer.buy_quantity(&req()), dec!(250));
    }

    #[test]
    fn below_min_trade_quantity_returns_zero() {
        let sizer = PositionSizer::from_config(SizingConfig {
            max_position_pct: dec!(0.80),
            min_trade_quantity: dec!(1000),
            ..Default::default()
        });
        assert_eq!(sizer.buy_quantity(&req()), Decimal::ZERO);
    }

    #[test]
    fn stop_and_take_profit_derive_from_single_config() {
        let cfg = SizingConfig {
            stop_loss_pct: dec!(0.03),
            take_profit_pct: dec!(0.02),
            ..Default::default()
        };
        let sizer = PositionSizer::from_config(cfg);
        assert_eq!(sizer.stop_loss_price(dec!(100)), dec!(97.00));
        assert_eq!(sizer.take_profit_price(dec!(100)), dec!(102.00));
    }
}
