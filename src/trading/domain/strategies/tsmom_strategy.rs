//! TSMOM — time-series momentum (M2.1, Moskowitz–Ooi–Pedersen 2012).
//!
//! Мета — ВАЛІДАЦІЯ ПЛАТФОРМИ, не прибуток: TSMOM — академічно
//! задокументований стійкий ефект. Якщо движок його не відтворює на
//! диверсифікованому кошику (OOS Sharpe ~0.4–0.8 після витрат) — баг у
//! движку, і Фаза 3 не починається (kill-критерій GATE).
//!
//! Сигнал: знак дохідності за lookback-вікном. Розмір: vol-таргетинг
//! 40%/σ_annual на інструмент (як у MOP), сумарний leverage обмежений 1.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

use crate::backtest::domain::metrics::TRADING_DAYS_PER_YEAR;
use crate::trading::domain::allocation::{AllocationStrategy, UniverseView};

pub struct TsmomStrategy {
    /// Вікно momentum-сигналу (класика MOP — 12 місяців ≈ 252 бари).
    lookback: usize,
    /// Вікно оцінки волатильності для сайзингу.
    vol_window: usize,
    /// Цільова річна волатильність на інструмент (частка, напр. 0.40 у MOP;
    /// для акційного кошика розумніше 0.10–0.15).
    vol_target_annual: Decimal,
    /// Ребаланс, у барах (місячний ≈ 21).
    rebalance_every: usize,
    /// Дозволені шорти (потрібен портфель із new_allowing_short).
    allow_short: bool,
}

impl TsmomStrategy {
    pub fn new(
        lookback: usize,
        vol_window: usize,
        vol_target_annual: Decimal,
        rebalance_every: usize,
        allow_short: bool,
    ) -> Self {
        Self {
            lookback: lookback.max(2),
            vol_window: vol_window.max(2),
            vol_target_annual,
            rebalance_every: rebalance_every.max(1),
            allow_short,
        }
    }
}

impl AllocationStrategy for TsmomStrategy {
    fn name(&self) -> &str {
        "tsmom"
    }

    fn warmup_bars(&self) -> usize {
        self.lookback.max(self.vol_window) + 1
    }

    fn rebalance_every(&self) -> usize {
        self.rebalance_every
    }

    fn target_weights(&mut self, view: &UniverseView) -> Option<BTreeMap<String, Decimal>> {
        let symbols: Vec<String> = view.symbols().cloned().collect();
        if symbols.is_empty() {
            return None;
        }
        let n = Decimal::from(symbols.len() as u64);

        let mut weights = BTreeMap::new();
        for sym in symbols {
            let Some(p_now) = view.price(&sym) else {
                continue;
            };
            let Some(p_then) = view.price_n_bars_ago(&sym, self.lookback) else {
                continue;
            };
            if p_then <= Decimal::ZERO || p_now <= Decimal::ZERO {
                continue;
            }

            // Сигнал: знак минулої дохідності (інваріант 3: тільки дані ≤ t —
            // гарантується типом UniverseView).
            let past_return = (p_now - p_then) / p_then;
            let direction = if past_return > Decimal::ZERO {
                Decimal::ONE
            } else if self.allow_short {
                Decimal::NEGATIVE_ONE
            } else {
                Decimal::ZERO
            };
            if direction == Decimal::ZERO {
                continue;
            }

            // Vol-таргетинг: w = (σ_target / σ_realized_annual) / N, кап 1/N.
            let vol_scalar = realized_annual_vol(view, &sym, self.vol_window)
                .filter(|v| *v > Decimal::ZERO)
                .map(|vol| (self.vol_target_annual / vol).min(Decimal::ONE))
                .unwrap_or(Decimal::ZERO);
            if vol_scalar <= Decimal::ZERO {
                continue;
            }

            weights.insert(sym, direction * vol_scalar / n);
        }

        Some(weights)
    }
}

/// Річна реалізована волатильність за останні `window` барів.
fn realized_annual_vol(view: &UniverseView, symbol: &str, window: usize) -> Option<Decimal> {
    let rets = view.returns_window(symbol, window);
    if rets.len() < 2 {
        return None;
    }
    let vals: Vec<f64> = rets.iter().filter_map(|r| r.to_f64()).collect();
    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
    let var = vals.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / vals.len() as f64;
    Decimal::from_f64(var.sqrt() * TRADING_DAYS_PER_YEAR.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::domain::allocation::AlignedMarketData;
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    /// Тренд угору для UP, униз для DOWN, з невеликим шумом у ціні.
    fn trending_data(n: usize) -> Arc<AlignedMarketData> {
        let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let timestamps: Vec<_> = (0..n).map(|i| t0 + Duration::days(i as i64)).collect();
        let mut closes = BTreeMap::new();
        let up: Vec<Decimal> = (0..n)
            .map(|i| dec!(100) + Decimal::from(i as u64) + if i % 2 == 0 { dec!(0.3) } else { dec!(-0.3) })
            .collect();
        let down: Vec<Decimal> = (0..n)
            .map(|i| dec!(500) - Decimal::from(i as u64) + if i % 3 == 0 { dec!(0.4) } else { dec!(-0.2) })
            .collect();
        closes.insert("UP".to_string(), up);
        closes.insert("DOWN".to_string(), down);
        Arc::new(AlignedMarketData::from_closes(timestamps, closes).unwrap())
    }

    #[test]
    fn goes_long_uptrend_and_short_downtrend() {
        let data = trending_data(80);
        let mut s = TsmomStrategy::new(30, 20, dec!(0.40), 21, true);
        let view = UniverseView::new(data, 79);
        let w = s.target_weights(&view).unwrap();
        assert!(w["UP"] > Decimal::ZERO, "аптренд → лонг: {:?}", w);
        assert!(w["DOWN"] < Decimal::ZERO, "даунтренд → шорт: {:?}", w);
    }

    #[test]
    fn long_only_mode_skips_downtrends() {
        let data = trending_data(80);
        let mut s = TsmomStrategy::new(30, 20, dec!(0.40), 21, false);
        let view = UniverseView::new(data, 79);
        let w = s.target_weights(&view).unwrap();
        assert!(w.contains_key("UP"));
        assert!(!w.contains_key("DOWN"));
    }

    #[test]
    fn total_leverage_bounded_by_one() {
        let data = trending_data(80);
        let mut s = TsmomStrategy::new(30, 20, dec!(10.0), 21, true); // абсурдний таргет
        let view = UniverseView::new(data, 79);
        let w = s.target_weights(&view).unwrap();
        let gross: Decimal = w.values().map(|v| v.abs()).sum();
        assert!(gross <= Decimal::ONE + dec!(0.0001), "gross leverage {gross} > 1");
    }
}
