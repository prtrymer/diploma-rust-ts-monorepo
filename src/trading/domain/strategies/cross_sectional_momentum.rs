//! Cross-sectional momentum (M3.2): лонг топ-квантиль / шорт нижній.
//!
//! Відмінність від absolute momentum: рейтинг по ЮНІВЕРСУ, а не поріг на
//! одному інструменті. Market-neutral за конструкцією: сума ваг ≈ 0.
//! Головний ризик — turnover з'їдає edge; тому ребаланс-банда: вага
//! змінюється тільки якщо |Δw| > min_weight_change (kill-критерій дивиться
//! на PnL після turnover-costs).

use rust_decimal::Decimal;
use std::collections::BTreeMap;

use crate::trading::domain::allocation::{AllocationStrategy, UniverseView};

pub struct CrossSectionalMomentum {
    /// Вікно momentum-скору в барах (класика 12-1: скіп останнього місяця).
    lookback: usize,
    /// Скільки останніх барів пропустити (reversal-ефект), напр. 21.
    skip_recent: usize,
    /// Частка юніверсу в кожному кошику (0..0.5], напр. 0.2 = децильні краї
    /// на малих юніверсах.
    quantile: Decimal,
    /// Ребаланс, у барах.
    rebalance_every: usize,
    /// Банда: ваги не змінюються, поки |Δw| ≤ поріг (контроль turnover).
    min_weight_change: Decimal,
    /// Валова експозиція (1.0 = 50% лонг + 50% шорт).
    gross_exposure: Decimal,
    last_weights: BTreeMap<String, Decimal>,
}

impl CrossSectionalMomentum {
    pub fn new(
        lookback: usize,
        skip_recent: usize,
        quantile: Decimal,
        rebalance_every: usize,
        min_weight_change: Decimal,
        gross_exposure: Decimal,
    ) -> Self {
        Self {
            lookback: lookback.max(2),
            skip_recent,
            quantile: quantile.clamp(Decimal::new(5, 2), Decimal::new(5, 1)),
            rebalance_every: rebalance_every.max(1),
            min_weight_change,
            gross_exposure,
            last_weights: BTreeMap::new(),
        }
    }
}

impl AllocationStrategy for CrossSectionalMomentum {
    fn name(&self) -> &str {
        "xs_momentum"
    }

    fn warmup_bars(&self) -> usize {
        self.lookback + self.skip_recent + 1
    }

    fn rebalance_every(&self) -> usize {
        self.rebalance_every
    }

    fn target_weights(&mut self, view: &UniverseView) -> Option<BTreeMap<String, Decimal>> {
        // Momentum-скор: дохідність за [t - skip - lookback, t - skip].
        let mut scores: Vec<(String, Decimal)> = Vec::new();
        for sym in view.symbols() {
            let p_end = view.price_n_bars_ago(sym, self.skip_recent);
            let p_start = view.price_n_bars_ago(sym, self.skip_recent + self.lookback);
            if let (Some(p1), Some(p0)) = (p_end, p_start) {
                if p0 > Decimal::ZERO {
                    scores.push((sym.clone(), (p1 - p0) / p0));
                }
            }
        }
        if scores.len() < 4 {
            return None; // нема з чого будувати крос-секцію
        }

        // Стабільне впорядкування: за скором, потім за символом (детермінізм).
        scores.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let n = scores.len();
        let bucket = ((Decimal::from(n as u64) * self.quantile)
            .floor()
            .to_string()
            .parse::<usize>()
            .unwrap_or(1))
        .max(1)
        .min(n / 2);

        let long_leg = &scores[..bucket];
        let short_leg = &scores[n - bucket..];

        let per_leg = self.gross_exposure / Decimal::TWO;
        let w_long = per_leg / Decimal::from(bucket as u64);
        let w_short = -per_leg / Decimal::from(bucket as u64);

        let mut target: BTreeMap<String, Decimal> = BTreeMap::new();
        for (sym, _) in long_leg {
            target.insert(sym.clone(), w_long);
        }
        for (sym, _) in short_leg {
            // Символ в обох кошиках неможливий при bucket ≤ n/2.
            target.insert(sym.clone(), w_short);
        }

        // Turnover-банда: якщо максимальна зміна ваги проти минулого ребалансу
        // менша за поріг — тримаємо стару алокацію (без угод узагалі).
        if !self.last_weights.is_empty() {
            let mut max_delta = Decimal::ZERO;
            let keys: std::collections::BTreeSet<&String> =
                target.keys().chain(self.last_weights.keys()).collect();
            for k in keys {
                let a = target.get(k).copied().unwrap_or(Decimal::ZERO);
                let b = self.last_weights.get(k).copied().unwrap_or(Decimal::ZERO);
                max_delta = max_delta.max((a - b).abs());
            }
            if max_delta <= self.min_weight_change {
                return None;
            }
        }

        self.last_weights = target.clone();
        Some(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::domain::allocation::AlignedMarketData;
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    /// 6 символів з різними трендами: S0 найсильніший ... S5 найслабший.
    fn universe(n: usize) -> Arc<AlignedMarketData> {
        let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let timestamps: Vec<_> = (0..n).map(|i| t0 + Duration::days(i as i64)).collect();
        let mut closes = BTreeMap::new();
        for k in 0..6u32 {
            let drift = Decimal::from(5i64 - k as i64); // +5 ... 0
            let series: Vec<Decimal> = (0..n)
                .map(|i| dec!(1000) + drift * Decimal::from(i as u64))
                .collect();
            closes.insert(format!("S{k}"), series);
        }
        Arc::new(AlignedMarketData::from_closes(timestamps, closes).unwrap())
    }

    // M3.2: market-neutral за конструкцією — сума ваг ≈ 0.
    #[test]
    fn weights_sum_to_zero() {
        let data = universe(80);
        let mut s = CrossSectionalMomentum::new(40, 0, dec!(0.34), 21, dec!(0.0), dec!(1.0));
        let w = s.target_weights(&UniverseView::new(data, 79)).unwrap();
        let sum: Decimal = w.values().copied().sum();
        assert_eq!(sum, Decimal::ZERO, "dollar-neutral: {w:?}");
        let gross: Decimal = w.values().map(|v| v.abs()).sum();
        assert_eq!(gross, dec!(1.0));
    }

    #[test]
    fn ranks_strongest_long_weakest_short() {
        let data = universe(80);
        let mut s = CrossSectionalMomentum::new(40, 0, dec!(0.34), 21, dec!(0.0), dec!(1.0));
        let w = s.target_weights(&UniverseView::new(data, 79)).unwrap();
        assert!(w["S0"] > Decimal::ZERO, "найсильніший у лонгу: {w:?}");
        assert!(w["S5"] < Decimal::ZERO, "найслабший у шорті: {w:?}");
    }

    // Банда по turnover: незмінний рейтинг → None (жодних угод).
    #[test]
    fn rebalance_band_suppresses_noise_trades() {
        let data = universe(80);
        let mut s = CrossSectionalMomentum::new(40, 0, dec!(0.34), 21, dec!(0.05), dec!(1.0));
        let w1 = s.target_weights(&UniverseView::new(data.clone(), 70));
        assert!(w1.is_some());
        let w2 = s.target_weights(&UniverseView::new(data, 79));
        assert!(w2.is_none(), "рейтинг не змінився — угод бути не повинно");
    }
}
