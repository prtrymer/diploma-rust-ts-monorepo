//! Бенчмарки як тривіальні імплементації `AllocationStrategy` (M0.2).
//!
//! Проходять ТОЙ САМИЙ движок виконання і ту саму модель витрат, що й
//! основна стратегія — порівняння чесне. Кожен звіт зобов'язаний друкувати
//! їх поруч зі стратегією: «нічого не робити» — базова лінія будь-якої альфи.

use rust_decimal::Decimal;
use std::collections::BTreeMap;

use crate::trading::domain::allocation::{AllocationStrategy, UniverseView};

/// Купив і тримаєш: рівні ваги на першому барі, далі жодних ребалансів.
pub struct BuyAndHold {
    invested: bool,
}

impl BuyAndHold {
    pub fn new() -> Self {
        Self { invested: false }
    }
}

impl Default for BuyAndHold {
    fn default() -> Self {
        Self::new()
    }
}

impl AllocationStrategy for BuyAndHold {
    fn name(&self) -> &str {
        "buy_and_hold"
    }

    fn target_weights(&mut self, view: &UniverseView) -> Option<BTreeMap<String, Decimal>> {
        if self.invested {
            return None; // тримаємо; жодного турноверу
        }
        self.invested = true;
        let symbols: Vec<&String> = view.symbols().collect();
        if symbols.is_empty() {
            return None;
        }
        let w = Decimal::ONE / Decimal::from(symbols.len() as u64);
        Some(symbols.into_iter().map(|s| (s.clone(), w)).collect())
    }
}

/// Рівні ваги з періодичним ребалансом.
pub struct EqualWeight {
    rebalance_every: usize,
}

impl EqualWeight {
    pub fn new(rebalance_every: usize) -> Self {
        Self {
            rebalance_every: rebalance_every.max(1),
        }
    }
}

impl AllocationStrategy for EqualWeight {
    fn name(&self) -> &str {
        "equal_weight"
    }

    fn target_weights(&mut self, view: &UniverseView) -> Option<BTreeMap<String, Decimal>> {
        let symbols: Vec<&String> = view.symbols().collect();
        if symbols.is_empty() {
            return None;
        }
        let w = Decimal::ONE / Decimal::from(symbols.len() as u64);
        Some(symbols.into_iter().map(|s| (s.clone(), w)).collect())
    }

    fn rebalance_every(&self) -> usize {
        self.rebalance_every
    }
}

/// Класичний 60/40: 60% на «акційний» кошик, 40% на «захисний».
/// Якщо захисних символів у юніверсі немає — 40% лишається в кеші
/// (чесний проксі за відсутності облігаційної ноги).
pub struct SixtyForty {
    equity_symbols: Vec<String>,
    defensive_symbols: Vec<String>,
    rebalance_every: usize,
}

impl SixtyForty {
    pub fn new(
        equity_symbols: Vec<String>,
        defensive_symbols: Vec<String>,
        rebalance_every: usize,
    ) -> Self {
        Self {
            equity_symbols,
            defensive_symbols,
            rebalance_every: rebalance_every.max(1),
        }
    }

    /// Автоконфігурація: увесь юніверс — «акції», захисна нога в кеші.
    pub fn all_equity(universe: Vec<String>, rebalance_every: usize) -> Self {
        Self::new(universe, Vec::new(), rebalance_every)
    }
}

impl AllocationStrategy for SixtyForty {
    fn name(&self) -> &str {
        "sixty_forty"
    }

    fn target_weights(&mut self, _view: &UniverseView) -> Option<BTreeMap<String, Decimal>> {
        let mut weights = BTreeMap::new();
        if !self.equity_symbols.is_empty() {
            let w = Decimal::new(60, 2) / Decimal::from(self.equity_symbols.len() as u64);
            for s in &self.equity_symbols {
                weights.insert(s.clone(), w);
            }
        }
        if !self.defensive_symbols.is_empty() {
            let w = Decimal::new(40, 2) / Decimal::from(self.defensive_symbols.len() as u64);
            for s in &self.defensive_symbols {
                weights.insert(s.clone(), w);
            }
        }
        if weights.is_empty() {
            None
        } else {
            Some(weights)
        }
    }

    fn rebalance_every(&self) -> usize {
        self.rebalance_every
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::domain::allocation::AlignedMarketData;
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    fn data_two_symbols() -> Arc<AlignedMarketData> {
        let timestamps = (1..=5)
            .map(|d| Utc.with_ymd_and_hms(2024, 1, d, 0, 0, 0).unwrap())
            .collect();
        let mut closes = BTreeMap::new();
        closes.insert("AAA".into(), vec![dec!(10); 5]);
        closes.insert("BBB".into(), vec![dec!(20); 5]);
        Arc::new(AlignedMarketData::from_closes(timestamps, closes).unwrap())
    }

    #[test]
    fn buy_and_hold_allocates_once() {
        let data = data_two_symbols();
        let mut s = BuyAndHold::new();
        let w = s.target_weights(&UniverseView::new(data.clone(), 0)).unwrap();
        assert_eq!(w.len(), 2);
        assert_eq!(w["AAA"], dec!(0.5));
        assert!(s.target_weights(&UniverseView::new(data, 1)).is_none());
    }

    #[test]
    fn sixty_forty_split() {
        let data = data_two_symbols();
        let mut s = SixtyForty::new(vec!["AAA".into()], vec!["BBB".into()], 21);
        let w = s.target_weights(&UniverseView::new(data, 0)).unwrap();
        assert_eq!(w["AAA"], dec!(0.60));
        assert_eq!(w["BBB"], dec!(0.40));
    }

    #[test]
    fn sixty_forty_without_defensive_keeps_cash() {
        let data = data_two_symbols();
        let mut s = SixtyForty::all_equity(vec!["AAA".into(), "BBB".into()], 21);
        let w = s.target_weights(&UniverseView::new(data, 0)).unwrap();
        let total: Decimal = w.values().copied().sum();
        assert_eq!(total, dec!(0.60), "40% лишається в кеші");
    }
}
