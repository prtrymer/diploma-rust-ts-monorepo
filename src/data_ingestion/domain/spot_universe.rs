//! Які перпи взагалі можна захеджувати спотом (рівень 1 роботи над базисом).
//!
//! Carry за визначенням тримає лонг спота проти шорта перпа. Якщо спот-ринку
//! не існує, другої ноги немає і угода неможлива — не дорожча, не
//! ризикованіша, а неможлива. Це не крайовий випадок: у кошику тіньового
//! журналу за 2026-08-02 спот-пару мали 0 із 10 символів в обох кошиках
//! (baseline_est і rf), і причина економічна — фандинг найбільший саме там,
//! де арбітражний канал закритий за відсутністю спота (токенізовані акції,
//! ETF, товари). Ранжування за величиною фандингу систематично відбирає
//! рівно ці контракти.
//!
//! Маніфест будує `scripts/fetch_spot_pairs.py` і він КОМІТИТЬСЯ: прогін
//! читає файл і в мережу не ходить. Інакше юніверс залежав би від того, що
//! біржа відповіла в мить прогону, — а це ламає і детермінізм (інваріант 4),
//! і провенанс (інваріант 5).

use std::collections::BTreeSet;

/// Перпи з відомим статусом спот-ноги.
#[derive(Debug, Clone, Default)]
pub struct SpotUniverse {
    hedgeable: BTreeSet<String>,
    known: BTreeSet<String>,
}

/// Розклад юніверсу після фільтра. Три категорії, а не дві: «немає спот-пари»
/// і «символа немає в маніфесті» — різні речі, і плутати їх не можна. Перше
/// означає перевірений факт, друге — що ми не перевіряли (напр. HTX-юніверс
/// проти маніфесту Binance), і мовчки викидати такі символи не можна.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FilterOutcome {
    pub kept: Vec<String>,
    pub no_spot: Vec<String>,
    pub unknown: Vec<String>,
}

impl SpotUniverse {
    pub fn from_rows<I, S>(rows: I) -> Self
    where
        I: IntoIterator<Item = (S, bool)>,
        S: Into<String>,
    {
        let mut hedgeable = BTreeSet::new();
        let mut known = BTreeSet::new();
        for (sym, has_spot) in rows {
            let sym = sym.into().to_uppercase();
            if has_spot {
                hedgeable.insert(sym.clone());
            }
            known.insert(sym);
        }
        Self { hedgeable, known }
    }

    pub fn is_hedgeable(&self, symbol: &str) -> bool {
        self.hedgeable.contains(&symbol.to_uppercase())
    }

    pub fn is_known(&self, symbol: &str) -> bool {
        self.known.contains(&symbol.to_uppercase())
    }

    pub fn hedgeable_len(&self) -> usize {
        self.hedgeable.len()
    }

    pub fn known_len(&self) -> usize {
        self.known.len()
    }

    /// Розкладає символи на три категорії, зберігаючи порядок входу.
    pub fn partition<'a, I>(&self, symbols: I) -> FilterOutcome
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut out = FilterOutcome::default();
        for sym in symbols {
            if !self.is_known(sym) {
                out.unknown.push(sym.to_string());
            } else if self.is_hedgeable(sym) {
                out.kept.push(sym.to_string());
            } else {
                out.no_spot.push(sym.to_string());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn universe() -> SpotUniverse {
        SpotUniverse::from_rows([
            ("BTCUSDT", true),
            ("ETHUSDT", true),
            ("NVDAUSDT", false),
            ("KORUUSDT", false),
        ])
    }

    #[test]
    fn splits_into_three_categories() {
        let out = universe().partition(["BTCUSDT", "NVDAUSDT", "SOMETHINGUSDT"]);
        assert_eq!(out.kept, vec!["BTCUSDT"]);
        assert_eq!(out.no_spot, vec!["NVDAUSDT"]);
        assert_eq!(
            out.unknown,
            vec!["SOMETHINGUSDT"],
            "невідомий символ — не те саме, що перевірено-нехеджований"
        );
    }

    // Символ без спот-пари відомий, але не хеджований. Якби ці два стани
    // злилися, фільтр мовчки пропускав би невідомі символи як торговані.
    #[test]
    fn known_without_spot_is_not_hedgeable() {
        let u = universe();
        assert!(u.is_known("NVDAUSDT"));
        assert!(!u.is_hedgeable("NVDAUSDT"));
        assert!(!u.is_known("SOMETHINGUSDT"));
    }

    #[test]
    fn symbol_case_does_not_matter() {
        let u = universe();
        assert!(u.is_hedgeable("btcusdt"));
        assert!(u.is_known("nvdausdt"));
    }

    #[test]
    fn counts_report_both_sides() {
        let u = universe();
        assert_eq!(u.hedgeable_len(), 2);
        assert_eq!(u.known_len(), 4);
    }
}
