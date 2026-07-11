//! Єдина типізована конфігурація прогону (інваріант 5: Provenance).
//!
//! Це ЄДИНЕ джерело правди для всіх торгових параметрів. Дефолти визначені
//! тільки тут (через `Default`), ніде в коді не дублюються. Кожен прогін
//! серіалізує конфіг, рахує SHA-256 і логовує разом із результатами.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Параметри стратегії сигналів.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfig {
    /// Вид стратегії: "momentum" | "tsmom" | "xs_momentum" | "funding_carry"
    /// | "buy_and_hold" | "equal_weight" | "sixty_forty".
    pub kind: String,
    /// Мінімальна впевненість моделі для емісії сигналу.
    pub min_confidence: Decimal,
    /// Розмір lookback-буфера (кількість барів) для фіч.
    pub lookback_size: usize,
    /// Мінімальний інтервал між сигналами, сек.
    pub min_signal_gap_secs: i64,
    /// Дозволені тільки лонги.
    pub long_only: bool,
    /// Lookback для momentum-скору (TSMOM / cross-sectional), у барах.
    pub momentum_lookback: usize,
    /// Частка юніверсу в топ/низ кошиках cross-sectional momentum (0..0.5].
    pub xs_quantile: Decimal,
    /// Цільова річна волатильність для vol-таргетингу TSMOM (частка).
    pub vol_target_annual: Decimal,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            kind: "momentum".to_string(),
            min_confidence: dec!(0.01),
            lookback_size: 60,
            min_signal_gap_secs: 900,
            long_only: true,
            momentum_lookback: 252,
            xs_quantile: dec!(0.2),
            vol_target_annual: dec!(0.10),
        }
    }
}

/// Консолідований position sizing (лікує розбіжність 65% vs 80% між розділами).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SizingConfig {
    /// Максимальна частка капіталу в одній позиції.
    pub max_position_pct: Decimal,
    /// Частка кешу, що завжди тримається в резерві.
    pub reserve_cash_pct: Decimal,
    /// Stop-loss, частка від ціни входу.
    pub stop_loss_pct: Decimal,
    /// Take-profit, частка від ціни входу.
    pub take_profit_pct: Decimal,
    /// Мінімальний розмір угоди (штук).
    pub min_trade_quantity: Decimal,
    /// Кап на Kelly-фракцію (0 = Kelly вимкнено, використовується max_position_pct).
    pub kelly_fraction_cap: Decimal,
}

impl Default for SizingConfig {
    fn default() -> Self {
        Self {
            max_position_pct: dec!(0.80),
            reserve_cash_pct: dec!(0.02),
            stop_loss_pct: dec!(0.01),
            take_profit_pct: dec!(0.025),
            min_trade_quantity: dec!(1),
            kelly_fraction_cap: dec!(0),
        }
    }
}

/// Параметри Almgren–Chriss market impact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AlmgrenChrissConfig {
    /// Коефіцієнт тимчасового impact (η).
    pub eta: Decimal,
    /// Коефіцієнт постійного impact (γ).
    pub gamma: Decimal,
    /// Дефолтна волатильність бару (частка), якщо контекст її не дає.
    pub default_volatility: Decimal,
    /// Дефолтний середній обсяг бару (штук), якщо контекст його не дає.
    pub default_avg_volume: Decimal,
}

impl Default for AlmgrenChrissConfig {
    fn default() -> Self {
        Self {
            eta: dec!(0.1),
            gamma: dec!(0.05),
            default_volatility: dec!(0.02),
            default_avg_volume: dec!(1000000),
        }
    }
}

/// Параметри моделі транзакційних витрат (інваріант 2: витрати завжди увімкнені).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CostConfig {
    /// Фіксована комісія за угоду (грошова).
    pub commission_fixed: Decimal,
    /// Комісія як частка нотіоналу.
    pub commission_pct: Decimal,
    /// Повний відносний спред; сторона платить половину.
    pub spread_pct: Decimal,
    /// Market impact за Almgren–Chriss; None = імпакт не моделюється.
    pub impact: Option<AlmgrenChrissConfig>,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            commission_fixed: dec!(0.10),
            commission_pct: dec!(0.0005),
            spread_pct: dec!(0.0004),
            impact: Some(AlmgrenChrissConfig::default()),
        }
    }
}

impl CostConfig {
    /// Нульові витрати — ТІЛЬКИ для діагностики gross==net (регресійний sanity).
    pub fn zero() -> Self {
        Self {
            commission_fixed: Decimal::ZERO,
            commission_pct: Decimal::ZERO,
            spread_pct: Decimal::ZERO,
            impact: None,
        }
    }
}

/// Склад та поведінка ML-ансамблю (інваріант 4: фіксовані сіди).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnsembleConfig {
    /// Ваги [fast, medium, slow].
    pub weights: Vec<Decimal>,
    /// Learning rates підмоделей [fast, medium, slow].
    pub learning_rates: Vec<f64>,
    /// Deadzone сигналу: |score| нижче — Exit.
    pub signal_deadzone: Decimal,
    /// Сід для будь-якої стохастики моделей.
    pub seed: u64,
    /// Адаптивне перезважування на льоту.
    pub adaptive_learning: bool,
}

impl Default for EnsembleConfig {
    fn default() -> Self {
        Self {
            weights: vec![dec!(0.40), dec!(0.35), dec!(0.25)],
            learning_rates: vec![0.0020, 0.0010, 0.0005],
            signal_deadzone: dec!(0.01),
            seed: 42,
            adaptive_learning: false,
        }
    }
}

/// Параметри walk-forward валідації (M1.1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalkForwardConfig {
    /// Кількість фолдів.
    pub folds: usize,
    /// Частка train у межах фолда.
    pub train_ratio: Decimal,
    /// Анкерований (train росте від початку) чи ковзний.
    pub anchored: bool,
    /// Ембарго між train і test, у барах (використовується і в purged CV).
    pub embargo_bars: usize,
    /// Тривалість лейбла в барах (для purge перекриття інформації).
    pub label_duration_bars: usize,
}

impl Default for WalkForwardConfig {
    fn default() -> Self {
        Self {
            folds: 4,
            train_ratio: dec!(0.70),
            anchored: true,
            embargo_bars: 5,
            label_duration_bars: 5,
        }
    }
}

/// Повна конфігурація прогону. Провенанс: SHA-256 цієї структури.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RunConfig {
    pub strategy: StrategyConfig,
    pub sizing: SizingConfig,
    pub costs: CostConfig,
    pub ensemble: EnsembleConfig,
    pub walk_forward: WalkForwardConfig,
}

impl RunConfig {
    /// Канонічна серіалізація: порядок полів структур стабільний у serde_json,
    /// тому байтове представлення детерміноване для того самого конфіга.
    pub fn canonical_json(&self) -> String {
        serde_json::to_string(self).expect("RunConfig serialization is infallible")
    }

    /// SHA-256 хеш повного конфіга (hex, lowercase).
    pub fn config_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.canonical_json().as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    pub fn to_pretty_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("RunConfig serialization is infallible")
    }

    /// Порівняння двох конфігів по полях верхнього рівня JSON (для CLI diff).
    pub fn diff(&self, other: &Self) -> Vec<String> {
        let a: serde_json::Value =
            serde_json::to_value(self).expect("RunConfig to_value is infallible");
        let b: serde_json::Value =
            serde_json::to_value(other).expect("RunConfig to_value is infallible");
        let mut out = Vec::new();
        diff_json("", &a, &b, &mut out);
        out
    }
}

fn diff_json(path: &str, a: &serde_json::Value, b: &serde_json::Value, out: &mut Vec<String>) {
    match (a, b) {
        (serde_json::Value::Object(ma), serde_json::Value::Object(mb)) => {
            let keys: std::collections::BTreeSet<&String> = ma.keys().chain(mb.keys()).collect();
            for k in keys {
                let sub = if path.is_empty() {
                    k.to_string()
                } else {
                    format!("{path}.{k}")
                };
                let av = ma.get(k).unwrap_or(&serde_json::Value::Null);
                let bv = mb.get(k).unwrap_or(&serde_json::Value::Null);
                diff_json(&sub, av, bv, out);
            }
        }
        _ if a != b => out.push(format!("{path}: {a} -> {b}")),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // M0.4: однаковий конфіг → однаковий hash.
    #[test]
    fn identical_config_same_hash() {
        let a = RunConfig::default();
        let b = RunConfig::default();
        assert_eq!(a.config_hash(), b.config_hash());
    }

    // M0.4: зміна одного поля → інший hash.
    #[test]
    fn changed_field_different_hash() {
        let a = RunConfig::default();
        let mut b = RunConfig::default();
        b.strategy.min_confidence = dec!(0.6);
        assert_ne!(a.config_hash(), b.config_hash());

        let mut c = RunConfig::default();
        c.sizing.max_position_pct = dec!(0.65);
        assert_ne!(a.config_hash(), c.config_hash());
    }

    #[test]
    fn hash_roundtrip_through_json() {
        let a = RunConfig::default();
        let restored = RunConfig::from_json(&a.canonical_json()).unwrap();
        assert_eq!(a.config_hash(), restored.config_hash());
    }

    #[test]
    fn diff_reports_changed_paths() {
        let a = RunConfig::default();
        let mut b = RunConfig::default();
        b.sizing.stop_loss_pct = dec!(0.03);
        b.strategy.min_confidence = dec!(0.6);
        let d = a.diff(&b);
        assert_eq!(d.len(), 2);
        assert!(d.iter().any(|l| l.starts_with("sizing.stop_loss_pct")));
        assert!(d.iter().any(|l| l.starts_with("strategy.min_confidence")));
    }
}
