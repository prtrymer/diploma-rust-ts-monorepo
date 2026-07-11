//! Чистий domain-модуль робастних метрик (M0.3) + deflated Sharpe (M1.3).
//!
//! Жодного I/O. Гроші — Decimal (інваріант 6); безрозмірні коефіцієнти
//! (Sharpe, Calmar, DSR) рахуються через f64 і повертаються як Decimal.
//!
//! Зведення по інструментах ведеться з МЕДІАНИ, не середнього: один MSFT
//! із 3 угодами більше ніколи не тягне агрегат (болячка захисту).

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::trading::domain::events::FillEvent;

/// Інструменти з меншою кількістю угод позначаються low-sample і не входять
/// до медіанного агрегату.
pub const LOW_SAMPLE_MIN_TRADES: usize = 10;

pub const TRADING_DAYS_PER_YEAR: f64 = 252.0;

// ── Базові будівельні блоки ──────────────────────────────────────────────────

/// Прості дохідності по кривій еквіті (пропускає невалідні точки).
pub fn simple_returns(equity_curve: &[Decimal]) -> Vec<f64> {
    let mut returns = Vec::with_capacity(equity_curve.len().saturating_sub(1));
    for w in equity_curve.windows(2) {
        let prev = w[0].to_f64().unwrap_or(0.0);
        let curr = w[1].to_f64().unwrap_or(0.0);
        if prev > 0.0 {
            returns.push((curr - prev) / prev);
        }
    }
    returns
}

/// Медіана; для парної довжини — середнє двох центральних.
pub fn median(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / Decimal::TWO
    })
}

// ── Метрики однієї кривої/набору угод ────────────────────────────────────────

/// Річний Sharpe (rf = 0) по кривій еквіті.
pub fn sharpe_annualized(equity_curve: &[Decimal]) -> Decimal {
    let returns = simple_returns(equity_curve);
    if returns.len() < 2 {
        return Decimal::ZERO;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    let std = var.sqrt();
    if std <= 0.0 {
        return Decimal::ZERO;
    }
    Decimal::from_f64(mean / std * TRADING_DAYS_PER_YEAR.sqrt()).unwrap_or(Decimal::ZERO)
}

/// Максимальна просадка: (абсолютна, у % від піку).
pub fn max_drawdown(equity_curve: &[Decimal]) -> (Decimal, Decimal) {
    let mut peak = Decimal::MIN;
    let mut max_dd = Decimal::ZERO;
    let mut max_dd_pct = Decimal::ZERO;
    for &point in equity_curve {
        if point > peak {
            peak = point;
        }
        let dd = peak - point;
        if dd > max_dd {
            max_dd = dd;
        }
        if peak > Decimal::ZERO {
            let dd_pct = dd / peak * Decimal::ONE_HUNDRED;
            if dd_pct > max_dd_pct {
                max_dd_pct = dd_pct;
            }
        }
    }
    (max_dd, max_dd_pct)
}

/// Calmar: річна дохідність (%) / max drawdown (%). None, якщо просадки немає.
pub fn calmar(annual_return_pct: Decimal, max_drawdown_pct: Decimal) -> Option<Decimal> {
    if max_drawdown_pct <= Decimal::ZERO {
        return None;
    }
    Some(annual_return_pct / max_drawdown_pct)
}

/// Оборот: сумарний торгований нотіонал / середню вартість портфеля.
/// (у «разах за період»; помножений на річний фактор дає annual turnover).
pub fn turnover(fills: &[FillEvent], avg_equity: Decimal) -> Decimal {
    if avg_equity <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let traded_notional: Decimal = fills
        .iter()
        .map(|f| (f.fill_price * f.quantity).abs())
        .sum();
    traded_notional / avg_equity
}

/// Частка прибуткових угод [0..1].
pub fn hit_rate(trade_pnls: &[Decimal]) -> Option<Decimal> {
    if trade_pnls.is_empty() {
        return None;
    }
    let wins = trade_pnls.iter().filter(|p| **p > Decimal::ZERO).count();
    Some(Decimal::from(wins as u64) / Decimal::from(trade_pnls.len() as u64))
}

/// Profit factor: Σ виграшів / Σ |програшів|. None — якщо програшів немає.
pub fn profit_factor(trade_pnls: &[Decimal]) -> Option<Decimal> {
    let wins: Decimal = trade_pnls
        .iter()
        .filter(|p| **p > Decimal::ZERO)
        .copied()
        .sum();
    let losses: Decimal = trade_pnls
        .iter()
        .filter(|p| **p < Decimal::ZERO)
        .map(|p| p.abs())
        .sum();
    if losses <= Decimal::ZERO {
        return None;
    }
    Some(wins / losses)
}

/// Net PnL = gross PnL − сумарні витрати. Інваріант 2: звіт веде з net;
/// gross існує тільки як діагностика поруч.
pub fn pnl_after_costs(gross_pnl: Decimal, total_costs: Decimal) -> Decimal {
    gross_pnl - total_costs
}

// ── Мульти-інструментне зведення (медіана, low-sample) ───────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentMetrics {
    pub symbol: String,
    pub sharpe: Decimal,
    pub net_return_pct: Decimal,
    pub trades: usize,
    /// < LOW_SAMPLE_MIN_TRADES угод — не входить у медіанний агрегат.
    pub low_sample: bool,
}

impl InstrumentMetrics {
    pub fn new(symbol: String, sharpe: Decimal, net_return_pct: Decimal, trades: usize) -> Self {
        Self {
            symbol,
            sharpe,
            net_return_pct,
            trades,
            low_sample: trades < LOW_SAMPLE_MIN_TRADES,
        }
    }
}

/// Медіанний Sharpe по інструментах З ДОСТАТНЬОЮ вибіркою. Якщо всі
/// low-sample — медіана по всіх, але це сигнал не довіряти результату.
pub fn median_sharpe(instruments: &[InstrumentMetrics]) -> Option<Decimal> {
    let qualified: Vec<Decimal> = instruments
        .iter()
        .filter(|m| !m.low_sample)
        .map(|m| m.sharpe)
        .collect();
    if !qualified.is_empty() {
        return median(&qualified);
    }
    median(&instruments.iter().map(|m| m.sharpe).collect::<Vec<_>>())
}

// ── Deflated Sharpe Ratio (M1.3, Bailey & López de Prado 2014) ───────────────

/// Вхід для DSR. `sharpe` — за період спостереження (НЕ річний),
/// `n_observations` — кількість дохідностей, з яких він порахований.
#[derive(Debug, Clone)]
pub struct DeflatedSharpeInput {
    pub sharpe: f64,
    pub n_observations: usize,
    /// Скільки незалежних конфігурацій випробувано (лічильник прогонів, M0.4).
    pub n_trials: usize,
    /// Дисперсія Sharpe по випробуваннях.
    pub sharpe_variance: f64,
    /// Скошеність дохідностей (0 для нормальних).
    pub skewness: f64,
    /// Ексцес дохідностей (3 для нормальних).
    pub kurtosis: f64,
}

/// Ймовірність, що спостережений Sharpe перевищує максимум, очікуваний від
/// `n_trials` випробувань чистого шуму. > 0.95 — знахідка переживає корекцію
/// на множинне тестування.
pub fn deflated_sharpe_ratio(input: &DeflatedSharpeInput) -> Option<f64> {
    if input.n_observations < 2 || input.n_trials == 0 {
        return None;
    }
    let n = input.n_trials as f64;
    let sr0 = if input.n_trials == 1 {
        0.0
    } else {
        // Очікуваний максимум SR серед N незалежних шумових випробувань.
        const EULER_MASCHERONI: f64 = 0.577_215_664_901_532_9;
        let std_sr = input.sharpe_variance.max(0.0).sqrt();
        std_sr
            * ((1.0 - EULER_MASCHERONI) * inverse_normal_cdf(1.0 - 1.0 / n)
                + EULER_MASCHERONI * inverse_normal_cdf(1.0 - 1.0 / (n * std::f64::consts::E)))
    };

    let sr = input.sharpe;
    let t = input.n_observations as f64;
    let denom = 1.0 - input.skewness * sr + (input.kurtosis - 1.0) / 4.0 * sr * sr;
    if denom <= 0.0 {
        return None;
    }
    let z = (sr - sr0) * (t - 1.0).sqrt() / denom.sqrt();
    Some(normal_cdf(z))
}

/// Φ(x) через erf (Abramowitz–Stegun 7.1.26, |ε| < 1.5e-7).
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

fn erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let y = 1.0
        - (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736)
            * t
            + 0.254_829_592)
            * t
            * (-x * x).exp();
    sign * y
}

/// Φ⁻¹(p), апроксимація Acklam (відносна похибка < 1.15e-9).
pub fn inverse_normal_cdf(p: f64) -> f64 {
    assert!(p > 0.0 && p < 1.0, "p must be in (0,1)");
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const P_LOW: f64 = 0.02425;

    if p < P_LOW {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= 1.0 - P_LOW {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use crate::trading::domain::events::OrderSide;

    // M0.3: один інструмент-викид не зсуває медіанний Sharpe
    // (на відміну від середнього).
    #[test]
    fn outlier_does_not_shift_median_sharpe() {
        let mk = |sym: &str, sharpe: Decimal, trades: usize| {
            InstrumentMetrics::new(sym.to_string(), sharpe, Decimal::ZERO, trades)
        };
        let instruments = vec![
            mk("AAA", dec!(0.4), 50),
            mk("BBB", dec!(0.5), 40),
            mk("CCC", dec!(0.6), 60),
            // Викид: величезний Sharpe на 3 угодах (кейс MSFT із захисту).
            mk("MSFT", dec!(25.0), 3),
        ];
        let med = median_sharpe(&instruments).unwrap();
        assert_eq!(med, dec!(0.5), "low-sample викид виключений з агрегату");

        let mean: Decimal = instruments.iter().map(|m| m.sharpe).sum::<Decimal>()
            / Decimal::from(instruments.len() as u64);
        assert!(mean > dec!(6), "середнє тягнеться викидом — тому НЕ mean");
    }

    // M0.3: MDD на монотонно спадній кривій == повне падіння.
    #[test]
    fn mdd_on_monotone_decreasing_curve_is_full_drop() {
        let curve = vec![dec!(100), dec!(90), dec!(80), dec!(50), dec!(20)];
        let (dd, dd_pct) = max_drawdown(&curve);
        assert_eq!(dd, dec!(80));
        assert_eq!(dd_pct, dec!(80));
    }

    #[test]
    fn mdd_recovers_after_new_peak() {
        let curve = vec![dec!(100), dec!(80), dec!(120), dec!(90)];
        let (dd, _) = max_drawdown(&curve);
        assert_eq!(dd, dec!(30)); // 120 → 90, а не 100 → 80
    }

    #[test]
    fn median_even_and_odd() {
        assert_eq!(median(&[dec!(3), dec!(1), dec!(2)]), Some(dec!(2)));
        assert_eq!(
            median(&[dec!(4), dec!(1), dec!(2), dec!(3)]),
            Some(dec!(2.5))
        );
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn turnover_counts_both_sides() {
        let fill = |side: OrderSide, qty: Decimal, price: Decimal| FillEvent {
            id: Uuid::nil(),
            order_id: Uuid::nil(),
            timestamp: Utc::now(),
            symbol: "T".into(),
            side,
            quantity: qty,
            fill_price: price,
            commission: Decimal::ZERO,
            slippage: Decimal::ZERO,
        };
        let fills = vec![
            fill(OrderSide::Buy, dec!(10), dec!(100)),
            fill(OrderSide::Sell, dec!(10), dec!(110)),
        ];
        // (1000 + 1100) / 1000 = 2.1
        assert_eq!(turnover(&fills, dec!(1000)), dec!(2.1));
    }

    #[test]
    fn hit_rate_and_profit_factor() {
        let pnls = vec![dec!(10), dec!(-5), dec!(20), dec!(-5)];
        assert_eq!(hit_rate(&pnls), Some(dec!(0.5)));
        assert_eq!(profit_factor(&pnls), Some(dec!(3)));
        assert_eq!(profit_factor(&[dec!(10)]), None, "без програшів PF не визначений");
    }

    #[test]
    fn calmar_undefined_without_drawdown() {
        assert_eq!(calmar(dec!(10), Decimal::ZERO), None);
        assert_eq!(calmar(dec!(10), dec!(5)), Some(dec!(2)));
    }

    #[test]
    fn pnl_after_costs_subtracts() {
        assert_eq!(pnl_after_costs(dec!(100), dec!(30)), dec!(70));
    }

    // M1.3: за фіксованого сирого Sharpe зростання N trials знижує DSR.
    #[test]
    fn deflated_sharpe_decreases_with_trials() {
        let dsr = |n_trials: usize| {
            deflated_sharpe_ratio(&DeflatedSharpeInput {
                sharpe: 0.1, // per-period SR
                n_observations: 252,
                n_trials,
                sharpe_variance: 0.005,
                skewness: 0.0,
                kurtosis: 3.0,
            })
            .unwrap()
        };
        let d1 = dsr(1);
        let d10 = dsr(10);
        let d100 = dsr(100);
        let d1000 = dsr(1000);
        assert!(d1 > d10, "{d1} !> {d10}");
        assert!(d10 > d100, "{d10} !> {d100}");
        assert!(d100 > d1000, "{d100} !> {d1000}");
    }

    #[test]
    fn normal_cdf_sanity() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-9);
        assert!((normal_cdf(1.96) - 0.975).abs() < 1e-3);
        assert!((inverse_normal_cdf(0.975) - 1.96).abs() < 1e-2);
    }
}
