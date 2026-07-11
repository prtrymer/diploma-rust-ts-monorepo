//! Walk-forward framework (M1.1) — application-сервіс.
//!
//! Ганяє стратегію по ковзних train/test-вікнах і агрегує МЕТРИКИ ТІЛЬКИ
//! OOS-СЕГМЕНТІВ (test), ніколи по всьому періоду разом. Параметри вікон —
//! з конфіга (M0.4). Між train і test — ембарго-зазор (симетрично до M1.2).

use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::backtest::domain::metrics::{self, DeflatedSharpeInput};
use crate::backtest::domain::portfolio_engine::{PortfolioBacktester, PortfolioRunResult};
use crate::shared::run_config::WalkForwardConfig;
use crate::trading::domain::allocation::{AllocationStrategy, AlignedMarketData};
use crate::trading::ports::{BrokerSimulatorPort, PortfolioPort};

/// Індексні межі фолда; всі інтервали напіввідкриті [start, end).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkForwardFold {
    pub train_start: usize,
    pub train_end: usize,
    pub test_start: usize,
    pub test_end: usize,
}

/// Генерує фолди: test-вікна послідовно тайлять кінець ряду, train — усе до
/// test мінус ембарго (anchored) або вікно фіксованої довжини (rolling).
pub fn generate_folds(n_samples: usize, cfg: &WalkForwardConfig) -> Vec<WalkForwardFold> {
    let folds = cfg.folds.max(2);
    if n_samples < folds * 10 {
        return Vec::new();
    }
    let train_ratio = cfg.train_ratio.to_f64().unwrap_or(0.7).clamp(0.5, 0.95);
    let initial_train = ((n_samples as f64) * train_ratio) as usize;
    let oos_total = n_samples - initial_train;
    let test_len = oos_total / folds;
    if test_len == 0 {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(folds);
    for k in 0..folds {
        let test_start = initial_train + k * test_len;
        let test_end = if k == folds - 1 {
            n_samples
        } else {
            test_start + test_len
        };
        let train_end = test_start.saturating_sub(cfg.embargo_bars);
        let train_start = if cfg.anchored {
            0
        } else {
            train_end.saturating_sub(initial_train)
        };
        if train_start >= train_end || test_start >= test_end {
            continue;
        }
        out.push(WalkForwardFold {
            train_start,
            train_end,
            test_start,
            test_end,
        });
    }
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardFoldResult {
    pub fold: WalkForwardFold,
    pub oos_return_pct: Decimal,
    pub oos_sharpe: Decimal,
    pub oos_max_drawdown_pct: Decimal,
    pub oos_trades: usize,
    pub oos_total_costs: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardSummary {
    pub folds: Vec<WalkForwardFoldResult>,
    /// Медіана OOS Sharpe по фолдах (M0.3: медіана, не середнє).
    pub median_oos_sharpe: Option<Decimal>,
    pub median_oos_return_pct: Option<Decimal>,
    /// Deflated Sharpe з урахуванням N trials (M1.3), якщо порахований.
    pub deflated_sharpe: Option<f64>,
    pub n_trials: usize,
}

pub struct WalkForwardRunner {
    data: Arc<AlignedMarketData>,
    broker: Arc<dyn BrokerSimulatorPort>,
    portfolio: Arc<dyn PortfolioPort>,
    initial_capital: Decimal,
}

impl WalkForwardRunner {
    pub fn new(
        data: Arc<AlignedMarketData>,
        broker: Arc<dyn BrokerSimulatorPort>,
        portfolio: Arc<dyn PortfolioPort>,
        initial_capital: Decimal,
    ) -> Self {
        Self {
            data,
            broker,
            portfolio,
            initial_capital,
        }
    }

    /// Прогін walk-forward: на кожен фолд — СВІЖА стратегія від фабрики
    /// (жоден стан не перетікає між фолдами), метрики тільки з test-вікна.
    /// `n_trials` — лічильник прогонів для deflated Sharpe (M0.4 → M1.3).
    pub async fn run(
        &self,
        strategy_factory: &dyn Fn() -> Box<dyn AllocationStrategy>,
        cfg: &WalkForwardConfig,
        n_trials: usize,
    ) -> Result<WalkForwardSummary> {
        let folds = generate_folds(self.data.len(), cfg);
        anyhow::ensure!(!folds.is_empty(), "not enough data for walk-forward");

        let engine = PortfolioBacktester::new(
            self.data.clone(),
            self.broker.clone(),
            self.portfolio.clone(),
            self.initial_capital,
        );

        let mut results = Vec::with_capacity(folds.len());
        for fold in folds {
            let mut strategy = strategy_factory();
            // Warmup стратегії живиться історією ДО test-вікна: движок
            // стартує рішення з test_start, а UniverseView бачить усе ≤ t.
            let run: PortfolioRunResult = engine
                .run_window(strategy.as_mut(), fold.test_start, fold.test_end)
                .await?;
            results.push(WalkForwardFoldResult {
                fold,
                oos_return_pct: run.report.total_return_pct,
                oos_sharpe: run.report.sharpe_ratio,
                oos_max_drawdown_pct: run.report.max_drawdown_pct,
                oos_trades: run.report.total_trades,
                oos_total_costs: run.report.total_costs,
            });
        }

        let sharpes: Vec<Decimal> = results.iter().map(|r| r.oos_sharpe).collect();
        let returns: Vec<Decimal> = results.iter().map(|r| r.oos_return_pct).collect();
        let median_oos_sharpe = metrics::median(&sharpes);
        let median_oos_return_pct = metrics::median(&returns);

        // Deflated Sharpe (M1.3): σ²(SR) по фолдах, N trials — з логу прогонів.
        let deflated_sharpe = median_oos_sharpe.and_then(|med| {
            let vals: Vec<f64> = sharpes.iter().filter_map(|s| s.to_f64()).collect();
            if vals.len() < 2 {
                return None;
            }
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
            // Sharpe за період: annualized / √252.
            let sr_period = med.to_f64()? / metrics::TRADING_DAYS_PER_YEAR.sqrt();
            let n_obs: usize = results
                .iter()
                .map(|r| r.fold.test_end - r.fold.test_start)
                .sum();
            metrics::deflated_sharpe_ratio(&DeflatedSharpeInput {
                sharpe: sr_period,
                n_observations: n_obs.max(2),
                n_trials: n_trials.max(1),
                sharpe_variance: var / metrics::TRADING_DAYS_PER_YEAR,
                skewness: 0.0,
                kurtosis: 3.0,
            })
        });

        Ok(WalkForwardSummary {
            folds: results,
            median_oos_sharpe,
            median_oos_return_pct,
            deflated_sharpe,
            n_trials,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn cfg(folds: usize, embargo: usize, anchored: bool) -> WalkForwardConfig {
        WalkForwardConfig {
            folds,
            train_ratio: dec!(0.70),
            anchored,
            embargo_bars: embargo,
            label_duration_bars: 5,
        }
    }

    // M1.1: train- і test-вікна не перетинаються за часом (плюс ембарго).
    #[test]
    fn train_and_test_never_overlap() {
        for anchored in [true, false] {
            let folds = generate_folds(1000, &cfg(5, 10, anchored));
            assert!(!folds.is_empty());
            for f in &folds {
                assert!(
                    f.train_end <= f.test_start,
                    "train [{},{}) перетинає test [{},{})",
                    f.train_start,
                    f.train_end,
                    f.test_start,
                    f.test_end
                );
                assert!(
                    f.test_start - f.train_end >= 10,
                    "ембарго-зазор відсутній: gap={}",
                    f.test_start - f.train_end
                );
                assert!(f.train_start < f.train_end);
                assert!(f.test_start < f.test_end);
            }
        }
    }

    // OOS-вікна послідовні й не перетинаються між собою.
    #[test]
    fn test_windows_are_sequential_and_disjoint() {
        let folds = generate_folds(1000, &cfg(4, 5, true));
        for w in folds.windows(2) {
            assert!(w[0].test_end <= w[1].test_start + 1);
            assert!(w[0].test_start < w[1].test_start);
        }
        // Разом OOS-вікна покривають хвіст ряду до кінця.
        assert_eq!(folds.last().unwrap().test_end, 1000);
    }

    #[test]
    fn too_short_series_gives_no_folds() {
        assert!(generate_folds(30, &cfg(4, 5, true)).is_empty());
    }
}
