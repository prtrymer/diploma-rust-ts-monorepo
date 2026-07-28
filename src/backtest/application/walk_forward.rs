//! Walk-forward framework (M1.1) — application-сервіс.
//!
//! Ганяє стратегію по ковзних train/test-вікнах і агрегує МЕТРИКИ ТІЛЬКИ
//! OOS-СЕГМЕНТІВ (test), ніколи по всьому періоду разом. Параметри вікон —
//! з конфіга (M0.4). Між train і test — ембарго-зазор (симетрично до M1.2).

use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
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
    /// Рахується ТІЛЬКИ по фолдах з `oos_trades > 0` — див. `from_folds`.
    pub median_oos_sharpe: Option<Decimal>,
    pub median_oos_return_pct: Option<Decimal>,
    /// Deflated Sharpe з урахуванням N trials (M1.3), якщо порахований.
    pub deflated_sharpe: Option<f64>,
    pub n_trials: usize,
    /// Розкид Sharpe по перебраних гіпотезах — те, що йде в deflated Sharpe.
    /// Виставлений окремо, бо без нього число DSR неможливо пояснити.
    pub trial_dispersion: Option<f64>,
    /// Скільки фолдів реально дали OOS-спостереження (увійшли в медіани).
    pub evaluated_folds: usize,
    /// Скільки фолдів відкинуто як порожні; `evaluated_folds + empty_folds
    /// == folds.len()`. Ненульове значення — привід дивитися на стратегію,
    /// а не на медіану.
    pub empty_folds: usize,
    /// Скільки з оцінених фолдів дали додатний Sharpe.
    pub positive_folds: usize,
}

impl WalkForwardSummary {
    /// Агрегує результати фолдів у зведення.
    ///
    /// Фолд без жодної угоди — це НЕ спостереження зі Sharpe 0, а відсутність
    /// спостереження: стратегія не відкрила позицій, і `sharpe_ratio` там
    /// вироджений нуль, а не виміряна дохідність на ризик. Включення таких
    /// фолдів у медіану підміняє гейт M2.1 артефактом: у прогонах
    /// 2026-07-20/27 медіани 0.541 і 0.439 наполовину складалися саме з
    /// порожнього останнього фолда (фактичні медіани по фолдах з угодами —
    /// 1.081 і 0.878). Тому агрегати рахуються лише по `oos_trades > 0`.
    ///
    /// Якщо угод не було в жодному фолді, медіани — `None` («нема з чого
    /// рахувати»), а не нуль: гейт мусить бути неоціненим, а не «провалено
    /// з рівно нульовим Sharpe».
    /// `prior_trial_sharpes` — медіанні OOS-Sharpe раніше перебраних
    /// конфігурацій ТІЄЇ Ж стратегії (`RunLogger::trial_sharpes`). Поточний
    /// прогін додається до них: він теж перебрана гіпотеза.
    pub fn from_folds(folds: Vec<WalkForwardFoldResult>, prior_trial_sharpes: &[f64]) -> Self {
        let evaluated: Vec<&WalkForwardFoldResult> =
            folds.iter().filter(|r| r.oos_trades > 0).collect();
        let evaluated_folds = evaluated.len();
        let empty_folds = folds.len() - evaluated_folds;
        let positive_folds = evaluated
            .iter()
            .filter(|r| r.oos_sharpe > Decimal::ZERO)
            .count();

        let sharpes: Vec<Decimal> = evaluated.iter().map(|r| r.oos_sharpe).collect();
        let returns: Vec<Decimal> = evaluated.iter().map(|r| r.oos_return_pct).collect();
        let median_oos_sharpe = metrics::median(&sharpes);
        let median_oos_return_pct = metrics::median(&returns);

        // Deflated Sharpe (M1.3). σ(SR) — розкид по ПЕРЕБРАНИХ ГІПОТЕЗАХ, як і
        // вимагає Bailey–López de Prado. Раніше сюди йшов розкид по фолдах
        // одного прогону: інша величина, оцінена по чотирьох точках, і вона
        // задирала очікуваний максимум так, що поріг ставав недосяжним.
        // n_obs лишається по оцінених фолдах — це довжина ряду дохідностей
        // саме цієї стратегії.
        let mut trials: Vec<f64> = prior_trial_sharpes.to_vec();
        if let Some(med) = median_oos_sharpe.and_then(|m| m.to_f64()) {
            trials.push(med);
        }
        let n_trials = trials.len().max(1);
        let trial_dispersion = (trials.len() >= 2).then(|| {
            let mean = trials.iter().sum::<f64>() / trials.len() as f64;
            (trials.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / trials.len() as f64).sqrt()
        });

        let deflated_sharpe = median_oos_sharpe.and_then(|med| {
            // Sharpe за період: annualized / √252.
            let sr_period = med.to_f64()? / metrics::TRADING_DAYS_PER_YEAR.sqrt();
            let n_obs: usize = evaluated
                .iter()
                .map(|r| r.fold.test_end - r.fold.test_start)
                .sum();
            // Єдина гіпотеза — очікуваний максимум нульовий, розкид не потрібен.
            let disp = trial_dispersion.unwrap_or(0.0);
            metrics::deflated_sharpe_ratio(&DeflatedSharpeInput {
                sharpe: sr_period,
                n_observations: n_obs.max(2),
                n_trials,
                sharpe_variance: disp * disp / metrics::TRADING_DAYS_PER_YEAR,
                skewness: 0.0,
                kurtosis: 3.0,
            })
        });

        Self {
            folds,
            median_oos_sharpe,
            median_oos_return_pct,
            deflated_sharpe,
            n_trials,
            trial_dispersion,
            evaluated_folds,
            empty_folds,
            positive_folds,
        }
    }
}

// ── GATE M2.1 ───────────────────────────────────────────────────────────────

/// Конвенційний поріг deflated Sharpe (Bailey–López de Prado). Використовується
/// ЛИШЕ для позначки ✓/⚠ у звіті — гейтом M2.1 він не є.
///
/// Чому не є: 0.95 недосяжний для явища силою Sharpe 0.4–0.8 на наявному
/// обсязі даних. Навіть за ідеальної стабільності між фолдами Sharpe 0.8
/// вимагає ~1066 OOS-днів (4.2 роки), а Sharpe 0.6 — близько 1900 днів;
/// у нас їх ~650. Гейт, який вимагає від інструмента втричі більше, ніж дає
/// явище, що ним вимірюють, не відсіює шум — він просто ніколи не світиться.
pub const DSR_SIGNIFICANCE_THRESHOLD: f64 = 0.95;

/// Вердикт гейта M2.1 (валідація движка на TSMOM).
///
/// Гейт питає три структурні речі, які на наявних даних перевірити МОЖНА:
/// чи движок бачить документований ефект, чи він бачив його на всьому
/// періоді, і чи результат не тримається на одному вдалому відрізку.
/// Deflated Sharpe друкується поруч як окреме число — воно інформує, але
/// не вирішує (див. `DSR_SIGNIFICANCE_THRESHOLD`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateVerdict {
    /// Усі три структурні умови виконані.
    Passed,
    /// Медіани нема: жоден OOS-фолд не містив угод. Не «провалено» — саме
    /// неоцінено, бо міряти нема чого.
    NotEvaluated,
    /// Є фолд, де стратегія не зробила жодної угоди. Найпідступніший випадок:
    /// медіана по решті може виглядати чудово, але міряли ми не весь період,
    /// а лише ті вікна, де стратегія була зряча (саме так довга-тільки
    /// конфігурація ховала ведмежий ринок 2026-02…07).
    BlindWindow,
    /// Sharpe ≤ 0 — движок не відтворює ефект. Kill-критерій «шукай баг».
    EngineFailure,
    /// Sharpe додатний, але нижчий за документований діапазон.
    BelowRange,
    /// Більшість оцінених фолдів не в плюсі: медіану витягнув один-два
    /// вдалі відрізки.
    Unstable,
}

impl GateVerdict {
    /// Тільки `Passed` відкриває Фазу 3. Усе інше — блокує.
    pub fn is_passed(self) -> bool {
        matches!(self, GateVerdict::Passed)
    }
}

/// Оцінює гейт M2.1 за зведенням walk-forward.
///
/// Порядок перевірок — від «міряли не те» до «зміряли, але слабо», щоб
/// повідомлення вказувало на справжню причину. Сліпе вікно йде першим:
/// поки воно є, будь-яка медіана описує неповний період і довіряти їй
/// не можна незалежно від значення.
pub fn evaluate_tsmom_gate(summary: &WalkForwardSummary) -> GateVerdict {
    let Some(med) = summary.median_oos_sharpe else {
        return GateVerdict::NotEvaluated;
    };
    if summary.empty_folds > 0 {
        return GateVerdict::BlindWindow;
    }
    if med <= Decimal::ZERO {
        return GateVerdict::EngineFailure;
    }
    // Нижня межа документованого TSMOM-ефекту (Moskowitz–Ooi–Pedersen).
    if med < dec!(0.4) {
        return GateVerdict::BelowRange;
    }
    // Строга більшість оцінених фолдів має бути в плюсі.
    if summary.positive_folds * 2 <= summary.evaluated_folds {
        return GateVerdict::Unstable;
    }
    GateVerdict::Passed
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
    /// `prior_trial_sharpes` — Sharpe раніше перебраних конфігурацій тієї ж
    /// стратегії з логу прогонів, вхід для deflated Sharpe (M0.4 → M1.3).
    pub async fn run(
        &self,
        strategy_factory: &dyn Fn() -> Box<dyn AllocationStrategy>,
        cfg: &WalkForwardConfig,
        prior_trial_sharpes: &[f64],
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

        Ok(WalkForwardSummary::from_folds(results, prior_trial_sharpes))
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

    fn fold_result(
        idx: usize,
        sharpe: Decimal,
        ret: Decimal,
        trades: usize,
    ) -> WalkForwardFoldResult {
        WalkForwardFoldResult {
            fold: WalkForwardFold {
                train_start: 0,
                train_end: 1500,
                test_start: 1500 + idx * 163,
                test_end: 1500 + (idx + 1) * 163,
            },
            oos_return_pct: ret,
            oos_sharpe: sharpe,
            oos_max_drawdown_pct: Decimal::ZERO,
            oos_trades: trades,
            oos_total_costs: Decimal::ZERO,
        }
    }

    // Регресія на реальний прогін 2026-07-27: TSMOM дав фолди
    // [0.878 (34 угоди), 1.384 (26), −1.121 (16), 0 (БЕЗ УГОД)], і медіана
    // по всіх чотирьох = 0.439 — рівно поріг гейта M2.1, наполовину зліплений
    // із порожнього фолда. Правильна відповідь — медіана трьох оцінених.
    #[test]
    fn fold_without_trades_is_excluded_from_medians() {
        let folds = vec![
            fold_result(0, dec!(0.877563749521359), dec!(4.745), 34),
            fold_result(1, dec!(1.383553), dec!(7.439), 26),
            fold_result(2, dec!(-1.121040), dec!(-4.117), 16),
            fold_result(3, Decimal::ZERO, Decimal::ZERO, 0),
        ];
        let s = WalkForwardSummary::from_folds(folds, &[]);

        assert_eq!(s.evaluated_folds, 3);
        assert_eq!(s.empty_folds, 1);
        assert_eq!(s.folds.len(), 4, "усі фолди лишаються у звіті для аудиту");
        assert_eq!(
            s.median_oos_sharpe,
            Some(dec!(0.877563749521359)),
            "медіана мусить бути по фолдах з угодами, а не (0 + 0.878)/2"
        );
        assert_eq!(s.median_oos_return_pct, Some(dec!(4.745)));
    }

    // Дзеркальний бік: без угод у жодному фолді немає й гейта. Нуль тут
    // означав би «виміряли рівно нульовий Sharpe», чого не було.
    #[test]
    fn all_folds_without_trades_give_no_median() {
        let folds = vec![
            fold_result(0, Decimal::ZERO, Decimal::ZERO, 0),
            fold_result(1, Decimal::ZERO, Decimal::ZERO, 0),
        ];
        let s = WalkForwardSummary::from_folds(folds, &[]);

        assert_eq!(s.evaluated_folds, 0);
        assert_eq!(s.empty_folds, 2);
        assert_eq!(s.median_oos_sharpe, None);
        assert_eq!(s.median_oos_return_pct, None);
        assert_eq!(s.deflated_sharpe, None);
    }

    // Порожній фолд не має підмішуватись і в deflated Sharpe: він додавав би
    // спостережень, яких не було.
    #[test]
    fn empty_fold_does_not_change_deflated_sharpe() {
        let evaluated = vec![
            fold_result(0, dec!(1.5), dec!(6.0), 20),
            fold_result(1, dec!(0.5), dec!(2.0), 18),
            fold_result(2, dec!(1.0), dec!(4.0), 22),
        ];
        let mut with_empty = evaluated.clone();
        with_empty.push(fold_result(3, Decimal::ZERO, Decimal::ZERO, 0));

        let a = WalkForwardSummary::from_folds(evaluated, &[0.4, 0.9]);
        let b = WalkForwardSummary::from_folds(with_empty, &[0.4, 0.9]);

        assert_eq!(a.median_oos_sharpe, b.median_oos_sharpe);
        assert_eq!(a.deflated_sharpe, b.deflated_sharpe);
    }

    // ── M1.3: розкид береться по СПРОБАХ, а не по фолдах ────────────────────

    // Ядро виправлення. Два зведення з ідентичними фолдами, але різною
    // історією перебору мусять давати різну DSR — раніше історія не впливала
    // взагалі, бо σ рахувалась по фолдах цього ж прогону.
    #[test]
    fn deflated_sharpe_reacts_to_trial_history_not_folds() {
        let folds = || {
            vec![
                fold_result(0, dec!(0.6), dec!(3.0), 20),
                fold_result(1, dec!(0.7), dec!(3.5), 22),
                fold_result(2, dec!(0.5), dec!(2.5), 18),
            ]
        };
        // Спроби купчаться біля результату — очікуваний максимум шуму низький.
        let tight = WalkForwardSummary::from_folds(folds(), &[0.55, 0.62, 0.58]);
        // Ті самі три спроби, але розкидані — максимум шуму вищий, DSR нижча.
        let wide = WalkForwardSummary::from_folds(folds(), &[-1.4, 2.1, 0.05]);

        assert_eq!(tight.median_oos_sharpe, wide.median_oos_sharpe);
        assert_eq!(tight.n_trials, 4, "три попередні + поточна");
        assert!(
            tight.trial_dispersion.unwrap() < wide.trial_dispersion.unwrap(),
            "розкид має відображати історію спроб"
        );
        assert!(
            tight.deflated_sharpe.unwrap() > wide.deflated_sharpe.unwrap(),
            "ширший перебір мусить сильніше карати той самий результат"
        );
    }

    // Поточний прогін — теж гіпотеза, і за неї теж треба платити.
    #[test]
    fn current_run_counts_as_a_trial() {
        let s = WalkForwardSummary::from_folds(
            vec![fold_result(0, dec!(0.6), dec!(3.0), 20)],
            &[0.5, 0.4],
        );
        assert_eq!(s.n_trials, 3);
    }

    // Перша спроба: очікуваний максимум шуму нульовий, розкид не потрібен,
    // DSR має бути порахована (а не None, як раніше при <2 фолдах).
    #[test]
    fn single_trial_still_yields_deflated_sharpe() {
        let s = WalkForwardSummary::from_folds(
            vec![
                fold_result(0, dec!(1.2), dec!(5.0), 20),
                fold_result(1, dec!(1.1), dec!(4.5), 19),
            ],
            &[],
        );
        assert_eq!(s.n_trials, 1);
        assert_eq!(s.trial_dispersion, None);
        assert!(s.deflated_sharpe.is_some());
    }

    // ── Гейт M2.1: три структурні умови ─────────────────────────────────────

    fn summary_with(
        median: Option<Decimal>,
        evaluated: usize,
        empty: usize,
        positive: usize,
    ) -> WalkForwardSummary {
        WalkForwardSummary {
            folds: Vec::new(),
            median_oos_sharpe: median,
            median_oos_return_pct: median,
            deflated_sharpe: Some(0.10),
            n_trials: 4,
            trial_dispersion: Some(0.45),
            evaluated_folds: evaluated,
            empty_folds: empty,
            positive_folds: positive,
        }
    }

    // Реальний крипто-прогін лонг+шорт: фолди −0.67 / 1.63 / 0.64 / 0.65,
    // медіана 0.642, жодного порожнього, 3 з 4 у плюсі. Слабка DSR (0.10)
    // більше не блокує — вона недосяжна за наявного обсягу даних.
    #[test]
    fn structurally_sound_run_passes_despite_low_dsr() {
        let v = evaluate_tsmom_gate(&summary_with(Some(dec!(0.642)), 4, 0, 3));
        assert_eq!(v, GateVerdict::Passed);
        assert!(v.is_passed());
    }

    // Довга-тільки конфігурація: медіана 1.081 виглядає чудово, але четвертий
    // відрізок порожній — міряли не весь період. Саме це нас і обдурило.
    #[test]
    fn blind_window_blocks_even_with_high_median() {
        let v = evaluate_tsmom_gate(&summary_with(Some(dec!(1.081)), 3, 1, 2));
        assert_eq!(v, GateVerdict::BlindWindow);
        assert!(!v.is_passed());
    }

    // Медіана в діапазоні, але половина відрізків збиткова — витягнув один.
    #[test]
    fn majority_of_folds_must_be_positive() {
        let v = evaluate_tsmom_gate(&summary_with(Some(dec!(0.5)), 4, 0, 2));
        assert_eq!(v, GateVerdict::Unstable);

        let ok = evaluate_tsmom_gate(&summary_with(Some(dec!(0.5)), 3, 0, 2));
        assert_eq!(ok, GateVerdict::Passed, "2 з 3 — строга більшість");
    }

    // Валютний кошик: медіана −0.049 → kill-критерій «шукай баг у движку».
    #[test]
    fn non_positive_median_is_engine_failure() {
        let v = evaluate_tsmom_gate(&summary_with(Some(dec!(-0.049)), 4, 0, 1));
        assert_eq!(v, GateVerdict::EngineFailure);

        let below = evaluate_tsmom_gate(&summary_with(Some(dec!(0.2)), 4, 0, 4));
        assert_eq!(below, GateVerdict::BelowRange);
    }

    // Порожній walk-forward — неоцінено, а не провалено.
    #[test]
    fn no_median_gives_not_evaluated() {
        let v = evaluate_tsmom_gate(&summary_with(None, 0, 4, 0));
        assert_eq!(v, GateVerdict::NotEvaluated);
        assert!(!v.is_passed());
    }
}
