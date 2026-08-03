//! Мультиактивний портфельний бектест (application-шар quant-CLI).
//!
//! Уся логіка раннерів `quant-backtest` живе тут, у бібліотеці; бінарник
//! (`src/bin/quant_backtest.rs`) — лише парсинг аргументів і диспетчеризація.
//! Модулі: дані (CSV), фабрика стратегій, carry / xs_carry / ml_match /
//! funding_ml / shadow_carry раннери, спільний run-лог (M0.4).

pub mod carry;
pub mod data;
pub mod funding_ml;
pub mod ml_match;
pub mod shadow_carry;
pub mod strategies;
pub mod universe;
pub mod xs_carry;

pub use carry::run_carry;
pub use data::load_csv_dir;
pub use funding_ml::run_funding_ml;
pub use ml_match::run_ml_match;
pub use shadow_carry::run_shadow_carry;
pub use strategies::build_strategy;
pub use xs_carry::run_xs_carry;

use crate::database::adapters::run_logger_file::FileRunLogger;
use crate::database::ports::run_logger::{RunLogger, RunRecord};
use crate::shared::run_config::RunConfig;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::path::PathBuf;

/// Сітка витрат carry-прогонів.
///
/// Спільна для `xs_carry` і `shadow_carry` навмисно: критерії рішення
/// (`shadow/DECISION_CRITERIA.md`, v2) порівнюють net тіньового журналу з
/// бектестом, і якщо ці дві сітки розійдуться, порівняння стане неправдою,
/// не подавши жодного сигналу. Який зі сценаріїв реалістичний — питання
/// відкрите (maker-філ на ребалансі не гарантований, див.
/// `docs/xs-carry-sharpe-audit.md`), тому обидва рахуються й звітуються поруч.
pub struct CostScenario {
    /// Короткий ключ у JSON (`maker` / `taker`).
    pub key: &'static str,
    /// Підпис для друку.
    pub label: &'static str,
    pub commission_pct: Decimal,
    pub spread_pct: Decimal,
}

impl CostScenario {
    /// Витрати на одиницю ТОРГОВАНОГО нотіоналу: комісія + півспреду.
    /// Половина, а не весь: сторона платить свою половину — та сама угода,
    /// що в `SimpleCommissionSpread` (`spread_component_charges_half_spread`).
    pub fn per_notional(&self) -> Decimal {
        self.commission_pct + self.spread_pct / Decimal::TWO
    }
}

pub fn cost_scenarios() -> Vec<CostScenario> {
    vec![
        CostScenario {
            key: "taker",
            label: "taker 0.045%+спред",
            commission_pct: dec!(0.00045),
            spread_pct: dec!(0.0002),
        },
        CostScenario {
            key: "maker",
            label: "maker 0.018%",
            commission_pct: dec!(0.00018),
            spread_pct: dec!(0),
        },
    ]
}

/// Параметри прогону quant-CLI (заповнюються з аргументів командного рядка).
pub struct QuantArgs {
    pub data_dir: Option<PathBuf>,
    pub funding_csv: Option<PathBuf>,
    pub funding_dir: Option<PathBuf>,
    pub strategy: String,
    pub symbol: String,
    pub capital: Decimal,
    pub walk_forward: bool,
    pub config: RunConfig,
    pub purged_cv: bool,
    /// Сітка чутливості до комісій (пункт 1 плану досліджень).
    pub cost_sweep: bool,
    pub top_k: usize,
    pub xs_trailing: Option<usize>,
    pub xs_rebalance: Option<usize>,
    /// Куди пишеться провенанс прогону і звідки читається лічильник trials
    /// для deflated Sharpe (M0.4 → M1.3). Не константа, бо фікстурні прогони
    /// мусять судитися за власною історією гіпотез, а не за дослідницькою:
    /// n_trials росте з часом і інакше робив би CI червоним на рівному місці.
    pub runs_log: PathBuf,
}

impl QuantArgs {
    /// Дефолтний журнал прогонів дослідницького пайплайну.
    pub const DEFAULT_RUNS_LOG: &'static str = "runs/runs.jsonl";

    /// Юніверс прогону одним рядком — пишеться в metrics і розрізняє спроби,
    /// які мають однаковий config_hash (див. `RunLogger::trial_sharpes`).
    pub fn dataset_label(&self) -> String {
        self.data_dir
            .as_ref()
            .or(self.funding_dir.as_ref())
            .or(self.funding_csv.as_ref())
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| self.symbol.clone())
    }
}

pub async fn log_run(args: &QuantArgs, metrics: serde_json::Value) {
    let record = RunRecord::new(
        args.config.config_hash(),
        args.config.canonical_json(),
        metrics,
    );
    let path = args.runs_log.display().to_string();
    let logger = FileRunLogger::new(&args.runs_log);
    match logger.log_run(&record).await {
        Ok(()) => println!(
            "Run {} logged to {path} (config {})",
            record.run_id,
            &record.config_hash[..12]
        ),
        Err(e) => eprintln!("run log failed: {e}"),
    }
}
