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
use std::path::PathBuf;

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
}

pub async fn log_run(args: &QuantArgs, metrics: serde_json::Value) {
    let record = RunRecord::new(
        args.config.config_hash(),
        args.config.canonical_json(),
        metrics,
    );
    let logger = FileRunLogger::new("runs/runs.jsonl");
    match logger.log_run(&record).await {
        Ok(()) => println!(
            "Run {} logged to runs/runs.jsonl (config {})",
            record.run_id,
            &record.config_hash[..12]
        ),
        Err(e) => eprintln!("run log failed: {e}"),
    }
}
