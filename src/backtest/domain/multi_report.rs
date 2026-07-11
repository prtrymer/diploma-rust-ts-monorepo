//! Порівняльний звіт: рядок стратегії + рядки бенчмарків поруч (M0.2).
//!
//! Без baseline цифра PnL висить у повітрі (пряме зауваження з захисту) —
//! тому рендер завжди містить BuyAndHold / EqualWeight / 60-40 за той самий
//! період, ті самі символи, той самий движок і ту саму модель витрат.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::backtest::domain::metrics::InstrumentMetrics;
use crate::backtest::domain::portfolio_engine::PortfolioRunResult;
use crate::backtest::domain::report::BacktestReport;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySummaryRow {
    pub name: String,
    pub report: BacktestReport,
    pub median_sharpe: Option<Decimal>,
    pub instruments: Vec<InstrumentMetrics>,
}

impl From<&PortfolioRunResult> for StrategySummaryRow {
    fn from(r: &PortfolioRunResult) -> Self {
        Self {
            name: r.strategy_name.clone(),
            report: r.report.clone(),
            median_sharpe: r.median_sharpe,
            instruments: r.instruments.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparativeReport {
    pub strategy: StrategySummaryRow,
    pub benchmarks: Vec<StrategySummaryRow>,
}

impl ComparativeReport {
    /// Табличний рендер: перший рядок — стратегія, далі бенчмарки.
    /// Зведення веде з медіанного Sharpe (M0.3), net-of-cost PnL (M0.1).
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "{:<22} {:>10} {:>10} {:>9} {:>9} {:>8} {:>9} {:>9} {:>7}\n",
            "Strategy",
            "NetRet%",
            "GrossRet%",
            "Costs",
            "Sharpe",
            "MedSh",
            "MDD%",
            "Turnover",
            "Trades"
        ));
        out.push_str(&"-".repeat(100));
        out.push('\n');
        out.push_str(&render_row(&self.strategy));
        for b in &self.benchmarks {
            out.push_str(&render_row(b));
        }
        // Low-sample інструменти — явний прапорець (M0.3).
        let low: Vec<&InstrumentMetrics> = self
            .strategy
            .instruments
            .iter()
            .filter(|i| i.low_sample)
            .collect();
        if !low.is_empty() {
            out.push_str("\nlow-sample інструменти (не входять у медіану): ");
            out.push_str(
                &low.iter()
                    .map(|i| format!("{} ({} угод)", i.symbol, i.trades))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            out.push('\n');
        }
        out
    }
}

fn render_row(row: &StrategySummaryRow) -> String {
    format!(
        "{:<22} {:>10.2} {:>10.2} {:>9.2} {:>9.2} {:>8} {:>9.2} {:>9.2} {:>7}\n",
        row.name,
        row.report.total_return_pct,
        row.report.gross_return_pct,
        row.report.total_costs,
        row.report.sharpe_ratio,
        row.median_sharpe
            .map(|m| format!("{m:.2}"))
            .unwrap_or_else(|| "N/A".into()),
        row.report.max_drawdown_pct,
        row.report.turnover,
        row.report.total_trades
    )
}
