//! Funding-rate carry на одному символі (M3.1) + cost-sweep сітка.

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

use super::{log_run, QuantArgs};
use crate::data_ingestion::adapters::funding_csv::CsvFundingAdapter;
use crate::data_ingestion::ports::funding::FundingDataPort;
use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use crate::trading::adapters::funding_carry::{FundingCarryBacktest, FundingCarryConfig};
use crate::trading::domain::costs::cost_model_from_config;
use crate::trading::ports::BrokerSimulatorPort;

pub async fn run_carry(args: &QuantArgs) -> Result<()> {
    let path = args
        .funding_csv
        .clone()
        .context("--funding-csv required for carry strategy")?;
    let adapter = CsvFundingAdapter::new(path);
    let series = adapter
        .funding_history(
            &args.symbol,
            Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
            Utc::now(),
        )
        .await?;
    anyhow::ensure!(!series.is_empty(), "no funding points for {}", args.symbol);

    // OOS-дисципліна: друга половина серії — out-of-sample.
    let split = series.len() / 2;
    let cfg = FundingCarryConfig::default();

    if args.cost_sweep {
        // Пункт 1 плану: та сама стратегія при різних комісіях за ногу.
        // Реальні орієнтири Binance: 0.018% futures-maker, 0.045% futures-taker,
        // 0.075–0.1% спот. Спред: maker-сценарії (≤0.02%) не перетинають
        // спред → 0; taker платить половину повного.
        let scenarios: Vec<(&str, Decimal, Decimal)> = vec![
            ("0.000% (нуль, діагностика)", dec!(0), dec!(0)),
            ("0.010% (VIP-maker)", dec!(0.0001), dec!(0)),
            ("0.018% (futures maker)", dec!(0.00018), dec!(0)),
            ("0.030% (maker+спот бленд)", dec!(0.0003), dec!(0)),
            ("0.045% (futures taker)", dec!(0.00045), dec!(0.0002)),
            ("0.050% (поточний дефолт)", dec!(0.0005), dec!(0.0004)),
            ("0.100% (стрес/спот-taker)", dec!(0.001), dec!(0.0004)),
        ];
        println!(
            "\n=== Cost sweep: Funding Carry ({}) — комісія за НОГУ ===",
            args.symbol
        );
        println!(
            "{:<28} {:>12} {:>12} {:>12} {:>9} {:>9}  вердикт",
            "Сценарій", "OOS net", "OOS funding", "OOS costs", "OOS Shp", "IS Shp"
        );
        println!("{}", "-".repeat(100));
        let mut sweep_results = Vec::new();
        for (label, commission_pct, spread_pct) in &scenarios {
            let mut costs = args.config.costs.clone();
            costs.commission_pct = *commission_pct;
            costs.spread_pct = *spread_pct;
            // Funding-серії не мають даних обсягу — market impact без них
            // вигаданий (дефолт у штуках вибухає на дешевих монетах).
            costs.impact = None;
            if *commission_pct == Decimal::ZERO {
                costs = crate::shared::run_config::CostConfig::zero();
            }
            let broker = Arc::new(SimpleBrokerSimulator {
                slippage_pct: Decimal::ZERO,
                cost_model: cost_model_from_config(&costs),
            }) as Arc<dyn BrokerSimulatorPort>;
            let bt = FundingCarryBacktest::new(broker);
            let is_report = bt.run(&series[..split], args.capital, &cfg).await?;
            let oos_report = bt.run(&series[split..], args.capital, &cfg).await?;
            let verdict = if oos_report.passes_kill_criterion() {
                "✓ живий"
            } else {
                "✗ мертвий"
            };
            println!(
                "{:<28} {:>12.2} {:>12.2} {:>12.2} {:>9.3} {:>9.3}  {}",
                label,
                oos_report.net_pnl,
                oos_report.funding_pnl,
                oos_report.total_costs,
                oos_report.sharpe,
                is_report.sharpe,
                verdict
            );
            sweep_results.push(serde_json::json!({
                "scenario": label,
                "commission_pct": commission_pct.to_string(),
                "oos_sharpe": oos_report.sharpe.to_string(),
                "oos_net": oos_report.net_pnl.to_string(),
                "passes": oos_report.passes_kill_criterion(),
            }));
        }
        log_run(args, serde_json::json!({
            "strategy": "funding_carry_cost_sweep",
            "symbol": args.symbol,
            "scenarios": sweep_results,
        }))
        .await;
        return Ok(());
    }

    let mut single_costs = args.config.costs.clone();
    single_costs.impact = None; // без volume-даних impact не моделюємо
    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: Decimal::ZERO,
        cost_model: cost_model_from_config(&single_costs),
    }) as Arc<dyn BrokerSimulatorPort>;
    let bt = FundingCarryBacktest::new(broker);
    let is_report = bt.run(&series[..split], args.capital, &cfg).await?;
    let oos_report = bt.run(&series[split..], args.capital, &cfg).await?;

    println!("\n=== Funding Carry ({}) — net-of-cost ===", args.symbol);
    for (label, r) in [("IS ", &is_report), ("OOS", &oos_report)] {
        println!(
            "{label}: net={} funding={} costs={} price={} sharpe={} mdd%={} rebalances={} hedged={}",
            r.net_pnl, r.funding_pnl, r.total_costs, r.price_pnl, r.sharpe, r.max_drawdown_pct,
            r.rebalances, r.hedged
        );
    }
    // Kill-критерій M3.1.
    if oos_report.passes_kill_criterion() {
        println!("KILL-КРИТЕРІЙ: пройдено (OOS Sharpe ≥ 0.5) — напрям живий.");
    } else {
        println!(
            "KILL-КРИТЕРІЙ: НЕ пройдено (OOS Sharpe {} < 0.5) — відкладай напрям.",
            oos_report.sharpe
        );
    }

    log_run(args, serde_json::json!({
        "strategy": "funding_carry",
        "symbol": args.symbol,
        "is": serde_json::to_value(&is_report)?,
        "oos": serde_json::to_value(&oos_report)?,
        "kill_criterion_passed": oos_report.passes_kill_criterion(),
    }))
    .await;
    Ok(())
}
