//! Крос-секційний funding carry на широкому кошику перпів (M3.2-стиль).

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

use super::{log_run, QuantArgs};
use crate::data_ingestion::adapters::funding_csv::CsvFundingAdapter;
use crate::data_ingestion::ports::funding::FundingDataPort;
use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use crate::backtest::domain::funding_carry::{FundingCarryBacktest, XsCarryConfig};
use crate::trading::domain::costs::cost_model_from_config;
use crate::trading::ports::BrokerSimulatorPort;

/// Пункт 2 плану: крос-секційний carry на широкому кошику перпів.
pub async fn run_xs_carry(args: &QuantArgs) -> Result<()> {
    use std::collections::BTreeMap;

    let dir = args
        .funding_dir
        .clone()
        .context("--funding-dir required for xs_carry")?;

    let mut universe: BTreeMap<String, Vec<crate::data_ingestion::domain::funding::FundingRatePoint>> =
        BTreeMap::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        let adapter = CsvFundingAdapter::new(&path);
        let series = adapter
            .funding_history(
                &sym,
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
                Utc::now(),
            )
            .await?;
        if series.len() >= 200 {
            universe.insert(sym, series);
        }
    }
    // Без спот-ноги carry-угоди не існує — фільтруємо ДО ранжування, інакше
    // топ заповнять нехеджовані перпи (саме це й сталось: 10 з 10 у кошику
    // тіньового журналу за 2026-08-02).
    super::universe::apply_spot_filter(&mut universe, &args.config.universe, "xs_carry")?;
    anyhow::ensure!(universe.len() >= 5, "need ≥5 symbols, got {}", universe.len());
    println!(
        "XS carry universe: {} перпів (survivorship-застереження: тільки ЖИВІ symbols з API)",
        universe.len()
    );

    // IS/OOS: розріз по медіанному часу всієї вибірки.
    let mut all_ts: Vec<chrono::DateTime<Utc>> = universe
        .values()
        .flat_map(|s| s.iter().map(|p| p.timestamp))
        .collect();
    all_ts.sort();
    let mid = all_ts[all_ts.len() / 2];
    let split_universe = |before: bool| -> BTreeMap<String, Vec<_>> {
        universe
            .iter()
            .map(|(k, v)| {
                let part: Vec<_> = v
                    .iter()
                    .filter(|p| (p.timestamp <= mid) == before)
                    .cloned()
                    .collect();
                (k.clone(), part)
            })
            .filter(|(_, v)| v.len() >= 100)
            .collect()
    };
    let is_universe = split_universe(true);
    let oos_universe = split_universe(false);

    let defaults = XsCarryConfig::default();
    let xs_cfg = XsCarryConfig {
        top_k: args.top_k,
        trailing_intervals: args.xs_trailing.unwrap_or(defaults.trailing_intervals),
        rebalance_intervals: args.xs_rebalance.unwrap_or(defaults.rebalance_intervals),
        ..defaults
    };
    println!(
        "xs-параметри: trailing={} інтервалів, ребаланс кожні {} інтервалів, банда {}",
        xs_cfg.trailing_intervals, xs_cfg.rebalance_intervals, xs_cfg.trade_band
    );

    // Дві сітки витрат: реалістичний taker і maker (лімітні ордери).
    let scenarios: Vec<(&str, Decimal, Decimal)> = vec![
        ("taker 0.045%+спред", dec!(0.00045), dec!(0.0002)),
        ("maker 0.018%", dec!(0.00018), dec!(0)),
    ];

    println!(
        "\n=== Cross-Sectional Funding Carry: top-{} з {} перпів, ребаланс раз на тиждень ===",
        xs_cfg.top_k,
        universe.len()
    );
    println!("Розріз IS/OOS: {}", mid);
    let mut logged = Vec::new();
    for (label, commission_pct, spread_pct) in &scenarios {
        let mut costs = args.config.costs.clone();
        costs.commission_pct = *commission_pct;
        costs.spread_pct = *spread_pct;
        costs.impact = None; // funding-серії без обсягів → impact не моделюємо
        let broker = Arc::new(SimpleBrokerSimulator {
            slippage_pct: Decimal::ZERO,
            cost_model: cost_model_from_config(&costs),
        }) as Arc<dyn BrokerSimulatorPort>;
        let bt = FundingCarryBacktest::new(broker);
        let is_report = bt
            .run_cross_sectional(&is_universe, args.capital, &xs_cfg)
            .await?;
        let oos_report = bt
            .run_cross_sectional(&oos_universe, args.capital, &xs_cfg)
            .await?;

        println!("\n--- Сценарій витрат: {label} ---");
        for (tag, r) in [("IS ", &is_report), ("OOS", &oos_report)] {
            println!(
                "{tag}: net={:.2} funding={:.2} costs={:.2} price={:.2} sharpe={:.3} mdd%={:.2} rebalances={} symbols={}",
                r.net_pnl, r.funding_pnl, r.total_costs, r.price_pnl, r.sharpe,
                r.max_drawdown_pct, r.rebalances, r.symbols_traded
            );
        }
        if oos_report.passes_kill_criterion() {
            println!("KILL-КРИТЕРІЙ (≥1.0 для кошика): пройдено ✓");
        } else {
            println!(
                "KILL-КРИТЕРІЙ (≥1.0 для кошика): НЕ пройдено (OOS Sharpe {:.3})",
                oos_report.sharpe
            );
        }
        logged.push(serde_json::json!({
            "scenario": label,
            "is": serde_json::to_value(&is_report)?,
            "oos": serde_json::to_value(&oos_report)?,
            "passes": oos_report.passes_kill_criterion(),
        }));
    }

    log_run(args, serde_json::json!({
        "strategy": "xs_funding_carry",
        "universe_size": universe.len(),
        "top_k": xs_cfg.top_k,
        "scenarios": logged,
        "caveat": "universe = live symbols only (survivorship bias присутній)",
    }))
    .await;
    Ok(())
}
