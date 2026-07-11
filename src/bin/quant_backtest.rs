//! Мультиактивний портфельний бектест-runner (M0.2 / M1.1 / M2.1 / M3.1 / M3.2).
//!
//! Офлайн-джерело даних: директорія CSV (по файлу на символ:
//! `timestamp,close[,volume]` або `date,...`). Кожен прогін:
//!   - стратегія + ВСІ бенчмарки тим самим движком і моделлю витрат (M0.2);
//!   - зведення з медіанного Sharpe і net-of-cost PnL (M0.3, інваріант 2);
//!   - walk-forward OOS + deflated Sharpe за бажанням (M1.1, M1.3);
//!   - provenance: SHA-256 конфіга + лог у runs/runs.jsonl (M0.4).
//!
//! Приклади:
//!   quant-backtest --data-dir datasets/daily --strategy tsmom --walk-forward
//!   quant-backtest --data-dir datasets/daily --strategy xsmom
//!   quant-backtest --funding-csv datasets/funding.csv --strategy carry --symbol BTCUSDT

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use db_con::backtest::application::benchmark_runner::BenchmarkRunner;
use db_con::backtest::application::walk_forward::WalkForwardRunner;
use db_con::backtest::domain::purged_cv::{combinatorial_purged_folds, PurgedCvConfig};
use db_con::data_ingestion::adapters::funding_csv::CsvFundingAdapter;
use db_con::data_ingestion::ports::funding::FundingDataPort;
use db_con::database::adapters::run_logger_file::FileRunLogger;
use db_con::database::ports::run_logger::{RunLogger, RunRecord};
use db_con::shared::run_config::RunConfig;
use db_con::trading::adapters::benchmark_strategies::{BuyAndHold, EqualWeight, SixtyForty};
use db_con::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use db_con::trading::adapters::cross_sectional_momentum::CrossSectionalMomentum;
use db_con::trading::adapters::funding_carry::{
    FundingCarryBacktest, FundingCarryConfig, XsCarryConfig,
};
use db_con::trading::adapters::portfolio_manager::PortfolioManager;
use db_con::trading::adapters::tsmom_strategy::TsmomStrategy;
use db_con::trading::domain::allocation::{AllocationStrategy, AlignedMarketData, UniverseView};
use db_con::trading::domain::costs::cost_model_from_config;
use db_con::trading::ports::{BrokerSimulatorPort, PortfolioPort};

struct Args {
    data_dir: Option<PathBuf>,
    funding_csv: Option<PathBuf>,
    funding_dir: Option<PathBuf>,
    strategy: String,
    symbol: String,
    capital: Decimal,
    walk_forward: bool,
    config: RunConfig,
    purged_cv: bool,
    /// Сітка чутливості до комісій (пункт 1 плану досліджень).
    cost_sweep: bool,
    top_k: usize,
    xs_trailing: Option<usize>,
    xs_rebalance: Option<usize>,
}

fn parse_args() -> Result<Args> {
    let argv: Vec<String> = env::args().collect();
    let mut data_dir = None;
    let mut funding_csv = None;
    let mut funding_dir = None;
    let mut strategy = "tsmom".to_string();
    let mut symbol = "BTCUSDT".to_string();
    let mut capital = dec!(100000);
    let mut walk_forward = false;
    let mut purged_cv = false;
    let mut cost_sweep = false;
    let mut top_k = 10usize;
    let mut xs_trailing: Option<usize> = None;
    let mut xs_rebalance: Option<usize> = None;
    let mut config = RunConfig::default();

    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--data-dir" => {
                data_dir = argv.get(i + 1).map(PathBuf::from);
                i += 2;
            }
            "--funding-csv" => {
                funding_csv = argv.get(i + 1).map(PathBuf::from);
                i += 2;
            }
            "--funding-dir" => {
                funding_dir = argv.get(i + 1).map(PathBuf::from);
                i += 2;
            }
            "--cost-sweep" => {
                cost_sweep = true;
                i += 1;
            }
            "--no-impact" => {
                config.costs.impact = None;
                i += 1;
            }
            "--allow-short" => {
                config.strategy.long_only = false;
                i += 1;
            }
            "--top-k" => {
                if let Some(v) = argv.get(i + 1) {
                    top_k = v.parse().unwrap_or(top_k);
                }
                i += 2;
            }
            "--xs-trailing" => {
                if let Some(v) = argv.get(i + 1) {
                    xs_trailing = v.parse().ok();
                }
                i += 2;
            }
            "--xs-rebalance" => {
                if let Some(v) = argv.get(i + 1) {
                    xs_rebalance = v.parse().ok();
                }
                i += 2;
            }
            "--commission-pct" => {
                if let Some(v) = argv.get(i + 1) {
                    config.costs.commission_pct =
                        v.parse().unwrap_or(config.costs.commission_pct);
                }
                i += 2;
            }
            "--spread-pct" => {
                if let Some(v) = argv.get(i + 1) {
                    config.costs.spread_pct = v.parse().unwrap_or(config.costs.spread_pct);
                }
                i += 2;
            }
            "--lookback" => {
                if let Some(v) = argv.get(i + 1) {
                    config.strategy.momentum_lookback =
                        v.parse().unwrap_or(config.strategy.momentum_lookback);
                }
                i += 2;
            }
            "--min-confidence" => {
                if let Some(v) = argv.get(i + 1) {
                    config.strategy.min_confidence =
                        v.parse().unwrap_or(config.strategy.min_confidence);
                }
                i += 2;
            }
            "--strategy" => {
                strategy = argv.get(i + 1).cloned().unwrap_or(strategy);
                i += 2;
            }
            "--symbol" => {
                symbol = argv.get(i + 1).cloned().unwrap_or(symbol);
                i += 2;
            }
            "--capital" => {
                if let Some(v) = argv.get(i + 1) {
                    capital = v.parse().unwrap_or(capital);
                }
                i += 2;
            }
            "--walk-forward" => {
                walk_forward = true;
                i += 1;
            }
            "--purged-cv" => {
                purged_cv = true;
                i += 1;
            }
            "--config" => {
                if let Some(path) = argv.get(i + 1) {
                    config = RunConfig::from_json(&std::fs::read_to_string(path)?)?;
                }
                i += 2;
            }
            "--print-config-hash" => {
                let c = RunConfig::default();
                println!("{}", c.config_hash());
                std::process::exit(0);
            }
            _ => i += 1,
        }
    }
    config.strategy.kind = strategy.clone();

    Ok(Args {
        data_dir,
        funding_csv,
        funding_dir,
        strategy,
        symbol,
        capital,
        walk_forward,
        config,
        purged_cv,
        cost_sweep,
        top_k,
        xs_trailing,
        xs_rebalance,
    })
}

/// Читає CSV-директорію: <SYMBOL>.csv з колонками timestamp/date, close, volume?
fn load_csv_dir(dir: &Path) -> Result<Arc<AlignedMarketData>> {
    let mut per_symbol: BTreeMap<String, BTreeMap<DateTime<Utc>, (Decimal, Decimal)>> =
        BTreeMap::new();

    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {dir:?}"))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let symbol = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        let content = std::fs::read_to_string(&path)?;
        let mut lines = content.lines();
        let header = lines.next().unwrap_or_default().to_lowercase();
        let cols: Vec<&str> = header.split(',').map(|c| c.trim()).collect();
        let ts_idx = cols
            .iter()
            .position(|c| *c == "timestamp" || *c == "date")
            .context("csv must have timestamp/date column")?;
        let close_idx = cols
            .iter()
            .position(|c| *c == "adj_close" || *c == "close")
            .context("csv must have close/adj_close column")?;
        let vol_idx = cols.iter().position(|c| *c == "volume");

        let series = per_symbol.entry(symbol).or_default();
        for line in lines {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() <= close_idx {
                continue;
            }
            let raw_ts = parts[ts_idx].trim();
            let ts = raw_ts
                .parse::<DateTime<Utc>>()
                .ok()
                .or_else(|| {
                    NaiveDate::parse_from_str(raw_ts, "%Y-%m-%d")
                        .ok()
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                        .map(|dt| Utc.from_utc_datetime(&dt))
                });
            let Some(ts) = ts else { continue };
            let Ok(close) = parts[close_idx].trim().parse::<Decimal>() else {
                continue;
            };
            let volume = vol_idx
                .and_then(|vi| parts.get(vi))
                .and_then(|v| v.trim().parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            series.insert(ts, (close, volume));
        }
    }
    anyhow::ensure!(!per_symbol.is_empty(), "no csv files found in {dir:?}");

    // Вирівнювання: inner join по датах, що є в УСІХ символах.
    let mut common: Option<std::collections::BTreeSet<DateTime<Utc>>> = None;
    for series in per_symbol.values() {
        let keys: std::collections::BTreeSet<DateTime<Utc>> = series.keys().copied().collect();
        common = Some(match common {
            None => keys,
            Some(c) => c.intersection(&keys).copied().collect(),
        });
    }
    let timestamps: Vec<DateTime<Utc>> = common.unwrap_or_default().into_iter().collect();
    anyhow::ensure!(
        timestamps.len() >= 30,
        "too few common bars across symbols: {}",
        timestamps.len()
    );

    let mut closes = BTreeMap::new();
    let mut volumes = BTreeMap::new();
    for (sym, series) in &per_symbol {
        let mut c = Vec::with_capacity(timestamps.len());
        let mut v = Vec::with_capacity(timestamps.len());
        for ts in &timestamps {
            let (close, volume) = series[ts];
            c.push(close);
            v.push(volume);
        }
        closes.insert(sym.clone(), c);
        volumes.insert(sym.clone(), v);
    }

    Ok(Arc::new(AlignedMarketData::new(
        timestamps,
        closes.clone(),
        closes,
        volumes,
    )?))
}

fn build_strategy(args: &Args, universe: Vec<String>) -> (Box<dyn AllocationStrategy>, bool) {
    let s = &args.config.strategy;
    match args.strategy.as_str() {
        "tsmom" => (
            Box::new(TsmomStrategy::new(
                s.momentum_lookback,
                20,
                s.vol_target_annual,
                21,
                !s.long_only,
            )),
            !s.long_only,
        ),
        "xsmom" => (
            Box::new(CrossSectionalMomentum::new(
                s.momentum_lookback,
                21,
                s.xs_quantile,
                21,
                dec!(0.02),
                dec!(1.0),
            )),
            true, // dollar-neutral потребує шортів
        ),
        "equal_weight" => (Box::new(EqualWeight::new(21)), false),
        "sixty_forty" => (Box::new(SixtyForty::all_equity(universe, 21)), false),
        _ => (Box::new(BuyAndHold::new()), false),
    }
}

async fn run_carry(args: &Args) -> Result<()> {
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
                costs = db_con::shared::run_config::CostConfig::zero();
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

/// Пункт 2 плану: крос-секційний carry на широкому кошику перпів.
async fn run_xs_carry(args: &Args) -> Result<()> {
    use std::collections::BTreeMap;

    let dir = args
        .funding_dir
        .clone()
        .context("--funding-dir required for xs_carry")?;

    let mut universe: BTreeMap<String, Vec<db_con::data_ingestion::domain::funding::FundingRatePoint>> =
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

/// Чесний матч ML-ансамблю проти TSMOM і бенчмарків (той самий період,
/// ті самі витрати, той самий execution-пайплайн).
///
/// Протокол як у дипломному бектесті: навчання на перших train_ratio даних
/// (сигнали вимкнені) → скидання стану → торгівля на решті БЕЗ донавчання.
/// TSMOM і бенчмарки ганяються на ТОМУ Ж тестовому вікні.
async fn run_ml_match(args: &Args, data: Arc<AlignedMarketData>) -> Result<()> {
    use db_con::backtest::domain::metrics as qmetrics;
    use db_con::backtest::domain::multi_report::{ComparativeReport, StrategySummaryRow};
    use db_con::backtest::domain::report::BacktestReport;
    use db_con::database::domain::models::StockTick;
    use db_con::model::domain::adaptive_linear::AdaptiveLinearModel;
    use db_con::model::domain::ensemble::WeightedEnsembleModel;
    use db_con::model::domain::models::PredictionModel;
    use db_con::model::domain::random_forest_like::RandomForestLikeModel;
    use db_con::trading::adapters::execution_handler::SimpleExecutionHandler;
    use db_con::trading::adapters::momentum_strategy::MomentumStrategy;
    use db_con::trading::domain::events::{OrderEvent, OrderSide, OrderType};
    use db_con::trading::domain::sizing::PositionSizer;
    use db_con::trading::ports::{ExecutionHandlerPort, StrategyPort};
    use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

    let n = data.len();
    let train_ratio = args
        .config
        .walk_forward
        .train_ratio
        .to_f64()
        .unwrap_or(0.7);
    let split = ((n as f64) * train_ratio) as usize;
    let split = split.clamp(100, n - 30);

    // Бари → потік тіків у хронологічному порядку (символи відсортовані —
    // детермінізм). Ціна = close, обсяг = обсяг бару.
    let symbols: Vec<String> = data.symbols().cloned().collect();
    let ticks_for = |from: usize, to: usize| -> Vec<StockTick> {
        let mut out = Vec::with_capacity((to - from) * symbols.len());
        for t in from..to {
            let view = UniverseView::new(data.clone(), t);
            for sym in &symbols {
                let Some(price) = view.price(sym) else { continue };
                let volume = view
                    .avg_volume(sym, 1)
                    .and_then(|v| v.to_i64())
                    .unwrap_or(0);
                out.push(StockTick {
                    symbol: sym.clone(),
                    timestamp: view.timestamp(),
                    price,
                    volume,
                    bid: None,
                    ask: None,
                    source: "csv".into(),
                });
            }
        }
        out
    };
    let train_ticks = ticks_for(0, split);
    let test_ticks = ticks_for(split, n);
    println!(
        "ML-матч: {} символів; train {} барів, test {} барів (з бару {})",
        symbols.len(),
        split,
        n - split,
        split
    );

    // Ансамбль — ТІЛЬКИ з конфіга (M0.4): ваги, learning rates, deadzone, сід.
    let ens = &args.config.ensemble;
    let feature_keys: Vec<String> = vec![
        "ema_12".into(),
        "ema_26".into(),
        "rsi_14".into(),
        "macd".into(),
        "momentum_5".into(),
        "momentum_20".into(),
        "mean_reversion_20".into(),
        "volatility_20".into(),
        "vol_cluster_5_20".into(),
        "bid_ask_spread_proxy".into(),
        "liquidity_imbalance_proxy_20".into(),
        "order_flow_proxy_20".into(),
    ];
    let build_ensemble = |forest: bool| -> Arc<WeightedEnsembleModel> {
        let lr = |i: usize, d: f64| ens.learning_rates.get(i).copied().unwrap_or(d);
        let w = |i: usize, d: Decimal| ens.weights.get(i).copied().unwrap_or(d);
        let submodels: Vec<(Arc<dyn PredictionModel>, Decimal)> = if forest {
            // Той самий склад RF-ансамблю, що в live http-handler.
            vec![
                (
                    Arc::new(RandomForestLikeModel::new_with_params_and_seed(
                        feature_keys.clone(),
                        500,
                        100,
                        256,
                        8,
                        4,
                        0.004,
                        ens.seed,
                    )) as Arc<dyn PredictionModel>,
                    w(0, dec!(0.40)),
                ),
                (
                    Arc::new(RandomForestLikeModel::new_with_params_and_seed(
                        feature_keys.clone(),
                        1200,
                        240,
                        512,
                        14,
                        6,
                        0.006,
                        ens.seed + 1,
                    )) as Arc<dyn PredictionModel>,
                    w(1, dec!(0.35)),
                ),
                (
                    Arc::new(RandomForestLikeModel::new_with_params_and_seed(
                        feature_keys.clone(),
                        2600,
                        520,
                        1024,
                        24,
                        8,
                        0.009,
                        ens.seed + 2,
                    )) as Arc<dyn PredictionModel>,
                    w(2, dec!(0.25)),
                ),
            ]
        } else {
            // Лінійний адаптивний ансамбль fast/medium/slow.
            vec![
                (
                    Arc::new(AdaptiveLinearModel::new(
                        feature_keys.clone(),
                        Decimal::from_f64(lr(0, 0.0020)).unwrap_or(dec!(0.002)),
                    )) as Arc<dyn PredictionModel>,
                    w(0, dec!(0.40)),
                ),
                (
                    Arc::new(AdaptiveLinearModel::new(
                        feature_keys.clone(),
                        Decimal::from_f64(lr(1, 0.0010)).unwrap_or(dec!(0.001)),
                    )) as Arc<dyn PredictionModel>,
                    w(1, dec!(0.35)),
                ),
                (
                    Arc::new(AdaptiveLinearModel::new(
                        feature_keys.clone(),
                        Decimal::from_f64(lr(2, 0.0005)).unwrap_or(dec!(0.0005)),
                    )) as Arc<dyn PredictionModel>,
                    w(2, dec!(0.25)),
                ),
            ]
        };
        Arc::new(WeightedEnsembleModel::new_with_signal_deadzone(
            submodels,
            ens.signal_deadzone,
            dec!(0.0),
        ))
    };

    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.0002),
        cost_model: cost_model_from_config(&args.config.costs),
    }) as Arc<dyn BrokerSimulatorPort>;

    // Прогін одного варіанта ансамблю через повний пайплайн.
    let run_variant = |name: &'static str, forest: bool| {
        let broker = broker.clone();
        let data = data.clone();
        let symbols = symbols.clone();
        let train_ticks = train_ticks.clone();
        let test_ticks = test_ticks.clone();
        let build = build_ensemble;
        async move {
            let model = build(forest);
            model
                .set_adaptive_learning_enabled(args.config.ensemble.adaptive_learning)
                .await;
            let registry = db_con::bootstrap::features::init_features();
            let mut strategy = MomentumStrategy::new(
                registry,
                model as Arc<dyn PredictionModel>,
                args.config.strategy.lookback_size,
                args.config.strategy.min_confidence,
                0, // денні бари: часовий гейт не потрібен
            );

            // Фаза 1: навчання (сигнали вимкнені).
            strategy.set_emit_signals(false);
            for tick in &train_ticks {
                let _ = strategy.on_market_event(tick).await?;
            }
            // Фаза 2: торгівля без донавчання (протокол диплому).
            strategy.reset_state();
            strategy.set_emit_signals(true);
            strategy.set_learning_enabled(false);

            let portfolio: Arc<dyn PortfolioPort> =
                Arc::new(PortfolioManager::new(args.capital));
            let execution = SimpleExecutionHandler {
                sizer: PositionSizer::from_config(args.config.sizing.clone()),
                portfolio: Some(portfolio.clone()),
            };

            let mut fills = Vec::new();
            let mut equity_curve = vec![args.capital];
            let mut current_bar_ts = None;
            for tick in &test_ticks {
                // Закриття бару: mark-to-market перед першим тіком нового бару.
                if current_bar_ts.is_some() && current_bar_ts != Some(tick.timestamp) {
                    // (еквіті фіксується нижче після обробки всіх тіків бару)
                }
                let signal_opt = strategy.on_market_event(tick).await?;
                if let Some(signal) = signal_opt {
                    if let Some(order) = execution.on_signal(&signal).await? {
                        let fill = broker.execute_order(&order).await?;
                        portfolio.update_on_fill(&fill).await?;
                        fills.push(fill);
                    }
                }
                current_bar_ts = Some(tick.timestamp);
                // Останній символ бару → зафіксувати еквіті бару.
                if tick.symbol == *symbols.last().unwrap() {
                    let snapshot = portfolio.get_portfolio().await?;
                    let mut equity = snapshot.cash;
                    for (sym, pos) in &snapshot.positions {
                        // Ціна закриття цього бару.
                        let bar_price = test_ticks
                            .iter()
                            .rev()
                            .find(|t| t.timestamp == tick.timestamp && t.symbol == *sym)
                            .map(|t| t.price)
                            .unwrap_or(pos.current_price);
                        equity += pos.quantity * bar_price;
                    }
                    equity_curve.push(equity);
                }
            }

            // Форс-закриття залишкових позицій на останньому барі.
            let last_view = UniverseView::new(data.clone(), data.len() - 1);
            let snapshot = portfolio.get_portfolio().await?;
            for (sym, pos) in snapshot.positions {
                if pos.quantity <= Decimal::ZERO {
                    continue;
                }
                let Some(price) = last_view.price(&sym) else { continue };
                let seed = format!("ml-final-exit|{name}|{sym}");
                let order = OrderEvent {
                    id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()),
                    signal_id: uuid::Uuid::nil(),
                    timestamp: last_view.timestamp(),
                    symbol: sym.clone(),
                    side: OrderSide::Sell,
                    quantity: pos.quantity,
                    order_type: OrderType::Market,
                    limit_price: Some(price),
                    stop_price: None,
                    market_context: None,
                };
                let fill = broker.execute_order(&order).await?;
                portfolio.update_on_fill(&fill).await?;
                fills.push(fill);
            }
            let final_portfolio = portfolio.get_portfolio().await?;
            if let Some(last) = equity_curve.last_mut() {
                *last = final_portfolio.get_total_value();
            }

            // Звіт: trade-статистика з філів, ризикові метрики — з bar-level
            // кривої (та сама шкала, що в TSMOM/бенчмарків).
            let mut report =
                BacktestReport::from_fills_and_portfolio(&fills, &final_portfolio, args.capital);
            let (mdd, mdd_pct) = qmetrics::max_drawdown(&equity_curve);
            report.sharpe_ratio = qmetrics::sharpe_annualized(&equity_curve);
            report.max_drawdown = mdd;
            report.max_drawdown_pct = mdd_pct;
            report.calmar = qmetrics::calmar(report.total_return_pct, mdd_pct);
            let avg_equity = equity_curve.iter().copied().sum::<Decimal>()
                / Decimal::from(equity_curve.len().max(1) as u64);
            report.turnover = qmetrics::turnover(&fills, avg_equity);

            Ok::<StrategySummaryRow, anyhow::Error>(StrategySummaryRow {
                name: name.to_string(),
                report,
                median_sharpe: None,
                instruments: Vec::new(),
            })
        }
    };

    println!("Ганяю лінійний ансамбль (fast/medium/slow adaptive linear)...");
    let ml_linear = run_variant("ml_linear_ensemble", false).await?;
    println!("Ганяю random-forest ансамбль (як у live)...");
    let ml_forest = run_variant("ml_forest_ensemble", true).await?;

    // TSMOM + бенчмарки на ТОМУ Ж тестовому вікні, тим самим брокером.
    let runner = BenchmarkRunner::new(data.clone(), broker.clone(), args.capital);
    let mut tsmom = TsmomStrategy::new(
        args.config.strategy.momentum_lookback,
        20,
        args.config.strategy.vol_target_annual,
        21,
        false,
    );
    let mut comparative = runner
        .run_with_benchmarks(&mut tsmom, false, split, n)
        .await?;

    // Зведена таблиця: обидва ML-варіанти + tsmom + пасивні бенчмарки.
    let mut benchmarks = vec![ml_forest, comparative.strategy.clone()];
    benchmarks.append(&mut comparative.benchmarks);
    let final_report = ComparativeReport {
        strategy: ml_linear,
        benchmarks,
    };
    println!(
        "\n=== ML-МАТЧ: той самий OOS-період (останні {} барів), ті самі витрати ===",
        n - split
    );
    print!("{}", final_report.render());

    log_run(args, serde_json::json!({
        "strategy": "ml_match",
        "test_window_bars": n - split,
        "comparative": serde_json::to_value(&final_report)?,
    }))
    .await;
    Ok(())
}

// ── Funding-ML: чи передбачає модель стійкість фандингу краще за наївне
//    «наступний тиждень ≈ минулий»? (роль №1 для ML у цій системі) ──────────

/// Один навчальний приклад: фічі з минулого, лейбл — середній funding за
/// НАСТУПНІ `hold` інтервалів. Перетину минуле/майбутнє немає за побудовою.
/// (rf_ext_pred, linear_base_pred, linear_ext_pred, baseline, label, sample)
type EvalRow<'a> = (Decimal, Decimal, Decimal, Decimal, Decimal, &'a FundingSample);

struct FundingSample {
    symbol: String,
    timestamp: DateTime<Utc>,
    features: db_con::features::domain::models::FeatureSet,
    /// Наївний базлайн: середній funding за останні 21 інтервал (частки).
    baseline: Decimal,
    /// Ціль: середній funding за наступні `hold` інтервалів (частки).
    label: Decimal,
}

/// Мета-дані перпа з 8h-барів Binance: базис і потік агресії.
/// (OI/long-short історію біржа не віддає — лише 30 днів; це найближчі
/// повноісторичні замінники.)
#[derive(Debug, Clone)]
struct MetaRow {
    /// Premium index close (частка): перп проти індексу — попередник фандингу.
    premium: Decimal,
    /// Обсяг бару (штук).
    volume: Decimal,
    /// Частина обсягу, ініційована агресивними покупцями.
    taker_buy_volume: Decimal,
}

const FUNDING_FEATURE_SCALE: Decimal = dec!(1000); // 0.0001 → 0.1
const FUNDING_PAST: usize = 63;
const FUNDING_HOLD: usize = 21;
/// Вікно мета-фіч у 8h-барах.
const META_WINDOW: usize = 21;

fn funding_feature_keys() -> Vec<String> {
    vec![
        "f_mean21".into(),
        "f_mean63".into(),
        "f_std21".into(),
        "f_last".into(),
        "f_slope".into(),
        "p_ret21".into(),
        "p_vol21".into(),
    ]
}

/// Розширений набір: базові + базис/потік (роль OI-замінників).
fn funding_feature_keys_ext() -> Vec<String> {
    let mut keys = funding_feature_keys();
    keys.extend([
        "prem_mean21".to_string(),
        "prem_last".to_string(),
        "prem_slope".to_string(),
        "taker_ratio21".to_string(),
        "vol_z21".to_string(),
    ]);
    keys
}

fn load_meta_dir(dir: &Path) -> Result<BTreeMap<String, BTreeMap<DateTime<Utc>, MetaRow>>> {
    let mut out: BTreeMap<String, BTreeMap<DateTime<Utc>, MetaRow>> = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        let content = std::fs::read_to_string(&path)?;
        let mut series = BTreeMap::new();
        for (i, line) in content.lines().enumerate() {
            if i == 0 {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 5 {
                continue;
            }
            let (Ok(ts), Ok(premium), Ok(volume), Ok(taker)) = (
                parts[0].parse::<DateTime<Utc>>(),
                parts[1].parse::<Decimal>(),
                parts[3].parse::<Decimal>(),
                parts[4].parse::<Decimal>(),
            ) else {
                continue;
            };
            series.insert(
                ts,
                MetaRow {
                    premium,
                    volume,
                    taker_buy_volume: taker,
                },
            );
        }
        if series.len() > META_WINDOW {
            out.insert(sym, series);
        }
    }
    Ok(out)
}

fn build_funding_samples(
    symbol: &str,
    series: &[db_con::data_ingestion::domain::funding::FundingRatePoint],
    past: usize,
    hold: usize,
    meta: Option<&BTreeMap<DateTime<Utc>, MetaRow>>,
) -> Vec<FundingSample> {
    use db_con::features::domain::models::{FeatureSet, FeatureValue};
    use rust_decimal::prelude::FromPrimitive;

    let n = series.len();
    if n < past + hold + 1 {
        return Vec::new();
    }
    let mean = |from: usize, to_incl: usize| -> Decimal {
        let cnt = to_incl + 1 - from;
        series[from..=to_incl]
            .iter()
            .map(|p| p.rate)
            .sum::<Decimal>()
            / Decimal::from(cnt as u64)
    };

    let mut out = Vec::new();
    for t in past..(n - hold) {
        // Фічі — ТІЛЬКИ з ≤ t; лейбл — ТІЛЬКИ з (t, t+hold].
        let mean21 = mean(t - 20, t);
        let mean63 = mean(t - 62, t);
        let var21: Decimal = series[t - 20..=t]
            .iter()
            .map(|p| (p.rate - mean21) * (p.rate - mean21))
            .sum::<Decimal>()
            / dec!(21);
        let std21 = Decimal::from_f64(
            var21
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0)
                .max(0.0)
                .sqrt(),
        )
        .unwrap_or(Decimal::ZERO);
        let slope = mean(t - 6, t) - mean(t - 20, t - 7);
        let p_now = series[t].mark_price;
        let p_then = series[t - 20].mark_price;
        let ret21 = if p_then > Decimal::ZERO {
            (p_now - p_then) / p_then
        } else {
            Decimal::ZERO
        };
        let mut rets = Vec::with_capacity(20);
        for i in (t - 19)..=t {
            let a = series[i - 1].mark_price;
            let b = series[i].mark_price;
            if a > Decimal::ZERO {
                rets.push(((b - a) / a).to_string().parse::<f64>().unwrap_or(0.0));
            }
        }
        let vol21 = if rets.len() > 1 {
            let m = rets.iter().sum::<f64>() / rets.len() as f64;
            let v = rets.iter().map(|r| (r - m).powi(2)).sum::<f64>() / rets.len() as f64;
            Decimal::from_f64(v.sqrt()).unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };

        let mut fs = FeatureSet::new(symbol.to_string());
        let s = FUNDING_FEATURE_SCALE;
        fs.insert("f_mean21".into(), FeatureValue::Scalar(mean21 * s));
        fs.insert("f_mean63".into(), FeatureValue::Scalar(mean63 * s));
        fs.insert("f_std21".into(), FeatureValue::Scalar(std21 * s));
        fs.insert("f_last".into(), FeatureValue::Scalar(series[t].rate * s));
        fs.insert("f_slope".into(), FeatureValue::Scalar(slope * s));
        fs.insert("p_ret21".into(), FeatureValue::Scalar(ret21));
        fs.insert("p_vol21".into(), FeatureValue::Scalar(vol21 * dec!(10)));

        // Мета-фічі: базис (premium) і потік агресії за останні META_WINDOW
        // 8h-барів СТРОГО ≤ t. Без повного вікна семпл пропускається,
        // щоб порівняння моделей ішло на ідентичних рядках.
        if let Some(meta_series) = meta {
            let window: Vec<&MetaRow> = meta_series
                .range(..=series[t].timestamp)
                .rev()
                .take(META_WINDOW)
                .map(|(_, r)| r)
                .collect();
            if window.len() < META_WINDOW {
                continue;
            }
            let m = Decimal::from(META_WINDOW as u64);
            let prem_mean: Decimal =
                window.iter().map(|r| r.premium).sum::<Decimal>() / m;
            let prem_last = window[0].premium; // rev(): [0] — найсвіжіший
            let prem_recent: Decimal =
                window[..7].iter().map(|r| r.premium).sum::<Decimal>() / dec!(7);
            let prem_older: Decimal =
                window[7..].iter().map(|r| r.premium).sum::<Decimal>()
                    / Decimal::from((META_WINDOW - 7) as u64);
            let taker_ratio: Decimal = {
                let vol_sum: Decimal = window.iter().map(|r| r.volume).sum();
                let buy_sum: Decimal = window.iter().map(|r| r.taker_buy_volume).sum();
                if vol_sum > Decimal::ZERO {
                    buy_sum / vol_sum
                } else {
                    dec!(0.5)
                }
            };
            let vol_z = {
                let vols: Vec<f64> = window
                    .iter()
                    .map(|r| r.volume.to_string().parse::<f64>().unwrap_or(0.0))
                    .collect();
                let mu = vols.iter().sum::<f64>() / vols.len() as f64;
                let sd = (vols.iter().map(|v| (v - mu).powi(2)).sum::<f64>()
                    / vols.len() as f64)
                    .sqrt();
                if sd > 0.0 {
                    ((vols[0] - mu) / sd).clamp(-3.0, 3.0) / 3.0
                } else {
                    0.0
                }
            };
            fs.insert("prem_mean21".into(), FeatureValue::Scalar(prem_mean * s));
            fs.insert("prem_last".into(), FeatureValue::Scalar(prem_last * s));
            fs.insert(
                "prem_slope".into(),
                FeatureValue::Scalar((prem_recent - prem_older) * s),
            );
            fs.insert(
                "taker_ratio21".into(),
                FeatureValue::Scalar((taker_ratio - dec!(0.5)) * dec!(10)),
            );
            fs.insert(
                "vol_z21".into(),
                FeatureValue::Scalar(Decimal::from_f64(vol_z).unwrap_or(Decimal::ZERO)),
            );
        }

        out.push(FundingSample {
            symbol: symbol.to_string(),
            timestamp: series[t].timestamp,
            features: fs,
            baseline: mean21,
            label: mean(t + 1, t + hold),
        });
    }
    out
}

async fn run_funding_ml(args: &Args) -> Result<()> {
    use db_con::model::domain::adaptive_linear::AdaptiveLinearModel;
    use db_con::model::domain::models::PredictionModel;
    use db_con::model::domain::random_forest_like::RandomForestLikeModel;
    use db_con::trading::domain::events::SignalDirection;
    use rust_decimal::prelude::ToPrimitive;

    let dir = args
        .funding_dir
        .clone()
        .context("--funding-dir required for funding_ml")?;

    // Мета-дані (базис + потік): datasets/perp_meta поруч із funding-диром.
    let meta_dir = dir
        .parent()
        .map(|p| p.join("perp_meta"))
        .unwrap_or_else(|| PathBuf::from("datasets/perp_meta"));
    let meta_by_symbol = if meta_dir.exists() {
        load_meta_dir(&meta_dir)?
    } else {
        BTreeMap::new()
    };
    anyhow::ensure!(
        !meta_by_symbol.is_empty(),
        "meta dir {meta_dir:?} порожній — потрібні premium/taker дані"
    );
    println!("Мета-дані: {} символів з {:?}", meta_by_symbol.len(), meta_dir);
    let mut skipped_no_meta = 0usize;

    // 1. Датасет по всіх символах.
    let mut samples: Vec<FundingSample> = Vec::new();
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
        let meta = meta_by_symbol.get(&sym);
        if meta.is_none() {
            skipped_no_meta += 1;
            continue;
        }
        samples.extend(build_funding_samples(
            &sym,
            &series,
            FUNDING_PAST,
            FUNDING_HOLD,
            meta,
        ));
    }
    if skipped_no_meta > 0 {
        println!("(пропущено {skipped_no_meta} символів без мета-даних)");
    }
    anyhow::ensure!(samples.len() > 2000, "too few samples: {}", samples.len());
    samples.sort_by_key(|s| (s.timestamp, s.symbol.clone()));

    // 2. Часовий розріз 70/30 — жодних майбутніх даних у навчанні.
    let split_ts = samples[(samples.len() * 7) / 10].timestamp;
    let (train, test): (Vec<&FundingSample>, Vec<&FundingSample>) =
        samples.iter().partition(|s| s.timestamp < split_ts);
    println!(
        "Funding-ML: {} семплів ({} train / {} test), розріз {}",
        samples.len(),
        train.len(),
        test.len(),
        split_ts
    );

    // 3. Моделі користувача. Матч на ІДЕНТИЧНИХ рядках:
    //    linear_base — старі 7 фіч; linear_ext і RF — 12 (з базисом/потоком).
    //    Лейбл масштабується тим самим фактором, що фічі.
    let rf = RandomForestLikeModel::new_with_params_and_seed(
        funding_feature_keys_ext(),
        200_000, // тримати всі семпли
        1_000,
        4_000, // періодичний рефіт під час train
        100,
        8,
        0.0, // без deadzone: потрібна сира регресія
        args.config.ensemble.seed,
    );
    let linear_base = AdaptiveLinearModel::new(funding_feature_keys(), dec!(0.01));
    let linear_ext = AdaptiveLinearModel::new(funding_feature_keys_ext(), dec!(0.01));

    for s in &train {
        let y = (s.label * FUNDING_FEATURE_SCALE).clamp(dec!(-1), dec!(1));
        rf.learn(&s.features, y).await?;
        linear_base.learn(&s.features, y).await?;
        linear_ext.learn(&s.features, y).await?;
    }

    // 4. Прогнози на test (без донавчання).
    let signed = |p: &db_con::model::domain::models::Prediction| -> Decimal {
        let sign = match p.direction {
            SignalDirection::Long => Decimal::ONE,
            SignalDirection::Short => Decimal::NEGATIVE_ONE,
            SignalDirection::Exit => Decimal::ZERO,
        };
        sign * p.confidence / FUNDING_FEATURE_SCALE
    };
    let mut rows: Vec<EvalRow> = Vec::new();
    for s in &test {
        let rf_pred = signed(&rf.predict(&s.features).await?);
        let lin_base_pred = signed(&linear_base.predict(&s.features).await?);
        let lin_ext_pred = signed(&linear_ext.predict(&s.features).await?);
        rows.push((rf_pred, lin_base_pred, lin_ext_pred, s.baseline, s.label, s));
    }

    // 5. Якість прогнозу: кореляція і MAE проти базлайну.
    let corr = |xs: &[f64], ys: &[f64]| -> f64 {
        let n = xs.len() as f64;
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let cov = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum::<f64>();
        let vx = xs.iter().map(|x| (x - mx).powi(2)).sum::<f64>();
        let vy = ys.iter().map(|y| (y - my).powi(2)).sum::<f64>();
        if vx <= 0.0 || vy <= 0.0 {
            0.0
        } else {
            cov / (vx.sqrt() * vy.sqrt())
        }
    };
    let f = |d: Decimal| d.to_f64().unwrap_or(0.0);
    let labels: Vec<f64> = rows.iter().map(|r| f(r.4)).collect();
    let rf_preds: Vec<f64> = rows.iter().map(|r| f(r.0)).collect();
    let lin_base_preds: Vec<f64> = rows.iter().map(|r| f(r.1)).collect();
    let lin_ext_preds: Vec<f64> = rows.iter().map(|r| f(r.2)).collect();
    let base_preds: Vec<f64> = rows.iter().map(|r| f(r.3)).collect();
    let mae = |ps: &[f64]| -> f64 {
        ps.iter()
            .zip(&labels)
            .map(|(p, l)| (p - l).abs())
            .sum::<f64>()
            / labels.len() as f64
            * 10_000.0 // у б.п. за інтервал
    };

    println!("\n=== Якість прогнозу майбутнього funding (test, {} семплів) ===", rows.len());
    println!("{:<34} {:>12} {:>16}", "Предиктор", "corr(pred,y)", "MAE (бпс/інтервал)");
    println!("{}", "-".repeat(66));
    println!("{:<34} {:>12.4} {:>16.4}", "базлайн mean21", corr(&base_preds, &labels), mae(&base_preds));
    println!("{:<34} {:>12.4} {:>16.4}", "linear (7 старих фіч)", corr(&lin_base_preds, &labels), mae(&lin_base_preds));
    println!("{:<34} {:>12.4} {:>16.4}", "linear +базис/потік (12 фіч)", corr(&lin_ext_preds, &labels), mae(&lin_ext_preds));
    println!("{:<34} {:>12.4} {:>16.4}", "RF +базис/потік (12 фіч)", corr(&rf_preds, &labels), mae(&rf_preds));

    // 6. Економічний тест: на кожному test-таймстемпі зібрати топ-5 кошик
    //    за кожним предиктором і порівняти РЕАЛЬНО зібраний майбутній funding.
    let mut by_ts: BTreeMap<DateTime<Utc>, Vec<&EvalRow>> = BTreeMap::new();
    for r in &rows {
        by_ts.entry(r.5.timestamp).or_default().push(r);
    }
    let top_k = args.top_k.clamp(2, 20);
    let mut sum_rf = Decimal::ZERO;
    let mut sum_lin_base = Decimal::ZERO;
    let mut sum_lin_ext = Decimal::ZERO;
    let mut sum_base = Decimal::ZERO;
    let mut sum_perfect = Decimal::ZERO;
    let mut n_ts = 0u64;
    for (_, group) in by_ts.iter().filter(|(_, g)| g.len() >= 10) {
        let basket_mean = |key: &dyn Fn(&&EvalRow) -> Decimal| -> Decimal {
            let mut sorted: Vec<_> = group.iter().collect();
            sorted.sort_by(|a, b| key(b).cmp(&key(a)).then_with(|| a.5.symbol.cmp(&b.5.symbol)));
            let top: Vec<_> = sorted.into_iter().take(top_k).collect();
            top.iter().map(|r| r.4).sum::<Decimal>() / Decimal::from(top_k as u64)
        };
        sum_rf += basket_mean(&|r| r.0);
        sum_lin_base += basket_mean(&|r| r.1);
        sum_lin_ext += basket_mean(&|r| r.2);
        sum_base += basket_mean(&|r| r.3);
        sum_perfect += basket_mean(&|r| r.4);
        n_ts += 1;
    }
    anyhow::ensure!(n_ts > 0, "no test timestamps with enough symbols");
    let annualize = |d: Decimal| {
        // Лейбл — середня ставка за інтервал; ~1095 інтервалів/рік (8h).
        (d / Decimal::from(n_ts) * dec!(1095) * dec!(100)).round_dp(2)
    };
    println!("\n=== Економічний тест: топ-{top_k} кошик, середній МАЙБУТНІЙ funding (≈% річних) ===");
    println!("{:<34} {:>10}", "Ранжування за", "≈%/рік");
    println!("{}", "-".repeat(46));
    println!("{:<34} {:>10}", "базлайн mean21", annualize(sum_base));
    println!("{:<34} {:>10}", "linear (7 старих фіч)", annualize(sum_lin_base));
    println!("{:<34} {:>10}", "linear +базис/потік (12 фіч)", annualize(sum_lin_ext));
    println!("{:<34} {:>10}", "RF +базис/потік (12 фіч)", annualize(sum_rf));
    println!("{:<34} {:>10}  (недосяжна стеля)", "ідеальне передбачення", annualize(sum_perfect));

    log_run(args, serde_json::json!({
        "strategy": "funding_ml_ext",
        "samples": samples.len(),
        "test_rows": rows.len(),
        "corr": {
            "baseline": corr(&base_preds, &labels),
            "linear_base": corr(&lin_base_preds, &labels),
            "linear_ext": corr(&lin_ext_preds, &labels),
            "rf_ext": corr(&rf_preds, &labels),
        },
        "mae_bps": {
            "baseline": mae(&base_preds),
            "linear_base": mae(&lin_base_preds),
            "linear_ext": mae(&lin_ext_preds),
            "rf_ext": mae(&rf_preds),
        },
        "basket_annualized_pct": {
            "baseline": annualize(sum_base).to_string(),
            "linear_base": annualize(sum_lin_base).to_string(),
            "linear_ext": annualize(sum_lin_ext).to_string(),
            "rf_ext": annualize(sum_rf).to_string(),
            "perfect": annualize(sum_perfect).to_string(),
        },
    }))
    .await;
    Ok(())
}

async fn log_run(args: &Args, metrics: serde_json::Value) {
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

// ── Тіньовий трейдинг carry-кошика (ex-ante, git-нотаризований) ──────────────
//
// Щотижня: обираємо кошик СЬОГОДНІ (тільки з даних ≤ сьогодні), пишемо в
// append-only журнал shadow/ledger.jsonl і комітимо — git-таймстемп доводить,
// що вибір зроблено ДО того, як тиждень відбувся. Наступні запуски оцінюють
// старі записи: спершу попередньо (за premium-оцінкою фандингу — свіжа
// щодня), потім фінально (за settled funding з місячного архіву).
// Ведемо ДВА кошики паралельно — A/B тест RF проти базлайну наживо.

/// Оцінка funding-ставки з premium-бару (формула Binance зі ставкою 0.01%/8h).
fn funding_estimate_from_premium(premium: Decimal) -> Decimal {
    let interest = dec!(0.0001);
    let clamp_component = (interest - premium).clamp(dec!(-0.0005), dec!(0.0005));
    premium + clamp_component
}

async fn run_shadow_carry(args: &Args) -> Result<()> {
    use db_con::features::domain::models::{FeatureSet, FeatureValue};
    use db_con::model::domain::models::PredictionModel;
    use db_con::model::domain::random_forest_like::RandomForestLikeModel;
    use db_con::trading::domain::events::SignalDirection;
    use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
    use std::io::Write;

    let dir = args
        .funding_dir
        .clone()
        .context("--funding-dir required for shadow_carry")?;
    let meta_dir = dir
        .parent()
        .map(|p| p.join("perp_meta"))
        .unwrap_or_else(|| PathBuf::from("datasets/perp_meta"));
    let meta_by_symbol = load_meta_dir(&meta_dir)?;
    anyhow::ensure!(!meta_by_symbol.is_empty(), "no meta data");

    // Якір «зараз»: останній спільний perp_meta-таймстемп (учора, свіже).
    // SHADOW_ANCHOR_DAYS_AGO зсуває якір у минуле (для тестів; retro=true).
    let retro_days: i64 = env::var("SHADOW_ANCHOR_DAYS_AGO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let anchor: DateTime<Utc> = {
        let latest = meta_by_symbol
            .values()
            .filter_map(|s| s.keys().next_back())
            .max()
            .copied()
            .context("empty meta")?;
        latest - chrono::Duration::days(retro_days)
    };
    let retro = retro_days > 0;

    // 1. Оцінка старих записів журналу.
    std::fs::create_dir_all("shadow")?;
    let ledger_path = "shadow/ledger.jsonl";
    let results_path = "shadow/results.jsonl";
    let ledger: Vec<serde_json::Value> = std::fs::read_to_string(ledger_path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let results: Vec<serde_json::Value> = std::fs::read_to_string(results_path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let has_result = |id: &str, kind: &str| {
        results
            .iter()
            .any(|r| r["entry_id"] == id && r["kind"] == kind)
    };

    // Settled funding: BTreeMap<sym, BTreeMap<ts, rate>> для фінальної оцінки.
    let mut settled: BTreeMap<String, BTreeMap<DateTime<Utc>, Decimal>> = BTreeMap::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_uppercase();
        let adapter = CsvFundingAdapter::new(&path);
        let series = adapter
            .funding_history(&sym, Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(), Utc::now())
            .await?;
        settled.insert(sym, series.iter().map(|p| (p.timestamp, p.rate)).collect());
    }

    let annualize = |mean_per_8h: Decimal| (mean_per_8h * dec!(1095) * dec!(100)).round_dp(2);
    let mut new_results = Vec::new();
    for e in &ledger {
        let id = e["id"].as_str().unwrap_or_default().to_string();
        let Some(entry_anchor) = e["anchor_ts"]
            .as_str()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
        else {
            continue;
        };
        let week_end = entry_anchor + chrono::Duration::days(7);
        for (kind, source_fresh) in [("provisional", true), ("final", false)] {
            if has_result(&id, kind) {
                continue;
            }
            // provisional: тиждень минув за meta-даними; final: settled funding
            // покриває тиждень.
            let coverage_ok = if source_fresh {
                anchor >= week_end
            } else {
                settled.values().any(|s| {
                    s.keys().next_back().map(|t| *t >= week_end).unwrap_or(false)
                })
            };
            if !coverage_ok {
                continue;
            }
            let mut per_basket = serde_json::Map::new();
            for basket_name in ["baseline_est", "rf"] {
                let Some(symbols) = e["baskets"][basket_name].as_array() else { continue };
                let mut vals = Vec::new();
                for s in symbols {
                    let sym = s["symbol"].as_str().unwrap_or_default();
                    let realized: Option<Decimal> = if source_fresh {
                        meta_by_symbol.get(sym).and_then(|m| {
                            let window: Vec<Decimal> = m
                                .range((
                                    std::ops::Bound::Excluded(entry_anchor),
                                    std::ops::Bound::Included(week_end),
                                ))
                                .map(|(_, r)| funding_estimate_from_premium(r.premium))
                                .collect();
                            if window.is_empty() {
                                None
                            } else {
                                Some(window.iter().copied().sum::<Decimal>()
                                    / Decimal::from(window.len() as u64))
                            }
                        })
                    } else {
                        settled.get(sym).and_then(|m| {
                            let window: Vec<Decimal> = m
                                .range((
                                    std::ops::Bound::Excluded(entry_anchor),
                                    std::ops::Bound::Included(week_end),
                                ))
                                .map(|(_, r)| *r)
                                .collect();
                            if window.is_empty() {
                                None
                            } else {
                                Some(window.iter().copied().sum::<Decimal>()
                                    / Decimal::from(window.len() as u64))
                            }
                        })
                    };
                    if let Some(r) = realized {
                        vals.push(r);
                    }
                }
                if !vals.is_empty() {
                    let mean = vals.iter().copied().sum::<Decimal>()
                        / Decimal::from(vals.len() as u64);
                    per_basket.insert(
                        basket_name.to_string(),
                        serde_json::json!(annualize(mean).to_string()),
                    );
                }
            }
            if !per_basket.is_empty() {
                new_results.push(serde_json::json!({
                    "entry_id": id,
                    "kind": kind,
                    "week": [entry_anchor, week_end],
                    "realized_annualized_pct": per_basket,
                    "evaluated_at": Utc::now(),
                }));
            }
        }
    }
    if !new_results.is_empty() {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(results_path)?;
        for r in &new_results {
            writeln!(f, "{}", serde_json::to_string(r)?)?;
            println!("ОЦІНЕНО: {}", serde_json::to_string(r)?);
        }
    }

    // 2. Новий запис: два кошики станом на якір.
    //    baseline_est: трейлінг premium-оцінка funding (свіжа);
    //    rf: модель, навчена на ВСІЙ доступній історії ≤ якоря.
    let mut universe: BTreeMap<String, Vec<db_con::data_ingestion::domain::funding::FundingRatePoint>> =
        BTreeMap::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_uppercase();
        let adapter = CsvFundingAdapter::new(&path);
        let series = adapter
            .funding_history(&sym, Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(), anchor)
            .await?;
        if series.len() > FUNDING_PAST + FUNDING_HOLD {
            universe.insert(sym, series);
        }
    }

    let rf = RandomForestLikeModel::new_with_params_and_seed(
        funding_feature_keys_ext(),
        200_000,
        1_000,
        4_000,
        100,
        8,
        0.0,
        args.config.ensemble.seed,
    );
    let mut n_train = 0usize;
    for (sym, series) in &universe {
        let meta = meta_by_symbol.get(sym);
        for s in build_funding_samples(sym, series, FUNDING_PAST, FUNDING_HOLD, meta) {
            if s.timestamp > anchor {
                continue;
            }
            let y = (s.label * FUNDING_FEATURE_SCALE).clamp(dec!(-1), dec!(1));
            rf.learn(&s.features, y).await?;
            n_train += 1;
        }
    }

    let signed = |p: &db_con::model::domain::models::Prediction| -> Decimal {
        let sign = match p.direction {
            SignalDirection::Long => Decimal::ONE,
            SignalDirection::Short => Decimal::NEGATIVE_ONE,
            SignalDirection::Exit => Decimal::ZERO,
        };
        sign * p.confidence / FUNDING_FEATURE_SCALE
    };

    // Скоринг «зараз»: f_* з останніх settled-точок, prem_*/потік — зі свіжих
    // meta-барів ≤ якоря (найкраще доступне на момент рішення, без майбутнього).
    let mut baseline_scores: Vec<(String, Decimal)> = Vec::new();
    let mut rf_scores: Vec<(String, Decimal)> = Vec::new();
    for (sym, series) in &universe {
        let Some(meta) = meta_by_symbol.get(sym) else { continue };
        let fresh: Vec<&MetaRow> = meta
            .range(..=anchor)
            .rev()
            .take(META_WINDOW)
            .map(|(_, r)| r)
            .collect();
        if fresh.len() < META_WINDOW {
            continue;
        }
        let est_mean = fresh
            .iter()
            .map(|r| funding_estimate_from_premium(r.premium))
            .sum::<Decimal>()
            / Decimal::from(META_WINDOW as u64);
        baseline_scores.push((sym.clone(), est_mean));

        let tail = &series[series.len().saturating_sub(FUNDING_PAST)..];
        if tail.len() < FUNDING_PAST {
            continue;
        }
        let mean_last = |k: usize| -> Decimal {
            tail[tail.len() - k..].iter().map(|p| p.rate).sum::<Decimal>()
                / Decimal::from(k as u64)
        };
        let mean21 = mean_last(21);
        let mean63 = mean_last(63.min(tail.len()));
        let var21: Decimal = tail[tail.len() - 21..]
            .iter()
            .map(|p| (p.rate - mean21) * (p.rate - mean21))
            .sum::<Decimal>()
            / dec!(21);
        let std21 = Decimal::from_f64(var21.to_f64().unwrap_or(0.0).max(0.0).sqrt())
            .unwrap_or(Decimal::ZERO);
        let slope = tail[tail.len() - 7..].iter().map(|p| p.rate).sum::<Decimal>() / dec!(7)
            - tail[tail.len() - 21..tail.len() - 7]
                .iter()
                .map(|p| p.rate)
                .sum::<Decimal>()
                / dec!(14);
        let prem_mean = fresh.iter().map(|r| r.premium).sum::<Decimal>()
            / Decimal::from(META_WINDOW as u64);
        let prem_last = fresh[0].premium;
        let prem_slope = fresh[..7].iter().map(|r| r.premium).sum::<Decimal>() / dec!(7)
            - fresh[7..].iter().map(|r| r.premium).sum::<Decimal>()
                / Decimal::from((META_WINDOW - 7) as u64);
        let vol_sum: Decimal = fresh.iter().map(|r| r.volume).sum();
        let buy_sum: Decimal = fresh.iter().map(|r| r.taker_buy_volume).sum();
        let taker_ratio = if vol_sum > Decimal::ZERO {
            buy_sum / vol_sum
        } else {
            dec!(0.5)
        };
        let s = FUNDING_FEATURE_SCALE;
        let mut fs = FeatureSet::new(sym.clone());
        fs.insert("f_mean21".into(), FeatureValue::Scalar(mean21 * s));
        fs.insert("f_mean63".into(), FeatureValue::Scalar(mean63 * s));
        fs.insert("f_std21".into(), FeatureValue::Scalar(std21 * s));
        fs.insert(
            "f_last".into(),
            FeatureValue::Scalar(tail.last().unwrap().rate * s),
        );
        fs.insert("f_slope".into(), FeatureValue::Scalar(slope * s));
        fs.insert("p_ret21".into(), FeatureValue::Scalar(Decimal::ZERO));
        fs.insert("p_vol21".into(), FeatureValue::Scalar(Decimal::ZERO));
        fs.insert("prem_mean21".into(), FeatureValue::Scalar(prem_mean * s));
        fs.insert("prem_last".into(), FeatureValue::Scalar(prem_last * s));
        fs.insert("prem_slope".into(), FeatureValue::Scalar(prem_slope * s));
        fs.insert(
            "taker_ratio21".into(),
            FeatureValue::Scalar((taker_ratio - dec!(0.5)) * dec!(10)),
        );
        fs.insert("vol_z21".into(), FeatureValue::Scalar(Decimal::ZERO));
        rf_scores.push((sym.clone(), signed(&rf.predict(&fs).await?)));
    }

    let top = |mut v: Vec<(String, Decimal)>| -> Vec<serde_json::Value> {
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.into_iter()
            .take(args.top_k)
            .map(|(sym, score)| {
                serde_json::json!({"symbol": sym, "score_per_8h": score.to_string()})
            })
            .collect()
    };
    let entry = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "created_at": Utc::now(),
        "anchor_ts": anchor,
        "retro": retro,
        "top_k": args.top_k,
        "train_samples": n_train,
        "config_hash": args.config.config_hash(),
        "baskets": {
            "baseline_est": top(baseline_scores),
            "rf": top(rf_scores),
        },
    });
    // Дубль-захист: не пишемо другий запис з тим самим якорем.
    let already = ledger.iter().any(|e| e["anchor_ts"] == entry["anchor_ts"]);
    if already {
        println!("Запис із якорем {anchor} вже є — пропускаю.");
    } else {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(ledger_path)?;
        writeln!(f, "{}", serde_json::to_string(&entry)?)?;
        println!("НОВИЙ ТІНЬОВИЙ ЗАПИС (anchor {anchor}, retro={retro}):");
        println!("{}", serde_json::to_string_pretty(&entry["baskets"])?);
    }
    println!(
        "Журнал: {} записів, {} оцінок (+{} нових).",
        ledger.len() + if already { 0 } else { 1 },
        results.len() + new_results.len(),
        new_results.len()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = parse_args()?;
    println!("Config hash: {}", args.config.config_hash());

    if args.strategy == "carry" {
        return run_carry(&args).await;
    }
    if args.strategy == "xs_carry" {
        return run_xs_carry(&args).await;
    }
    if args.strategy == "funding_ml" {
        return run_funding_ml(&args).await;
    }
    if args.strategy == "shadow_carry" {
        return run_shadow_carry(&args).await;
    }

    let dir = args
        .data_dir
        .clone()
        .context("--data-dir required (csv per symbol)")?;
    let data = load_csv_dir(&dir)?;
    let symbols: Vec<String> = data.symbols().cloned().collect();
    println!(
        "Loaded {} symbols × {} bars: {:?}",
        symbols.len(),
        data.len(),
        symbols
    );

    if args.strategy == "ml_match" {
        return run_ml_match(&args, data).await;
    }

    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: Decimal::ZERO,
        cost_model: cost_model_from_config(&args.config.costs),
    }) as Arc<dyn BrokerSimulatorPort>;

    // Основний прогін + бенчмарки (M0.2: завжди поруч, той самий движок).
    let (mut strategy, allow_short) = build_strategy(&args, symbols.clone());
    let runner = BenchmarkRunner::new(data.clone(), broker.clone(), args.capital);
    let report = runner
        .run_with_benchmarks(strategy.as_mut(), allow_short, 0, data.len())
        .await?;
    println!("\n=== Comparative Report (net-of-cost, single period) ===");
    print!("{}", report.render());

    let mut run_metrics = serde_json::json!({
        "strategy": args.strategy,
        "comparative": serde_json::to_value(&report)?,
    });

    // Walk-forward OOS (M1.1) + deflated Sharpe (M1.3).
    if args.walk_forward {
        let n_trials = FileRunLogger::new("runs/runs.jsonl")
            .count_distinct_configs()
            .await
            .unwrap_or(0)
            .max(1);
        let portfolio: Arc<dyn PortfolioPort> = if allow_short {
            Arc::new(PortfolioManager::new_allowing_short(args.capital))
        } else {
            Arc::new(PortfolioManager::new(args.capital))
        };
        let wf = WalkForwardRunner::new(data.clone(), broker.clone(), portfolio, args.capital);
        let args_ref = &args;
        let symbols_ref = symbols.clone();
        let factory = move || build_strategy(args_ref, symbols_ref.clone()).0;
        let summary = wf.run(&factory, &args.config.walk_forward, n_trials).await?;

        println!("\n=== Walk-Forward (OOS only, net-of-cost) ===");
        for f in &summary.folds {
            println!(
                "  test [{}, {}): ret {}% | sharpe {} | mdd {}% | trades {} | costs {}",
                f.fold.test_start,
                f.fold.test_end,
                f.oos_return_pct.round_dp(2),
                f.oos_sharpe.round_dp(2),
                f.oos_max_drawdown_pct.round_dp(2),
                f.oos_trades,
                f.oos_total_costs.round_dp(2)
            );
        }
        println!(
            "Median OOS Sharpe: {} | Median OOS Return: {}%",
            summary
                .median_oos_sharpe
                .map(|s| s.round_dp(3).to_string())
                .unwrap_or_else(|| "N/A".into()),
            summary
                .median_oos_return_pct
                .map(|s| s.round_dp(2).to_string())
                .unwrap_or_else(|| "N/A".into()),
        );
        match summary.deflated_sharpe {
            Some(d) => println!(
                "Deflated Sharpe (N trials={}): {:.4} {}",
                summary.n_trials,
                d,
                if d >= 0.95 { "✓" } else { "⚠" }
            ),
            None => println!("Deflated Sharpe: N/A"),
        }

        // GATE M2.1 для TSMOM: додатний OOS Sharpe ~0.4–0.8 після витрат.
        if args.strategy == "tsmom" {
            if let Some(med) = summary.median_oos_sharpe {
                if med >= dec!(0.4) {
                    println!("GATE M2.1: OOS Sharpe {med} ≥ 0.4 — движок відтворює TSMOM ✓");
                } else if med > Decimal::ZERO {
                    println!(
                        "GATE M2.1: OOS Sharpe {med} додатний, але < 0.4 — межовий результат, \
                         перевір період/кошик"
                    );
                } else {
                    println!(
                        "GATE M2.1 ПРОВАЛЕНО: OOS Sharpe {med} ≤ 0 — СТОП, шукай баг у движку \
                         (kill-критерій), Фаза 3 не починається"
                    );
                }
            }
        }
        run_metrics["walk_forward"] = serde_json::to_value(&summary)?;
    }

    // Purged CV розбиття (M1.2) — друк структури фолдів для аудиту.
    if args.purged_cv {
        let folds = combinatorial_purged_folds(
            data.len(),
            &PurgedCvConfig {
                n_groups: args.config.walk_forward.folds.max(4),
                n_test_groups: 1,
                label_duration: args.config.walk_forward.label_duration_bars,
                embargo: args.config.walk_forward.embargo_bars,
            },
        );
        println!("\n=== Purged CV structure ===");
        for (i, f) in folds.iter().enumerate() {
            println!(
                "  fold {i}: test groups {:?}, |train|={}, |test|={}",
                f.test_groups,
                f.train.len(),
                f.test.len()
            );
        }
    }

    log_run(&args, run_metrics).await;
    Ok(())
}

#[cfg(test)]
mod funding_ml_tests {
    use super::*;
    use chrono::Duration;
    use db_con::data_ingestion::domain::funding::FundingRatePoint;

    /// Ступінчаста серія: до бару 100 ставка 0.0001, після — 0.0005.
    fn step_series(n: usize) -> Vec<FundingRatePoint> {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        (0..n)
            .map(|i| FundingRatePoint {
                symbol: "T".into(),
                timestamp: t0 + Duration::hours(8 * i as i64),
                rate: if i < 100 { dec!(0.0001) } else { dec!(0.0005) },
                mark_price: dec!(100),
                spot_price: Some(dec!(100)),
            })
            .collect()
    }

    // Лейбл — строго з майбутнього: семпл на t=99 (фічі бачать лише 0.0001)
    // має лейбл 0.0005 (наступні 21 інтервалів уже після стрибка).
    #[test]
    fn label_uses_only_future_features_only_past() {
        let samples = build_funding_samples("T", &step_series(200), 63, 21, None);
        let at_99 = samples.iter().find(|s| {
            s.timestamp
                == Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                    + Duration::hours(8 * 99)
        });
        let s = at_99.expect("sample at t=99 must exist");
        assert_eq!(s.baseline, dec!(0.0001), "фічі не бачать стрибка");
        assert_eq!(s.label, dec!(0.0005), "лейбл — повністю після стрибка");
        // А семпл на t=98: лейбл включає 1 інтервал старої ставки.
        let s98 = samples
            .iter()
            .find(|s| {
                s.timestamp
                    == Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                        + Duration::hours(8 * 98)
            })
            .unwrap();
        assert!(s98.label < dec!(0.0005) && s98.label > dec!(0.0004));
    }

    #[test]
    fn too_short_series_gives_no_samples() {
        assert!(build_funding_samples("T", &step_series(50), 63, 21, None).is_empty());
    }

    // Мета-фічі теж не бачать майбутнього: premium стрибає на барі 100,
    // семпл на t=99 має prem_mean зі старих значень.
    #[test]
    fn meta_features_use_only_past() {
        let series = step_series(200);
        let meta: BTreeMap<DateTime<Utc>, MetaRow> = series
            .iter()
            .enumerate()
            .map(|(i, p)| {
                (
                    p.timestamp,
                    MetaRow {
                        premium: if i < 100 { dec!(0.0001) } else { dec!(0.0009) },
                        volume: dec!(1000),
                        taker_buy_volume: dec!(500),
                    },
                )
            })
            .collect();
        let samples = build_funding_samples("T", &series, 63, 21, Some(&meta));
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let s99 = samples
            .iter()
            .find(|s| s.timestamp == t0 + chrono::Duration::hours(8 * 99))
            .unwrap();
        // prem_mean21 на t=99: усі 21 барів зі старим premium 0.0001 → ×1000 = 0.1.
        assert_eq!(s99.features.get_scalar("prem_mean21"), Some(dec!(0.1)));
        // prem_last теж старий.
        assert_eq!(s99.features.get_scalar("prem_last"), Some(dec!(0.1)));
        // А на t=101 свіжий premium уже видно.
        let s101 = samples
            .iter()
            .find(|s| s.timestamp == t0 + chrono::Duration::hours(8 * 101))
            .unwrap();
        assert_eq!(s101.features.get_scalar("prem_last"), Some(dec!(0.9)));
    }
}
