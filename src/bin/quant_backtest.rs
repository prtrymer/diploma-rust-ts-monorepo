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
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use db_con::backtest::application::benchmark_runner::BenchmarkRunner;
use db_con::backtest::application::quant::{
    build_strategy, load_csv_dir, log_run, run_carry, run_funding_ml, run_ml_match,
    run_shadow_carry, run_xs_carry, QuantArgs,
};
use db_con::backtest::application::walk_forward::{
    evaluate_tsmom_gate, GateVerdict, WalkForwardRunner, DSR_SIGNIFICANCE_THRESHOLD,
};
use db_con::backtest::domain::purged_cv::{combinatorial_purged_folds, PurgedCvConfig};
use db_con::database::adapters::run_logger_file::FileRunLogger;
use db_con::database::ports::run_logger::RunLogger;
use db_con::shared::run_config::RunConfig;
use db_con::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use db_con::trading::adapters::portfolio_manager::PortfolioManager;
use db_con::trading::domain::costs::cost_model_from_config;
use db_con::trading::ports::{BrokerSimulatorPort, PortfolioPort};

fn parse_args() -> Result<QuantArgs> {
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
    let mut runs_log = PathBuf::from(QuantArgs::DEFAULT_RUNS_LOG);
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
            // Вимикає фільтр спот-ноги. Потрібен лише для порівняння зі
            // старими числами: без нього кошик набирається з перпів, які
            // нічим хеджувати, і результат не є торговим. Йде в config_hash,
            // тож такі прогони не змішуються з фільтрованими в журналі.
            "--no-spot-filter" => {
                config.universe.require_spot_leg = false;
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
            "--runs-log" => {
                if let Some(v) = argv.get(i + 1) {
                    runs_log = PathBuf::from(v);
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

    Ok(QuantArgs {
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
        runs_log,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    // Логи бібліотечного коду → stderr (default: warn, керується RUST_LOG),
    // щоб stdout лишався чистим звітом CLI.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
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

    // Джерело даних НЕ входить у config_hash (конфіг описує параметри
    // стратегії, не юніверс), тому той самий tsmom по ETF і по крипті дає
    // однаковий хеш. Без цього поля вони злилися б в одну «спробу» в
    // trial_sharpes, і пізніший прогін затирав би ранішній. Це різні
    // гіпотези про те, де живе ефект, і рахуватись мусять окремо.
    let mut run_metrics = serde_json::json!({
        "strategy": args.strategy,
        "dataset": args.dataset_label(),
        "comparative": serde_json::to_value(&report)?,
    });

    // Walk-forward OOS (M1.1) + deflated Sharpe (M1.3).
    let mut m21_gate_failed = false;
    if args.walk_forward {
        // N trials і σ(SR) для deflated Sharpe — по перебраних конфігураціях
        // ТІЄЇ Ж стратегії. Раніше бралась кількість усіх конфігів у журналі,
        // тож TSMOM карався за гіпотези carry і funding_ml.
        let prior_trials = FileRunLogger::new(&args.runs_log)
            .trial_sharpes(&args.strategy)
            .await
            .unwrap_or_default();
        let portfolio: Arc<dyn PortfolioPort> = if allow_short {
            Arc::new(PortfolioManager::new_allowing_short(args.capital))
        } else {
            Arc::new(PortfolioManager::new(args.capital))
        };
        let wf = WalkForwardRunner::new(data.clone(), broker.clone(), portfolio, args.capital);
        let args_ref = &args;
        let symbols_ref = symbols.clone();
        let factory = move || build_strategy(args_ref, symbols_ref.clone()).0;
        let summary = wf
            .run(&factory, &args.config.walk_forward, &prior_trials)
            .await?;

        println!("\n=== Walk-Forward (OOS only, net-of-cost) ===");
        for f in &summary.folds {
            println!(
                "  test [{}, {}): ret {}% | sharpe {} | mdd {}% | trades {} | costs {}{}",
                f.fold.test_start,
                f.fold.test_end,
                f.oos_return_pct.round_dp(2),
                f.oos_sharpe.round_dp(2),
                f.oos_max_drawdown_pct.round_dp(2),
                f.oos_trades,
                f.oos_total_costs.round_dp(2),
                if f.oos_trades == 0 {
                    "  ← без угод, поза медіаною"
                } else {
                    ""
                }
            );
        }
        println!(
            "Median OOS Sharpe: {} | Median OOS Return: {}%  (по {} з {} фолдів)",
            summary
                .median_oos_sharpe
                .map(|s| s.round_dp(3).to_string())
                .unwrap_or_else(|| "N/A".into()),
            summary
                .median_oos_return_pct
                .map(|s| s.round_dp(2).to_string())
                .unwrap_or_else(|| "N/A".into()),
            summary.evaluated_folds,
            summary.folds.len(),
        );
        if summary.empty_folds > 0 {
            println!(
                "  ⚠ {} фолд(ів) без жодної угоди виключено з медіан — у цих вікнах \
                 стратегія не давала сигналів, це не Sharpe 0",
                summary.empty_folds
            );
        }
        // Довідкове число, не умова гейта: поріг 0.95 недосяжний для явища
        // силою Sharpe 0.4–0.8 на ~650 OOS-днях (треба 1000+ навіть за нульового
        // розкиду). Друкуємо разом із входами, щоб його можна було пояснити.
        match summary.deflated_sharpe {
            Some(d) => println!(
                "Deflated Sharpe (довідково): {:.4} {} | спроб: {}, розкид по спробах: {}",
                d,
                if d >= DSR_SIGNIFICANCE_THRESHOLD {
                    "✓"
                } else {
                    "⚠"
                },
                summary.n_trials,
                summary
                    .trial_dispersion
                    .map(|v| format!("{v:.3}"))
                    .unwrap_or_else(|| "—".into()),
            ),
            None => println!("Deflated Sharpe: N/A"),
        }

        // GATE M2.1 для TSMOM — три структурні умови, які на наявному обсязі
        // даних перевірити можна: движок бачить ефект, бачив його на всьому
        // періоді, і результат не тримається на одному вдалому відрізку.
        if args.strategy == "tsmom" {
            let verdict = evaluate_tsmom_gate(&summary);
            let med = summary
                .median_oos_sharpe
                .map(|m| m.round_dp(3).to_string())
                .unwrap_or_else(|| "N/A".into());
            let (ok, tot) = (summary.positive_folds, summary.evaluated_folds);
            match verdict {
                GateVerdict::Passed => println!(
                    "GATE M2.1 ПРОЙДЕНО ✓: OOS Sharpe {med} ≥ 0.4, жодного порожнього відрізка, \
                     {ok} з {tot} відрізків у плюсі — движок відтворює TSMOM на всьому періоді"
                ),
                GateVerdict::BlindWindow => println!(
                    "GATE M2.1 ПРОВАЛЕНО: {} відрізк(ів) без жодної угоди. Медіана {med} описує \
                     лише ті вікна, де стратегія була зряча, а не весь період — спершу розберись, \
                     чому вона там мовчала (типова причина: long_only на падінні), і лише потім \
                     дивись на число",
                    summary.empty_folds
                ),
                GateVerdict::Unstable => println!(
                    "GATE M2.1 ПРОВАЛЕНО: лише {ok} з {tot} відрізків у плюсі — медіану {med} \
                     витягнули один-два вдалі періоди, стійкості немає"
                ),
                GateVerdict::BelowRange => println!(
                    "GATE M2.1 ПРОВАЛЕНО: OOS Sharpe {med} додатний, але < 0.4 — движок бачить \
                     тренд слабше за документований ефект, перевір період/кошик"
                ),
                GateVerdict::EngineFailure => println!(
                    "GATE M2.1 ПРОВАЛЕНО: OOS Sharpe {med} ≤ 0 — СТОП, шукай баг у движку \
                     (kill-критерій), Фаза 3 не починається"
                ),
                // Мовчання тут читалося б як «пройдено»: гейт без жодного
                // оціненого фолда не пройдений, він неоцінений.
                GateVerdict::NotEvaluated => println!(
                    "GATE M2.1 НЕ ОЦІНЕНО: жоден OOS-фолд не містить угод — Sharpe нема з чого \
                     рахувати, гейт не пройдено"
                ),
            }
            m21_gate_failed = !verdict.is_passed();
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

    // Прогін логується ЗАВЖДИ, навіть коли гейт провалено (M0.4: провенанс не
    // залежить від результату) — і лише після цього ненульовий вихід.
    log_run(&args, run_metrics).await;

    // Kill-критерій M2.1: провалений гейт валить процес, щоб CI/research
    // ставали червоними, а не ховали вердикт у хвості логу.
    anyhow::ensure!(
        !m21_gate_failed,
        "GATE M2.1 не пройдено — Фаза 3 закрита до виправлення (див. вердикт вище)"
    );
    Ok(())
}
