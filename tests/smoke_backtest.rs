//! Smoke-бектест на фікстурному датасеті (наскрізна задача: CI).
//!
//! Ганяє повний портфельний стек (стратегія + бенчмарки + walk-forward)
//! на детермінованому синтетичному кошику. Валить білд, якщо:
//!   - витрати перестали вираховуватись (інваріант 2, M0.1);
//!   - бенчмарки зникли зі звіту (M0.2);
//!   - walk-forward вікна зламались (M1.1);
//!   - двигун перестав бути детермінованим (інваріант 4);
//!   - движок перестав відтворювати тренд на синтетиці (GATE M2.1).

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use db_con::backtest::application::benchmark_runner::BenchmarkRunner;
use db_con::backtest::application::walk_forward::WalkForwardRunner;
use db_con::shared::run_config::RunConfig;
use db_con::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use db_con::trading::adapters::portfolio_manager::PortfolioManager;
use db_con::trading::adapters::tsmom_strategy::TsmomStrategy;
use db_con::trading::domain::allocation::{AllocationStrategy, AlignedMarketData};
use db_con::trading::domain::costs::cost_model_from_config;
use db_con::trading::ports::{BrokerSimulatorPort, PortfolioPort};

fn load_fixture_dir(dir: &Path) -> Arc<AlignedMarketData> {
    let mut closes: BTreeMap<String, Vec<Decimal>> = BTreeMap::new();
    let mut volumes: BTreeMap<String, Vec<Decimal>> = BTreeMap::new();
    let mut timestamps: Vec<DateTime<Utc>> = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .expect("fixtures dir")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("csv"))
        .collect();
    entries.sort();

    for (file_idx, path) in entries.iter().enumerate() {
        let sym = path.file_stem().unwrap().to_str().unwrap().to_string();
        let content = std::fs::read_to_string(path).unwrap();
        let mut c = Vec::new();
        let mut v = Vec::new();
        for (i, line) in content.lines().enumerate() {
            if i == 0 {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            let ts = parts[0]
                .parse::<DateTime<Utc>>()
                .ok()
                .or_else(|| {
                    NaiveDate::parse_from_str(parts[0], "%Y-%m-%d")
                        .ok()
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                        .map(|dt| Utc.from_utc_datetime(&dt))
                })
                .unwrap();
            if file_idx == 0 {
                timestamps.push(ts);
            }
            c.push(parts[1].parse::<Decimal>().unwrap());
            v.push(parts[2].parse::<Decimal>().unwrap());
        }
        closes.insert(sym.clone(), c);
        volumes.insert(sym, v);
    }

    Arc::new(AlignedMarketData::new(timestamps, closes.clone(), closes, volumes).unwrap())
}

fn fixture_data() -> Arc<AlignedMarketData> {
    load_fixture_dir(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/daily"
    )))
}

fn broker() -> Arc<dyn BrokerSimulatorPort> {
    Arc::new(SimpleBrokerSimulator {
        slippage_pct: Decimal::ZERO,
        cost_model: cost_model_from_config(&RunConfig::default().costs),
    })
}

fn tsmom() -> TsmomStrategy {
    TsmomStrategy::new(60, 20, dec!(0.20), 21, true)
}

// CI-регресія M0.1/M0.2: бенчмарки поруч, витрати вираховані, net ≤ gross.
#[tokio::test]
async fn smoke_comparative_run_on_fixture() {
    let data = fixture_data();
    let runner = BenchmarkRunner::new(data.clone(), broker(), dec!(100000));
    let mut strategy = tsmom();
    let report = runner
        .run_with_benchmarks(&mut strategy, true, 0, data.len())
        .await
        .expect("smoke run");

    // M0.2: всі три бенчмарки присутні.
    let names: Vec<&str> = report.benchmarks.iter().map(|b| b.name.as_str()).collect();
    assert!(names.contains(&"buy_and_hold"));
    assert!(names.contains(&"equal_weight"));
    assert!(names.contains(&"sixty_forty"));

    // M0.1 (інваріант 2): витрати увімкнені й вираховані з net.
    for row in std::iter::once(&report.strategy).chain(report.benchmarks.iter()) {
        assert!(
            row.report.total_costs > Decimal::ZERO,
            "{}: витрати мають бути > 0",
            row.name
        );
        assert_eq!(
            row.report.gross_return - row.report.total_costs,
            row.report.total_return,
            "{}: gross − costs ≠ net",
            row.name
        );
    }
}

// CI-регресія M1.1 + інваріант 4: walk-forward детермінований від прогону до прогону.
#[tokio::test]
async fn smoke_walk_forward_is_deterministic() {
    let data = fixture_data();
    let run_once = || async {
        let portfolio: Arc<dyn PortfolioPort> =
            Arc::new(PortfolioManager::new_allowing_short(dec!(100000)));
        let wf = WalkForwardRunner::new(data.clone(), broker(), portfolio, dec!(100000));
        let factory = || Box::new(tsmom()) as Box<dyn AllocationStrategy>;
        wf.run(&factory, &RunConfig::default().walk_forward, 1)
            .await
            .expect("wf run")
    };
    let a = run_once().await;
    let b = run_once().await;
    assert!(!a.folds.is_empty());
    assert_eq!(
        serde_json::to_string(&a).unwrap(),
        serde_json::to_string(&b).unwrap(),
        "walk-forward має бути побайтово відтворюваним"
    );
}

// CI-регресія GATE M2.1: на синтетиці з вбудованими трендами TSMOM мусить давати
// додатний OOS Sharpe (фактично ~3.9 при порозі гейта 0.4 — десятикратний запас).
// Рядок «GATE M2.1 ✓» у виводі бінарника — інформаційний println без впливу на
// exit code, тож запобіжником від «движок перестав бачити тренд» є саме цей assert.
#[tokio::test]
async fn smoke_tsmom_gate_holds_on_fixture() {
    let data = fixture_data();
    let portfolio: Arc<dyn PortfolioPort> =
        Arc::new(PortfolioManager::new_allowing_short(dec!(100000)));
    let wf = WalkForwardRunner::new(data.clone(), broker(), portfolio, dec!(100000));
    let factory = || Box::new(tsmom()) as Box<dyn AllocationStrategy>;
    let summary = wf
        .run(&factory, &RunConfig::default().walk_forward, 1)
        .await
        .expect("wf run");
    let med = summary
        .median_oos_sharpe
        .expect("walk-forward на фікстурі має дати OOS-фолди з Sharpe");
    assert!(
        med >= dec!(0.4),
        "GATE M2.1 на фікстурі: median OOS Sharpe {med} < 0.4 — \
         движок перестав відтворювати тренд, шукай баг у движку"
    );
}

// CI-регресія M0.1: нульова конфігурація витрат → net == gross на повному стеку.
#[tokio::test]
async fn smoke_zero_costs_net_equals_gross() {
    use db_con::shared::run_config::CostConfig;
    let data = fixture_data();
    let zero_broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: Decimal::ZERO,
        cost_model: cost_model_from_config(&CostConfig::zero()),
    }) as Arc<dyn BrokerSimulatorPort>;
    let runner = BenchmarkRunner::new(data.clone(), zero_broker, dec!(100000));
    let mut strategy = tsmom();
    let report = runner
        .run_with_benchmarks(&mut strategy, true, 0, data.len())
        .await
        .unwrap();
    assert_eq!(report.strategy.report.total_costs, Decimal::ZERO);
    assert_eq!(
        report.strategy.report.total_return,
        report.strategy.report.gross_return
    );
}
