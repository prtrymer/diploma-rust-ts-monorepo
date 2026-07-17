/// Latency benchmark for Table 4.3 in the diploma thesis.
/// Measures each pipeline stage over N iterations and reports min/mean/P95 in ms.
/// Stages 1 and 5 connect to real ScyllaDB and Kafka (via docker-compose).
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use uuid::Uuid;

use db_con::bootstrap::features::init_features;
use db_con::bootstrap::models::init_models;
use db_con::data_ingestion::adapters::kafka_producer::KafkaProducerAdapter;
use db_con::data_ingestion::domain::models::StreamMessage;
use db_con::data_ingestion::ports::MessageProducerPort;
use db_con::database::adapters::scylladb::ScyllaRepository;
use db_con::database::domain::models::{Candle, StockTick, Timeframe};
use db_con::database::ports::repository::Repository;
use db_con::features::domain::registry::FeatureRegistry;
use db_con::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use db_con::trading::domain::strategies::momentum_strategy::MomentumStrategy;
use db_con::trading::domain::events::{OrderEvent, OrderSide, OrderType, SignalDirection, SignalEvent};
use db_con::trading::ports::{BrokerSimulatorPort, StrategyPort};

const ITERATIONS: usize = 200;
const LOOKBACK: usize = 10;
const SYMBOL: &str = "BENCH";

// ── helpers ──────────────────────────────────────────────────────────────────

fn percentile(sorted: &[u128], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)] as f64 / 1_000.0
}

fn stats(mut samples: Vec<u128>) -> (f64, f64, f64) {
    samples.sort_unstable();
    let min = samples[0] as f64 / 1_000.0;
    let mean = samples.iter().sum::<u128>() as f64 / samples.len() as f64 / 1_000.0;
    let p95 = percentile(&samples, 95.0);
    (min, mean, p95)
}

fn make_candle(i: usize) -> Candle {
    let base: f64 = 190.0 + (i as f64) * 0.01;
    Candle {
        symbol: SYMBOL.to_string(),
        timestamp: Utc::now(),
        timeframe: Timeframe::OneMin,
        open: Decimal::try_from(base).unwrap(),
        high: Decimal::try_from(base + 0.5).unwrap(),
        low: Decimal::try_from(base - 0.5).unwrap(),
        close: Decimal::try_from(base + 0.1).unwrap(),
        volume: 10_000 + i as i64,
        trades_count: Some(50),
        vwap: None,
    }
}

fn make_tick(i: usize) -> StockTick {
    StockTick {
        symbol: SYMBOL.to_string(),
        timestamp: Utc::now(),
        price: Decimal::try_from(190.0 + i as f64 * 0.01).unwrap(),
        volume: 1_000,
        bid: None,
        ask: None,
        source: "bench".to_string(),
    }
}

fn make_signal(price: f64) -> SignalEvent {
    SignalEvent {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        symbol: SYMBOL.to_string(),
        direction: SignalDirection::Long,
        strength: dec!(0.75),
        strategy_name: "momentum".to_string(),
        metadata: Some(
            serde_json::to_string(&serde_json::json!({ "last_price": price.to_string() }))
                .unwrap(),
        ),
        market_context: None,
    }
}

fn make_order(price: f64) -> OrderEvent {
    OrderEvent {
        id: Uuid::new_v4(),
        signal_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        symbol: SYMBOL.to_string(),
        side: OrderSide::Buy,
        quantity: dec!(10),
        order_type: OrderType::Market,
        limit_price: Some(Decimal::try_from(price).unwrap()),
        stop_price: None,
        market_context: None,
    }
}

// ── Stage 1: ScyllaDB real write ──────────────────────────────────────────────

async fn bench_scylladb(repo: &Arc<dyn Repository>) -> Vec<u128> {
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let tick = make_tick(i);
        let start = Instant::now();
        let _ = repo.insert_stock_tick(&tick).await;
        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── Stage 2: Candle aggregation (in-process) ─────────────────────────────────

async fn bench_candle_aggregation() -> Vec<u128> {
    use db_con::aggregation::models::{get_1min_window, CandleWindow};
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let tick = make_tick(i);
        let start = Instant::now();
        let window_start = get_1min_window(tick.timestamp);
        let mut window = CandleWindow::new(tick.symbol.clone(), window_start);
        window.add_tick(tick.price, tick.volume, tick.timestamp);
        let _complete = window.is_complete();
        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── Stage 3: Feature calculation ─────────────────────────────────────────────

fn bench_feature_calculation(registry: &Arc<FeatureRegistry>, candles: &[Candle]) -> Vec<u128> {
    let mut samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        let _fs = registry.calculate_all(SYMBOL, candles).unwrap();
        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── Stage 4: Model prediction ─────────────────────────────────────────────────

async fn bench_model_prediction(
    registry: &Arc<FeatureRegistry>,
    model: &Arc<dyn db_con::model::domain::models::PredictionModel>,
    candles: &[Candle],
) -> Vec<u128> {
    let mut samples = Vec::with_capacity(ITERATIONS);
    let features = registry.calculate_all(SYMBOL, candles).unwrap();
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        let _pred = model.predict(&features).await.unwrap();
        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── Stage 5: Kafka real publish ───────────────────────────────────────────────

async fn bench_kafka(producer: &KafkaProducerAdapter) -> Vec<u128> {
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let signal = make_signal(190.0 + i as f64 * 0.01);
        let json = serde_json::to_string(&signal).unwrap();
        let msg = StreamMessage {
            topic: "trading-signals".to_string(),
            key: Some(signal.symbol.clone()),
            value: json,
        };
        let start = Instant::now();
        let _ = producer.send_message(msg).await;
        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── Stage 6: BrokerSimulator ──────────────────────────────────────────────────

async fn bench_broker_simulator(broker: &SimpleBrokerSimulator) -> Vec<u128> {
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let order = make_order(190.0 + i as f64 * 0.01);
        let start = Instant::now();
        let _fill = broker.execute_order(&order).await.unwrap();
        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── End-to-end (tick → fill) ──────────────────────────────────────────────────

async fn bench_end_to_end(
    strategy: &Arc<RwLock<dyn StrategyPort>>,
    broker: &SimpleBrokerSimulator,
    repo: &Arc<dyn Repository>,
    producer: &KafkaProducerAdapter,
    registry: &Arc<FeatureRegistry>,
    candles: &[Candle],
) -> Vec<u128> {
    let mut samples = Vec::with_capacity(ITERATIONS);
    // Pre-warm the strategy with a known-signal tick
    {
        let mut strat = strategy.write().await;
        strat.warmup(candles.to_vec()).await.unwrap();
    }

    for i in 0..ITERATIONS {
        let tick = make_tick(i);
        let start = Instant::now();

        // 1. ScyllaDB write
        let _ = repo.insert_stock_tick(&tick).await;

        // 2. Candle aggregation
        use db_con::aggregation::models::{get_1min_window, CandleWindow};
        let ws = get_1min_window(tick.timestamp);
        let mut win = CandleWindow::new(tick.symbol.clone(), ws);
        win.add_tick(tick.price, tick.volume, tick.timestamp);

        // 3+4. Features + model (via strategy)
        let signal_opt = {
            let mut strat = strategy.write().await;
            strat.on_market_event(&tick).await.unwrap()
        };

        // 5. Kafka publish + 6. Broker (when signal is emitted)
        if let Some(ref signal) = signal_opt {
            let json = serde_json::to_string(signal).unwrap();
            let _ = producer
                .send_message(StreamMessage {
                    topic: "trading-signals".to_string(),
                    key: Some(signal.symbol.clone()),
                    value: json,
                })
                .await;

            let order = make_order(190.0 + i as f64 * 0.01);
            let _ = broker.execute_order(&order).await;
        } else {
            // Even without a signal, measure feature+model path for consistency
            let features = registry.calculate_all(SYMBOL, candles).unwrap();
            let _ = db_con::bootstrap::models::init_models(true)
                .predict(&features)
                .await;
        }

        samples.push(start.elapsed().as_micros());
    }
    samples
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("=== Latency Benchmark — Table 4.3 ===");
    println!("Iterations: {ITERATIONS}  |  Lookback: {LOOKBACK}\n");

    // Connect to ScyllaDB
    let scylla_host =
        std::env::var("SCYLLA_HOST").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let nodes: Vec<String> = scylla_host.split(',').map(|s| s.trim().to_string()).collect();
    println!("Connecting to ScyllaDB at {:?}...", nodes);
    let repo: Arc<dyn Repository> =
        Arc::new(ScyllaRepository::new(nodes, "market_data").await?);
    println!("  ✓ ScyllaDB connected\n");

    // Connect to Kafka
    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    println!("Connecting to Kafka at {}...", kafka_brokers);
    let producer = KafkaProducerAdapter::new(&kafka_brokers)?;
    println!("  ✓ Kafka producer ready\n");

    // Build shared ML components
    let registry = init_features();
    let model = init_models(true);
    let candles: Vec<Candle> = (0..LOOKBACK).map(make_candle).collect();

    let strategy = Arc::new(RwLock::new(MomentumStrategy::new(
        registry.clone(),
        model.clone(),
        LOOKBACK,
        dec!(0.01),
        1,
    ))) as Arc<RwLock<dyn StrategyPort>>;
    {
        let mut strat = strategy.write().await;
        strat.warmup(candles.clone()).await?;
    }

    let broker = SimpleBrokerSimulator {
        slippage_pct: dec!(0.001),
        cost_model: db_con::trading::domain::costs::cost_model_from_config(
            &db_con::shared::run_config::RunConfig::default().costs,
        ),
    };

    // ── Run each stage ──
    println!("Stage 1: ScyllaDB write (tick record)...");
    let s1 = bench_scylladb(&repo).await;
    let r1 = stats(s1);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", r1.0, r1.1, r1.2);

    println!("Stage 2: Tick → 1-min candle aggregation...");
    let s2 = bench_candle_aggregation().await;
    let r2 = stats(s2);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", r2.0, r2.1, r2.2);

    println!("Stage 3: 13 indicators (FeatureRegistry)...");
    let s3 = bench_feature_calculation(&registry, &candles);
    let r3 = stats(s3);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", r3.0, r3.1, r3.2);

    println!("Stage 4: WeightedEnsembleModel prediction...");
    let s4 = bench_model_prediction(&registry, &model, &candles).await;
    let r4 = stats(s4);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", r4.0, r4.1, r4.2);

    println!("Stage 5: Signal → Kafka (trading-signals)...");
    let s5 = bench_kafka(&producer).await;
    let r5 = stats(s5);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", r5.0, r5.1, r5.2);

    println!("Stage 6: BrokerSimulator order execution...");
    let s6 = bench_broker_simulator(&broker).await;
    let r6 = stats(s6);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", r6.0, r6.1, r6.2);

    println!("End-to-end (tick → fill, all stages)...");
    let se = bench_end_to_end(&strategy, &broker, &repo, &producer, &registry, &candles).await;
    let re = stats(se);
    println!("  min={:.3} ms  mean={:.3} ms  P95={:.3} ms\n", re.0, re.1, re.2);

    // ── Final table ──
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║      Таблиця 4.3 — Показники затримки обробки (мілісекунди)         ║");
    println!("╠══════════════════════════════════════════════╦════════╦════════╦══════╣");
    println!("║  Стадія обробки                              ║  Мін.  ║ Серед. ║  P95 ║");
    println!("╠══════════════════════════════════════════════╬════════╬════════╬══════╣");

    // For sub-ms stages, show in µs with a note
    let fmt = |v: f64| -> String {
        if v < 0.01 {
            "<0.01".to_string()
        } else {
            format!("{:.3}", v)
        }
    };

    let rows: Vec<(&str, f64, f64, f64)> = vec![
        ("Отримання тіку --> ScyllaDB (запис)", r1.0, r1.1, r1.2),
        ("Агрегація тіку --> свічка 1min", r2.0, r2.1, r2.2),
        ("Обчислення 13 індикаторів (FeatureRegistry)", r3.0, r3.1, r3.2),
        ("Прогноз WeightedEnsembleModel", r4.0, r4.1, r4.2),
        ("Публікація сигналу у Kafka (trading-signals)", r5.0, r5.1, r5.2),
        ("Виконання ордеру (BrokerSimulator)", r6.0, r6.1, r6.2),
        ("Наскрізна затримка (тік --> fill)", re.0, re.1, re.2),
    ];

    for (name, mn, av, p95) in &rows {
        println!("║  {:<44}  ║ {:>6} ║ {:>6} ║{:>6}║", name, fmt(*mn), fmt(*av), fmt(*p95));
    }
    println!("╚══════════════════════════════════════════════╩════════╩════════╩══════╝");
    println!();
    println!("Примітка: n={ITERATIONS} ітерацій. ScyllaDB і Kafka — локальний docker-compose.");
    println!("Стадії 2, 3, 4, 6 — in-process Rust/Tokio (без мережі), <0.01 мс ≈ <10 мкс.");

    Ok(())
}
