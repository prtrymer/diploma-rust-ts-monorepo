//! M0.5 — property-тест детермінізму replay.
//!
//! Головна перевага системи (єдиний шлях коду для бектесту і live,
//! інваріант 1) гарантується ТЕСТОМ, а не обіцянкою:
//!
//! Для згенерованого потоку тіків результат прямого бектест-циклу
//! (strategy → execution → broker → portfolio, як у bin/backtest --mode direct)
//! і результат прогону через live-пайплайн повідомлень
//! (StrategyHandler → ExecutionKafkaHandler → BrokerKafkaHandler →
//! BacktestFillCollector на in-memory bus, як у Kafka replay) —
//! ІДЕНТИЧНІ ПОБАЙТОВО.

use chrono::{Duration, TimeZone, Utc};
use proptest::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::sync::RwLock;

use db_con::backtest::domain::report::BacktestReport;
use db_con::data_ingestion::domain::models::StreamMessage;
use db_con::data_ingestion::ports::MessageProducerPort;
use db_con::database::domain::models::StockTick;
use db_con::features::domain::indicators::{
    BollingerBandsFeature, EmaFeature, MacdFeature, MeanReversionFeature, MomentumFeature,
    RsiFeature, VolatilityFeature, VolumeSmaFeature,
};
use db_con::features::domain::registry::FeatureRegistry;
use db_con::message_broker::adapters::in_memory_bus::InMemoryBus;
use db_con::message_broker::ports::MessageHandler;
use db_con::model::domain::adaptive_linear::AdaptiveLinearModel;
use db_con::model::domain::ensemble::WeightedEnsembleModel;
use db_con::model::domain::models::PredictionModel;
use db_con::shared::run_config::{RunConfig, SizingConfig};
use db_con::trading::adapters::broker_handler::BrokerKafkaHandler;
use db_con::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use db_con::trading::adapters::execution_handler::SimpleExecutionHandler;
use db_con::trading::adapters::execution_handler_kafka::ExecutionKafkaHandler;
use db_con::trading::domain::strategies::momentum_strategy::MomentumStrategy;
use db_con::trading::adapters::portfolio_manager::PortfolioManager;
use db_con::trading::adapters::strategy_handler::StrategyHandler;
use db_con::trading::domain::costs::cost_model_from_config;
use db_con::trading::domain::events::FillEvent;
use db_con::trading::domain::sizing::PositionSizer;
use db_con::trading::ports::{
    BrokerSimulatorPort, ExecutionHandlerPort, PortfolioPort, StrategyPort,
};

const LOOKBACK: usize = 12;
const CAPITAL: Decimal = dec!(100000);

fn feature_registry() -> Arc<FeatureRegistry> {
    let mut r = FeatureRegistry::new();
    r.register("rsi_14".into(), Arc::new(RsiFeature::new(14)));
    r.register("ema_12".into(), Arc::new(EmaFeature::new(12)));
    r.register("ema_26".into(), Arc::new(EmaFeature::new(26)));
    r.register("macd".into(), Arc::new(MacdFeature::new()));
    r.register("bbands_20".into(), Arc::new(BollingerBandsFeature::new(20, dec!(2))));
    r.register("volume_sma_20".into(), Arc::new(VolumeSmaFeature::new(20)));
    r.register("momentum_5".into(), Arc::new(MomentumFeature::new(5)));
    r.register("momentum_20".into(), Arc::new(MomentumFeature::new(20)));
    r.register("mean_reversion_20".into(), Arc::new(MeanReversionFeature::new(20)));
    r.register("volatility_20".into(), Arc::new(VolatilityFeature::new(20)));
    Arc::new(r)
}

fn feature_keys() -> Vec<String> {
    vec![
        "ema_12".into(),
        "ema_26".into(),
        "rsi_14".into(),
        "macd".into(),
        "momentum_5".into(),
        "momentum_20".into(),
        "mean_reversion_20".into(),
        "volatility_20".into(),
        "volume_sma_20".into(),
    ]
}

/// Ансамбль тієї ж структури, що й у bin/backtest (фіксовані ваги/сіди).
fn build_model() -> Arc<dyn PredictionModel> {
    let fast = Arc::new(AdaptiveLinearModel::new(feature_keys(), dec!(0.0020)))
        as Arc<dyn PredictionModel>;
    let slow = Arc::new(AdaptiveLinearModel::new(feature_keys(), dec!(0.0005)))
        as Arc<dyn PredictionModel>;
    Arc::new(WeightedEnsembleModel::new_with_signal_deadzone(
        vec![(fast, dec!(0.6)), (slow, dec!(0.4))],
        dec!(0.0005),
        dec!(0.0),
    ))
}

fn build_strategy(min_confidence: Decimal) -> Arc<RwLock<MomentumStrategy>> {
    Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry(),
        build_model(),
        LOOKBACK,
        min_confidence,
        0, // без часового гейту: детермінізм залежить тільки від даних
    )))
}

fn build_broker() -> Arc<dyn BrokerSimulatorPort> {
    let config = RunConfig::default();
    Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.0002),
        cost_model: cost_model_from_config(&config.costs),
    })
}

fn build_execution(
    portfolio: Arc<dyn PortfolioPort>,
    sizing: SizingConfig,
) -> Arc<dyn ExecutionHandlerPort> {
    Arc::new(SimpleExecutionHandler {
        sizer: PositionSizer::from_config(sizing),
        portfolio: Some(portfolio),
    })
}

fn make_ticks(prices: &[f64]) -> Vec<StockTick> {
    let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
    prices
        .iter()
        .enumerate()
        .map(|(i, p)| StockTick {
            symbol: "PROP".to_string(),
            timestamp: t0 + Duration::seconds(60 * i as i64),
            price: Decimal::from_f64_retain(*p).unwrap().round_dp(4),
            volume: 1_000 + (i as i64 % 7) * 100,
            bid: None,
            ask: None,
            source: "prop".to_string(),
        })
        .collect()
}

/// Пряма оркестрація: цикл по тіках, як direct-режим бектесту.
async fn run_direct(ticks: &[StockTick], sizing: SizingConfig) -> (Vec<FillEvent>, BacktestReport) {
    let strategy = build_strategy(dec!(0.0001));
    let portfolio: Arc<dyn PortfolioPort> = Arc::new(PortfolioManager::new(CAPITAL));
    let execution = build_execution(portfolio.clone(), sizing);
    let broker = build_broker();

    let mut fills: Vec<FillEvent> = Vec::new();
    for tick in ticks {
        let signal_opt = {
            let mut s = strategy.write().await;
            s.on_market_event(tick).await.unwrap()
        };
        if let Some(signal) = signal_opt {
            if let Some(order) = execution.on_signal(&signal).await.unwrap() {
                let fill = broker.execute_order(&order).await.unwrap();
                portfolio.update_on_fill(&fill).await.unwrap();
                fills.push(fill);
            }
        }
    }
    let report = BacktestReport::from_fills_and_portfolio(
        &fills,
        &portfolio.get_portfolio().await.unwrap(),
        CAPITAL,
    );
    (fills, report)
}

/// Live-пайплайн: ті САМІ доменні компоненти, але через event-bus хендлери
/// (як Kafka replay), адаптер — InMemoryBus замість Kafka.
async fn run_replay(ticks: &[StockTick], sizing: SizingConfig) -> (Vec<FillEvent>, BacktestReport) {
    let strategy = build_strategy(dec!(0.0001));
    let portfolio: Arc<dyn PortfolioPort> = Arc::new(PortfolioManager::new(CAPITAL));
    let execution = build_execution(portfolio.clone(), sizing);
    let broker = build_broker();

    let bus = Arc::new(InMemoryBus::new());
    let producer = bus.clone() as Arc<dyn MessageProducerPort>;

    let fills = Arc::new(RwLock::new(Vec::<FillEvent>::new()));
    bus.register_handler(Arc::new(StrategyHandler::new(
        strategy.clone() as Arc<RwLock<dyn StrategyPort>>,
        producer.clone(),
        "market".into(),
        "signals".into(),
    )) as Arc<dyn MessageHandler>)
    .await;
    bus.register_handler(Arc::new(ExecutionKafkaHandler::new(
        execution,
        producer.clone(),
        "signals".into(),
        "orders".into(),
    )) as Arc<dyn MessageHandler>)
    .await;
    bus.register_handler(Arc::new(BrokerKafkaHandler::new(
        broker,
        producer.clone(),
        "orders".into(),
        "fills".into(),
    )) as Arc<dyn MessageHandler>)
    .await;
    bus.register_handler(Arc::new(
        db_con::backtest::domain::fill_collector::BacktestFillCollector::new(
            portfolio.clone(),
            fills.clone(),
            "fills".into(),
        ),
    ) as Arc<dyn MessageHandler>)
    .await;

    // Replay: публікація тіків у market-топік (як робить Kafka-loader).
    for tick in ticks {
        producer
            .send_message(StreamMessage {
                topic: "market".into(),
                key: Some(tick.symbol.clone()),
                value: serde_json::to_string(tick).unwrap(),
            })
            .await
            .unwrap();
    }

    let collected = fills.read().await.clone();
    let report = BacktestReport::from_fills_and_portfolio(
        &collected,
        &portfolio.get_portfolio().await.unwrap(),
        CAPITAL,
    );
    (collected, report)
}

fn price_walk_strategy() -> impl Strategy<Value = Vec<f64>> {
    // Випадкова走 ціна: старт 50..150, кроки ±2%, довжина 40..120 тіків.
    (
        50.0f64..150.0,
        proptest::collection::vec(-0.02f64..0.02, 40..120),
    )
        .prop_map(|(start, steps)| {
            let mut p = start;
            let mut out = Vec::with_capacity(steps.len() + 1);
            out.push(p);
            for s in steps {
                p = (p * (1.0 + s)).max(1.0);
                out.push(p);
            }
            out
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    // M0.5: backtest_result == replay_result для кожного згенерованого сценарію.
    #[test]
    fn direct_and_replay_pipelines_are_byte_identical(prices in price_walk_strategy()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let ticks = make_ticks(&prices);
            let sizing = RunConfig::default().sizing;

            let (fills_a, report_a) = run_direct(&ticks, sizing.clone()).await;
            let (fills_b, report_b) = run_replay(&ticks, sizing).await;

            let fills_a_json = serde_json::to_string(&fills_a).unwrap();
            let fills_b_json = serde_json::to_string(&fills_b).unwrap();
            prop_assert_eq!(
                fills_a_json, fills_b_json,
                "філи прямого і replay пайплайнів розійшлись"
            );

            let report_a_json = serde_json::to_string(&report_a).unwrap();
            let report_b_json = serde_json::to_string(&report_b).unwrap();
            prop_assert_eq!(
                report_a_json, report_b_json,
                "звіти прямого і replay пайплайнів розійшлись"
            );
            Ok(())
        })?;
    }

    // Той самий пайплайн двічі → побайтово той самий результат
    // (інваріант 4: детермінізм, включно з ML-ансамблем).
    #[test]
    fn same_input_twice_gives_identical_result(prices in price_walk_strategy()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let ticks = make_ticks(&prices);
            let sizing = RunConfig::default().sizing;
            let (_, r1) = run_direct(&ticks, sizing.clone()).await;
            let (_, r2) = run_direct(&ticks, sizing).await;
            prop_assert_eq!(
                serde_json::to_string(&r1).unwrap(),
                serde_json::to_string(&r2).unwrap()
            );
            Ok(())
        })?;
    }
}

/// Синтетичний потік Long-сигналів зі зростаючою ціною — гарантовано
/// генерує угоди в execution → broker → portfolio ланцюжку.
fn make_signals(n: usize) -> Vec<db_con::trading::domain::events::SignalEvent> {
    use db_con::trading::domain::events::{SignalDirection, SignalEvent};
    let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
    (0..n)
        .map(|i| {
            let price = 100.0 + i as f64;
            SignalEvent {
                id: uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_OID,
                    format!("sig-{i}").as_bytes(),
                ),
                timestamp: t0 + Duration::seconds(60 * i as i64),
                symbol: "PROP".to_string(),
                direction: if i % 4 == 3 {
                    SignalDirection::Exit
                } else {
                    SignalDirection::Long
                },
                strength: dec!(0.8),
                strategy_name: "synthetic".into(),
                metadata: Some(format!(r#"{{"last_price":"{price}"}}"#)),
                market_context: None,
            }
        })
        .collect()
}

/// Негативний контроль: тест ЗОБОВ'ЯЗАНИЙ ловити розсинхрон логіки.
/// Навмисно розсинхронізований параметр виконання у replay-гілці →
/// результати РІЗНІ; той самий параметр → ідентичні.
#[tokio::test]
async fn harness_detects_deliberate_desync() {
    use db_con::backtest::domain::fill_collector::BacktestFillCollector;

    let signals = make_signals(24);
    let honest = RunConfig::default().sizing;
    let mut desynced = honest.clone();
    desynced.max_position_pct = dec!(0.30); // «форк логіки під бектест»

    // Пряма гілка (чесна конфігурація).
    let run_signal_chain_direct = |sizing: SizingConfig| {
        let signals = signals.clone();
        async move {
            let portfolio: Arc<dyn PortfolioPort> = Arc::new(PortfolioManager::new(CAPITAL));
            let execution = build_execution(portfolio.clone(), sizing);
            let broker = build_broker();
            let mut fills = Vec::new();
            for s in &signals {
                if let Some(order) = execution.on_signal(s).await.unwrap() {
                    let fill = broker.execute_order(&order).await.unwrap();
                    portfolio.update_on_fill(&fill).await.unwrap();
                    fills.push(fill);
                }
            }
            fills
        }
    };

    // Replay-гілка через event-bus хендлери.
    let run_signal_chain_replay = |sizing: SizingConfig| {
        let signals = signals.clone();
        async move {
            let portfolio: Arc<dyn PortfolioPort> = Arc::new(PortfolioManager::new(CAPITAL));
            let execution = build_execution(portfolio.clone(), sizing);
            let broker = build_broker();
            let bus = Arc::new(InMemoryBus::new());
            let producer = bus.clone() as Arc<dyn MessageProducerPort>;
            let fills = Arc::new(RwLock::new(Vec::<FillEvent>::new()));
            bus.register_handler(Arc::new(ExecutionKafkaHandler::new(
                execution,
                producer.clone(),
                "signals".into(),
                "orders".into(),
            )) as Arc<dyn MessageHandler>)
            .await;
            bus.register_handler(Arc::new(BrokerKafkaHandler::new(
                broker,
                producer.clone(),
                "orders".into(),
                "fills".into(),
            )) as Arc<dyn MessageHandler>)
            .await;
            bus.register_handler(Arc::new(BacktestFillCollector::new(
                portfolio.clone(),
                fills.clone(),
                "fills".into(),
            )) as Arc<dyn MessageHandler>)
            .await;
            for s in &signals {
                producer
                    .send_message(StreamMessage {
                        topic: "signals".into(),
                        key: Some(s.symbol.clone()),
                        value: serde_json::to_string(s).unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let collected = fills.read().await.clone();
            collected
        }
    };

    let fills_direct = run_signal_chain_direct(honest.clone()).await;
    assert!(!fills_direct.is_empty(), "сценарій має генерувати угоди");

    // Позитивний контроль: однакова конфігурація → побайтово однаково.
    let fills_replay_same = run_signal_chain_replay(honest).await;
    assert_eq!(
        serde_json::to_string(&fills_direct).unwrap(),
        serde_json::to_string(&fills_replay_same).unwrap(),
        "однакова логіка зобов'язана давати однаковий результат"
    );

    // Негативний контроль: розсинхронізована — результати РІЗНІ.
    let fills_replay_desynced = run_signal_chain_replay(desynced).await;
    assert_ne!(
        serde_json::to_string(&fills_direct).unwrap(),
        serde_json::to_string(&fills_replay_desynced).unwrap(),
        "розсинхронізовані пайплайни НЕ мають збігатися — інакше тест нічого не ловить"
    );
}
