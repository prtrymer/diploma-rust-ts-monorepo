use std::sync::Arc;
use std::collections::VecDeque;
use tokio::sync::{RwLock, broadcast};
use anyhow::Result;
use crate::database::ports::repository::Repository;
use crate::data_ingestion::ports::MessageProducerPort;
use crate::message_broker::adapters::kafka_consumer::KafkaConsumerAdapter;
use crate::message_broker::adapters::market_data_handler::MarketDataHandler;
use crate::message_broker::domain::services::MessageBrokerService;
use crate::message_broker::ports::MessageHandler;
use crate::trading::adapters::broker_handler::BrokerKafkaHandler;
use crate::trading::adapters::execution_handler_kafka::ExecutionKafkaHandler;
use crate::trading::adapters::fill_handler::FillKafkaHandler;
use crate::trading::adapters::strategy_handler::StrategyHandler;
use crate::trading::ports::{BrokerSimulatorPort, ExecutionHandlerPort, PortfolioPort, StrategyPort};
use crate::http::handlers::signals::HttpSignalHandler;
use crate::shared::config::kafka_brokers;

pub struct MessageBrokerWiring {
    pub svc_market: MessageBrokerService,
    pub svc_strategy: MessageBrokerService,
    pub svc_execution: MessageBrokerService,
    pub svc_broker: MessageBrokerService,
    pub svc_portfolio: MessageBrokerService,
    pub svc_http_signals: MessageBrokerService,
    pub signals_tx: broadcast::Sender<String>,
    pub recent_signals: Arc<RwLock<VecDeque<String>>>,
}

pub fn wire_message_broker(
    repository: Arc<dyn Repository>,
    producer: Arc<dyn MessageProducerPort>,
    strategy: Arc<RwLock<dyn StrategyPort>>,
    execution_handler: Arc<dyn ExecutionHandlerPort>,
    portfolio_manager: Arc<dyn PortfolioPort>,
    broker: Arc<dyn BrokerSimulatorPort>,
) -> Result<MessageBrokerWiring> {
    let brokers = kafka_brokers();

    // --- Kafka Consumers (each with unique consumer group) ---
    let consumer_market =
        Arc::new(KafkaConsumerAdapter::new(&brokers, "market-data-persist-group", &["market-data-raw".to_string()])?);

    let consumer_strategy =
        Arc::new(KafkaConsumerAdapter::new(&brokers, "strategy-group", &["market-data-raw".to_string()])?);

    let consumer_execution =
        Arc::new(KafkaConsumerAdapter::new(&brokers, "execution-group", &["trading-signals".to_string()])?);

    let consumer_broker =
        Arc::new(KafkaConsumerAdapter::new(&brokers, "broker-group", &["trading-orders".to_string()])?);

    let consumer_portfolio =
        Arc::new(KafkaConsumerAdapter::new(&brokers, "portfolio-group", &["trading-fills".to_string()])?);

    let consumer_http_signals =
        Arc::new(KafkaConsumerAdapter::new(&brokers, "http-signals-group", &["trading-signals".to_string()])?);

    // --- Message Handlers ---
    let market_data_handler =
        Arc::new(MarketDataHandler::new(repository.clone())) as Arc<dyn MessageHandler>;

    let strategy_handler = Arc::new(StrategyHandler::new(
        strategy.clone(),
        producer.clone(),
        "market-data-raw".to_string(),
        "trading-signals".to_string(),
    )) as Arc<dyn MessageHandler>;

    let execution_kafka_handler = Arc::new(ExecutionKafkaHandler::new(
        execution_handler,
        producer.clone(),
        "trading-signals".to_string(),
        "trading-orders".to_string(),
    )) as Arc<dyn MessageHandler>;

    let broker_kafka_handler = Arc::new(BrokerKafkaHandler::new(
        broker,
        producer.clone(),
        "trading-orders".to_string(),
        "trading-fills".to_string(),
    )) as Arc<dyn MessageHandler>;

    let fill_kafka_handler = Arc::new(FillKafkaHandler::new(
        portfolio_manager.clone(),
        "trading-fills".to_string(),
    )) as Arc<dyn MessageHandler>;

    let (signals_tx, _) = broadcast::channel(100);
    let recent_signals = Arc::new(RwLock::new(VecDeque::new()));

    let http_signal_handler = Arc::new(HttpSignalHandler {
        tx: signals_tx.clone(),
        recent_signals: recent_signals.clone(),
        topic: "trading-signals".to_string(),
    }) as Arc<dyn MessageHandler>;

    // --- Wire Broker Services ---
    let mut svc_market = MessageBrokerService::new(consumer_market);
    svc_market.register_handler(market_data_handler);

    let mut svc_strategy = MessageBrokerService::new(consumer_strategy);
    svc_strategy.register_handler(strategy_handler);

    let mut svc_execution = MessageBrokerService::new(consumer_execution);
    svc_execution.register_handler(execution_kafka_handler);

    let mut svc_broker = MessageBrokerService::new(consumer_broker);
    svc_broker.register_handler(broker_kafka_handler);

    let mut svc_portfolio = MessageBrokerService::new(consumer_portfolio);
    svc_portfolio.register_handler(fill_kafka_handler);

    let mut svc_http_signals = MessageBrokerService::new(consumer_http_signals);
    svc_http_signals.register_handler(http_signal_handler);

    Ok(MessageBrokerWiring {
        svc_market,
        svc_strategy,
        svc_execution,
        svc_broker,
        svc_portfolio,
        svc_http_signals,
        signals_tx,
        recent_signals,
    })
}
