use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use crate::data_ingestion::adapters::{
    kafka_producer::KafkaProducerAdapter, yahoo_finance::YahooFinanceAdapter,
};
use crate::data_ingestion::domain::services::DataIngestionService;
use crate::data_ingestion::ports::{DataSourcePort, MessageProducerPort};
use crate::shared::config::kafka_brokers;

pub async fn init_ingestion(
    is_simulated: bool,
    symbols: Arc<RwLock<Vec<String>>>,
) -> Result<(Arc<dyn MessageProducerPort>, Arc<dyn DataSourcePort>, Arc<DataIngestionService>)> {
    println!("Waiting for Kafka...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    let producer = Arc::new(KafkaProducerAdapter::new(&kafka_brokers())?) as Arc<dyn MessageProducerPort>;

    let data_source = if is_simulated {
        println!("Using Simulated Data Source (Random Walk)...");
        Arc::new(crate::data_ingestion::adapters::simulated::SimulatedDataSource::new(0.25)) as Arc<dyn DataSourcePort>
    } else {
        Arc::new(YahooFinanceAdapter::new()) as Arc<dyn DataSourcePort>
    };

    let ingestion_service = Arc::new(DataIngestionService::new(
        data_source.clone(),
        producer.clone(),
        symbols,
        "market-data-raw".to_string(),
    ));

    Ok((producer, data_source, ingestion_service))
}
