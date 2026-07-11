use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::{DataSourcePort, MessageProducerPort};
use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// Domain service for data ingestion
pub struct DataIngestionService {
    data_source: Arc<dyn DataSourcePort>,
    producer: Arc<dyn MessageProducerPort>,
    symbols: Arc<RwLock<Vec<String>>>,
    last_sent_by_symbol: Arc<RwLock<HashMap<String, LastSentQuote>>>,
    topic: String,
}

#[derive(Clone)]
struct LastSentQuote {
    price_bits: u64,
    volume: u64,
    timestamp: chrono::DateTime<Utc>,
}

impl DataIngestionService {
    pub fn new(
        data_source: Arc<dyn DataSourcePort>,
        producer: Arc<dyn MessageProducerPort>,
        symbols: Arc<RwLock<Vec<String>>>,
        topic: String,
    ) -> Self {
        Self {
            data_source,
            producer,
            symbols,
            last_sent_by_symbol: Arc::new(RwLock::new(HashMap::new())),
            topic,
        }
    }

    /// Start streaming data at specified interval
    pub async fn start_streaming(&self, interval_secs: u64) -> Result<()> {
        let mut ticker = interval(Duration::from_secs(interval_secs));

        println!("📈 Starting data ingestion service...");

        loop {
            ticker.tick().await;

            let symbols = self.symbols.read().await.clone();

            if symbols.is_empty() {
                continue;
            }

            for symbol in symbols {
                let data_source = self.data_source.clone();
                let producer = self.producer.clone();
                let last_sent_by_symbol = self.last_sent_by_symbol.clone();
                let topic = self.topic.clone();
                
                let svc = DataIngestionService {
                    data_source,
                    producer,
                    symbols: Arc::new(RwLock::new(vec![])),
                    last_sent_by_symbol,
                    topic,
                };
                
                match svc.fetch_and_send(&symbol).await {
                    Ok(true) => println!("✓ Sent quote for {}", symbol),
                    Ok(false) => println!("↷ Skipped duplicate quote for {}", symbol),
                    Err(e) => eprintln!("❌ Error processing {}: {}", symbol, e),
                }
                
                // Add a small delay between symbol fetches to avoid triggering Yahoo Finance rate limits
                tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            }
        }
    }

    fn normalize_quote_timestamp(
        &self,
        mut quote: crate::data_ingestion::domain::models::StockQuote,
    ) -> crate::data_ingestion::domain::models::StockQuote {
        const MAX_PROVIDER_STALENESS_SECS: i64 = 5 * 60;
        const MAX_FUTURE_SKEW_SECS: i64 = 30;

        let now = Utc::now();
        let age_secs = (now - quote.timestamp).num_seconds();
        let future_skew_secs = (quote.timestamp - now).num_seconds();

        if age_secs > MAX_PROVIDER_STALENESS_SECS || future_skew_secs > MAX_FUTURE_SKEW_SECS {
            quote.timestamp = now;
        }

        quote
    }

    async fn fetch_and_send(&self, symbol: &str) -> Result<bool> {
        // Fetch from data source
        let raw_quote = self.data_source.fetch_quote(symbol).await?;
        let quote = self.normalize_quote_timestamp(raw_quote);

        let mut map = self.last_sent_by_symbol.write().await;
        let now = Utc::now();
        if let Some(last) = map.get(symbol) {
            let is_duplicate = quote.price.to_bits() == last.price_bits && quote.volume == last.volume;
            let time_since_last = (now - last.timestamp).num_seconds();
            
            // Allow duplicate if it's been more than 5 minutes (heartbeat)
            if is_duplicate && time_since_last < 300 {
                return Ok(false);
            }
        }
        map.insert(
            symbol.to_string(),
            LastSentQuote {
                price_bits: quote.price.to_bits(),
                volume: quote.volume,
                timestamp: now,
            },
        );
        drop(map);

        // Convert to JSON
        let json = serde_json::to_string(&quote)?;

        // Create message
        let message = StreamMessage {
            topic: self.topic.clone(),
            key: Some(symbol.to_string()),
            value: json,
        };

        // Send to Kafka
        self.producer.send_message(message).await?;
        Ok(true)
    }
}
