use crate::data_ingestion::domain::models::{HistoricalQuote, StockQuote};
use crate::data_ingestion::ports::DataSourcePort;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A simulated data source that generates random walk prices.
/// Useful for testing the trading pipeline when real-time markets are closed
/// or when the provider is rate-limited.
pub struct SimulatedDataSource {
    last_prices: Arc<RwLock<HashMap<String, f64>>>,
    volatility: f64,
}

impl SimulatedDataSource {
    pub fn new(volatility: f64) -> Self {
        Self {
            last_prices: Arc::new(RwLock::new(HashMap::new())),
            volatility,
        }
    }

    async fn get_base_price(&self, symbol: &str) -> f64 {
        match symbol {
            "AAPL" => 190.0,
            "GOOGL" => 150.0,
            "MSFT" => 410.0,
            "TSLA" => 180.0,
            "AMZN" => 175.0,
            _ => 100.0,
        }
    }
}

#[async_trait]
impl DataSourcePort for SimulatedDataSource {
    async fn fetch_quote(&self, symbol: &str) -> Result<StockQuote> {
        let mut map = self.last_prices.write().await;
        
        let current_price = if let Some(last) = map.get(symbol) {
            *last
        } else {
            let base = self.get_base_price(symbol).await;
            map.insert(symbol.to_string(), base);
            base
        };

        // Random walk: price = price * (1 + random_change * volatility)
        let mut rng = rand::thread_rng();
        let change = (rng.gen::<f64>() - 0.5) * 2.0 * self.volatility;
        let new_price = current_price * (1.0 + change);
        
        map.insert(symbol.to_string(), new_price);

        Ok(StockQuote {
            symbol: symbol.to_string(),
            price: new_price,
            volume: rng.gen_range(1000..10000),
            timestamp: Utc::now(),
        })
    }

    async fn fetch_historical_quotes(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<HistoricalQuote>> {
        let mut quotes = Vec::new();
        let mut current_ts = start;
        let step = match interval {
            "1m" | "1min" => chrono::Duration::minutes(1),
            "5m" | "5min" => chrono::Duration::minutes(5),
            "1h" | "1hour" => chrono::Duration::hours(1),
            _ => chrono::Duration::days(1),
        };

        let mut price = self.get_base_price(symbol).await;
        let mut rng = rand::thread_rng();

        while current_ts < end {
            let change = (rng.gen::<f64>() - 0.5) * 2.0 * self.volatility;
            price *= 1.0 + change;
            
            let high = price * (1.0 + rng.gen::<f64>() * self.volatility);
            let low = price * (1.0 - rng.gen::<f64>() * self.volatility);
            let open = price * (1.0 + (rng.gen::<f64>() - 0.5) * self.volatility);

            quotes.push(HistoricalQuote {
                symbol: symbol.to_string(),
                timestamp: current_ts,
                open,
                high,
                low,
                close: price,
                adj_close: Some(price),
                volume: rng.gen_range(10000..100000),
                timeframe: interval.to_string(),
                source: "simulation".to_string(),
            });

            current_ts = current_ts + step;
        }

        Ok(quotes)
    }
}
