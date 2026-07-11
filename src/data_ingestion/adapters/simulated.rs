use crate::data_ingestion::domain::models::{HistoricalQuote, StockQuote};
use crate::data_ingestion::ports::DataSourcePort;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::Rng;
use rand_distr::{Normal, Distribution};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Realistic simulated data source using Geometric Brownian Motion (GBM).
/// Produces proper OHLC candles with guaranteed low ≤ open,close ≤ high.
/// Includes a slight upward drift and intraday volume variation.
pub struct SimulatedDataSource {
    last_prices: Arc<RwLock<HashMap<String, f64>>>,
    /// Annualised daily volatility (e.g. 0.25 = 25% / year).
    annual_volatility: f64,
    /// Annualised drift (e.g. 0.08 = 8% / year upward trend).
    drift: f64,
}

impl SimulatedDataSource {
    pub fn new(annual_volatility: f64) -> Self {
        Self {
            last_prices: Arc::new(RwLock::new(HashMap::new())),
            annual_volatility,
            drift: 0.08,
        }
    }

    fn base_price(symbol: &str) -> f64 {
        match symbol {
            "AAPL"  => 190.0,
            "GOOGL" => 178.0,
            "MSFT"  => 415.0,
            "TSLA"  => 180.0,
            "AMZN"  => 185.0,
            "NVDA"  => 875.0,
            "PFE"   => 28.0,
            _       => 100.0,
        }
    }

    /// Simulate a single GBM step and return a realistic OHLC candle.
    /// `dt` is the fraction of a trading year (252 days) for this bar.
    fn gbm_step(price: f64, drift: f64, vol: f64, dt: f64, rng: &mut impl Rng) -> (f64, f64, f64, f64) {
        let normal = Normal::new(0.0_f64, 1.0_f64).unwrap();

        // Geometric Brownian Motion: S(t+dt) = S(t) * exp((mu - sigma²/2)*dt + sigma*sqrt(dt)*Z)
        let close = price * ((drift - 0.5 * vol * vol) * dt + vol * dt.sqrt() * normal.sample(rng)).exp();

        // Build intra-bar OHLC by simulating two more GBM sub-steps for the wick extremes
        let intra_high = price.max(close) * (1.0 + vol * dt.sqrt() * rng.gen::<f64>() * 0.6);
        let intra_low  = price.min(close) * (1.0 - vol * dt.sqrt() * rng.gen::<f64>() * 0.6);
        // Open is the previous close with a tiny overnight gap
        let open = price * (1.0 + (rng.gen::<f64>() - 0.5) * vol * dt.sqrt() * 0.3);

        // Guarantee OHLC relationship: low ≤ open,close ≤ high
        let high = intra_high.max(open).max(close);
        let low  = intra_low.min(open).min(close);

        (open, high, low, close)
    }

    /// Realistic volume: base * (0.5 + Pareto-like noise)
    fn simulate_volume(base_vol: u64, rng: &mut impl Rng) -> u64 {
        let factor = 0.4 + rng.gen::<f64>() * 1.6; // 0.4x – 2.0x
        ((base_vol as f64 * factor) as u64).max(1_000)
    }
}

#[async_trait]
impl DataSourcePort for SimulatedDataSource {
    async fn fetch_quote(&self, symbol: &str) -> Result<StockQuote> {
        let mut map = self.last_prices.write().await;
        let price = *map.get(symbol).unwrap_or(&Self::base_price(symbol));

        let mut rng = rand::thread_rng();
        // dt for one 10-second tick in trading-year fraction (252 * 6.5 * 3600 seconds / year)
        let dt = 10.0 / (252.0 * 6.5 * 3600.0);
        let (_, _, _, close) = Self::gbm_step(price, self.drift, self.annual_volatility, dt, &mut rng);

        map.insert(symbol.to_string(), close);
        Ok(StockQuote {
            symbol: symbol.to_string(),
            price: close,
            volume: Self::simulate_volume(5_000, &mut rng),
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
        let step = match interval {
            "1m" | "1min"   => chrono::Duration::minutes(1),
            "5m" | "5min"   => chrono::Duration::minutes(5),
            "1h" | "1hour"  => chrono::Duration::hours(1),
            _               => chrono::Duration::days(1),
        };

        // dt as fraction of a trading year
        let seconds_per_year = 252.0 * 6.5 * 3600.0;
        let dt = step.num_seconds() as f64 / seconds_per_year;

        let base_volume: u64 = match interval {
            "1m" | "1min"  => 50_000,
            "5m" | "5min"  => 200_000,
            "1h" | "1hour" => 1_000_000,
            _              => 5_000_000,
        };

        let mut price = Self::base_price(symbol);
        let mut rng = rand::thread_rng();
        let mut quotes = Vec::new();
        let mut current_ts = start;

        while current_ts < end {
            let (open, high, low, close) =
                Self::gbm_step(price, self.drift, self.annual_volatility, dt, &mut rng);
            price = close;

            quotes.push(HistoricalQuote {
                symbol:    symbol.to_string(),
                timestamp: current_ts,
                open,
                high,
                low,
                close,
                adj_close: Some(close),
                volume:    Self::simulate_volume(base_volume, &mut rng),
                timeframe: interval.to_string(),
                source:    "simulation".to_string(),
            });

            current_ts += step;
        }

        Ok(quotes)
    }
}
