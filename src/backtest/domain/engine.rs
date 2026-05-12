use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::loader::HistoricalDataLoader;
use super::report::BacktestReport;
use crate::trading::domain::events::FillEvent;
use crate::trading::ports::PortfolioPort;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub symbol: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub topic: String,
}

pub struct BacktestEngine {
    loader: Arc<dyn HistoricalDataLoader>,
    portfolio: Arc<dyn PortfolioPort>,
    fills: Arc<RwLock<Vec<FillEvent>>>,
    config: BacktestConfig,
}

impl BacktestEngine {
    pub fn new(
        loader: Arc<dyn HistoricalDataLoader>,
        portfolio: Arc<dyn PortfolioPort>,
        fills: Arc<RwLock<Vec<FillEvent>>>,
        config: BacktestConfig,
    ) -> Self {
        Self {
            loader,
            portfolio,
            fills,
            config,
        }
    }

    pub async fn run(&self) -> Result<BacktestReport> {
        println!(
            "Starting backtest for {} from {} to {}",
            self.config.symbol, self.config.start, self.config.end
        );

        let count = self
            .loader
            .load_and_replay(
                &self.config.symbol,
                self.config.start,
                self.config.end,
                &self.config.topic,
            )
            .await?;
        println!("Replayed {} historical events", count);

        // Wait for the Kafka pipeline to process all events
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let portfolio = self.portfolio.get_portfolio().await?;

        let fills = self.fills.read().await;
        let report = BacktestReport::from_fills_and_portfolio(
            &fills,
            &portfolio,
            self.config.initial_capital,
        );

        println!(
            "Backtest complete. Final value: {}",
            report.final_portfolio_value
        );
        Ok(report)
    }
}
