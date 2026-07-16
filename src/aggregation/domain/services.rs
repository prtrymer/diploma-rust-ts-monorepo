use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::aggregation::models::{get_1min_window, get_5min_window, CandleWindow};
use crate::database::domain::models::{Candle, StockTick, Timeframe};
use crate::database::ports::repository::Repository;

pub struct CandleAggregator {
    repository: Arc<dyn Repository>,
    windows_1min: Arc<RwLock<HashMap<String, CandleWindow>>>,
    windows_5min: Arc<RwLock<HashMap<String, CandleWindow>>>,
}

impl CandleAggregator {
    pub fn new(repository: Arc<dyn Repository>) -> Self {
        Self {
            repository,
            windows_1min: Arc::new(RwLock::new(HashMap::new())),
            windows_5min: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn process_tick(&self, tick: &StockTick) -> Result<()> {
        // Process 1-minute candle
        self.process_1min_candle(tick).await?;

        // Process 5-minute candle
        self.process_5min_candle(tick).await?;

        Ok(())
    }

    async fn process_1min_candle(&self, tick: &StockTick) -> Result<()> {
        let current_window_start = get_1min_window(tick.timestamp);
        let key = format!("{}_{}", tick.symbol, current_window_start.timestamp());

        let mut windows = self.windows_1min.write().await;

        let window = windows
            .entry(key.clone())
            .or_insert_with(|| CandleWindow::new(tick.symbol.clone(), current_window_start));

        window.add_tick(tick.price, tick.volume, tick.timestamp);

        let cutoff = current_window_start - chrono::Duration::minutes(1);
        let ready_keys: Vec<String> = windows
            .iter()
            .filter_map(|(k, w)| {
                if w.symbol == tick.symbol && w.start_time <= cutoff && w.is_complete() {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();

        for ready_key in ready_keys {
            if let Some(window) = windows.remove(&ready_key) {
                let candle = self.window_to_candle(&window, Timeframe::OneMin);
                self.repository.insert_candle_1min(&candle).await?;
                tracing::debug!(symbol = %candle.symbol, timestamp = %candle.timestamp, "saved 1min candle");
            }
        }

        Ok(())
    }

    async fn process_5min_candle(&self, tick: &StockTick) -> Result<()> {
        let current_window_start = get_5min_window(tick.timestamp);
        let key = format!("{}_{}", tick.symbol, current_window_start.timestamp());

        let mut windows = self.windows_5min.write().await;

        let window = windows
            .entry(key.clone())
            .or_insert_with(|| CandleWindow::new(tick.symbol.clone(), current_window_start));

        window.add_tick(tick.price, tick.volume, tick.timestamp);

        let cutoff = current_window_start - chrono::Duration::minutes(5);
        let ready_keys: Vec<String> = windows
            .iter()
            .filter_map(|(k, w)| {
                if w.symbol == tick.symbol && w.start_time <= cutoff && w.is_complete() {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();

        for ready_key in ready_keys {
            if let Some(window) = windows.remove(&ready_key) {
                let candle = self.window_to_candle(&window, Timeframe::FiveMin);
                self.repository.insert_candle_5min(&candle).await?;
                tracing::debug!(symbol = %candle.symbol, timestamp = %candle.timestamp, "saved 5min candle");
            }
        }

        Ok(())
    }

    fn window_to_candle(&self, window: &CandleWindow, timeframe: Timeframe) -> Candle {
        Candle {
            symbol: window.symbol.clone(),
            timestamp: window.start_time,
            timeframe,
            open: window.open.unwrap(),
            high: window.high.unwrap(),
            low: window.low.unwrap(),
            close: window.close.unwrap(),
            volume: window.volume,
            trades_count: Some(window.trades_count),
            vwap: None,
        }
    }
}
