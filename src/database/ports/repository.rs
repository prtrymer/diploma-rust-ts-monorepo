use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::database::domain::models::{
    Candle, DailyCandle, HistoricalCandle, KafkaMessage, StockMetadata, StockTick,
};

/// Repository for all market/candle/tick/ML training data (backed by ScyllaDB).
#[async_trait]
pub trait Repository: Send + Sync {
    // Legacy method for backward compatibility
    async fn save_message(&self, content: &str, symbol: &str) -> Result<()>;
    async fn get_all_messages(&self) -> Result<Vec<(i32, String, String, DateTime<Utc>)>>;

    // Stock tick methods
    async fn insert_stock_tick(&self, tick: &StockTick) -> Result<()>;

    // Candle methods
    async fn insert_candle(&self, _candle: &Candle) -> Result<()> {
        Ok(())
    }

    async fn insert_candle_1min(&self, _candle: &Candle) -> Result<()> {
        Ok(())
    }

    async fn insert_candle_5min(&self, _candle: &Candle) -> Result<()> {
        Ok(())
    }

    async fn insert_daily_candle(&self, _candle: &DailyCandle) -> Result<()> {
        Ok(())
    }

    // Metadata methods
    async fn insert_stock_metadata(&self, _metadata: &StockMetadata) -> Result<()> {
        Ok(())
    }

    // Kafka tracking
    async fn track_kafka_message(&self, _message: &KafkaMessage) -> Result<()> {
        Ok(())
    }

    // Trading event persistence
    async fn insert_signal(
        &self,
        _signal: &crate::trading::domain::events::SignalEvent,
    ) -> Result<()> {
        Ok(())
    }

    async fn insert_order(
        &self,
        _order: &crate::trading::domain::events::OrderEvent,
    ) -> Result<()> {
        Ok(())
    }

    async fn insert_fill(&self, _fill: &crate::trading::domain::events::FillEvent) -> Result<()> {
        Ok(())
    }

    // Historical data queries
    async fn get_candles(
        &self,
        _symbol: &str,
        _timeframe: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> Result<Vec<Candle>> {
        Ok(vec![])
    }

    async fn get_latest_candles(
        &self,
        _symbol: &str,
        _timeframe: &str,
        _limit: usize,
    ) -> Result<Vec<Candle>> {
        Ok(vec![])
    }

    async fn get_ticks(
        &self,
        _symbol: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> Result<Vec<StockTick>> {
        Ok(vec![])
    }

    async fn insert_historical_candle(&self, _candle: &HistoricalCandle) -> Result<()> {
        Ok(())
    }

    async fn get_historical_candles(
        &self,
        _symbol: &str,
        _timeframe: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> Result<Vec<HistoricalCandle>> {
        Ok(vec![])
    }
}

/// Repository for user authentication data and global configurations (backed by PostgreSQL).
#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn get_user_by_username(&self, username: &str) -> Result<Option<crate::database::domain::models::User>>;
    async fn create_user(&self, user: &crate::database::domain::models::User) -> Result<()>;
    
    // Active symbols persistence
    async fn get_active_symbols(&self) -> Result<Vec<String>>;
    async fn add_active_symbol(&self, symbol: &str) -> Result<()>;
    async fn remove_active_symbol(&self, symbol: &str) -> Result<()>;
}
