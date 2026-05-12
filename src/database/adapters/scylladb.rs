use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use scylla::frame::value::CqlTimestamp;
use scylla::{Session, SessionBuilder};
use std::str::FromStr;
use std::sync::Arc;

use crate::database::domain::models::{
    Candle, DailyCandle, HistoricalCandle, KafkaMessage, StockTick, Timeframe,
};
use crate::database::ports::repository::Repository;
use crate::trading::domain::events::{FillEvent, OrderEvent, SignalEvent};

pub struct ScyllaRepository {
    session: Arc<Session>,
}

impl ScyllaRepository {
    pub async fn new(nodes: Vec<String>, keyspace: &str) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_nodes(&nodes)
            .build()
            .await
            .context("Failed to connect to ScyllaDB")?;

        session
            .query(format!("USE {}", keyspace), &[])
            .await
            .context("Failed to use keyspace")?;

        Ok(Self {
            session: Arc::new(session),
        })
    }
}

#[async_trait]
impl Repository for ScyllaRepository {
    async fn save_message(&self, _content: &str, _symbol: &str) -> Result<()> {
        Ok(())
    }

    async fn get_all_messages(
        &self,
    ) -> Result<Vec<(i32, String, String, chrono::DateTime<chrono::Utc>)>> {
        Ok(vec![])
    }

    async fn insert_stock_tick(&self, tick: &StockTick) -> Result<()> {
        let bucket = tick.timestamp.format("%Y-%m-%d").to_string();
        let tick_time = CqlTimestamp(tick.timestamp.timestamp_millis());

        let query = "INSERT INTO stock_ticks (symbol, bucket, tick_time, price, volume, source) VALUES (?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &tick.symbol,
                    &bucket,
                    tick_time,
                    tick.price.to_string(),
                    tick.volume,
                    &tick.source,
                ),
            )
            .await
            .map_err(|e| anyhow::anyhow!("ScyllaDB insert failed: {:?}", e))?;

        Ok(())
    }

    async fn insert_candle_1min(&self, candle: &Candle) -> Result<()> {
        let bucket = candle.timestamp.format("%Y-%m-%d").to_string();
        let tick_time = CqlTimestamp(candle.timestamp.timestamp_millis());

        let query = "INSERT INTO stock_1min (symbol, bucket, tick_time, open, high, low, close, volume, trades_count, vwap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &candle.symbol,
                    &bucket,
                    tick_time,
                    candle.open.to_string(),
                    candle.high.to_string(),
                    candle.low.to_string(),
                    candle.close.to_string(),
                    candle.volume,
                    candle.trades_count.unwrap_or(0),
                    candle.vwap.map(|v| v.to_string()),
                ),
            )
            .await
            .context("Failed to insert 1min candle")?;

        Ok(())
    }

    async fn insert_candle_5min(&self, candle: &Candle) -> Result<()> {
        let date = candle.timestamp.date_naive();
        let tick_time = CqlTimestamp(candle.timestamp.timestamp_millis());

        let query = "INSERT INTO stock_5min (symbol, date, tick_time, open, high, low, close, volume, trades_count, vwap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &candle.symbol,
                    date,
                    tick_time,
                    candle.open.to_string(),
                    candle.high.to_string(),
                    candle.low.to_string(),
                    candle.close.to_string(),
                    candle.volume,
                    candle.trades_count.unwrap_or(0),
                    candle.vwap.map(|v| v.to_string()),
                ),
            )
            .await
            .context("Failed to insert 5min candle")?;

        Ok(())
    }

    async fn insert_daily_candle(&self, candle: &DailyCandle) -> Result<()> {
        let query = "INSERT INTO stock_daily (symbol, date, open, high, low, close, volume, trades_count, vwap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        self.session
            .query(
                query,
                (
                    &candle.symbol,
                    candle.date,
                    candle.open.to_string(),
                    candle.high.to_string(),
                    candle.low.to_string(),
                    candle.close.to_string(),
                    candle.volume,
                    candle.trades_count.unwrap_or(0),
                    candle.vwap.map(|v| v.to_string()),
                ),
            )
            .await
            .context("Failed to insert daily candle")?;
        Ok(())
    }

    async fn track_kafka_message(&self, message: &KafkaMessage) -> Result<()> {
        let tick_time = CqlTimestamp(message.timestamp.timestamp_millis());
        let processed_at = CqlTimestamp(message.processed_at.timestamp_millis());

        let query = "INSERT INTO kafka_messages (topic, partition, offset, message_id, symbol, tick_time, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &message.topic,
                    message.partition,
                    message.offset,
                    &message.message_id,
                    &message.symbol,
                    tick_time,
                    processed_at,
                ),
            )
            .await
            .context("Failed to track Kafka message")?;

        Ok(())
    }

    async fn insert_signal(&self, signal: &SignalEvent) -> Result<()> {
        let bucket = signal.timestamp.format("%Y-%m-%d").to_string();
        let signal_time = CqlTimestamp(signal.timestamp.timestamp_millis());

        let query = "INSERT INTO trading_signals (symbol, bucket, signal_time, signal_id, direction, strength, strategy_name, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &signal.symbol,
                    &bucket,
                    signal_time,
                    signal.id.to_string(),
                    format!("{:?}", signal.direction),
                    signal.strength.to_string(),
                    &signal.strategy_name,
                    &signal.metadata,
                ),
            )
            .await
            .context("Failed to insert signal")?;

        Ok(())
    }

    async fn insert_order(&self, order: &OrderEvent) -> Result<()> {
        let bucket = order.timestamp.format("%Y-%m-%d").to_string();
        let order_time = CqlTimestamp(order.timestamp.timestamp_millis());

        let query = "INSERT INTO trading_orders (symbol, bucket, order_time, order_id, signal_id, side, quantity, order_type, limit_price, stop_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &order.symbol,
                    &bucket,
                    order_time,
                    order.id.to_string(),
                    order.signal_id.to_string(),
                    format!("{:?}", order.side),
                    order.quantity.to_string(),
                    format!("{:?}", order.order_type),
                    order.limit_price.map(|v| v.to_string()),
                    order.stop_price.map(|v| v.to_string()),
                ),
            )
            .await
            .context("Failed to insert order")?;

        Ok(())
    }

    async fn insert_fill(&self, fill: &FillEvent) -> Result<()> {
        let bucket = fill.timestamp.format("%Y-%m-%d").to_string();
        let fill_time = CqlTimestamp(fill.timestamp.timestamp_millis());

        let query = "INSERT INTO trading_fills (symbol, bucket, fill_time, fill_id, order_id, side, quantity, fill_price, commission, slippage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &fill.symbol,
                    &bucket,
                    fill_time,
                    fill.id.to_string(),
                    fill.order_id.to_string(),
                    format!("{:?}", fill.side),
                    fill.quantity.to_string(),
                    fill.fill_price.to_string(),
                    fill.commission.to_string(),
                    fill.slippage.to_string(),
                ),
            )
            .await
            .context("Failed to insert fill")?;

        Ok(())
    }

    async fn insert_historical_candle(&self, candle: &HistoricalCandle) -> Result<()> {
        let bucket = candle.timestamp.format("%Y-%m-%d").to_string();
        let tick_time = CqlTimestamp(candle.timestamp.timestamp_millis());
        let query = "INSERT INTO historical_candles (symbol, timeframe, bucket, tick_time, open, high, low, close, adj_close, volume, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &candle.symbol,
                    &candle.timeframe,
                    &bucket,
                    tick_time,
                    candle.open.to_string(),
                    candle.high.to_string(),
                    candle.low.to_string(),
                    candle.close.to_string(),
                    candle.adj_close.map(|v| v.to_string()),
                    candle.volume,
                    &candle.source,
                ),
            )
            .await
            .context("Failed to insert historical candle")?;
        Ok(())
    }

    async fn get_historical_candles(
        &self,
        symbol: &str,
        timeframe: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<HistoricalCandle>> {
        let query = "SELECT symbol, timeframe, tick_time, open, high, low, close, adj_close, volume, source FROM historical_candles WHERE symbol = ? AND timeframe = ? AND bucket = ? AND tick_time >= ? AND tick_time <= ?";
        let start_ts = CqlTimestamp(start.timestamp_millis());
        let end_ts = CqlTimestamp(end.timestamp_millis());

        let mut candles = Vec::new();
        let mut current = start.date_naive();
        let end_date = end.date_naive();

        while current <= end_date {
            let bucket = current.format("%Y-%m-%d").to_string();
            if let Ok(result) = self
                .session
                .query(query, (symbol, timeframe, &bucket, start_ts, end_ts))
                .await
            {
                if let Some(rows) = result.rows {
                    for row in rows {
                        if let Ok((
                            sym,
                            tf,
                            ts,
                            open_s,
                            high_s,
                            low_s,
                            close_s,
                            adj_close_s,
                            vol,
                            source,
                        )) = row.into_typed::<(
                            String,
                            String,
                            CqlTimestamp,
                            String,
                            String,
                            String,
                            String,
                            Option<String>,
                            i64,
                            String,
                        )>() {
                            let timestamp =
                                DateTime::from_timestamp_millis(ts.0).unwrap_or(Utc::now());
                            candles.push(HistoricalCandle {
                                symbol: sym,
                                timeframe: tf,
                                timestamp,
                                open: Decimal::from_str(&open_s).unwrap_or_default(),
                                high: Decimal::from_str(&high_s).unwrap_or_default(),
                                low: Decimal::from_str(&low_s).unwrap_or_default(),
                                close: Decimal::from_str(&close_s).unwrap_or_default(),
                                adj_close: adj_close_s.and_then(|v| Decimal::from_str(&v).ok()),
                                volume: vol,
                                source,
                            });
                        }
                    }
                }
            }

            current = current.succ_opt().unwrap_or(current);
        }

        candles.sort_by_key(|c| c.timestamp);
        Ok(candles)
    }

    async fn get_candles(
        &self,
        symbol: &str,
        timeframe: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Candle>> {
        let tf = Timeframe::from_str(timeframe).unwrap_or(Timeframe::OneMin);

        if timeframe == "1d" {
            let query = "SELECT symbol, date, open, high, low, close, volume FROM stock_daily WHERE symbol = ? AND date >= ? AND date <= ?";
            let start_date = start.date_naive();
            let end_date = end.date_naive();

            let result = self
                .session
                .query(query, (symbol, start_date, end_date))
                .await
                .context("Failed to query stock_daily")?;

            let mut candles = Vec::new();
            if let Some(rows) = result.rows {
                for row in rows {
                    if let Ok((sym, date, open_s, high_s, low_s, close_s, vol)) =
                        row.into_typed::<(String, NaiveDate, String, String, String, String, i64)>()
                    {
                        let timestamp = date
                            .and_hms_opt(0, 0, 0)
                            .map(|dt| Utc.from_utc_datetime(&dt))
                            .unwrap_or(Utc::now());
                        candles.push(Candle {
                            symbol: sym,
                            timestamp,
                            timeframe: tf,
                            open: Decimal::from_str(&open_s).unwrap_or_default(),
                            high: Decimal::from_str(&high_s).unwrap_or_default(),
                            low: Decimal::from_str(&low_s).unwrap_or_default(),
                            close: Decimal::from_str(&close_s).unwrap_or_default(),
                            volume: vol,
                            trades_count: None,
                            vwap: None,
                        });
                    }
                }
            }
            candles.sort_by_key(|c| c.timestamp);
            return Ok(candles);
        }

        let table = match timeframe {
            "5min" => "stock_5min",
            _ => "stock_1min",
        };

        let start_ts = CqlTimestamp(start.timestamp_millis());
        let end_ts = CqlTimestamp(end.timestamp_millis());

        let query = format!(
            "SELECT symbol, tick_time, open, high, low, close, volume FROM {} WHERE symbol = ? AND bucket = ? AND tick_time >= ? AND tick_time <= ?",
            table
        );

        let mut candles = Vec::new();
        let mut current = start.date_naive();
        let end_date = end.date_naive();

        while current <= end_date {
            let bucket = current.format("%Y-%m-%d").to_string();

            if let Ok(result) = self
                .session
                .query(query.as_str(), (symbol, &bucket, start_ts, end_ts))
                .await
            {
                if let Some(rows) = result.rows {
                    for row in rows {
                        if let Ok((sym, ts, open_s, high_s, low_s, close_s, vol)) =
                            row.into_typed::<(String, CqlTimestamp, String, String, String, String, i64)>()
                        {
                            let timestamp =
                                DateTime::from_timestamp_millis(ts.0).unwrap_or(Utc::now());
                            candles.push(Candle {
                                symbol: sym,
                                timestamp,
                                timeframe: tf,
                                open: Decimal::from_str(&open_s).unwrap_or_default(),
                                high: Decimal::from_str(&high_s).unwrap_or_default(),
                                low: Decimal::from_str(&low_s).unwrap_or_default(),
                                close: Decimal::from_str(&close_s).unwrap_or_default(),
                                volume: vol,
                                trades_count: None,
                                vwap: None,
                            });
                        }
                    }
                }
            }

            current = current.succ_opt().unwrap_or(current);
        }

        candles.sort_by_key(|c| c.timestamp);
        Ok(candles)
    }

    async fn get_ticks(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<StockTick>> {
        let start_ts = CqlTimestamp(start.timestamp_millis());
        let end_ts = CqlTimestamp(end.timestamp_millis());

        let query = "SELECT symbol, tick_time, price, volume, source FROM stock_ticks WHERE symbol = ? AND bucket = ? AND tick_time >= ? AND tick_time <= ?";

        let mut ticks = Vec::new();
        let mut current = start.date_naive();
        let end_date = end.date_naive();

        while current <= end_date {
            let bucket = current.format("%Y-%m-%d").to_string();

            if let Ok(result) = self
                .session
                .query(query, (symbol, &bucket, start_ts, end_ts))
                .await
            {
                if let Some(rows) = result.rows {
                    for row in rows {
                        if let Ok((sym, ts, price_s, vol, source)) =
                            row.into_typed::<(String, CqlTimestamp, String, i64, String)>()
                        {
                            let timestamp =
                                DateTime::from_timestamp_millis(ts.0).unwrap_or(Utc::now());
                            ticks.push(StockTick {
                                symbol: sym,
                                timestamp,
                                price: Decimal::from_str(&price_s).unwrap_or_default(),
                                volume: vol,
                                bid: None,
                                ask: None,
                                source,
                            });
                        }
                    }
                }
            }

            current = current.succ_opt().unwrap_or(current);
        }

        ticks.sort_by_key(|t| t.timestamp);
        Ok(ticks)
    }
    async fn insert_candle(&self, candle: &Candle) -> Result<()> {
        let bucket = candle.timestamp.format("%Y-%m-%d").to_string();
        let tick_time = CqlTimestamp(candle.timestamp.timestamp_millis());
        let timeframe = match candle.timeframe {
            Timeframe::OneMin => "1min",
            Timeframe::FiveMin => "5min",
            Timeframe::FifteenMin => "15min",
            Timeframe::OneHour => "1hour",
            Timeframe::Daily => "daily",
            Timeframe::Weekly => "weekly",
            Timeframe::Monthly => "monthly",
            Timeframe::Tick => "tick",
        };
        let query = "INSERT INTO candles (symbol, timeframe, bucket, tick_time, open, high, low, close, volume, trades_count, vwap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query(
                query,
                (
                    &candle.symbol,
                    timeframe,
                    &bucket,
                    tick_time,
                    candle.open.to_string(),
                    candle.high.to_string(),
                    candle.low.to_string(),
                    candle.close.to_string(),
                    candle.volume,
                    candle.trades_count.unwrap_or(0),
                    candle.vwap.map(|v| v.to_string()),
                ),
            )
            .await
            .context("Failed to insert candle into unified candles table")?;
        Ok(())
    }

    async fn get_latest_candles(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<Candle>> {
        let mut all_candles = Vec::new();
        let mut current_date = Utc::now().date_naive();
        let days_to_search = 30; // Search back up to 30 days
        
        let table = match timeframe {
            "5min" => "stock_5min",
            _ => "stock_1min",
        };

        // 1. Search in live tables (backwards day-by-day)
        for _ in 0..days_to_search {
            let bucket = current_date.format("%Y-%m-%d").to_string();
            let query = format!(
                "SELECT symbol, tick_time, open, high, low, close, volume FROM {} WHERE symbol = ? AND bucket = ? ORDER BY tick_time DESC LIMIT ?",
                table
            );

            if let Ok(result) = self.session.query(query, (symbol, &bucket, (limit - all_candles.len()) as i32)).await {
                if let Some(rows) = result.rows {
                    for row in rows {
                        if let Ok((sym, ts, open_s, high_s, low_s, close_s, vol)) =
                            row.into_typed::<(String, CqlTimestamp, String, String, String, String, i64)>()
                        {
                            let timestamp = DateTime::from_timestamp_millis(ts.0).unwrap_or(Utc::now());
                            all_candles.push(Candle {
                                symbol: sym,
                                timestamp,
                                timeframe: Timeframe::from_str(timeframe).unwrap_or(Timeframe::OneMin),
                                open: Decimal::from_str(&open_s).unwrap_or_default(),
                                high: Decimal::from_str(&high_s).unwrap_or_default(),
                                low: Decimal::from_str(&low_s).unwrap_or_default(),
                                close: Decimal::from_str(&close_s).unwrap_or_default(),
                                volume: vol,
                                trades_count: None,
                                vwap: None,
                            });
                        }
                    }
                }
            }

            if all_candles.len() >= limit {
                break;
            }
            current_date = current_date.pred_opt().unwrap_or(current_date);
        }

        // 2. If still not enough, search in historical_candles table
        if all_candles.len() < limit {
            let mut hist_date = Utc::now().date_naive();
            for _ in 0..days_to_search {
                let bucket = hist_date.format("%Y-%m-%d").to_string();
                let query = "SELECT symbol, timeframe, tick_time, open, high, low, close, volume FROM historical_candles WHERE symbol = ? AND timeframe = ? AND bucket = ? ORDER BY tick_time DESC LIMIT ?";
                
                if let Ok(result) = self.session.query(query, (symbol, timeframe, &bucket, (limit - all_candles.len()) as i32)).await {
                    if let Some(rows) = result.rows {
                        for row in rows {
                            if let Ok((sym, _tf, ts, open_s, high_s, low_s, close_s, vol)) =
                                row.into_typed::<(String, String, CqlTimestamp, String, String, String, String, i64)>()
                            {
                                let timestamp = DateTime::from_timestamp_millis(ts.0).unwrap_or(Utc::now());
                                all_candles.push(Candle {
                                    symbol: sym,
                                    timestamp,
                                    timeframe: Timeframe::from_str(timeframe).unwrap_or(Timeframe::OneMin),
                                    open: Decimal::from_str(&open_s).unwrap_or_default(),
                                    high: Decimal::from_str(&high_s).unwrap_or_default(),
                                    low: Decimal::from_str(&low_s).unwrap_or_default(),
                                    close: Decimal::from_str(&close_s).unwrap_or_default(),
                                    volume: vol,
                                    trades_count: None,
                                    vwap: None,
                                });
                            }
                        }
                    }
                }

                if all_candles.len() >= limit {
                    break;
                }
                hist_date = hist_date.pred_opt().unwrap_or(hist_date);
            }
        }

        all_candles.sort_by_key(|c| c.timestamp);
        Ok(all_candles)
    }
}
