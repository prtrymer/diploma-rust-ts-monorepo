//! Адаптер історичних даних: Scylla як сховище, Yahoo як джерело догрузки,
//! Kafka-producer для replay. Імплементує порт `HistoricalDataLoader`.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use crate::backtest::ports::HistoricalDataLoader;
use crate::data_ingestion::adapters::yahoo_finance::YahooFinanceAdapter;
use crate::data_ingestion::domain::models::StreamMessage;
use crate::data_ingestion::ports::{DataSourcePort, MessageProducerPort};
use crate::database::domain::models::{HistoricalCandle, StockTick};
use crate::database::ports::repository::Repository;

pub struct ScyllaHistoricalLoader {
    repository: Arc<dyn Repository>,
    producer: Option<Arc<dyn MessageProducerPort>>,
    fetch_missing: bool,
}

impl ScyllaHistoricalLoader {
    pub fn new(
        repository: Arc<dyn Repository>,
        producer: Arc<dyn MessageProducerPort>,
        fetch_missing: bool,
    ) -> Self {
        Self {
            repository,
            producer: Some(producer),
            fetch_missing,
        }
    }

    pub fn for_query_only(repository: Arc<dyn Repository>, fetch_missing: bool) -> Self {
        Self {
            repository,
            producer: None,
            fetch_missing,
        }
    }

    pub async fn load_candles(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<HistoricalCandle>> {
        let mut candles = self
            .repository
            .get_historical_candles(symbol, "1m", start, end)
            .await?;

        let needs_sync = if candles.is_empty() {
            true
        } else {
            let first_ts = candles.first().map(|c| c.timestamp).unwrap_or(start);
            let last_ts = candles.last().map(|c| c.timestamp).unwrap_or(end);
            first_ts > start || last_ts < end - chrono::Duration::minutes(1)
        };

        if needs_sync && self.fetch_missing {
            let adapter = YahooFinanceAdapter::new();
            let max_chunk_days = 7;
            let mut cursor_start = start;

            while cursor_start < end {
                let cursor_end =
                    std::cmp::min(cursor_start + chrono::Duration::days(max_chunk_days), end);
                match adapter
                    .fetch_historical_quotes(symbol, cursor_start, cursor_end, "1m")
                    .await
                {
                    Ok(quotes) => {
                        for q in quotes {
                            let candle = HistoricalCandle {
                                symbol: q.symbol,
                                timeframe: "1m".to_string(),
                                timestamp: q.timestamp,
                                open: rust_decimal::Decimal::try_from(q.open).unwrap_or_default(),
                                high: rust_decimal::Decimal::try_from(q.high).unwrap_or_default(),
                                low: rust_decimal::Decimal::try_from(q.low).unwrap_or_default(),
                                close: rust_decimal::Decimal::try_from(q.close).unwrap_or_default(),
                                adj_close: q
                                    .adj_close
                                    .and_then(|v| rust_decimal::Decimal::try_from(v).ok()),
                                volume: i64::try_from(q.volume).unwrap_or(i64::MAX),
                                source: q.source,
                            };
                            let _ = self.repository.insert_historical_candle(&candle).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            %symbol,
                            range = %format!("{}..{}", cursor_start, cursor_end),
                            error = %e,
                            "backtest sync chunk failed"
                        );
                    }
                }

                cursor_start = cursor_end;
            }

            candles = self
                .repository
                .get_historical_candles(symbol, "1m", start, end)
                .await?;
        }

        Ok(candles)
    }
}

#[async_trait]
impl HistoricalDataLoader for ScyllaHistoricalLoader {
    async fn load_and_replay(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        topic: &str,
    ) -> Result<usize> {
        let candles = self.load_candles(symbol, start, end).await?;
        if candles.is_empty() {
            tracing::warn!(
                %symbol,
                range = %format!("{}..{}", start, end),
                fetch_missing = self.fetch_missing,
                "no historical candles available"
            );
            return Ok(0);
        }
        let mut count = 0;
        let Some(producer) = &self.producer else {
            return Err(anyhow::anyhow!(
                "load_and_replay requires a producer; use load_candles for direct mode"
            ));
        };

        for candle in &candles {
            let tick = StockTick {
                symbol: candle.symbol.clone(),
                timestamp: candle.timestamp,
                price: candle.close,
                volume: candle.volume,
                bid: None,
                ask: None,
                source: candle.source.clone(),
            };
            let json = serde_json::to_string(&tick)?;
            producer
                .send_message(StreamMessage {
                    topic: topic.to_string(),
                    key: Some(symbol.to_string()),
                    value: json,
                })
                .await?;
            count += 1;
        }

        tracing::info!(%symbol, candles = count, "replayed historical candles as ticks");
        Ok(count)
    }
}
