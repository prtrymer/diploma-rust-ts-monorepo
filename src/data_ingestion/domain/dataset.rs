use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;

#[cfg(feature = "candle-ml")]
use candle_core::{Device, Tensor};
#[cfg(feature = "polars-df")]
use polars::prelude::*;
#[cfg(feature = "polars-df")]
use rust_decimal::Decimal;

use crate::data_ingestion::ports::DataSourcePort;
use crate::database::ports::repository::Repository;
#[cfg(feature = "polars-df")]
use crate::{
    data_ingestion::domain::models::HistoricalQuote,
    database::domain::models::{Candle, DailyCandle, HistoricalCandle, Timeframe},
};

#[cfg_attr(not(feature = "polars-df"), allow(dead_code))]
pub struct HistoricalDatasetService {
    data_source: Arc<dyn DataSourcePort>,
    repository: Arc<dyn Repository>,
}

impl HistoricalDatasetService {
    pub fn new(data_source: Arc<dyn DataSourcePort>, repository: Arc<dyn Repository>) -> Self {
        Self {
            data_source,
            repository,
        }
    }

    #[cfg(feature = "polars-df")]
    pub async fn load_symbol_frame(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<DataFrame> {
        let mut quotes = self
            .data_source
            .fetch_historical_quotes(symbol, start, end, interval)
            .await?;
        quotes.sort_by_key(|q| q.timestamp);
        quotes.dedup_by_key(|q| q.timestamp);
        quotes_to_frame(&quotes)
    }

    pub async fn sync_symbol(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<usize> {
        #[cfg(feature = "polars-df")]
        {
            let df = self.load_symbol_frame(symbol, start, end, interval).await?;
            self.persist_frame_to_scylla(&df).await?;
            return Ok(df.height());
        }

        #[cfg(not(feature = "polars-df"))]
        {
            let _ = (symbol, start, end, interval);
            anyhow::bail!("sync_symbol requires `polars-df` feature");
        }
    }

    #[cfg(feature = "polars-df")]
    pub async fn sync_frame(&self, df: &DataFrame) -> Result<usize> {
        self.persist_frame_to_scylla(df).await?;
        Ok(df.height())
    }

    pub async fn export_symbol_dataset(
        &self,
        path: &str,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<usize> {
        #[cfg(feature = "polars-df")]
        {
            let mut df = self.load_symbol_frame(symbol, start, end, interval).await?;
            if let Some(parent) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::File::create(path)?;
            CsvWriter::new(&mut file)
                .include_header(true)
                .finish(&mut df)?;
            return Ok(df.height());
        }

        #[cfg(not(feature = "polars-df"))]
        {
            let _ = (path, symbol, start, end, interval);
            anyhow::bail!("export_symbol_dataset requires `polars-df` feature");
        }
    }

    #[cfg(feature = "polars-df")]
    async fn persist_frame_to_scylla(&self, df: &DataFrame) -> Result<()> {
        let symbol_col = df.column("symbol")?.str()?;
        let timeframe_col = df.column("timeframe")?.str()?;
        let source_col = df.column("source")?.str()?;
        let ts_col = df.column("timestamp_ms")?.i64()?;

        let open_col = df.column("open")?.f64()?;
        let high_col = df.column("high")?.f64()?;
        let low_col = df.column("low")?.f64()?;
        let close_col = df.column("close")?.f64()?;
        let adj_close_col = df.column("adj_close")?.f64()?;
        let volume_col = df.column("volume")?.i64()?;

        for i in 0..df.height() {
            let symbol = symbol_col.get(i).unwrap_or_default().to_string();
            let timeframe = timeframe_col.get(i).unwrap_or("1m").to_string();
            let source = source_col.get(i).unwrap_or("unknown").to_string();
            let ts_ms = ts_col.get(i).unwrap_or_default();
            let timestamp = DateTime::from_timestamp_millis(ts_ms).unwrap_or(Utc::now());

            let open = Decimal::try_from(open_col.get(i).unwrap_or_default()).unwrap_or_default();
            let high = Decimal::try_from(high_col.get(i).unwrap_or_default()).unwrap_or_default();
            let low = Decimal::try_from(low_col.get(i).unwrap_or_default()).unwrap_or_default();
            let close = Decimal::try_from(close_col.get(i).unwrap_or_default()).unwrap_or_default();
            let adj_close = adj_close_col.get(i).and_then(|v| Decimal::try_from(v).ok());
            let volume = volume_col.get(i).unwrap_or_default();

            let historical = HistoricalCandle {
                symbol: symbol.clone(),
                timeframe: timeframe.clone(),
                timestamp,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source: source.clone(),
            };
            self.repository
                .insert_historical_candle(&historical)
                .await?;

            if let Some(tf) = timeframe_from_interval(&timeframe) {
                let unified = Candle {
                    symbol: symbol.clone(),
                    timestamp,
                    timeframe: tf,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    trades_count: None,
                    vwap: None,
                };
                self.repository.insert_candle(&unified).await?;
            }

            match timeframe.as_str() {
                "1m" | "1min" => {
                    self.repository
                        .insert_candle_1min(&Candle {
                            symbol: symbol.clone(),
                            timestamp,
                            timeframe: Timeframe::OneMin,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            trades_count: None,
                            vwap: None,
                        })
                        .await?;
                }
                "5m" | "5min" => {
                    self.repository
                        .insert_candle_5min(&Candle {
                            symbol: symbol.clone(),
                            timestamp,
                            timeframe: Timeframe::FiveMin,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            trades_count: None,
                            vwap: None,
                        })
                        .await?;
                }
                "1d" | "daily" => {
                    self.repository
                        .insert_daily_candle(&DailyCandle {
                            symbol: symbol.clone(),
                            date: timestamp.date_naive(),
                            timestamp,
                            open,
                            high,
                            low,
                            close,
                            adj_close,
                            volume,
                            trades_count: None,
                            vwap: None,
                            price_change: None,
                            price_change_percent: None,
                            high_low_range: None,
                            sma_20: None,
                            sma_50: None,
                            sma_200: None,
                            ema_12: None,
                            ema_26: None,
                            atr: None,
                            volatility: None,
                            volume_sma_20: None,
                            volume_ratio: None,
                        })
                        .await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[cfg(all(feature = "polars-df", feature = "candle-ml"))]
    pub fn frame_to_tensor(df: &DataFrame, feature_cols: &[&str]) -> Result<Tensor> {
        let rows = df.height();
        let cols = feature_cols.len();
        if cols == 0 {
            anyhow::bail!("feature_cols is empty");
        }

        let mut data = Vec::with_capacity(rows * cols);
        for row in 0..rows {
            for col_name in feature_cols {
                let value = df.column(col_name)?.f64()?.get(row).unwrap_or(0.0) as f32;
                data.push(value);
            }
        }

        let device = Device::Cpu;
        let t = Tensor::from_vec(data, (rows, cols), &device)?;
        Ok(t)
    }
}

#[cfg(feature = "polars-df")]
fn quotes_to_frame(quotes: &[HistoricalQuote]) -> Result<DataFrame> {
    let mut timestamp_ms: Vec<i64> = Vec::with_capacity(quotes.len());
    let mut symbol: Vec<String> = Vec::with_capacity(quotes.len());
    let mut open: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut high: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut low: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut close: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut adj_close: Vec<f64> = Vec::with_capacity(quotes.len());
    let mut volume: Vec<i64> = Vec::with_capacity(quotes.len());
    let mut timeframe: Vec<String> = Vec::with_capacity(quotes.len());
    let mut source: Vec<String> = Vec::with_capacity(quotes.len());
    let mut return_1: Vec<f64> = Vec::with_capacity(quotes.len());

    for (i, q) in quotes.iter().enumerate() {
        let prev_close = if i > 0 { quotes[i - 1].close } else { q.close };
        let r = if prev_close != 0.0 {
            (q.close - prev_close) / prev_close
        } else {
            0.0
        };

        timestamp_ms.push(q.timestamp.timestamp_millis());
        symbol.push(q.symbol.clone());
        open.push(q.open);
        high.push(q.high);
        low.push(q.low);
        close.push(q.close);
        adj_close.push(q.adj_close.unwrap_or(q.close));
        volume.push(i64::try_from(q.volume).unwrap_or(i64::MAX));
        timeframe.push(q.timeframe.clone());
        source.push(q.source.clone());
        return_1.push(r);
    }

    let df = DataFrame::new(vec![
        Series::new("timestamp_ms".into(), timestamp_ms).into(),
        Series::new("symbol".into(), symbol).into(),
        Series::new("open".into(), open).into(),
        Series::new("high".into(), high).into(),
        Series::new("low".into(), low).into(),
        Series::new("close".into(), close).into(),
        Series::new("adj_close".into(), adj_close).into(),
        Series::new("volume".into(), volume).into(),
        Series::new("timeframe".into(), timeframe).into(),
        Series::new("source".into(), source).into(),
        Series::new("return_1".into(), return_1).into(),
    ])?;
    Ok(df)
}

#[cfg(feature = "polars-df")]
fn timeframe_from_interval(interval: &str) -> Option<Timeframe> {
    match interval {
        "1m" | "1min" => Some(Timeframe::OneMin),
        "5m" | "5min" => Some(Timeframe::FiveMin),
        "15m" | "15min" => Some(Timeframe::FifteenMin),
        "1h" => Some(Timeframe::OneHour),
        "1d" | "daily" => Some(Timeframe::Daily),
        _ => None,
    }
}
