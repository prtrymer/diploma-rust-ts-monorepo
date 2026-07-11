//! CSV-адаптер funding-даних (M3.1): офлайн-фікстури та дешева фальсифікація.
//! Формат: timestamp,symbol,rate,mark_price[,spot_price]

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::path::PathBuf;

/// Парсер, толерантний до наукової нотації («5.7e-05» з архівів Binance).
fn parse_decimal(s: &str) -> Result<Decimal> {
    let s = s.trim();
    s.parse::<Decimal>()
        .or_else(|_| Decimal::from_scientific(s))
        .with_context(|| format!("bad decimal: {s}"))
}

use crate::data_ingestion::domain::funding::FundingRatePoint;
use crate::data_ingestion::ports::funding::FundingDataPort;

pub struct CsvFundingAdapter {
    path: PathBuf,
}

impl CsvFundingAdapter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl FundingDataPort for CsvFundingAdapter {
    async fn funding_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<FundingRatePoint>> {
        let content = std::fs::read_to_string(&self.path)
            .with_context(|| format!("reading funding csv {:?}", self.path))?;
        let mut out = Vec::new();
        for (i, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || (i == 0 && line.to_lowercase().starts_with("timestamp")) {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            anyhow::ensure!(parts.len() >= 4, "funding csv line {i}: expected ≥4 fields");
            let ts: DateTime<Utc> = parts[0]
                .parse()
                .with_context(|| format!("line {i}: bad timestamp {}", parts[0]))?;
            if parts[1] != symbol || ts < start || ts > end {
                continue;
            }
            out.push(FundingRatePoint {
                symbol: parts[1].to_string(),
                timestamp: ts,
                rate: parse_decimal(parts[2])?,
                mark_price: parse_decimal(parts[3])?,
                spot_price: parts
                    .get(4)
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| parse_decimal(s))
                    .transpose()?,
            });
        }
        out.sort_by_key(|p| p.timestamp);
        // Захист від дублікатів у сирих CSV (перекриття сторінок пагінації).
        out.dedup_by_key(|p| p.timestamp);
        Ok(out)
    }
}
