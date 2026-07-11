//! Binance-адаптер funding-даних (M3.1). Безкоштовний публічний REST:
//! GET /fapi/v1/fundingRate (без ключа). Пагінація по 1000 записів.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::data_ingestion::domain::funding::FundingRatePoint;
use crate::data_ingestion::ports::funding::FundingDataPort;

const BINANCE_FAPI: &str = "https://fapi.binance.com";

pub struct BinanceFundingAdapter {
    client: reqwest::Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct BinanceFundingRow {
    #[serde(rename = "fundingTime")]
    funding_time: i64,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "markPrice", default)]
    mark_price: Option<String>,
}

impl BinanceFundingAdapter {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: BINANCE_FAPI.to_string(),
        }
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }
}

impl Default for BinanceFundingAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FundingDataPort for BinanceFundingAdapter {
    async fn funding_history(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<FundingRatePoint>> {
        let mut out: Vec<FundingRatePoint> = Vec::new();
        let mut cursor = start.timestamp_millis();
        let end_ms = end.timestamp_millis();

        while cursor < end_ms {
            let url = format!(
                "{}/fapi/v1/fundingRate?symbol={}&startTime={}&endTime={}&limit=1000",
                self.base_url, symbol, cursor, end_ms
            );
            let rows: Vec<BinanceFundingRow> =
                self.client.get(&url).send().await?.json().await?;
            if rows.is_empty() {
                break;
            }
            let mut max_ts = cursor;
            for row in &rows {
                max_ts = max_ts.max(row.funding_time);
                let ts = Utc
                    .timestamp_millis_opt(row.funding_time)
                    .single()
                    .ok_or_else(|| anyhow::anyhow!("bad fundingTime {}", row.funding_time))?;
                out.push(FundingRatePoint {
                    symbol: symbol.to_string(),
                    timestamp: ts,
                    rate: row.funding_rate.parse::<Decimal>()?,
                    mark_price: row
                        .mark_price
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO),
                    spot_price: None,
                });
            }
            // Наступна сторінка: за останнім часом + 1мс (уникнення дублю).
            cursor = max_ts + 1;
        }

        out.sort_by_key(|p| p.timestamp);
        out.dedup_by_key(|p| p.timestamp);
        Ok(out)
    }
}
