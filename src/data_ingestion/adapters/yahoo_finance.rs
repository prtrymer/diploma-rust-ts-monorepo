use crate::data_ingestion::domain::models::{HistoricalQuote, StockQuote};
use crate::data_ingestion::ports::DataSourcePort;
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, NaiveTime, TimeZone, Utc};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{sleep, Duration};
use yahoo_finance_api as yahoo;
use yahoo_finance_api::time::OffsetDateTime;

/// Yahoo Finance adapter
pub struct YahooFinanceAdapter {
    provider: yahoo::YahooConnector,
    http_client: reqwest::Client,
}

impl Default for YahooFinanceAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl YahooFinanceAdapter {
    pub fn new() -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("db-con/0.1 (market-data-ingestion)"),
        );

        Self {
            provider: yahoo::YahooConnector::new().unwrap(),
            http_client: reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .unwrap(),
        }
    }

    async fn fetch_from_connector(&self, symbol: &str) -> Result<StockQuote> {
        let response = self
            .provider
            .get_latest_quotes(symbol, "1m")
            .await
            .context("Failed to fetch quote from Yahoo Finance connector")?;

        let quote = response.last_quote().context("No quote data available")?;
        let connector_ts = i64::try_from(quote.timestamp)
            .context("Yahoo connector quote timestamp out of range")?;
        let timestamp = DateTime::from_timestamp(connector_ts, 0)
            .context("Yahoo connector quote has invalid timestamp")?
            .with_timezone(&Utc);

        Ok(StockQuote {
            symbol: symbol.to_string(),
            price: quote.close,
            volume: quote.volume,
            timestamp,
        })
    }

    async fn fetch_from_http_quote_api(&self, symbol: &str) -> Result<StockQuote> {
        let url = "https://query1.finance.yahoo.com/v7/finance/quote";
        let response = self
            .http_client
            .get(url)
            .query(&[("symbols", symbol)])
            .send()
            .await
            .context("Failed to call Yahoo Finance quote endpoint")?
            .error_for_status()
            .context("Yahoo Finance quote endpoint returned error status")?;

        let payload: YahooQuoteResponse = response
            .json()
            .await
            .context("Failed to decode Yahoo quote response")?;

        let quote = payload
            .quote_response
            .result
            .into_iter()
            .next()
            .context("No quote result returned from Yahoo quote endpoint")?;

        let post_market_time = quote.post_market_time.unwrap_or(0);
        let pre_market_time = quote.pre_market_time.unwrap_or(0);

        let (timestamp_secs, price, volume) = if let Some(post_price) = quote.post_market_price {
            if post_market_time > quote.regular_market_time {
                (
                    post_market_time,
                    post_price,
                    quote.post_market_volume.or(quote.regular_market_volume),
                )
            } else {
                (
                    quote.regular_market_time,
                    quote
                        .regular_market_price
                        .context("Yahoo quote missing regularMarketPrice")?,
                    quote.regular_market_volume,
                )
            }
        } else if let Some(pre_price) = quote.pre_market_price {
            if pre_market_time > quote.regular_market_time {
                (
                    pre_market_time,
                    pre_price,
                    quote.pre_market_volume.or(quote.regular_market_volume),
                )
            } else {
                (
                    quote.regular_market_time,
                    quote
                        .regular_market_price
                        .context("Yahoo quote missing regularMarketPrice")?,
                    quote.regular_market_volume,
                )
            }
        } else {
            (
                quote.regular_market_time,
                quote
                    .regular_market_price
                    .context("Yahoo quote missing regularMarketPrice")?,
                quote.regular_market_volume,
            )
        };

        let timestamp = DateTime::from_timestamp(timestamp_secs, 0)
            .context("Yahoo quote missing/invalid market time")?
            .with_timezone(&Utc);

        Ok(StockQuote {
            symbol: quote.symbol,
            price,
            volume: volume.unwrap_or(0),
            timestamp,
        })
    }

    async fn fetch_from_stooq(&self, symbol: &str) -> Result<StockQuote> {
        let stooq_symbol = format!("{}.us", symbol.to_lowercase());
        let url = "https://stooq.com/q/l/";
        let body = self
            .http_client
            .get(url)
            .query(&[
                ("s", stooq_symbol.as_str()),
                ("f", "sd2t2ohlcv"),
                ("h", ""),
                ("e", "csv"),
            ])
            .send()
            .await
            .context("Failed to call Stooq quote endpoint")?
            .error_for_status()
            .context("Stooq quote endpoint returned error status")?
            .text()
            .await
            .context("Failed to decode Stooq quote response")?;

        // CSV: Symbol,Date,Time,Open,High,Low,Close,Volume
        let mut lines = body.lines();
        let _header = lines.next().context("Stooq response missing header")?;
        let line = lines.next().context("Stooq response missing data row")?;
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 8 {
            anyhow::bail!("Stooq response has invalid column count");
        }

        if cols[1] == "N/D" || cols[2] == "N/D" || cols[6] == "N/D" {
            anyhow::bail!("Stooq returned N/D quote");
        }

        let price = cols[6]
            .parse::<f64>()
            .context("Failed to parse Stooq close price")?;
        let volume = cols[7]
            .parse::<u64>()
            .context("Failed to parse Stooq volume")?;
        let datetime = format!("{} {}", cols[1], cols[2]);
        let naive = NaiveDateTime::parse_from_str(&datetime, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| {
                let date = chrono::NaiveDate::parse_from_str(cols[1], "%Y-%m-%d")?;
                Ok::<NaiveDateTime, chrono::ParseError>(date.and_time(NaiveTime::MIN))
            })
            .context("Failed to parse Stooq quote timestamp")?;
        let timestamp = Utc.from_utc_datetime(&naive);

        Ok(StockQuote {
            symbol: symbol.to_string(),
            price,
            volume,
            timestamp,
        })
    }

    async fn fetch_historical_from_http(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<HistoricalQuote>> {
        let url = format!(
            "https://query1.finance.yahoo.com/v8/finance/chart/{}",
            symbol
        );
        let response = self
            .http_client
            .get(url)
            .query(&[
                ("period1", start.timestamp().to_string()),
                ("period2", end.timestamp().to_string()),
                ("interval", interval.to_string()),
                ("events", "history".to_string()),
                ("includePrePost", "false".to_string()),
            ])
            .send()
            .await
            .context("Failed to call Yahoo Finance chart endpoint")?
            .error_for_status()
            .context("Yahoo Finance chart endpoint returned error status")?;

        let json: Value = response
            .json()
            .await
            .context("Failed to decode Yahoo chart response")?;

        let result = json
            .get("chart")
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get(0))
            .context("Yahoo chart response missing result data")?;

        let timestamps = result
            .get("timestamp")
            .and_then(|v| v.as_array())
            .context("Yahoo chart response missing timestamps")?;

        let quote = result
            .get("indicators")
            .and_then(|v| v.get("quote"))
            .and_then(|v| v.get(0))
            .context("Yahoo chart response missing quote indicators")?;

        let opens = quote
            .get("open")
            .and_then(|v| v.as_array())
            .context("Yahoo chart response missing open series")?;
        let highs = quote
            .get("high")
            .and_then(|v| v.as_array())
            .context("Yahoo chart response missing high series")?;
        let lows = quote
            .get("low")
            .and_then(|v| v.as_array())
            .context("Yahoo chart response missing low series")?;
        let closes = quote
            .get("close")
            .and_then(|v| v.as_array())
            .context("Yahoo chart response missing close series")?;
        let volumes = quote
            .get("volume")
            .and_then(|v| v.as_array())
            .context("Yahoo chart response missing volume series")?;

        let adj_closes = result
            .get("indicators")
            .and_then(|v| v.get("adjclose"))
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("adjclose"))
            .and_then(|v| v.as_array());

        let len = timestamps
            .len()
            .min(opens.len())
            .min(highs.len())
            .min(lows.len())
            .min(closes.len())
            .min(volumes.len());

        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let ts = match timestamps[i].as_i64() {
                Some(v) => v,
                None => continue,
            };
            let open = match opens[i].as_f64() {
                Some(v) => v,
                None => continue,
            };
            let high = match highs[i].as_f64() {
                Some(v) => v,
                None => continue,
            };
            let low = match lows[i].as_f64() {
                Some(v) => v,
                None => continue,
            };
            let close = match closes[i].as_f64() {
                Some(v) => v,
                None => continue,
            };
            let volume = volumes[i].as_u64().unwrap_or(0);
            let timestamp = DateTime::from_timestamp(ts, 0)
                .context("Yahoo chart response contains invalid timestamp")?
                .with_timezone(&Utc);
            let adj_close = adj_closes
                .and_then(|arr| arr.get(i))
                .and_then(|v| v.as_f64());

            out.push(HistoricalQuote {
                symbol: symbol.to_string(),
                timestamp,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                timeframe: interval.to_string(),
                source: "yahoo_finance_http".to_string(),
            });
        }

        Ok(out)
    }

    fn effective_interval(
        &self,
        requested_interval: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> String {
        let requested = requested_interval.trim().to_lowercase();
        if requested != "1m" && requested != "1min" {
            return requested_interval.to_string();
        }

        let now = Utc::now();
        let age_days = (now - start).num_days();
        let span_days = (end - start).num_days();

        if age_days > 60 || span_days > 60 {
            "1h".to_string()
        } else if age_days > 30 || span_days > 30 {
            "5m".to_string()
        } else {
            "1m".to_string()
        }
    }

    fn chunk_days_for_interval(&self, interval: &str) -> i64 {
        match interval {
            "1m" | "1min" | "2m" | "5m" | "15m" | "30m" => 7,
            "60m" | "90m" | "1h" => 30,
            "1d" | "1wk" | "1mo" | "3mo" => 365,
            _ => 30,
        }
    }

    fn coarser_interval(&self, interval: &str) -> Option<&'static str> {
        match interval {
            "1m" | "1min" => Some("5m"),
            "2m" | "5m" => Some("15m"),
            "15m" | "30m" => Some("1h"),
            "60m" | "90m" | "1h" => Some("1d"),
            _ => None,
        }
    }

    async fn fetch_historical_from_http_chunked(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<HistoricalQuote>> {
        let mut all = Vec::new();
        let mut cursor_start = start;
        let chunk_days = self.chunk_days_for_interval(interval);
        let mut had_success = false;

        while cursor_start < end {
            let mut cursor_end =
                std::cmp::min(cursor_start + chrono::Duration::days(chunk_days), end);
            if cursor_end <= cursor_start {
                cursor_end = cursor_start + chrono::Duration::minutes(1);
            }

            let mut attempt_interval = interval.to_string();
            let mut chunk_opt: Option<Vec<HistoricalQuote>> = None;
            let mut fail_chain: Vec<String> = Vec::new();
            loop {
                match self
                    .fetch_historical_from_http(
                        symbol,
                        cursor_start,
                        cursor_end,
                        attempt_interval.as_str(),
                    )
                    .await
                {
                    Ok(c) => {
                        chunk_opt = Some(c);
                        break;
                    }
                    Err(err) => {
                        fail_chain.push(format!("{}: {}", attempt_interval, err));
                        if let Some(next) = self.coarser_interval(attempt_interval.as_str()) {
                            attempt_interval = next.to_string();
                            continue;
                        }
                        break;
                    }
                }
            }

            let mut chunk = if let Some(c) = chunk_opt {
                c
            } else {
                tracing::warn!(
                    %symbol,
                    range = %format!("{}..{}", cursor_start, cursor_end),
                    failures = %fail_chain.join(" | "),
                    "yahoo HTTP chunk skipped"
                );
                cursor_start = cursor_end;
                continue;
            };
            had_success = true;
            all.append(&mut chunk);
            cursor_start = cursor_end;
        }

        if !had_success {
            anyhow::bail!(
                "Yahoo HTTP fallback returned no successful chunks for {} [{}..{}], interval={}",
                symbol,
                start,
                end,
                interval
            );
        }

        all.sort_by_key(|q| q.timestamp);
        all.dedup_by_key(|q| q.timestamp);
        Ok(all)
    }
}

#[async_trait]
impl DataSourcePort for YahooFinanceAdapter {
    async fn fetch_quote(&self, symbol: &str) -> Result<StockQuote> {
        const MAX_ATTEMPTS: usize = 3;
        let mut connector_error = None;

        for attempt in 1..=MAX_ATTEMPTS {
            match self.fetch_from_connector(symbol).await {
                Ok(quote) => return Ok(quote),
                Err(e) => {
                    connector_error = Some(e);
                    if attempt < MAX_ATTEMPTS {
                        sleep(Duration::from_millis(250 * attempt as u64)).await;
                    }
                }
            }
        }

        match self.fetch_from_http_quote_api(symbol).await {
            Ok(quote) => Ok(quote),
            Err(http_err) => {
                let connector_err = connector_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown connector error".to_string());
                match self.fetch_from_stooq(symbol).await {
                    Ok(quote) => Ok(quote),
                    Err(stooq_err) => Err(stooq_err).context(format!(
                        "Failed to fetch quote from Yahoo Finance (connector error: {connector_err}; yahoo-http error: {http_err})"
                    )),
                }
            }
        }
    }

    async fn fetch_historical_quotes(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<HistoricalQuote>> {
        let end = std::cmp::min(end, Utc::now());
        if start >= end {
            anyhow::bail!(
                "historical range is invalid: start {} >= end {}",
                start,
                end
            );
        }

        let effective_interval = self.effective_interval(interval, start, end);
        let start_odt = OffsetDateTime::from_unix_timestamp(start.timestamp())
            .context("Invalid historical start timestamp")?;
        let end_odt = OffsetDateTime::from_unix_timestamp(end.timestamp())
            .context("Invalid historical end timestamp")?;
        let response = self
            .provider
            .get_quote_history_interval(symbol, start_odt, end_odt, &effective_interval)
            .await;

        let quotes = match response {
            Ok(resp) => resp
                .quotes()
                .context("Failed to parse historical quotes from Yahoo Finance SDK")?,
            Err(sdk_err) => {
                let fallback = self
                    .fetch_historical_from_http_chunked(symbol, start, end, &effective_interval)
                    .await
                    .context(format!(
                        "Failed historical fetch from Yahoo SDK and HTTP fallback (sdk_error: {sdk_err}, requested_interval={}, effective_interval={})",
                        interval,
                        effective_interval
                    ))?;
                return Ok(fallback);
            }
        };

        let mut result = Vec::with_capacity(quotes.len());
        for q in quotes {
            let ts =
                i64::try_from(q.timestamp).context("Historical quote timestamp out of range")?;
            let timestamp = DateTime::from_timestamp(ts, 0)
                .context("Historical quote has invalid timestamp")?
                .with_timezone(&Utc);
            result.push(HistoricalQuote {
                symbol: symbol.to_string(),
                timestamp,
                open: q.open,
                high: q.high,
                low: q.low,
                close: q.close,
                adj_close: Some(q.adjclose),
                volume: q.volume,
                timeframe: effective_interval.clone(),
                source: "yahoo_finance".to_string(),
            });
        }

        Ok(result)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YahooQuoteResponse {
    quote_response: YahooQuoteResult,
}

#[derive(Debug, Deserialize)]
struct YahooQuoteResult {
    result: Vec<YahooQuoteItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct YahooQuoteItem {
    symbol: String,
    regular_market_price: Option<f64>,
    regular_market_volume: Option<u64>,
    regular_market_time: i64,
    post_market_price: Option<f64>,
    post_market_volume: Option<u64>,
    post_market_time: Option<i64>,
    pre_market_price: Option<f64>,
    pre_market_volume: Option<u64>,
    pre_market_time: Option<i64>,
}
