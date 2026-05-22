use axum::{extract::{State, Query}, http::StatusCode, Json};
use crate::http::models::{AddSymbolRequest, MessageResponse, RemoveSymbolRequest, SymbolsResponse, SymbolSearchResponse, SymbolSearchItem};
use crate::http::middlewares::auth::Claims;
use super::AppState;

use crate::data_ingestion::adapters::yahoo_finance::YahooFinanceAdapter;
use crate::data_ingestion::ports::DataSourcePort;
use crate::data_ingestion::domain::models::StreamMessage;
use crate::database::domain::models::StockTick;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::sync::OnceLock;
use tokio::sync::RwLock;
use std::collections::HashMap;

type ChartCacheMap = HashMap<String, (chrono::DateTime<Utc>, Vec<crate::http::models::ChartCandle>)>;

fn chart_cache() -> &'static RwLock<ChartCacheMap> {
    static CACHE: OnceLock<RwLock<ChartCacheMap>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

#[utoipa::path(
    get,
    path = "/api/symbols",
    tag = "Symbols",
    responses(
        (status = 200, description = "List of current symbols", body = SymbolsResponse),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn get_symbols(State(state): State<AppState>) -> (StatusCode, Json<SymbolsResponse>) {
    let symbols = state.symbols.read().await;
    (
        StatusCode::OK,
        Json(SymbolsResponse {
            symbols: symbols.clone(),
        }),
    )
}

#[utoipa::path(
    post,
    path = "/api/symbols",
    tag = "Symbols",
    request_body = AddSymbolRequest,
    responses(
        (status = 201, description = "Symbol added successfully", body = MessageResponse),
        (status = 409, description = "Symbol already exists", body = MessageResponse),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn add_symbol(
    State(state): State<AppState>,
    _claims: Claims,
    Json(payload): Json<AddSymbolRequest>,
) -> (StatusCode, Json<MessageResponse>) {
    let symbol = payload.symbol.to_uppercase();
    let mut symbols = state.symbols.write().await;

    if symbols.contains(&symbol) {
        return (
            StatusCode::CONFLICT,
            Json(MessageResponse {
                message: format!("Symbol {} already exists", symbol),
            }),
        );
    }

    symbols.push(symbol.clone());
    let _ = state.user_repo.add_active_symbol(&symbol).await;
    
    // Fast Warmup: Fetch recent historical data and dispatch to market-data-raw
    let producer = state.producer.clone();
    let symbol_clone = symbol.clone();
    
    tokio::spawn(async move {
        println!("🚀 Starting fast warmup for newly added symbol: {}", symbol_clone);
        let data_source = YahooFinanceAdapter::new();
        let end = Utc::now();
        // 120 hours = 5 days to ensure enough trading minutes across weekends
        let start = end - chrono::Duration::hours(120);

        match data_source.fetch_historical_quotes(&symbol_clone, start, end, "1m").await {
            Ok(quotes) => {
                let recent: Vec<_> = quotes.into_iter().rev().take(1000).collect::<Vec<_>>().into_iter().rev().collect();
                println!("Fetched {} historical 1m candles for {} warmup", recent.len(), symbol_clone);

                let mut last_price: Option<Decimal> = None;

                // Send historical ticks with their real timestamps for ML model training
                for quote in recent {
                    if let Some(price) = Decimal::from_f64_retain(quote.close) {
                        last_price = Some(price);
                        let tick = StockTick {
                            symbol: quote.symbol.clone(),
                            timestamp: quote.timestamp,
                            price,
                            volume: quote.volume as i64,
                            bid: None,
                            ask: None,
                            source: "warmup".to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&tick) {
                            let _ = producer.send_message(StreamMessage {
                                topic: "market-data-raw".to_string(),
                                key: Some(quote.symbol),
                                value: json,
                            }).await;
                        }
                    }
                }

                // Send one final "now" tick to anchor the generated signal to the current time.
                // Without this, the signal timestamp would be the last historical candle's time (days ago).
                if let Some(price) = last_price {
                    let now_tick = StockTick {
                        symbol: symbol_clone.clone(),
                        timestamp: Utc::now(),
                        price,
                        volume: 0,
                        bid: None,
                        ask: None,
                        source: "warmup_anchor".to_string(),
                    };
                    if let Ok(json) = serde_json::to_string(&now_tick) {
                        let _ = producer.send_message(StreamMessage {
                            topic: "market-data-raw".to_string(),
                            key: Some(symbol_clone.clone()),
                            value: json,
                        }).await;
                    }
                }

                println!("✅ Warmup complete for {} — signal timestamps anchored to now", symbol_clone);
            }
            Err(e) => eprintln!("❌ Failed to fetch warmup data for {}: {}", symbol_clone, e),
        }
    });

    (
        StatusCode::CREATED,
        Json(MessageResponse {
            message: format!("Symbol {} added successfully", symbol),
        }),
    )
}


#[utoipa::path(
    delete,
    path = "/api/symbols",
    tag = "Symbols",
    request_body = RemoveSymbolRequest,
    responses(
        (status = 200, description = "Symbol removed successfully", body = MessageResponse),
        (status = 404, description = "Symbol not found", body = MessageResponse),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn remove_symbol(
    State(state): State<AppState>,
    _claims: Claims,
    Json(payload): Json<RemoveSymbolRequest>,
) -> (StatusCode, Json<MessageResponse>) {
    let symbol = payload.symbol.to_uppercase();
    let mut symbols = state.symbols.write().await;

    if let Some(pos) = symbols.iter().position(|s| s == &symbol) {
        symbols.remove(pos);
        let _ = state.user_repo.remove_active_symbol(&symbol).await;
        (
            StatusCode::OK,
            Json(MessageResponse {
                message: format!("Symbol {} removed successfully", symbol),
            }),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Symbol {} not found", symbol),
            }),
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
}

#[utoipa::path(
    get,
    path = "/api/symbols/search",
    tag = "Symbols",
    params(
        ("q" = String, Query, description = "Search query for symbol lookup")
    ),
    responses(
        (status = 200, description = "Matching symbols", body = SymbolSearchResponse),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn search_symbols(
    _claims: Claims,
    Query(query): Query<SearchQuery>,
) -> (StatusCode, Json<SymbolSearchResponse>) {
    let q = query.q.trim().to_uppercase();
    if q.is_empty() {
        return (
            StatusCode::OK,
            Json(SymbolSearchResponse { results: vec![] }),
        );
    }

    let url = format!(
        "https://query2.finance.yahoo.com/v1/finance/search?q={}&quotesCount=10&newsCount=0&listsCount=0&enableFuzzyQuery=false&quotesQueryId=tss_match_phrase_query&lang=en-US&region=US",
        urlencoding::encode(&q)
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("User-Agent", "db-con/0.1 (market-data-ingestion)")
        .send()
        .await;

    match response {
        Ok(resp) => {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                let results: Vec<SymbolSearchItem> = json
                    .get("quotes")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                let symbol = item.get("symbol")?.as_str()?.to_string();
                                let name = item
                                    .get("longname")
                                    .or_else(|| item.get("shortname"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let type_display = item
                                    .get("quoteType")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("EQUITY")
                                    .to_string();
                                let exchange = item
                                    .get("exchDisp")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                Some(SymbolSearchItem {
                                    symbol,
                                    name,
                                    type_display,
                                    exchange,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                (StatusCode::OK, Json(SymbolSearchResponse { results }))
            } else {
                (
                    StatusCode::OK,
                    Json(SymbolSearchResponse { results: vec![] }),
                )
            }
        }
        Err(_) => (
            StatusCode::OK,
            Json(SymbolSearchResponse { results: vec![] }),
        ),
    }
}

#[derive(Debug, Deserialize)]
pub struct ChartQuery {
    pub symbol: String,
    pub timeframe: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/symbols/chart",
    tag = "Symbols",
    params(
        ("symbol" = String, Query, description = "Symbol to fetch chart data for"),
        ("timeframe" = Option<String>, Query, description = "Timeframe (default 1m)")
    ),
    responses(
        (status = 200, description = "Chart data", body = crate::http::models::ChartResponse),
    ),
    security(
        ("BearerAuth" = [])
    )
)]
pub async fn get_chart_data(
    State(state): State<AppState>,
    _claims: Claims,
    Query(query): Query<ChartQuery>,
) -> (StatusCode, Json<crate::http::models::ChartResponse>) {
    use rust_decimal::prelude::ToPrimitive;
    let symbol = query.symbol.to_uppercase();
    let timeframe = query.timeframe.unwrap_or_else(|| "1m".to_string());
    
    let db_timeframe = if timeframe == "1m" { "1min".to_string() } else { timeframe.clone() };
    
    let mut chart_candles = Vec::new();
    let end = Utc::now();
    let cache_key = format!("{}_{}", symbol, timeframe);
    
    let is_simulated = std::env::var("USE_SIMULATION").unwrap_or_default() == "true";

    // 1. Fetch from Yahoo Finance if NOT in simulation mode and not cached within last 5 minutes.
    //    In simulation mode we skip this entirely — real historical prices are on a completely
    //    different scale than the simulated GBM prices, which would produce a massive cliff drop
    //    at the stitch point and make the chart look broken.
    if !is_simulated {
        let mut needs_fetch = true;
        if let Some((ts, cached)) = chart_cache().read().await.get(&cache_key) {
            if (end - *ts).num_minutes() < 5 {
                chart_candles = cached.clone();
                needs_fetch = false;
            }
        }

        if needs_fetch {
            let data_source = YahooFinanceAdapter::new();
            let start = end - chrono::Duration::hours(120); // 5 days back
            if let Ok(quotes) = data_source.fetch_historical_quotes(&symbol, start, end, &timeframe).await {
                // Check if data is dense enough to be intraday (ignore 1d fallback if timeframe=1m)
                let is_intraday = timeframe != "1m" || quotes.len() < 3 || {
                    let diff = quotes[1].timestamp.timestamp() - quotes[0].timestamp.timestamp();
                    diff <= 900 // max 15 minutes between candles
                };

                if is_intraday {
                    let mut fetched = Vec::new();
                    for q in quotes {
                        fetched.push(crate::http::models::ChartCandle {
                            time: q.timestamp.timestamp(),
                            open: q.open,
                            high: q.high,
                            low: q.low,
                            close: q.close,
                            volume: q.volume,
                        });
                    }
                    chart_candles = fetched.clone();
                    chart_cache().write().await.insert(cache_key, (end, fetched));
                }
            }
        }
    }
    
    // 2. Append live ScyllaDB data for up-to-the-second candles
    if let Ok(db_candles) = state.repository.get_latest_candles(&symbol, &db_timeframe, 2000).await {
        for c in db_candles {
            chart_candles.push(crate::http::models::ChartCandle {
                time: c.timestamp.timestamp(),
                open: c.open.to_f64().unwrap_or(0.0),
                high: c.high.to_f64().unwrap_or(0.0),
                low: c.low.to_f64().unwrap_or(0.0),
                close: c.close.to_f64().unwrap_or(0.0),
                volume: c.volume as u64,
            });
        }
    }
    
    // 3. Ensure order and deduplicate
    chart_candles.sort_by_key(|c| c.time);
    chart_candles.dedup_by_key(|c| c.time);
    
    // 4. Filter out artificial "flatline" heartbeat candles (e.g. during weekends)
    let mut filtered_candles: Vec<crate::http::models::ChartCandle> = Vec::new();
    for c in chart_candles {
        if let Some(prev) = filtered_candles.last() {
            let is_flat = c.open == c.close && c.high == c.low && c.open == c.high;
            if is_flat && (c.close - prev.close).abs() < 0.0001 {
                continue;
            }
        }
        filtered_candles.push(c);
    }
    chart_candles = filtered_candles;
    
    (
        StatusCode::OK,
        Json(crate::http::models::ChartResponse {
            symbol,
            timeframe,
            candles: chart_candles,
        }),
    )
}
