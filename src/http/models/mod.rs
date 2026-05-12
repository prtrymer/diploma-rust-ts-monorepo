use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct AddSymbolRequest {
    pub symbol: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RemoveSymbolRequest {
    pub symbol: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SymbolsResponse {
    pub symbols: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MessageResponse {
    pub message: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct BacktestRequest {
    pub symbol: String,
    pub start: String,
    pub end: String,
    pub initial_capital: f64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuthResponse {
    pub token: String,
    pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SymbolSearchItem {
    pub symbol: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_display: String,
    pub exchange: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SymbolSearchResponse {
    pub results: Vec<SymbolSearchItem>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ChartCandle {
    pub time: i64, // Unix timestamp in seconds
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChartResponse {
    pub symbol: String,
    pub timeframe: String,
    pub candles: Vec<ChartCandle>,
}

