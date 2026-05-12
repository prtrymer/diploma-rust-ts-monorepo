use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignalDirection {
    Long,
    Short,
    Exit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEvent {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub direction: SignalDirection,
    pub strength: Decimal,
    pub strategy_name: String,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub id: Uuid,
    pub signal_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub order_type: OrderType,
    pub limit_price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvent {
    pub id: Uuid,
    pub order_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub fill_price: Decimal,
    pub commission: Decimal,
    pub slippage: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum TradingEvent {
    Signal(SignalEvent),
    Order(OrderEvent),
    Fill(FillEvent),
}

impl TradingEvent {
    pub fn topic(&self) -> &'static str {
        match self {
            TradingEvent::Signal(_) => "trading-signals",
            TradingEvent::Order(_) => "trading-orders",
            TradingEvent::Fill(_) => "trading-fills",
        }
    }
}
