use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Timeframe {
    Tick,
    OneMin,
    FiveMin,
    FifteenMin,
    OneHour,
    Daily,
    Weekly,
    Monthly,
}

impl Timeframe {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::OneMin => "1min",
            Self::FiveMin => "5min",
            Self::FifteenMin => "15min",
            Self::OneHour => "1h",
            Self::Daily => "1d",
            Self::Weekly => "1w",
            Self::Monthly => "1M",
        }
    }

    pub fn table_name(&self) -> &'static str {
        match self {
            Self::Tick => "stock_ticks",
            Self::OneMin => "stock_1min",
            Self::FiveMin => "stock_5min",
            Self::FifteenMin => "stock_15min",
            Self::OneHour => "stock_1h",
            Self::Daily => "stock_daily",
            Self::Weekly => "stock_weekly",
            Self::Monthly => "stock_monthly",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "tick" => Some(Self::Tick),
            "1min" => Some(Self::OneMin),
            "5min" => Some(Self::FiveMin),
            "15min" => Some(Self::FifteenMin),
            "1h" => Some(Self::OneHour),
            "1d" => Some(Self::Daily),
            "1w" => Some(Self::Weekly),
            "1M" => Some(Self::Monthly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockTick {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: Decimal,
    pub volume: i64,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    #[serde(default = "default_source")]
    pub source: String,
}

fn default_source() -> String {
    "kafka".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub timeframe: Timeframe,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
    pub trades_count: Option<i32>,
    pub vwap: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalCandle {
    pub symbol: String,
    pub timeframe: String,
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub adj_close: Option<Decimal>,
    pub volume: i64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyCandle {
    pub symbol: String,
    pub date: NaiveDate,
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub adj_close: Option<Decimal>,
    pub volume: i64,
    pub trades_count: Option<i32>,
    pub vwap: Option<Decimal>,
    pub price_change: Option<Decimal>,
    pub price_change_percent: Option<Decimal>,
    pub high_low_range: Option<Decimal>,
    pub sma_20: Option<Decimal>,
    pub sma_50: Option<Decimal>,
    pub sma_200: Option<Decimal>,
    pub ema_12: Option<Decimal>,
    pub ema_26: Option<Decimal>,
    pub atr: Option<Decimal>,
    pub volatility: Option<Decimal>,
    pub volume_sma_20: Option<i64>,
    pub volume_ratio: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StockMetadata {
    pub symbol: String,
    pub company_name: Option<String>,
    pub sector: Option<String>,
    pub industry: Option<String>,
    pub exchange: Option<String>,
    pub currency: Option<String>,
    pub last_price: Option<Decimal>,
    pub last_updated: DateTime<Utc>,
    pub data_start_date: Option<NaiveDate>,
    pub data_end_date: Option<NaiveDate>,
}

#[derive(Debug, Clone)]
pub struct TimeframeBucket {
    pub symbol: String,
    pub timeframe: Timeframe,
    pub bucket: String,
}

impl TimeframeBucket {
    pub fn new(symbol: String, timeframe: Timeframe, timestamp: DateTime<Utc>) -> Self {
        let bucket = match timeframe {
            Timeframe::Tick | Timeframe::OneMin => timestamp.format("%Y-%m-%d").to_string(),
            Timeframe::FiveMin | Timeframe::FifteenMin => timestamp.format("%Y-%m-%d").to_string(),
            Timeframe::OneHour => timestamp.format("%Y-%m").to_string(),
            Timeframe::Daily => timestamp.format("%Y-%m-%d").to_string(),
            Timeframe::Weekly => {
                format!("{}-W{:02}", timestamp.year(), timestamp.iso_week().week())
            }
            Timeframe::Monthly => timestamp.format("%Y-%m").to_string(),
        };

        Self {
            symbol,
            timeframe,
            bucket,
        }
    }
}

impl DailyCandle {
    pub fn calculate_basic_metrics(&mut self) {
        self.price_change = Some(self.close - self.open);
        if self.open > Decimal::ZERO {
            self.price_change_percent =
                Some(((self.close - self.open) / self.open) * Decimal::from(100));
        }
        self.high_low_range = Some(self.high - self.low);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub message_id: String,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

impl KafkaMessage {
    pub fn new(
        topic: String,
        partition: i32,
        offset: i64,
        message_id: String,
        symbol: String,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            topic,
            partition,
            offset,
            message_id,
            symbol,
            timestamp,
            processed_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
}
