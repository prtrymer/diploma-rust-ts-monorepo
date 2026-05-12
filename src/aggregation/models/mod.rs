use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct CandleWindow {
    pub symbol: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub open: Option<Decimal>,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    pub close: Option<Decimal>,
    pub volume: i64,
    pub trades_count: i32,
}

impl CandleWindow {
    pub fn new(symbol: String, start_time: DateTime<Utc>) -> Self {
        Self {
            symbol,
            start_time,
            end_time: start_time,
            open: None,
            high: None,
            low: None,
            close: None,
            volume: 0,
            trades_count: 0,
        }
    }

    pub fn add_tick(&mut self, price: Decimal, volume: i64, timestamp: DateTime<Utc>) {
        // Set open price if not set
        if self.open.is_none() {
            self.open = Some(price);
        }

        // Update high
        self.high = Some(
            self.high
                .map(|h| if price > h { price } else { h })
                .unwrap_or(price),
        );

        // Update low
        self.low = Some(
            self.low
                .map(|l| if price < l { price } else { l })
                .unwrap_or(price),
        );

        // Always update close to latest price
        self.close = Some(price);
        self.end_time = timestamp;
        self.volume += volume;
        self.trades_count += 1;
    }

    pub fn is_complete(&self) -> bool {
        self.open.is_some() && self.high.is_some() && self.low.is_some() && self.close.is_some()
    }
}

pub fn get_1min_window(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    timestamp
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}

pub fn get_5min_window(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let minute = (timestamp.minute() / 5) * 5;
    timestamp
        .with_minute(minute)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}
