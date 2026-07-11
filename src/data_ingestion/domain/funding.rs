//! Domain-моделі funding-rate даних (M3.1).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Точка funding-ставки перпетуала на момент розрахунку.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FundingRatePoint {
    pub symbol: String,
    /// Час funding-розрахунку (зазвичай кожні 8 годин).
    pub timestamp: DateTime<Utc>,
    /// Ставка за інтервал (частка, напр. 0.0001 = 1 б.п. за 8h).
    pub rate: Decimal,
    /// Mark-ціна перпа на момент розрахунку.
    pub mark_price: Decimal,
    /// Спот-ціна (для basis), якщо відома.
    pub spot_price: Option<Decimal>,
}

impl FundingRatePoint {
    /// Basis = (perp - spot) / spot, якщо спот відомий.
    pub fn basis(&self) -> Option<Decimal> {
        let spot = self.spot_price?;
        if spot <= Decimal::ZERO {
            return None;
        }
        Some((self.mark_price - spot) / spot)
    }
}
