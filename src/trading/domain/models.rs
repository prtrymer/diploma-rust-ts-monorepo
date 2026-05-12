use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub avg_entry_price: Decimal,
    pub current_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Portfolio {
    pub positions: HashMap<String, Position>,
    pub cash: Decimal,
    pub initial_capital: Decimal,
    pub last_updated: DateTime<Utc>,
}

impl Portfolio {
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            positions: HashMap::new(),
            cash: initial_capital,
            initial_capital,
            last_updated: Utc::now(),
        }
    }

    pub fn get_total_value(&self) -> Decimal {
        let positions_value: Decimal = self
            .positions
            .values()
            .map(|p| p.quantity * p.current_price)
            .sum();
        self.cash + positions_value
    }

    pub fn get_total_pnl(&self) -> Decimal {
        self.get_total_value() - self.initial_capital
    }
}
