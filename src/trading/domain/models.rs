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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn position(symbol: &str, quantity: Decimal, avg_entry: Decimal, current: Decimal) -> Position {
        Position {
            symbol: symbol.to_string(),
            quantity,
            avg_entry_price: avg_entry,
            current_price: current,
            unrealized_pnl: (current - avg_entry) * quantity,
            realized_pnl: Decimal::ZERO,
            last_updated: Utc::now(),
        }
    }

    #[test]
    fn new_portfolio_has_no_positions_and_full_cash() {
        let p = Portfolio::new(dec!(10000));
        assert!(p.positions.is_empty());
        assert_eq!(p.cash, dec!(10000));
        assert_eq!(p.get_total_pnl(), Decimal::ZERO);
    }

    #[test]
    fn total_value_sums_cash_and_positions() {
        let mut p = Portfolio::new(dec!(100000));
        p.cash = dec!(50000);
        p.positions.insert(
            "AAPL".to_string(),
            position("AAPL", dec!(100), dec!(400), dec!(450)),
        );
        assert_eq!(p.get_total_value(), dec!(95000));
    }

    #[test]
    fn total_pnl_is_relative_to_initial_capital() {
        let mut p = Portfolio::new(dec!(100000));
        p.cash = dec!(50000);
        p.positions.insert(
            "AAPL".to_string(),
            position("AAPL", dec!(100), dec!(400), dec!(450)),
        );
        assert_eq!(p.get_total_pnl(), dec!(-5000));
    }
}
