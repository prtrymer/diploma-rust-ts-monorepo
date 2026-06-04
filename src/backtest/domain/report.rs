use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::trading::domain::events::{FillEvent, OrderSide};
use crate::trading::domain::models::Portfolio;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub total_return: Decimal,
    pub total_return_pct: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub max_drawdown_pct: Decimal,
    pub win_rate: Decimal,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub profit_factor: Decimal,
    pub final_portfolio_value: Decimal,
}

impl BacktestReport {
    pub fn from_fills_and_portfolio(
        fills: &[FillEvent],
        portfolio: &Portfolio,
        initial_capital: Decimal,
    ) -> Self {
        let final_value = portfolio.get_total_value();
        let total_return = final_value - initial_capital;
        let total_return_pct = pct(total_return, initial_capital);

        let mut cash = initial_capital;
        let mut position_qty = Decimal::ZERO;
        let mut avg_entry = Decimal::ZERO;

        let mut peak_equity = initial_capital;
        let mut max_drawdown = Decimal::ZERO;
        let mut equity_curve: Vec<Decimal> = vec![initial_capital];

        let mut trade_pnls: Vec<Decimal> = Vec::new();
        for fill in fills {
            if fill.quantity <= Decimal::ZERO || fill.fill_price <= Decimal::ZERO {
                continue;
            }
            match fill.side {
                OrderSide::Buy => {
                    let qty = fill.quantity;
                    let notional = fill.fill_price * qty;
                    let new_qty = position_qty + qty;
                    let weighted_entry = if new_qty > Decimal::ZERO {
                        ((avg_entry * position_qty) + notional + fill.commission) / new_qty
                    } else {
                        Decimal::ZERO
                    };
                    cash -= notional + fill.commission;
                    position_qty = new_qty;
                    avg_entry = weighted_entry;
                }
                OrderSide::Sell => {
                    let sell_qty = fill.quantity.min(position_qty.max(Decimal::ZERO));
                    if sell_qty <= Decimal::ZERO {
                        continue;
                    }
                    let proceeds = fill.fill_price * sell_qty - fill.commission;
                    let pnl = (fill.fill_price - avg_entry) * sell_qty - fill.commission;
                    cash += proceeds;
                    position_qty -= sell_qty;
                    if position_qty <= Decimal::ZERO {
                        position_qty = Decimal::ZERO;
                        avg_entry = Decimal::ZERO;
                    }
                    trade_pnls.push(pnl);
                }
            }

            let mark_price = fill.fill_price;
            let equity = cash + position_qty * mark_price;
            equity_curve.push(equity);
            if equity > peak_equity {
                peak_equity = equity;
            }
            let dd = peak_equity - equity;
            if dd > max_drawdown {
                max_drawdown = dd;
            }
        }

        let max_drawdown_pct = pct(max_drawdown, peak_equity);

        let winning: Vec<Decimal> = trade_pnls
            .iter()
            .copied()
            .filter(|p| *p > Decimal::ZERO)
            .collect();
        let losing: Vec<Decimal> = trade_pnls
            .iter()
            .copied()
            .filter(|p| *p < Decimal::ZERO)
            .map(|p| p.abs())
            .collect();

        let total_trades = trade_pnls.len();
        let winning_trades = winning.len();
        let losing_trades = losing.len();

        let total_wins: Decimal = winning.iter().copied().sum();
        let total_losses: Decimal = losing.iter().copied().sum();

        let win_rate = if total_trades > 0 {
            Decimal::from(winning_trades as u64) / Decimal::from(total_trades as u64)
                * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        let avg_win = if winning_trades > 0 {
            total_wins / Decimal::from(winning_trades as u64)
        } else {
            Decimal::ZERO
        };

        let avg_loss = if losing_trades > 0 {
            total_losses / Decimal::from(losing_trades as u64)
        } else {
            Decimal::ZERO
        };

        // Undefined when there are no losing trades; keep 0 and let caller print "N/A".
        let profit_factor = if total_losses > Decimal::ZERO {
            total_wins / total_losses
        } else {
            Decimal::ZERO
        };

        let sharpe_ratio = calc_sharpe_annualized(&equity_curve);
        let sortino_ratio = calc_sortino_annualized(&equity_curve);

        Self {
            total_return,
            total_return_pct,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown,
            max_drawdown_pct,
            win_rate,
            total_trades,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            profit_factor,
            final_portfolio_value: final_value,
        }
    }
}

fn pct(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator > Decimal::ZERO {
        numerator / denominator * Decimal::from(100)
    } else {
        Decimal::ZERO
    }
}

const TRADING_DAYS_PER_YEAR: f64 = 252.0;

fn build_returns(equity_curve: &[Decimal]) -> Vec<f64> {
    let mut returns = Vec::with_capacity(equity_curve.len().saturating_sub(1));
    for i in 1..equity_curve.len() {
        let prev = equity_curve[i - 1].to_f64().unwrap_or(0.0);
        let curr = equity_curve[i].to_f64().unwrap_or(0.0);
        if prev > 0.0 {
            returns.push((curr - prev) / prev);
        }
    }
    returns
}

// Annualized Sharpe ratio (risk-free rate = 0), annualization factor √252.
fn calc_sharpe_annualized(equity_curve: &[Decimal]) -> Decimal {
    if equity_curve.len() < 3 {
        return Decimal::ZERO;
    }
    let returns = build_returns(equity_curve);
    if returns.len() < 2 {
        return Decimal::ZERO;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    let std = var.sqrt();
    if std <= 0.0 {
        return Decimal::ZERO;
    }
    Decimal::from_f64_retain(mean / std * TRADING_DAYS_PER_YEAR.sqrt()).unwrap_or(Decimal::ZERO)
}

// Annualized Sortino ratio (risk-free rate = 0), downside deviation uses all periods.
fn calc_sortino_annualized(equity_curve: &[Decimal]) -> Decimal {
    if equity_curve.len() < 3 {
        return Decimal::ZERO;
    }
    let returns = build_returns(equity_curve);
    if returns.len() < 2 {
        return Decimal::ZERO;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    // Downside variance: sum of squared negative returns divided by total N.
    let downside_var = returns
        .iter()
        .map(|&r| if r < 0.0 { r * r } else { 0.0 })
        .sum::<f64>()
        / returns.len() as f64;
    let downside_std = downside_var.sqrt();
    if downside_std <= 0.0 {
        return if mean > 0.0 { Decimal::from(99) } else { Decimal::ZERO };
    }
    Decimal::from_f64_retain(mean / downside_std * TRADING_DAYS_PER_YEAR.sqrt())
        .unwrap_or(Decimal::ZERO)
}
