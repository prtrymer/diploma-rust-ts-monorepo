use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::metrics;
use crate::trading::domain::events::{FillEvent, OrderSide};
use crate::trading::domain::models::Portfolio;

/// Звіт бектесту. Головні цифри — ЗАВЖДИ net-of-cost (інваріант 2);
/// gross-поля існують лише як діагностика поруч із net, ніколи замість.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    /// Net-of-cost результат (головний).
    pub total_return: Decimal,
    pub total_return_pct: Decimal,
    /// Діагностика: сумарні транзакційні витрати (комісія+спред+impact).
    pub total_costs: Decimal,
    /// Діагностика: gross = net + витрати. Не замінює net.
    pub gross_return: Decimal,
    pub gross_return_pct: Decimal,
    pub sharpe_ratio: Decimal,
    /// None означає: або угод менше 10, або σ_down = 0 (усі trades прибуткові — Sortino не визначений).
    pub sortino_ratio: Option<Decimal>,
    pub max_drawdown: Decimal,
    pub max_drawdown_pct: Decimal,
    /// Calmar = annual return % / MDD %. None без просадки.
    pub calmar: Option<Decimal>,
    /// Оборот за період: торгований нотіонал / середня вартість портфеля.
    pub turnover: Decimal,
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

        let mut equity_curve: Vec<Decimal> = vec![initial_capital];
        let mut trade_pnls: Vec<Decimal> = Vec::new();
        let mut total_costs = Decimal::ZERO;

        for fill in fills {
            if fill.quantity <= Decimal::ZERO || fill.fill_price <= Decimal::ZERO {
                continue;
            }
            total_costs += fill.commission;
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
            equity_curve.push(cash + position_qty * mark_price);
        }

        let (max_drawdown, max_drawdown_pct) = metrics::max_drawdown(&equity_curve);

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

        let win_rate = metrics::hit_rate(&trade_pnls)
            .map(|h| h * Decimal::ONE_HUNDRED)
            .unwrap_or(Decimal::ZERO);

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
        let profit_factor = metrics::profit_factor(&trade_pnls).unwrap_or(Decimal::ZERO);

        let sharpe_ratio = metrics::sharpe_annualized(&equity_curve);
        // Sortino визначений лише при достатній кількості угод і наявності хоча б одного
        // негативного equity-step (σ_down > 0). При total_trades < 10 вибірка занадто мала.
        const MIN_TRADES_FOR_SORTINO: usize = 10;
        let sortino_ratio = if total_trades >= MIN_TRADES_FOR_SORTINO {
            calc_sortino_annualized(&equity_curve)
        } else {
            None
        };

        // Gross — тільки діагностика поруч із net (інваріант 2).
        let gross_return = metrics::pnl_after_costs(total_return, -total_costs);
        let gross_return_pct = pct(gross_return, initial_capital);

        let avg_equity = if equity_curve.is_empty() {
            initial_capital
        } else {
            equity_curve.iter().copied().sum::<Decimal>()
                / Decimal::from(equity_curve.len() as u64)
        };
        let turnover = metrics::turnover(fills, avg_equity);
        let calmar = metrics::calmar(total_return_pct, max_drawdown_pct);

        Self {
            total_return,
            total_return_pct,
            total_costs,
            gross_return,
            gross_return_pct,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown,
            max_drawdown_pct,
            calmar,
            turnover,
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

// Annualized Sortino ratio (risk-free rate = 0), downside deviation uses all periods.
// Повертає None, якщо σ_down = 0 (жодного негативного equity-step — Sortino не визначений).
fn calc_sortino_annualized(equity_curve: &[Decimal]) -> Option<Decimal> {
    use rust_decimal::prelude::FromPrimitive;

    if equity_curve.len() < 3 {
        return None;
    }
    let returns = metrics::simple_returns(equity_curve);
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    // Downside variance: sum of squared negative returns divided by total N.
    let downside_var = returns
        .iter()
        .map(|&r| if r < 0.0 { r * r } else { 0.0 })
        .sum::<f64>()
        / returns.len() as f64;
    let downside_std = downside_var.sqrt();
    if downside_std <= 1e-12 {
        // σ_down ≈ 0: всі equity-кроки невід'ємні — Sortino математично не визначений (→ +∞).
        return None;
    }
    Decimal::from_f64(mean / downside_std * TRADING_DAYS_PER_YEAR.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn fill(side: OrderSide, qty: Decimal, price: Decimal, commission: Decimal) -> FillEvent {
        FillEvent {
            id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            symbol: "TEST".into(),
            side,
            quantity: qty,
            fill_price: price,
            commission,
            slippage: Decimal::ZERO,
        }
    }

    // M0.1: net завжди у звіті; gross — окремо; gross - costs == net по угодах.
    #[test]
    fn report_separates_net_and_gross() {
        let initial = dec!(10000);
        let fills = vec![
            fill(OrderSide::Buy, dec!(10), dec!(100), dec!(5)),
            fill(OrderSide::Sell, dec!(10), dec!(110), dec!(5)),
        ];
        let mut portfolio = Portfolio::new(initial);
        // Емуляція фінального стану: 10 куплено за 1000 (+5), продано за 1100 (−5).
        portfolio.cash = initial - dec!(1000) - dec!(5) + dec!(1100) - dec!(5);

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial);
        assert_eq!(report.total_costs, dec!(10));
        assert_eq!(report.total_return, dec!(90)); // 100 gross − 10 costs
        assert_eq!(report.gross_return, dec!(100));
        assert_eq!(report.gross_return - report.total_costs, report.total_return);
    }

    // M0.1: нульові витрати → net == gross (регресійний sanity).
    #[test]
    fn zero_costs_make_net_equal_gross() {
        let initial = dec!(10000);
        let fills = vec![
            fill(OrderSide::Buy, dec!(10), dec!(100), Decimal::ZERO),
            fill(OrderSide::Sell, dec!(10), dec!(110), Decimal::ZERO),
        ];
        let mut portfolio = Portfolio::new(initial);
        portfolio.cash = initial + dec!(100);

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial);
        assert_eq!(report.total_costs, Decimal::ZERO);
        assert_eq!(report.total_return, report.gross_return);
        assert_eq!(report.total_return_pct, report.gross_return_pct);
    }

    #[test]
    fn turnover_present_in_report() {
        let initial = dec!(10000);
        let fills = vec![
            fill(OrderSide::Buy, dec!(10), dec!(100), Decimal::ZERO),
            fill(OrderSide::Sell, dec!(10), dec!(100), Decimal::ZERO),
        ];
        let portfolio = Portfolio::new(initial);
        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial);
        assert!(report.turnover > Decimal::ZERO);
    }
}
