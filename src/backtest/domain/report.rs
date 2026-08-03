use std::collections::BTreeMap;

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
    /// `allow_short` — режим обліку портфеля, що підписує цей звіт
    /// (`PortfolioPort::allows_short`). Не косметика: у long-only Sell при
    /// нульовій позиції портфель ігнорує, у режимі шортів — відкриває шорт.
    /// Реконструкція мусить робити те саме, інакше книга розійдеться з
    /// портфелем.
    pub fn from_fills_and_portfolio(
        fills: &[FillEvent],
        portfolio: &Portfolio,
        initial_capital: Decimal,
        allow_short: bool,
    ) -> Self {
        let final_value = portfolio.get_total_value();
        let total_return = final_value - initial_capital;
        let total_return_pct = pct(total_return, initial_capital);

        let TradeBook {
            equity_curve,
            trade_pnls,
            total_costs,
        } = replay_fills(fills, initial_capital, allow_short);

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

/// Позиція в реконструйованій книзі. `qty` знаковий (>0 лонг, <0 шорт),
/// `avg_entry` завжди додатний і вже містить комісію входу — так само, як
/// `Position::avg_entry_price` у портфелі.
#[derive(Default, Clone)]
struct BookPosition {
    qty: Decimal,
    avg_entry: Decimal,
    last_price: Decimal,
}

struct TradeBook {
    equity_curve: Vec<Decimal>,
    /// PnL кожної ЗАКРИТОЇ угоди — і лонгової, і шортової.
    trade_pnls: Vec<Decimal>,
    total_costs: Decimal,
}

/// Реплей філів у трейд-книгу. ДЗЕРКАЛО `PortfolioManager::update_on_fill`
/// (`src/trading/adapters/portfolio_manager.rs`) — правило в правило.
///
/// Портфель уже веде цей облік, але накопичує `realized_pnl` однією сумою на
/// символ, а звіту потрібна серія по кожній закритій угоді (win rate, profit
/// factor, avg win/loss). Тому книга відтворюється тут — і будь-яке
/// відхилення від правил портфеля означає, що звіт описує не той портфель,
/// який його підписує. Стереже `reconstruction_matches_portfolio`.
///
/// Дві речі, яких бракувало попередній версії:
///   * позиція велася ОДНА на всі символи — філи SPY і TLT ділили спільну
///     `avg_entry`, тож для кошика trade-статистика описувала неіснуючий
///     актив (звідси profit factor 0.9986 поруч із дохідністю +51.79%);
///   * шорт-входи мовчки пропускались, тому в прогонах з `--allow-short`
///     шортова половина угод не потрапляла в книгу взагалі.
///
/// Інваріант джерела: філ ніколи не перетинає нуль — движок ділить ордер
/// (`ensure!("fill crosses zero")` в обох гілках менеджера), тож розворот
/// приходить двома філами.
fn replay_fills(fills: &[FillEvent], initial_capital: Decimal, allow_short: bool) -> TradeBook {
    let mut cash = initial_capital;
    let mut positions: BTreeMap<&str, BookPosition> = BTreeMap::new();
    let mut equity_curve: Vec<Decimal> = vec![initial_capital];
    let mut trade_pnls: Vec<Decimal> = Vec::new();
    let mut total_costs = Decimal::ZERO;

    for fill in fills {
        if fill.quantity <= Decimal::ZERO || fill.fill_price <= Decimal::ZERO {
            continue;
        }
        let qty = fill.quantity;
        let price = fill.fill_price;
        let commission = fill.commission;
        let pos = positions.entry(fill.symbol.as_str()).or_default();

        match fill.side {
            OrderSide::Buy if pos.qty >= Decimal::ZERO => {
                // Відкриття/нарощення лонга: комісія входить у середню ціну.
                let total_cost = pos.avg_entry * pos.qty + price * qty + commission;
                let new_qty = pos.qty + qty;
                pos.avg_entry = if new_qty > Decimal::ZERO {
                    total_cost / new_qty
                } else {
                    Decimal::ZERO
                };
                pos.qty = new_qty;
                cash -= price * qty + commission;
            }
            OrderSide::Buy => {
                // Покриття шорта.
                let cover = qty.min(-pos.qty);
                trade_pnls.push((pos.avg_entry - price) * cover - commission);
                pos.qty += cover;
                if pos.qty == Decimal::ZERO {
                    pos.avg_entry = Decimal::ZERO;
                }
                cash -= price * qty + commission;
            }
            OrderSide::Sell if pos.qty > Decimal::ZERO || !allow_short => {
                // Закриття лонга. Клемп до наявної кількості — та сама стара
                // семантика, що в портфелі; у long-only Sell при нульовій
                // позиції так само лишається no-op (комісія не списується,
                // бо портфель її не платить).
                let sell = qty.min(pos.qty.max(Decimal::ZERO));
                if sell <= Decimal::ZERO {
                    continue;
                }
                trade_pnls.push((price - pos.avg_entry) * sell - commission);
                pos.qty -= sell;
                if pos.qty == Decimal::ZERO {
                    pos.avg_entry = Decimal::ZERO;
                }
                cash += price * sell - commission;
            }
            OrderSide::Sell => {
                // Відкриття/нарощення шорта: комісія зменшує ефективну ціну входу.
                let short_qty = -pos.qty;
                let total_entry = pos.avg_entry * short_qty + price * qty - commission;
                let new_short = short_qty + qty;
                pos.avg_entry = if new_short > Decimal::ZERO {
                    total_entry / new_short
                } else {
                    Decimal::ZERO
                };
                pos.qty = -new_short;
                cash += price * qty - commission;
            }
        }
        pos.last_price = price;
        total_costs += commission;

        // Оцінка по ВСІХ відкритих позиціях, кожна за своєю останньою ціною.
        // Стара версія оцінювала спільну позицію ціною поточного філа, тобто
        // переоцінювала TLT ціною SPY.
        let marked: Decimal = positions.values().map(|p| p.qty * p.last_price).sum();
        equity_curve.push(cash + marked);
    }

    TradeBook {
        equity_curve,
        trade_pnls,
        total_costs,
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
        fill_sym("TEST", side, qty, price, commission)
    }

    fn fill_sym(
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        commission: Decimal,
    ) -> FillEvent {
        FillEvent {
            id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            symbol: symbol.into(),
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

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial, false);
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

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial, false);
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
        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial, false);
        assert!(report.turnover > Decimal::ZERO);
    }

    // Шортовий round-trip — це УГОДА. Стара книга пропускала Sell при нульовій
    // позиції, тож у прогонах з --allow-short шортова половина не рахувалась
    // узагалі: тут це дало б 0 угод замість 1.
    #[test]
    fn short_round_trip_is_recorded() {
        let initial = dec!(10000);
        let fills = vec![
            fill(OrderSide::Sell, dec!(10), dec!(100), Decimal::ZERO),
            fill(OrderSide::Buy, dec!(10), dec!(90), Decimal::ZERO),
        ];
        let mut portfolio = Portfolio::new(initial);
        portfolio.cash = initial + dec!(1000) - dec!(900);

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial, true);
        assert_eq!(report.total_trades, 1, "шорт закрито — це одна угода");
        assert_eq!(report.winning_trades, 1);
        assert_eq!(report.avg_win, dec!(100), "(100 − 90) × 10");
        assert_eq!(report.total_return, dec!(100));
    }

    // Кожен символ веде СВОЮ позицію. Спільна `avg_entry` на всі символи
    // змішувала ціни різних активів: тут обидві угоди прибуткові, а стара
    // книга бачила одну виграшну і одну програшну (avg_entry = 200 на обох).
    #[test]
    fn symbols_keep_separate_positions() {
        let initial = dec!(10000);
        let fills = vec![
            fill_sym("AAA", OrderSide::Buy, dec!(10), dec!(100), Decimal::ZERO),
            fill_sym("BBB", OrderSide::Buy, dec!(10), dec!(300), Decimal::ZERO),
            fill_sym("AAA", OrderSide::Sell, dec!(10), dec!(110), Decimal::ZERO),
            fill_sym("BBB", OrderSide::Sell, dec!(10), dec!(330), Decimal::ZERO),
        ];
        let mut portfolio = Portfolio::new(initial);
        portfolio.cash = initial + dec!(100) + dec!(300);

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial, false);
        assert_eq!(report.winning_trades, 2, "обидва інструменти в плюсі");
        assert_eq!(report.losing_trades, 0);
        assert_eq!(report.avg_win, dec!(200), "(100 + 300) / 2");
    }

    // Головний інваріант: книга звіту й портфель — той самий облік. Портфель
    // тут авторитет, звіт лише реконструює серію по угодах. Саме розбіжність
    // цих двох книг дала profit factor 0.9986 поруч із дохідністю +51.79%.
    #[tokio::test]
    async fn reconstruction_matches_portfolio() {
        use crate::trading::adapters::portfolio_manager::PortfolioManager;
        use crate::trading::ports::PortfolioPort;

        let initial = dec!(10000);
        // Лонги і шорти на двох символах; жоден філ не перетинає нуль.
        let fills = vec![
            fill_sym("AAA", OrderSide::Buy, dec!(10), dec!(100), dec!(1)),
            fill_sym("BBB", OrderSide::Sell, dec!(5), dec!(200), dec!(2)),
            fill_sym("AAA", OrderSide::Sell, dec!(10), dec!(120), dec!(1)),
            fill_sym("AAA", OrderSide::Sell, dec!(4), dec!(120), dec!(1)),
            fill_sym("BBB", OrderSide::Buy, dec!(5), dec!(180), dec!(2)),
            fill_sym("AAA", OrderSide::Buy, dec!(4), dec!(115), dec!(1)),
        ];

        let pm = PortfolioManager::new_allowing_short(initial);
        for f in &fills {
            pm.update_on_fill(f).await.unwrap();
        }
        let portfolio = pm.get_portfolio().await.unwrap();

        let book = replay_fills(&fills, initial, true);
        assert_eq!(
            *book.equity_curve.last().unwrap(),
            portfolio.get_total_value(),
            "реконструйована еквіті розійшлася з портфелем"
        );

        let report = BacktestReport::from_fills_and_portfolio(&fills, &portfolio, initial, true);
        assert_eq!(report.total_trades, 3, "лонг AAA, шорт BBB, шорт AAA");
        let sum_pnl: Decimal = book.trade_pnls.iter().copied().sum();
        assert_eq!(
            sum_pnl, report.total_return,
            "книга закрита в нуль — сума PnL угод мусить дорівнювати net-результату"
        );
    }

    // Режим обліку не косметичний: у long-only Sell при нульовій позиції —
    // no-op (портфель клемпить), і книга мусить робити те саме, інакше
    // з'явиться фантомний шорт.
    #[tokio::test]
    async fn long_only_ignores_naked_sell() {
        use crate::trading::adapters::portfolio_manager::PortfolioManager;
        use crate::trading::ports::PortfolioPort;

        let initial = dec!(10000);
        let fills = vec![
            fill(OrderSide::Buy, dec!(5), dec!(100), Decimal::ZERO),
            // Продаж 10 при позиції 5 — клемп до 5.
            fill(OrderSide::Sell, dec!(10), dec!(100), Decimal::ZERO),
            // Продаж при нульовій позиції — портфель ігнорує цілком.
            fill(OrderSide::Sell, dec!(3), dec!(100), Decimal::ZERO),
        ];

        let pm = PortfolioManager::new(initial);
        for f in &fills {
            pm.update_on_fill(f).await.unwrap();
        }
        let portfolio = pm.get_portfolio().await.unwrap();

        let book = replay_fills(&fills, initial, false);
        assert_eq!(*book.equity_curve.last().unwrap(), portfolio.get_total_value());
        assert_eq!(book.trade_pnls.len(), 1, "лише закриття лонга");
    }
}
