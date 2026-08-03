//! Портфельний бектест-движок рівня барів (M0.2 / M2.1 / M3.2).
//!
//! Ганяє БУДЬ-ЯКУ `AllocationStrategy` (основну чи бенчмарк) через ті самі
//! порти, що й tick-пайплайн: `BrokerSimulatorPort` (з CostModel, M0.1) і
//! `PortfolioPort`. Інваріант 1: бенчмарки рахуються тим самим движком, а не
//! окремим скриптом; інваріант 2: витрати застосовуються до кожного філа.
//!
//! Детермінізм (інваріант 4): ордери отримують id, виведені з
//! (символ, час бару, сторона), час — час бару; жодних Utc::now()/rng.

use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::backtest::domain::metrics::{self, InstrumentMetrics};
use crate::backtest::domain::report::BacktestReport;
use crate::trading::domain::allocation::{AllocationStrategy, AlignedMarketData, UniverseView};
use crate::trading::domain::costs::MarketContext;
use crate::trading::domain::events::{FillEvent, OrderEvent, OrderSide, OrderType};
use crate::trading::domain::models::Portfolio;
use crate::trading::ports::{BrokerSimulatorPort, PortfolioPort};

/// Кількість барів для оцінки середнього обсягу в MarketContext.
const CONTEXT_VOLUME_WINDOW: usize = 20;
/// Вікно оцінки волатильності бару для MarketContext.
const CONTEXT_VOL_WINDOW: usize = 20;

#[derive(Debug, Clone)]
pub struct PortfolioRunResult {
    pub strategy_name: String,
    pub report: BacktestReport,
    pub instruments: Vec<InstrumentMetrics>,
    pub median_sharpe: Option<Decimal>,
    pub equity_curve: Vec<Decimal>,
    pub fills: Vec<FillEvent>,
    pub final_portfolio: Portfolio,
}

pub struct PortfolioBacktester {
    data: Arc<AlignedMarketData>,
    broker: Arc<dyn BrokerSimulatorPort>,
    portfolio: Arc<dyn PortfolioPort>,
    initial_capital: Decimal,
}

impl PortfolioBacktester {
    pub fn new(
        data: Arc<AlignedMarketData>,
        broker: Arc<dyn BrokerSimulatorPort>,
        portfolio: Arc<dyn PortfolioPort>,
        initial_capital: Decimal,
    ) -> Self {
        Self {
            data,
            broker,
            portfolio,
            initial_capital,
        }
    }

    /// Прогін стратегії по всьому датасету (або по вікну [start_idx, end_idx)).
    pub async fn run(
        &self,
        strategy: &mut dyn AllocationStrategy,
    ) -> Result<PortfolioRunResult> {
        self.run_window(strategy, 0, self.data.len()).await
    }

    /// Прогін по вікну барів [start_idx, end_idx). Warmup стратегії
    /// задовольняється ДАНИМИ ДО вікна, якщо вони є (point-in-time, M0.6).
    pub async fn run_window(
        &self,
        strategy: &mut dyn AllocationStrategy,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<PortfolioRunResult> {
        let end_idx = end_idx.min(self.data.len());
        anyhow::ensure!(start_idx < end_idx, "empty backtest window");

        self.portfolio.reset(self.initial_capital).await?;

        let mut fills: Vec<FillEvent> = Vec::new();
        let mut equity_curve: Vec<Decimal> = vec![self.initial_capital];
        // Пер-інструментні серії PnL (для median Sharpe, M0.3).
        let mut instrument_pnl: BTreeMap<String, Vec<Decimal>> = BTreeMap::new();
        let mut instrument_trades: BTreeMap<String, usize> = BTreeMap::new();
        let mut prev_prices: BTreeMap<String, Decimal> = BTreeMap::new();
        let mut prev_qty: BTreeMap<String, Decimal> = BTreeMap::new();

        let first_decision = start_idx.max(strategy.warmup_bars());
        let rebalance_every = strategy.rebalance_every().max(1);

        for t in first_decision..end_idx {
            let view = UniverseView::new(self.data.clone(), t);

            // Пер-інструментний PnL за бар (до ребалансу): qty_{t-1} × Δp.
            for (sym, qty) in &prev_qty {
                if *qty == Decimal::ZERO {
                    continue;
                }
                if let (Some(prev_p), Some(cur_p)) = (prev_prices.get(sym), view.price(sym)) {
                    let pnl = *qty * (cur_p - *prev_p);
                    instrument_pnl.entry(sym.clone()).or_default().push(pnl);
                }
            }

            let bars_into_run = t - first_decision;
            if bars_into_run.is_multiple_of(rebalance_every) {
                if let Some(weights) = strategy.target_weights(&view) {
                    let orders = self
                        .orders_for_target_weights(&view, &weights)
                        .await?;
                    for order in orders {
                        let fill = self.broker.execute_order(&order).await?;
                        self.portfolio.update_on_fill(&fill).await?;
                        *instrument_trades.entry(fill.symbol.clone()).or_default() += 1;
                        fills.push(fill);
                    }
                }
            }

            // Mark-to-market еквіті на закритті бару t.
            let snapshot = self.portfolio.get_portfolio().await?;
            let mut equity = snapshot.cash;
            for (sym, pos) in &snapshot.positions {
                if let Some(p) = view.price(sym) {
                    equity += pos.quantity * p;
                } else {
                    equity += pos.quantity * pos.current_price;
                }
            }
            equity_curve.push(equity);

            for sym in self.data.symbols() {
                if let Some(p) = view.price(sym) {
                    prev_prices.insert(sym.clone(), p);
                }
            }
            prev_qty = snapshot
                .positions
                .iter()
                .map(|(s, p)| (s.clone(), p.quantity))
                .collect();
        }

        // Закриття всіх позицій на останньому барі вікна — PnL реалізований,
        // порівняння стратегій чесне (усі закінчують у кеші).
        let last_view = UniverseView::new(self.data.clone(), end_idx - 1);
        let close_orders = self
            .orders_for_target_weights(&last_view, &BTreeMap::new())
            .await?;
        for order in close_orders {
            let fill = self.broker.execute_order(&order).await?;
            self.portfolio.update_on_fill(&fill).await?;
            *instrument_trades.entry(fill.symbol.clone()).or_default() += 1;
            fills.push(fill);
        }
        let final_portfolio = self.portfolio.get_portfolio().await?;
        if let Some(last) = equity_curve.last_mut() {
            *last = final_portfolio.get_total_value();
        }

        let report = report_from_equity_curve(
            &equity_curve,
            &fills,
            &final_portfolio,
            self.initial_capital,
            self.portfolio.allows_short(),
        );

        let instruments: Vec<InstrumentMetrics> = instrument_pnl
            .iter()
            .map(|(sym, pnls)| {
                // Псевдо-еквіті інструмента: капітал + кумулятивний PnL.
                let mut curve = Vec::with_capacity(pnls.len() + 1);
                let mut acc = self.initial_capital;
                curve.push(acc);
                for p in pnls {
                    acc += *p;
                    curve.push(acc);
                }
                let sharpe = metrics::sharpe_annualized(&curve);
                let net: Decimal = pnls.iter().copied().sum();
                let net_pct = if self.initial_capital > Decimal::ZERO {
                    net / self.initial_capital * Decimal::ONE_HUNDRED
                } else {
                    Decimal::ZERO
                };
                InstrumentMetrics::new(
                    sym.clone(),
                    sharpe,
                    net_pct,
                    instrument_trades.get(sym).copied().unwrap_or(0),
                )
            })
            .collect();

        let median_sharpe = metrics::median_sharpe(&instruments);

        Ok(PortfolioRunResult {
            strategy_name: strategy.name().to_string(),
            report,
            instruments,
            median_sharpe,
            equity_curve,
            fills,
            final_portfolio,
        })
    }

    /// Дельта цільових ваг → ордери. Продажі перші (звільняють кеш), ордер
    /// через нуль ріжеться на закриття + відкриття (інваріант обліку).
    async fn orders_for_target_weights(
        &self,
        view: &UniverseView,
        weights: &BTreeMap<String, Decimal>,
    ) -> Result<Vec<OrderEvent>> {
        let snapshot = self.portfolio.get_portfolio().await?;
        let mut equity = snapshot.cash;
        for (sym, pos) in &snapshot.positions {
            if let Some(p) = view.price(sym) {
                equity += pos.quantity * p;
            } else {
                equity += pos.quantity * pos.current_price;
            }
        }
        if equity <= Decimal::ZERO {
            return Ok(Vec::new());
        }

        // Усі символи: цільові + наявні позиції (цільова 0 → закрити).
        let mut symbols: Vec<String> = weights.keys().cloned().collect();
        for sym in snapshot.positions.keys() {
            if !weights.contains_key(sym) {
                symbols.push(sym.clone());
            }
        }
        symbols.sort();
        symbols.dedup();

        let mut sells: Vec<OrderEvent> = Vec::new();
        let mut buys: Vec<OrderEvent> = Vec::new();

        for sym in symbols {
            let Some(price) = view.price(&sym) else {
                continue;
            };
            if price <= Decimal::ZERO {
                continue;
            }
            let current_qty = snapshot
                .positions
                .get(&sym)
                .map(|p| p.quantity)
                .unwrap_or(Decimal::ZERO);
            let target_weight = weights.get(&sym).copied().unwrap_or(Decimal::ZERO);
            let target_qty = (equity * target_weight / price).round_dp(4);
            let delta = target_qty - current_qty;
            if delta.abs() * price < Decimal::ONE {
                // Мікроребаланс дешевший за поріг у 1 грошову одиницю — шум.
                continue;
            }

            let context = MarketContext {
                avg_volume: view.avg_volume(&sym, CONTEXT_VOLUME_WINDOW),
                volatility: bar_volatility(view, &sym),
                spread_pct: None,
            };

            // Розрізання через нуль: спершу закриття існуючої позиції.
            let mut legs: Vec<(OrderSide, Decimal)> = Vec::new();
            if current_qty > Decimal::ZERO && target_qty < Decimal::ZERO {
                legs.push((OrderSide::Sell, current_qty));
                legs.push((OrderSide::Sell, -target_qty));
            } else if current_qty < Decimal::ZERO && target_qty > Decimal::ZERO {
                legs.push((OrderSide::Buy, -current_qty));
                legs.push((OrderSide::Buy, target_qty));
            } else if delta > Decimal::ZERO {
                legs.push((OrderSide::Buy, delta));
            } else {
                legs.push((OrderSide::Sell, -delta));
            }

            for (leg_idx, (side, qty)) in legs.into_iter().enumerate() {
                if qty <= Decimal::ZERO {
                    continue;
                }
                let order = OrderEvent {
                    id: deterministic_order_id(&sym, view, side, leg_idx),
                    signal_id: Uuid::nil(),
                    timestamp: view.timestamp(),
                    symbol: sym.clone(),
                    side,
                    quantity: qty,
                    order_type: OrderType::Market,
                    limit_price: Some(price),
                    stop_price: None,
                    market_context: Some(context.clone()),
                };
                match side {
                    OrderSide::Sell => sells.push(order),
                    OrderSide::Buy => buys.push(order),
                }
            }
        }

        sells.extend(buys);
        Ok(sells)
    }
}

fn deterministic_order_id(
    symbol: &str,
    view: &UniverseView,
    side: OrderSide,
    leg: usize,
) -> Uuid {
    let seed = format!(
        "{}|{}|{:?}|{}",
        symbol,
        view.timestamp().timestamp_nanos_opt().unwrap_or_default(),
        side,
        leg
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes())
}

fn bar_volatility(view: &UniverseView, symbol: &str) -> Option<Decimal> {
    use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
    let rets = view.returns_window(symbol, CONTEXT_VOL_WINDOW);
    if rets.len() < 2 {
        return None;
    }
    let vals: Vec<f64> = rets.iter().filter_map(|r| r.to_f64()).collect();
    let mean = vals.iter().sum::<f64>() / vals.len() as f64;
    let var = vals.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / vals.len() as f64;
    Decimal::from_f64(var.sqrt())
}

/// Звіт із портфельної кривої еквіті (bar-level, а не по-fill).
fn report_from_equity_curve(
    equity_curve: &[Decimal],
    fills: &[FillEvent],
    final_portfolio: &Portfolio,
    initial_capital: Decimal,
    allow_short: bool,
) -> BacktestReport {
    // Використовуємо стандартний конструктор для trade-статистики,
    // але Sharpe/MDD/turnover перераховуємо з bar-level кривої.
    let mut report =
        BacktestReport::from_fills_and_portfolio(fills, final_portfolio, initial_capital, allow_short);

    let (mdd, mdd_pct) = metrics::max_drawdown(equity_curve);
    report.sharpe_ratio = metrics::sharpe_annualized(equity_curve);
    report.max_drawdown = mdd;
    report.max_drawdown_pct = mdd_pct;
    report.calmar = metrics::calmar(report.total_return_pct, mdd_pct);
    let avg_equity = if equity_curve.is_empty() {
        initial_capital
    } else {
        equity_curve.iter().copied().sum::<Decimal>() / Decimal::from(equity_curve.len() as u64)
    };
    report.turnover = metrics::turnover(fills, avg_equity);
    report
}
