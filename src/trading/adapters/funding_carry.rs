//! Funding-rate carry (M3.1): майже механічний едж перп/спот.
//!
//! Логіка: EWMA funding-ставки визначає сторону. Стійко додатний funding →
//! шорт перпа (отримуємо funding) + лонг спота тим самим нотіоналом (хедж
//! цінового ризику), стійко від'ємний → дзеркально. Ребаланс ТІЛЬКИ на
//! funding-інтервалах. Без спот-серії ноги не хеджуються (документована
//! деградація до directional carry).
//!
//! Облік без подвійного рахунку (критерій M3.1):
//!   - ціновий PnL — через кеш-потоки філів обох ніг (рівно один раз);
//!   - funding — окремий акрут на позицію, що ТРИМАЛАСЬ у інтервал (один раз);
//!   - витрати — тільки через CostModel у fill.commission (один раз, M0.1).
//!
//! Kill-критерій: OOS Sharpe < 0.5 після витрат → напрям відкладається.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::backtest::domain::metrics;
use crate::data_ingestion::domain::funding::FundingRatePoint;
use crate::trading::domain::costs::MarketContext;
use crate::trading::domain::events::{FillEvent, OrderEvent, OrderSide, OrderType};
use crate::trading::ports::BrokerSimulatorPort;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FundingCarryConfig {
    /// Поріг входу: |EWMA ставки| за інтервал (напр. 0.00005 = 0.5 б.п.).
    pub entry_threshold: Decimal,
    /// Поріг виходу (гістерезис; ≤ entry).
    pub exit_threshold: Decimal,
    /// Згладжування EWMA (0..1], більший = швидша реакція.
    pub ewma_alpha: Decimal,
    /// Нотіонал позиції як частка еквіті.
    pub position_notional_pct: Decimal,
}

impl Default for FundingCarryConfig {
    fn default() -> Self {
        Self {
            entry_threshold: Decimal::new(5, 5),   // 0.00005 за інтервал
            exit_threshold: Decimal::new(1, 5),    // 0.00001
            ewma_alpha: Decimal::new(2, 1),        // 0.2
            position_notional_pct: Decimal::new(5, 1), // 0.5 еквіті
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingCarryReport {
    pub symbol: String,
    /// Net PnL (ціна + funding − витрати).
    pub net_pnl: Decimal,
    /// Накопичений funding (додатний = отримали).
    pub funding_pnl: Decimal,
    /// Сумарні транзакційні витрати (обидві ноги).
    pub total_costs: Decimal,
    /// Ціновий PnL без funding і витрат (діагностика; у хеджі ≈ 0).
    pub price_pnl: Decimal,
    pub sharpe: Decimal,
    pub max_drawdown_pct: Decimal,
    pub intervals: usize,
    pub rebalances: usize,
    pub hedged: bool,
    pub final_equity: Decimal,
    #[serde(skip)]
    pub equity_curve: Vec<Decimal>,
    #[serde(skip)]
    pub fills: Vec<FillEvent>,
}

impl FundingCarryReport {
    /// Kill-критерій M3.1: після реалістичних витрат і фандингу
    /// OOS-Sharpe < 0.5 → відкладай напрям.
    pub fn passes_kill_criterion(&self) -> bool {
        self.sharpe >= Decimal::new(5, 1)
    }
}

pub struct FundingCarryBacktest {
    broker: Arc<dyn BrokerSimulatorPort>,
}

impl FundingCarryBacktest {
    pub fn new(broker: Arc<dyn BrokerSimulatorPort>) -> Self {
        Self { broker }
    }

    pub async fn run(
        &self,
        series: &[FundingRatePoint],
        initial_capital: Decimal,
        cfg: &FundingCarryConfig,
    ) -> Result<FundingCarryReport> {
        anyhow::ensure!(series.len() >= 3, "funding series too short");
        anyhow::ensure!(
            series.windows(2).all(|w| w[0].timestamp < w[1].timestamp),
            "funding series must be strictly increasing in time"
        );
        let symbol = series[0].symbol.clone();
        let hedged = series.iter().all(|p| p.spot_price.is_some());

        let mut cash = initial_capital;
        let mut perp_qty = Decimal::ZERO; // знак: шорт < 0
        let mut spot_qty = Decimal::ZERO;
        let mut funding_pnl = Decimal::ZERO;
        let mut total_costs = Decimal::ZERO;
        let mut fills: Vec<FillEvent> = Vec::new();
        let mut equity_curve = vec![initial_capital];
        let mut ewma: Option<Decimal> = None;
        let mut rebalances = 0usize;

        for point in series {
            let mark = point.mark_price;
            if mark <= Decimal::ZERO {
                continue;
            }

            // 1) Funding-акрут на позицію, ЩО ТРИМАЛАСЬ у цей інтервал.
            //    Лонг перпа платить при rate > 0, шорт — отримує.
            let payment = -perp_qty * mark * point.rate;
            cash += payment;
            funding_pnl += payment;

            // 2) EWMA сигналу (включає поточну ставку: інформація моменту t
            //    використовується для позиції ПІСЛЯ t — без look-ahead).
            let e = match ewma {
                None => point.rate,
                Some(prev) => cfg.ewma_alpha * point.rate + (Decimal::ONE - cfg.ewma_alpha) * prev,
            };
            ewma = Some(e);

            // 3) Цільова позиція з гістерезисом.
            let equity_now = cash
                + perp_qty * mark
                + spot_qty * point.spot_price.unwrap_or(mark);
            let target_notional = equity_now * cfg.position_notional_pct;
            let current_side = if perp_qty < Decimal::ZERO {
                1i8 // шорт перпа = отримуємо додатний funding
            } else if perp_qty > Decimal::ZERO {
                -1i8
            } else {
                0i8
            };
            let desired_side = if e >= cfg.entry_threshold {
                1i8
            } else if e <= -cfg.entry_threshold {
                -1i8
            } else if e.abs() <= cfg.exit_threshold {
                0i8
            } else {
                current_side // усередині гістерезису — тримаємо
            };

            if desired_side != current_side {
                let target_perp_qty = match desired_side {
                    1 => -(target_notional / mark),
                    -1 => target_notional / mark,
                    _ => Decimal::ZERO,
                };
                let target_spot_qty = if hedged {
                    -target_perp_qty
                } else {
                    Decimal::ZERO
                };

                // Перп-нога.
                let perp_fills = self
                    .trade_to_target(&symbol, "PERP", perp_qty, target_perp_qty, mark, point)
                    .await?;
                for f in &perp_fills {
                    cash += cash_flow(f);
                    total_costs += f.commission;
                }
                fills.extend(perp_fills);
                perp_qty = target_perp_qty;

                // Спот-нога (хедж).
                if hedged {
                    let spot_price = point
                        .spot_price
                        .context("hedged-режим, але в точці немає спот-ціни")?;
                    let spot_fills = self
                        .trade_to_target(&symbol, "SPOT", spot_qty, target_spot_qty, spot_price, point)
                        .await?;
                    for f in &spot_fills {
                        cash += cash_flow(f);
                        total_costs += f.commission;
                    }
                    fills.extend(spot_fills);
                    spot_qty = target_spot_qty;
                }
                rebalances += 1;
            }

            let equity = cash
                + perp_qty * mark
                + spot_qty * point.spot_price.unwrap_or(mark);
            equity_curve.push(equity);
        }

        // Закриття обох ніг на останній точці.
        let last = series.last().context("порожня funding-серія")?;
        if perp_qty != Decimal::ZERO {
            let f = self
                .trade_to_target(&symbol, "PERP", perp_qty, Decimal::ZERO, last.mark_price, last)
                .await?;
            for fill in &f {
                cash += cash_flow(fill);
                total_costs += fill.commission;
            }
            fills.extend(f);
        }
        if spot_qty != Decimal::ZERO {
            let price = last.spot_price.unwrap_or(last.mark_price);
            let f = self
                .trade_to_target(&symbol, "SPOT", spot_qty, Decimal::ZERO, price, last)
                .await?;
            for fill in &f {
                cash += cash_flow(fill);
                total_costs += fill.commission;
            }
            fills.extend(f);
        }
        if let Some(last_eq) = equity_curve.last_mut() {
            *last_eq = cash;
        }

        let net_pnl = cash - initial_capital;
        let price_pnl = net_pnl - funding_pnl + total_costs;
        let (_, mdd_pct) = metrics::max_drawdown(&equity_curve);

        Ok(FundingCarryReport {
            symbol,
            net_pnl,
            funding_pnl,
            total_costs,
            price_pnl,
            sharpe: metrics::sharpe_annualized(&equity_curve),
            max_drawdown_pct: mdd_pct,
            intervals: series.len(),
            rebalances,
            hedged,
            final_equity: cash,
            equity_curve,
            fills,
        })
    }

    async fn trade_to_target(
        &self,
        symbol: &str,
        leg: &str,
        current: Decimal,
        target: Decimal,
        price: Decimal,
        point: &FundingRatePoint,
    ) -> Result<Vec<FillEvent>> {
        let delta = target - current;
        if delta == Decimal::ZERO {
            return Ok(Vec::new());
        }
        let side = if delta > Decimal::ZERO {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let seed = format!(
            "{symbol}|{leg}|{}|{:?}",
            point.timestamp.timestamp_nanos_opt().unwrap_or_default(),
            side
        );
        let order = OrderEvent {
            id: Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes()),
            signal_id: Uuid::nil(),
            timestamp: point.timestamp,
            symbol: format!("{symbol}-{leg}"),
            side,
            quantity: delta.abs(),
            order_type: OrderType::Market,
            limit_price: Some(price),
            stop_price: None,
            market_context: Some(MarketContext::default()),
        };
        Ok(vec![self.broker.execute_order(&order).await?])
    }
}

/// Кеш-потік філа: покупка — мінус, продаж — плюс; комісія завжди мінус.
fn cash_flow(fill: &FillEvent) -> Decimal {
    let notional = fill.fill_price * fill.quantity;
    match fill.side {
        OrderSide::Buy => -notional - fill.commission,
        OrderSide::Sell => notional - fill.commission,
    }
}

// ── Крос-секційний carry: кошик топ-платників funding ────────────────────────

/// Конфіг крос-секційного carry (rank усього юніверсу перпів за трейлінг-
/// funding → шорт топ-K з хеджем у спот).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XsCarryConfig {
    /// Вікно трейлінг-середнього funding, в інтервалах (21 = 7 днів по 8h).
    pub trailing_intervals: usize,
    /// Розмір кошика.
    pub top_k: usize,
    /// Частота перегляду кошика, в інтервалах (21 = раз на тиждень).
    pub rebalance_intervals: usize,
    /// Мінімальний середній funding за інтервал для входу.
    pub entry_threshold: Decimal,
    /// Сумарний нотіонал перп-ноги як частка еквіті.
    pub gross_exposure: Decimal,
    /// Гістерезис членства: утримуваний символ лишається, поки його ранг
    /// ≤ top_k × exit_rank_multiple (проти щотижневого перетрушування).
    pub exit_rank_multiple: usize,
    /// Банда: не чіпати позицію, поки |Δнотіонал| < ця частка цільового.
    pub trade_band: Decimal,
    /// Скільки інтервалів терпіти відсутність даних символу, перш ніж
    /// вважати його делістингнутим (у Binance частина перпів має funding
    /// кожні 4h, частина — 8h; звичайні пропуски — НЕ делістинг).
    pub max_gap_intervals: usize,
}

impl Default for XsCarryConfig {
    fn default() -> Self {
        Self {
            trailing_intervals: 21,
            top_k: 10,
            rebalance_intervals: 21,
            entry_threshold: Decimal::new(3, 5), // 0.00003 за 8h ≈ 3.3%/рік
            gross_exposure: Decimal::new(5, 1),  // 50% еквіті
            exit_rank_multiple: 2,
            trade_band: Decimal::new(15, 2), // 15% цільового нотіоналу
            max_gap_intervals: 12,           // ~2 доби на 4h-сітці
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XsCarryReport {
    pub net_pnl: Decimal,
    pub funding_pnl: Decimal,
    pub total_costs: Decimal,
    /// Ціновий PnL (у хеджі ≈ 0; ≠0 лише від форс-закриттів делістингів).
    pub price_pnl: Decimal,
    pub sharpe: Decimal,
    pub max_drawdown_pct: Decimal,
    pub intervals: usize,
    pub rebalances: usize,
    /// Скільки різних символів побувало в кошику.
    pub symbols_traded: usize,
    pub final_equity: Decimal,
    #[serde(skip)]
    pub equity_curve: Vec<Decimal>,
}

impl XsCarryReport {
    /// Kill-критерій для диверсифікованого кошика суворіший за одиночний:
    /// OOS Sharpe ≥ 1.0 після витрат, інакше складність не окупається.
    pub fn passes_kill_criterion(&self) -> bool {
        self.sharpe >= Decimal::ONE
    }
}

impl FundingCarryBacktest {
    /// Крос-секційний прогін. Приймає «рвані» серії: символи можуть
    /// з'являтися пізніше і зникати (делістинг) — позиція без даних
    /// форс-закривається за останньою відомою ціною.
    pub async fn run_cross_sectional(
        &self,
        series_by_symbol: &std::collections::BTreeMap<String, Vec<FundingRatePoint>>,
        initial_capital: Decimal,
        cfg: &XsCarryConfig,
    ) -> Result<XsCarryReport> {
        use std::collections::{BTreeMap, BTreeSet};

        anyhow::ensure!(!series_by_symbol.is_empty(), "empty universe");
        for (sym, s) in series_by_symbol {
            anyhow::ensure!(
                s.windows(2).all(|w| w[0].timestamp < w[1].timestamp),
                "series for {sym} must be strictly increasing"
            );
        }

        // Єдина вісь часу (funding-інтервали синхронні по біржі).
        let timeline: Vec<chrono::DateTime<chrono::Utc>> = series_by_symbol
            .values()
            .flat_map(|s| s.iter().map(|p| p.timestamp))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        anyhow::ensure!(timeline.len() >= cfg.trailing_intervals + 2, "series too short");

        // Курсор по кожному символу: індекс останньої точки ≤ t.
        let mut cursor: BTreeMap<&str, usize> = BTreeMap::new();
        let mut last_seen: BTreeMap<String, FundingRatePoint> = BTreeMap::new();
        let mut last_seen_step: BTreeMap<String, usize> = BTreeMap::new();

        let mut cash = initial_capital;
        let mut perp_qty: BTreeMap<String, Decimal> = BTreeMap::new();
        let mut spot_qty: BTreeMap<String, Decimal> = BTreeMap::new();
        let mut funding_pnl = Decimal::ZERO;
        let mut total_costs = Decimal::ZERO;
        let mut rebalances = 0usize;
        let mut symbols_traded: BTreeSet<String> = BTreeSet::new();
        let mut equity_curve = vec![initial_capital];

        for (step, &t) in timeline.iter().enumerate() {
            // 1) Просунути курсори; зібрати точки, що існують саме на t.
            let mut points_at_t: BTreeMap<&str, &FundingRatePoint> = BTreeMap::new();
            for (sym, series) in series_by_symbol {
                let idx = cursor.entry(sym.as_str()).or_insert(0);
                while *idx < series.len() && series[*idx].timestamp < t {
                    *idx += 1;
                }
                if *idx < series.len() && series[*idx].timestamp == t {
                    points_at_t.insert(sym.as_str(), &series[*idx]);
                    last_seen.insert(sym.clone(), series[*idx].clone());
                    last_seen_step.insert(sym.clone(), step);
                }
            }

            // 2) Funding-акрут на позиції, що трималися в цей інтервал.
            for (sym, qty) in perp_qty.iter() {
                if *qty == Decimal::ZERO {
                    continue;
                }
                if let Some(p) = points_at_t.get(sym.as_str()) {
                    let payment = -*qty * p.mark_price * p.rate;
                    cash += payment;
                    funding_pnl += payment;
                }
            }

            // 3) Делістинг: даних немає ДОВШЕ за max_gap_intervals → закрити
            //    за останньою відомою ціною. Звичайні пропуски (символи з
            //    4h/8h funding-сіткою на спільній осі) — НЕ делістинг.
            let held: Vec<String> = perp_qty
                .keys()
                .chain(spot_qty.keys())
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            for sym in held {
                let fresh = last_seen_step
                    .get(&sym)
                    .map(|s| step - *s <= cfg.max_gap_intervals)
                    .unwrap_or(false);
                if fresh {
                    continue;
                }
                let Some(last) = last_seen.get(&sym) else { continue };
                let pq = perp_qty.remove(&sym).unwrap_or(Decimal::ZERO);
                if pq != Decimal::ZERO {
                    let fills = self
                        .trade_to_target(&sym, "PERP", pq, Decimal::ZERO, last.mark_price, last)
                        .await?;
                    for f in &fills {
                        cash += cash_flow(f);
                        total_costs += f.commission;
                    }
                }
                let sq = spot_qty.remove(&sym).unwrap_or(Decimal::ZERO);
                if sq != Decimal::ZERO {
                    let price = last.spot_price.unwrap_or(last.mark_price);
                    let fills = self
                        .trade_to_target(&sym, "SPOT", sq, Decimal::ZERO, price, last)
                        .await?;
                    for f in &fills {
                        cash += cash_flow(f);
                        total_costs += f.commission;
                    }
                }
            }

            // 4) Ребаланс кошика.
            if step % cfg.rebalance_intervals.max(1) == 0 {
                // Скоринг: трейлінг-середній funding по символах зі «свіжими»
                // даними (точка на t або в межах max_gap — 4h/8h сітки різні)
                // і повним вікном історії.
                let mut scored: Vec<(String, Decimal, FundingRatePoint)> = Vec::new();
                for (sym, series) in series_by_symbol {
                    let fresh = last_seen_step
                        .get(sym)
                        .map(|s| step - *s <= cfg.max_gap_intervals)
                        .unwrap_or(false);
                    if !fresh {
                        continue;
                    }
                    let idx = cursor[sym.as_str()];
                    let idx = idx.min(series.len().saturating_sub(1));
                    // Точка ціни для торгівлі — остання відома.
                    let Some(p) = last_seen.get(sym).cloned() else {
                        continue;
                    };
                    if idx + 1 < cfg.trailing_intervals {
                        continue;
                    }
                    let window = &series[idx + 1 - cfg.trailing_intervals..=idx];
                    let mean: Decimal = window.iter().map(|q| q.rate).sum::<Decimal>()
                        / Decimal::from(cfg.trailing_intervals as u64);
                    scored.push((sym.clone(), mean, p));
                }
                // Детермінований порядок: за скором ↓, потім за символом ↑.
                scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                let rank: BTreeMap<&str, usize> = scored
                    .iter()
                    .enumerate()
                    .map(|(i, (s, _, _))| (s.as_str(), i))
                    .collect();
                let mean_of: BTreeMap<&str, Decimal> =
                    scored.iter().map(|(s, m, _)| (s.as_str(), *m)).collect();
                let point_of: BTreeMap<&str, &FundingRatePoint> =
                    scored.iter().map(|(s, _, p)| (s.as_str(), p)).collect();

                // Гістерезис членства: утримувані лишаються, поки ранг у межах
                // top_k × exit_rank_multiple і funding не впав нижче половини
                // порога входу. Вільні місця добираються з верхівки рейтингу.
                let exit_rank = cfg.top_k * cfg.exit_rank_multiple.max(1);
                let exit_threshold = cfg.entry_threshold / Decimal::TWO;
                let mut basket: Vec<String> = Vec::new();
                for sym in perp_qty.keys() {
                    let keep = rank
                        .get(sym.as_str())
                        .map(|r| *r < exit_rank)
                        .unwrap_or(false)
                        && mean_of
                            .get(sym.as_str())
                            .map(|m| *m >= exit_threshold)
                            .unwrap_or(false);
                    if keep {
                        basket.push(sym.clone());
                    }
                }
                for (sym, mean, _) in &scored {
                    if basket.len() >= cfg.top_k {
                        break;
                    }
                    if *mean >= cfg.entry_threshold && !basket.contains(sym) {
                        basket.push(sym.clone());
                    }
                }
                basket.truncate(cfg.top_k);
                basket.sort();

                // Поточний еквіті для сайзингу.
                let mut equity = cash;
                for (sym, qty) in &perp_qty {
                    if let Some(p) = last_seen.get(sym) {
                        equity += *qty * p.mark_price;
                    }
                }
                for (sym, qty) in &spot_qty {
                    if let Some(p) = last_seen.get(sym) {
                        equity += *qty * p.spot_price.unwrap_or(p.mark_price);
                    }
                }

                if equity > Decimal::ZERO {
                    let per_symbol_notional = if basket.is_empty() {
                        Decimal::ZERO
                    } else {
                        equity * cfg.gross_exposure / Decimal::from(basket.len() as u64)
                    };

                    // Символи до торгівлі: кошик + наявні позиції (ціль 0 → вихід).
                    let mut to_trade: BTreeSet<String> = basket.iter().cloned().collect();
                    to_trade.extend(perp_qty.keys().cloned());
                    to_trade.extend(spot_qty.keys().cloned());

                    let mut traded_this_rebalance = false;
                    for sym in to_trade {
                        let Some(p_now) = point_of.get(sym.as_str()).copied() else {
                            continue; // немає свіжої ціни — не торгуємо
                        };
                        let in_basket = basket.contains(&sym);
                        let (tgt_perp, tgt_spot) = if in_basket {
                            let spot_price = p_now.spot_price.unwrap_or(p_now.mark_price);
                            (
                                -(per_symbol_notional / p_now.mark_price),
                                per_symbol_notional / spot_price,
                            )
                        } else {
                            (Decimal::ZERO, Decimal::ZERO)
                        };
                        let cur_perp = perp_qty.get(&sym).copied().unwrap_or(Decimal::ZERO);
                        let cur_spot = spot_qty.get(&sym).copied().unwrap_or(Decimal::ZERO);

                        // Банда: утримувану позицію ресайзимо тільки якщо
                        // відхилення нотіоналу суттєве (проти мікрочурну).
                        let band_notional = per_symbol_notional * cfg.trade_band;
                        let perp_delta_notional =
                            ((tgt_perp - cur_perp) * p_now.mark_price).abs();
                        let skip_resize = in_basket
                            && cur_perp != Decimal::ZERO
                            && perp_delta_notional < band_notional;

                        if cur_perp != tgt_perp && !skip_resize {
                            let fills = self
                                .trade_to_target(&sym, "PERP", cur_perp, tgt_perp, p_now.mark_price, p_now)
                                .await?;
                            for f in &fills {
                                cash += cash_flow(f);
                                total_costs += f.commission;
                            }
                            traded_this_rebalance = traded_this_rebalance || !fills.is_empty();
                            if tgt_perp == Decimal::ZERO {
                                perp_qty.remove(&sym);
                            } else {
                                perp_qty.insert(sym.clone(), tgt_perp);
                                symbols_traded.insert(sym.clone());
                            }

                            let spot_price = p_now.spot_price.unwrap_or(p_now.mark_price);
                            let fills = self
                                .trade_to_target(&sym, "SPOT", cur_spot, tgt_spot, spot_price, p_now)
                                .await?;
                            for f in &fills {
                                cash += cash_flow(f);
                                total_costs += f.commission;
                            }
                            if tgt_spot == Decimal::ZERO {
                                spot_qty.remove(&sym);
                            } else {
                                spot_qty.insert(sym.clone(), tgt_spot);
                            }
                        }
                    }
                    if traded_this_rebalance {
                        rebalances += 1;
                    }
                }
            }

            // 5) Mark-to-market.
            let mut equity = cash;
            for (sym, qty) in &perp_qty {
                if let Some(p) = last_seen.get(sym) {
                    equity += *qty * p.mark_price;
                }
            }
            for (sym, qty) in &spot_qty {
                if let Some(p) = last_seen.get(sym) {
                    equity += *qty * p.spot_price.unwrap_or(p.mark_price);
                }
            }
            equity_curve.push(equity);
        }

        // Фінальне закриття всіх позицій за останніми відомими цінами.
        let held: Vec<String> = perp_qty
            .keys()
            .chain(spot_qty.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for sym in held {
            let Some(last) = last_seen.get(&sym) else { continue };
            let pq = perp_qty.remove(&sym).unwrap_or(Decimal::ZERO);
            if pq != Decimal::ZERO {
                let fills = self
                    .trade_to_target(&sym, "PERP", pq, Decimal::ZERO, last.mark_price, last)
                    .await?;
                for f in &fills {
                    cash += cash_flow(f);
                    total_costs += f.commission;
                }
            }
            let sq = spot_qty.remove(&sym).unwrap_or(Decimal::ZERO);
            if sq != Decimal::ZERO {
                let price = last.spot_price.unwrap_or(last.mark_price);
                let fills = self
                    .trade_to_target(&sym, "SPOT", sq, Decimal::ZERO, price, last)
                    .await?;
                for f in &fills {
                    cash += cash_flow(f);
                    total_costs += f.commission;
                }
            }
        }
        if let Some(last_eq) = equity_curve.last_mut() {
            *last_eq = cash;
        }

        let net_pnl = cash - initial_capital;
        let price_pnl = net_pnl - funding_pnl + total_costs;
        let (_, mdd_pct) = metrics::max_drawdown(&equity_curve);

        Ok(XsCarryReport {
            net_pnl,
            funding_pnl,
            total_costs,
            price_pnl,
            sharpe: metrics::sharpe_annualized(&equity_curve),
            max_drawdown_pct: mdd_pct,
            intervals: timeline.len(),
            rebalances,
            symbols_traded: symbols_traded.len(),
            final_equity: cash,
            equity_curve,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::run_config::CostConfig;
    use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
    use crate::trading::domain::costs::{cost_model_from_config, ZeroCost};
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn series(rate: Decimal, n: usize) -> Vec<FundingRatePoint> {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        (0..n)
            .map(|i| FundingRatePoint {
                symbol: "BTCUSDT".into(),
                timestamp: t0 + Duration::hours(8 * i as i64),
                rate,
                mark_price: dec!(50000),
                spot_price: Some(dec!(49950)),
            })
            .collect()
    }

    fn broker_zero_cost() -> Arc<dyn BrokerSimulatorPort> {
        Arc::new(SimpleBrokerSimulator {
            slippage_pct: Decimal::ZERO,
            cost_model: Arc::new(ZeroCost),
        })
    }

    // Стабільно додатний funding + стабільні ціни → carry збирає funding.
    #[tokio::test]
    async fn collects_positive_funding_when_hedged() {
        let bt = FundingCarryBacktest::new(broker_zero_cost());
        let report = bt
            .run(&series(dec!(0.0001), 90), dec!(100000), &FundingCarryConfig::default())
            .await
            .unwrap();
        assert!(report.hedged);
        assert!(report.funding_pnl > Decimal::ZERO, "funding зібраний: {report:?}");
        assert!(report.net_pnl > Decimal::ZERO);
        // Хедж: ціновий PnL ≈ 0 (ціни константні в фікстурі).
        assert_eq!(report.price_pnl, Decimal::ZERO);
    }

    // Подвійного обліку немає: net = funding + price − costs (точна рівність).
    #[tokio::test]
    async fn no_double_counting_identity() {
        let broker = Arc::new(SimpleBrokerSimulator {
            slippage_pct: Decimal::ZERO,
            cost_model: cost_model_from_config(&CostConfig::default()),
        }) as Arc<dyn BrokerSimulatorPort>;
        let bt = FundingCarryBacktest::new(broker);
        let report = bt
            .run(&series(dec!(0.0001), 90), dec!(100000), &FundingCarryConfig::default())
            .await
            .unwrap();
        assert!(report.total_costs > Decimal::ZERO);
        assert_eq!(
            report.net_pnl,
            report.funding_pnl + report.price_pnl - report.total_costs,
            "PnL identity порушена — десь подвійний облік"
        );
    }

    // Від'ємний funding → дзеркальна позиція теж заробляє.
    #[tokio::test]
    async fn negative_funding_reverses_position() {
        let bt = FundingCarryBacktest::new(broker_zero_cost());
        let report = bt
            .run(&series(dec!(-0.0001), 90), dec!(100000), &FundingCarryConfig::default())
            .await
            .unwrap();
        assert!(report.funding_pnl > Decimal::ZERO, "{report:?}");
    }

    // Нульовий funding → жодних входів, жодних витрат.
    #[tokio::test]
    async fn stays_flat_below_threshold() {
        let bt = FundingCarryBacktest::new(broker_zero_cost());
        let report = bt
            .run(&series(Decimal::ZERO, 30), dec!(100000), &FundingCarryConfig::default())
            .await
            .unwrap();
        assert_eq!(report.rebalances, 0);
        assert_eq!(report.net_pnl, Decimal::ZERO);
    }

    // ── Крос-секційний carry ──

    fn named_series(sym: &str, rate: Decimal, n: usize, start_offset: usize) -> Vec<FundingRatePoint> {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        (start_offset..n)
            .map(|i| FundingRatePoint {
                symbol: sym.into(),
                timestamp: t0 + Duration::hours(8 * i as i64),
                rate,
                mark_price: dec!(100),
                spot_price: Some(dec!(100)),
            })
            .collect()
    }

    fn universe_fat_and_thin(n: usize) -> std::collections::BTreeMap<String, Vec<FundingRatePoint>> {
        let mut u = std::collections::BTreeMap::new();
        // Два «жирні платники» funding і чотири нульові.
        u.insert("FAT1".to_string(), named_series("FAT1", dec!(0.0005), n, 0));
        u.insert("FAT2".to_string(), named_series("FAT2", dec!(0.0003), n, 0));
        for k in 0..4 {
            u.insert(format!("THIN{k}"), named_series(&format!("THIN{k}"), Decimal::ZERO, n, 0));
        }
        u
    }

    fn xs_cfg() -> XsCarryConfig {
        XsCarryConfig {
            trailing_intervals: 6,
            top_k: 2,
            rebalance_intervals: 6,
            entry_threshold: dec!(0.0001),
            gross_exposure: dec!(0.5),
            max_gap_intervals: 3,
            ..Default::default()
        }
    }

    // Кошик збирає funding саме з жирних символів; тонкі не торгуються.
    #[tokio::test]
    async fn xs_carry_collects_funding_from_top_payers() {
        let bt = FundingCarryBacktest::new(broker_zero_cost());
        let report = bt
            .run_cross_sectional(&universe_fat_and_thin(90), dec!(100000), &xs_cfg())
            .await
            .unwrap();
        assert!(report.funding_pnl > Decimal::ZERO, "{report:?}");
        assert_eq!(report.symbols_traded, 2, "тільки FAT1+FAT2: {report:?}");
        assert!(report.net_pnl > Decimal::ZERO);
        // Хедж: ціни константні → ціновий PnL ≈ 0 (з точністю до
        // округлення Decimal-ділення на 28-й значущій цифрі).
        assert!(
            report.price_pnl.abs() < dec!(0.000000000001),
            "price_pnl {} має бути ≈ 0",
            report.price_pnl
        );
    }

    // Тотожність PnL: net = funding + price − costs (нема подвійного обліку).
    #[tokio::test]
    async fn xs_carry_no_double_counting() {
        let broker = Arc::new(SimpleBrokerSimulator {
            slippage_pct: Decimal::ZERO,
            cost_model: cost_model_from_config(&CostConfig::default()),
        }) as Arc<dyn BrokerSimulatorPort>;
        let bt = FundingCarryBacktest::new(broker);
        let report = bt
            .run_cross_sectional(&universe_fat_and_thin(90), dec!(100000), &xs_cfg())
            .await
            .unwrap();
        assert!(report.total_costs > Decimal::ZERO);
        assert_eq!(
            report.net_pnl,
            report.funding_pnl + report.price_pnl - report.total_costs
        );
    }

    // «Рвані» серії: символ делістився посеред історії — позиція
    // форс-закривається, движок доживає до кінця без помилок.
    #[tokio::test]
    async fn xs_carry_survives_delisting() {
        let mut u = universe_fat_and_thin(90);
        // FAT1 зникає після 30-го інтервалу (делістинг).
        u.insert("FAT1".to_string(), {
            let mut s = named_series("FAT1", dec!(0.0005), 30, 0);
            s.truncate(30);
            s
        });
        let bt = FundingCarryBacktest::new(broker_zero_cost());
        let report = bt
            .run_cross_sectional(&u, dec!(100000), &xs_cfg())
            .await
            .unwrap();
        assert!(report.funding_pnl > Decimal::ZERO);
        // Після делістингу FAT1 кошик має жити на FAT2.
        assert_eq!(report.symbols_traded, 2);
    }

    // Символ, що з'являється пізніше, входить у кошик тільки після
    // повного трейлінг-вікна (без look-ahead на неповних даних).
    #[tokio::test]
    async fn xs_carry_waits_for_full_trailing_window() {
        let mut u = std::collections::BTreeMap::new();
        // LATE стартує з 50-го інтервалу з космічним funding.
        u.insert("LATE".to_string(), named_series("LATE", dec!(0.01), 90, 50));
        u.insert("BASE".to_string(), named_series("BASE", dec!(0.0002), 90, 0));
        for k in 0..3 {
            u.insert(format!("THIN{k}"), named_series(&format!("THIN{k}"), Decimal::ZERO, 90, 0));
        }
        let bt = FundingCarryBacktest::new(broker_zero_cost());
        let report = bt
            .run_cross_sectional(&u, dec!(100000), &xs_cfg())
            .await
            .unwrap();
        // LATE все ж встигає набрати вікно і взятись у кошик до кінця.
        assert_eq!(report.symbols_traded, 2);
        assert!(report.net_pnl > Decimal::ZERO);
    }
}
