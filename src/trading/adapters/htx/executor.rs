//! Тижневий live-виконавець carry-кошика на HTX.
//!
//! Джерело сигналу — shadow/ledger.jsonl: торгуємо РІВНО той кошик, який
//! нотаризував research.yml (інваріант 1: логіку вибору не дублюємо).
//! Позиція на символ дельта-нейтральна: шорт перпа + лонг спота, дохід —
//! funding.
//!
//! Захисні шари:
//! 1. `build_plan` — чиста функція, нічого не шле (dry-run за замовчуванням);
//! 2. реальна відправка вимагає `HTX_TRADING_ENABLED=true` в env;
//! 3. капи `max_order_usdt`/`max_total_usdt` з LiveConfig;
//! 4. ідемпотентність: журнал client-order-id + наявні позиції на біржі;
//! 5. кожен крок пишеться в live/orders.jsonl ДО відправки (crash-safe).

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;

use crate::shared::run_config::LiveConfig;
use crate::trading::adapters::htx::client::HtxClient;
use crate::trading::adapters::htx::types::*;

// ── Мапінг символів: Binance-стиль → HTX ─────────────────────────────────────

/// "VELVETUSDT" → "VELVET-USDT" (контракт USDT-M свопу на HTX).
pub fn to_contract_code(binance_symbol: &str) -> Option<String> {
    let base = binance_symbol.strip_suffix("USDT")?;
    if base.is_empty() {
        return None;
    }
    Some(format!("{base}-USDT"))
}

/// "VELVETUSDT" → "velvetusdt" (спот-пара HTX).
pub fn to_spot_symbol(binance_symbol: &str) -> String {
    binance_symbol.to_lowercase()
}

/// "VELVETUSDT" → "velvet" (базова монета для балансу).
pub fn base_currency(binance_symbol: &str) -> Option<String> {
    binance_symbol
        .strip_suffix("USDT")
        .map(|b| b.to_lowercase())
}

// ── Округлення (інваріант 6: Decimal, завжди вниз — ніколи не перевищуємо) ───

pub fn floor_to_dp(v: Decimal, dp: u32) -> Decimal {
    v.round_dp_with_strategy(dp, RoundingStrategy::ToZero)
}

/// Ціла кількість контрактів, що вміщується в алокацію.
pub fn contracts_for(alloc_usdt: Decimal, price: Decimal, contract_size: Decimal) -> i64 {
    let per_contract = price * contract_size;
    if per_contract <= Decimal::ZERO {
        return 0;
    }
    (alloc_usdt / per_contract)
        .floor()
        .to_i64()
        .unwrap_or(0)
        .max(0)
}

// ── Ідентифікатори ордерів ────────────────────────────────────────────────────

/// Спот: рядковий client-order-id, детермінований від (запис журналу, символ,
/// нога) — повторний запуск шле той самий id, а наш журнал ловить дубль.
pub fn spot_client_id(entry_id: &str, symbol: &str, leg: &str) -> String {
    let entry8: String = entry_id.chars().take(8).collect();
    format!("dbc-{entry8}-{symbol}-{leg}")
}

/// Своп вимагає ЧИСЛОВИЙ client_order_id: детермінований позитивний i64
/// із SHA-256 того самого рядкового id.
pub fn swap_client_id(spot_like_id: &str) -> i64 {
    let digest = Sha256::digest(spot_like_id.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    (i64::from_be_bytes(bytes) & i64::MAX).max(1)
}

// ── Журнал (shadow-філософія: append-only jsonl) ─────────────────────────────

pub struct OrderJournal {
    pub path: PathBuf,
}

impl OrderJournal {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn append(&self, record: &serde_json::Value) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(f, "{}", serde_json::to_string(record)?)?;
        Ok(())
    }

    /// client-order-id, які НЕ можна слати повторно. Журнал програється
    /// хронологічно, рахується останній стан ноги: "sending" без розв'язки
    /// (краш посеред відправки — стан на біржі невідомий, сліпий ретрай
    /// заборонено) або "placed" — блокують; явна відмова біржі ("error")
    /// знімає блок — таку ногу безпечно ретраїти перезапуском execute.
    pub fn sent_client_ids(&self) -> HashSet<String> {
        let mut blocked: std::collections::HashMap<String, bool> = std::collections::HashMap::new();
        for r in std::fs::read_to_string(&self.path)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        {
            let Some(id) = r["client_order_id"].as_str() else { continue };
            match r["phase"].as_str() {
                Some("sending" | "placed") => {
                    blocked.insert(id.to_string(), true);
                }
                Some("error") => {
                    blocked.insert(id.to_string(), false);
                }
                _ => {}
            }
        }
        blocked
            .into_iter()
            .filter_map(|(id, b)| b.then_some(id))
            .collect()
    }
}

// ── Запис shadow-журналу ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LedgerBasketEntry {
    pub id: String,
    pub anchor_ts: String,
    pub retro: bool,
    pub basket_name: String,
    /// Символи Binance-стилю у порядку скорингу.
    pub symbols: Vec<String>,
}

pub fn parse_ledger_entry(v: &serde_json::Value, basket: &str) -> Option<LedgerBasketEntry> {
    let symbols: Vec<String> = v["baskets"][basket]
        .as_array()?
        .iter()
        .filter_map(|s| s["symbol"].as_str().map(String::from))
        .collect();
    if symbols.is_empty() {
        return None;
    }
    Some(LedgerBasketEntry {
        id: v["id"].as_str()?.to_string(),
        anchor_ts: v["anchor_ts"].as_str().unwrap_or_default().to_string(),
        retro: v["retro"].as_bool().unwrap_or(false),
        basket_name: basket.to_string(),
        symbols,
    })
}

/// Останній справжній (не retro) запис журналу з потрібним кошиком.
pub fn last_real_entry(ledger_jsonl: &str, basket: &str) -> Option<LedgerBasketEntry> {
    ledger_jsonl
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| parse_ledger_entry(&v, basket))
        .rfind(|e| !e.retro)
}

// ── План ребалансу ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegKind {
    SpotBuy,
    SpotSell,
    PerpOpenShort,
    PerpCloseShort,
}

impl LegKind {
    pub fn label(&self) -> &'static str {
        match self {
            LegKind::SpotBuy => "спот купівля",
            LegKind::SpotSell => "спот продаж",
            LegKind::PerpOpenShort => "перп шорт (відкриття)",
            LegKind::PerpCloseShort => "перп шорт (закриття)",
        }
    }
    fn suffix(&self) -> &'static str {
        match self {
            LegKind::SpotBuy => "sb",
            LegKind::SpotSell => "ss",
            LegKind::PerpOpenShort => "po",
            LegKind::PerpCloseShort => "pc",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlannedLeg {
    pub kind: LegKind,
    /// Спот-пара ("velvetusdt") або контракт ("VELVET-USDT").
    pub instrument: String,
    /// Спот: кількість базової монети. Перп: КОНТРАКТИ.
    pub qty: Decimal,
    /// Лімітна ціна (post_only: свій бік топу книги; taker: через спред).
    pub price: Decimal,
    pub notional_usdt: Decimal,
    pub client_order_id: String,
}

#[derive(Debug, Clone)]
pub struct SymbolPlan {
    pub binance_symbol: String,
    pub htx_funding_per_8h: Option<Decimal>,
    pub legs: Vec<PlannedLeg>,
}

#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub entry_id: String,
    pub anchor_ts: String,
    pub basket_name: String,
    pub live_config_hash: String,
    /// Людський підсумок авто-розподілу капіталу (тільки sizing_mode=auto).
    pub capital_note: Option<String>,
    pub opens: Vec<SymbolPlan>,
    pub closes: Vec<SymbolPlan>,
    pub skipped: Vec<(String, String)>,
    pub warnings: Vec<String>,
    pub total_open_usdt: Decimal,
    /// Оцінка funding-доходу за тиждень за ПОТОЧНИМИ ставками HTX (21 інтервал).
    pub est_weekly_funding_usdt: Decimal,
}

impl RebalancePlan {
    pub fn is_empty(&self) -> bool {
        self.opens.is_empty() && self.closes.is_empty()
    }
}

/// Все, що потрібно для плану. Заповнюється з біржі (gather_inputs) або
/// руками в тестах — build_plan лишається чистою функцією.
#[derive(Debug, Default)]
pub struct PlanInputs {
    /// contract_code → інфо контракту (лише торговані).
    pub contracts: HashMap<String, SwapContractInfo>,
    /// contract_code → поточний funding за 8г.
    pub funding: HashMap<String, Decimal>,
    /// спот-пара → метадані (лише online).
    pub spot_meta: HashMap<String, SpotSymbolMeta>,
    /// contract_code → (bid, ask).
    pub perp_prices: HashMap<String, (Decimal, Decimal)>,
    /// спот-пара → (bid, ask).
    pub spot_prices: HashMap<String, (Decimal, Decimal)>,
    /// contract_code → контракти в наявному крос-шорті (available).
    pub current_shorts: HashMap<String, Decimal>,
    /// базова монета (lowercase) → доступний спот-баланс.
    pub spot_balances: HashMap<String, Decimal>,
    /// client-order-id, які вже відправлялись (ідемпотентність).
    pub already_sent: HashSet<String>,
    /// Реальний доступний капітал (спот-USDT + вільна крос-маржа) —
    /// для sizing_mode=auto з capital_usdt=0. None без ключів.
    pub available_capital: Option<Decimal>,
}

// ── Драбина розподілу капіталу (sizing_mode = "auto") ───────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct SizedAllocation {
    /// Скільки символів торгуємо (≤ top_k).
    pub k: usize,
    /// Розмір однієї ноги, USDT.
    pub leg_usdt: Decimal,
    /// capital × deploy_pct.
    pub working_usdt: Decimal,
    /// Недоторканний запас (маржинальна подушка шорт-ноги).
    pub buffer_usdt: Decimal,
    /// Частина working, що не розгорнулась: нога вперлась у max_order_usdt
    /// (кап місткості дрібних перпів) — чесно лишається в буфері.
    pub undeployed_usdt: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CapitalAllocation {
    TooSmall { min_capital_usdt: Decimal },
    Sized(SizedAllocation),
}

/// Капітал → структура кошика. Семантика auto: deploy_pct — частка ВСІХ
/// грошей у роботі (спот-ноги + перп-маржа при 1x разом), решта — буфер.
/// K максимізується (диверсифікація понад розмір ноги), доки нога ≥
/// min_leg_usdt і K ≤ top_k; далі росте нога до капу max_order_usdt.
pub fn capital_allocation(capital_usdt: Decimal, cfg: &LiveConfig) -> CapitalAllocation {
    let working = capital_usdt * cfg.deploy_pct;
    let per_symbol_min = cfg.min_leg_usdt * dec!(2);
    let k = if per_symbol_min > Decimal::ZERO {
        (working / per_symbol_min).floor().to_usize().unwrap_or(0)
    } else {
        cfg.top_k
    }
    .min(cfg.top_k);
    if k == 0 {
        return CapitalAllocation::TooSmall {
            min_capital_usdt: if cfg.deploy_pct > Decimal::ZERO {
                per_symbol_min / cfg.deploy_pct
            } else {
                per_symbol_min
            },
        };
    }
    let legs = Decimal::from(2 * k as u64);
    let leg_usdt = (working / legs).min(cfg.max_order_usdt);
    CapitalAllocation::Sized(SizedAllocation {
        k,
        leg_usdt,
        working_usdt: working,
        buffer_usdt: capital_usdt - working,
        undeployed_usdt: working - leg_usdt * legs,
    })
}

fn post_only_price(kind: LegKind, bid: Decimal, ask: Decimal, taker: bool) -> Decimal {
    let buying = matches!(kind, LegKind::SpotBuy | LegKind::PerpCloseShort);
    match (buying, taker) {
        // Мейкер: стаємо на СВІЙ бік топу книги.
        (true, false) => bid,
        (false, false) => ask,
        // Тейкер: ліміт через спред — виконується одразу, без ринкового
        // прослизання глибше топу.
        (true, true) => ask,
        (false, true) => bid,
    }
}

/// Чистий планувальник: цільовий кошик → ордери відкриття/закриття.
pub fn build_plan(
    entry: &LedgerBasketEntry,
    inputs: &PlanInputs,
    cfg: &LiveConfig,
) -> RebalancePlan {
    let taker = cfg.order_style == "taker";
    let mut plan = RebalancePlan {
        entry_id: entry.id.clone(),
        anchor_ts: entry.anchor_ts.clone(),
        basket_name: entry.basket_name.clone(),
        live_config_hash: cfg.config_hash(),
        capital_note: None,
        opens: vec![],
        closes: vec![],
        skipped: vec![],
        warnings: vec![],
        total_open_usdt: Decimal::ZERO,
        est_weekly_funding_usdt: Decimal::ZERO,
    };
    let skip = |plan: &mut RebalancePlan, sym: &str, reason: String| {
        plan.skipped.push((sym.to_string(), reason));
    };

    // Скільки символів і якою ногою: auto — драбина від капіталу,
    // fixed — стара пряма формула.
    let (symbols, alloc_per_symbol): (&[String], Decimal) = if cfg.sizing_mode == "auto" {
        let capital = if cfg.capital_usdt > Decimal::ZERO {
            Some(cfg.capital_usdt)
        } else {
            inputs.available_capital
        };
        let Some(capital) = capital else {
            plan.warnings.push(
                "sizing auto з capital_usdt=0 потребує ключів HTX (капітал з балансів) — \
                 задай capital_usdt явно або додай ключі"
                    .to_string(),
            );
            return plan;
        };
        match capital_allocation(capital, cfg) {
            CapitalAllocation::TooSmall { min_capital_usdt } => {
                plan.warnings.push(format!(
                    "капітал {capital:.2} USDT замалий навіть для 1 символа — \
                     мінімум ≈ {min_capital_usdt:.2} (2×min_leg_usdt/deploy_pct); торгувати не варто"
                ));
                return plan;
            }
            CapitalAllocation::Sized(s) => {
                let deployed = s.working_usdt - s.undeployed_usdt;
                plan.capital_note = Some(format!(
                    "Розподіл (auto): капітал {capital:.2} USDT → у роботі {deployed:.2} \
                     ({} символ(и) × 2 ноги × {:.2}), буфер {:.2}{}",
                    s.k,
                    s.leg_usdt,
                    s.buffer_usdt + s.undeployed_usdt,
                    if s.undeployed_usdt > Decimal::ZERO {
                        format!(
                            " — з них {:.2} не розгорнуто: нога вперлась у max_order_usdt \
                             (перш ніж піднімати кап, зваж глибину стаканів дрібних перпів)",
                            s.undeployed_usdt
                        )
                    } else {
                        String::new()
                    }
                ));
                (&entry.symbols[..s.k.min(entry.symbols.len())], s.leg_usdt)
            }
        }
    } else {
        (
            &entry.symbols[..],
            (cfg.capital_usdt * cfg.deploy_pct
                / Decimal::from(entry.symbols.len().max(1) as u64))
            .min(cfg.max_order_usdt),
        )
    };

    let mut target_codes: HashSet<String> = HashSet::new();

    // ── Відкриття ──
    for sym in symbols {
        let Some(code) = to_contract_code(sym) else {
            skip(&mut plan, sym, "не-USDT символ".into());
            continue;
        };
        let Some(contract) = inputs.contracts.get(&code) else {
            skip(&mut plan, sym, "перпа немає на HTX".into());
            continue;
        };
        if !contract.is_trading() {
            skip(&mut plan, sym, "перп не в статусі торгів".into());
            continue;
        }
        if !contract.supports_cross() {
            skip(&mut plan, sym, "контракт без крос-маржі".into());
            continue;
        }
        let spot_sym = to_spot_symbol(sym);
        let Some(spot_meta) = inputs.spot_meta.get(&spot_sym) else {
            skip(&mut plan, sym, "спот-пари немає на HTX (нема другої ноги)".into());
            continue;
        };
        let funding = inputs.funding.get(&code).copied();
        match funding {
            None => {
                skip(&mut plan, sym, "HTX не віддає funding".into());
                continue;
            }
            Some(f) if f < cfg.min_htx_funding_per_8h => {
                skip(
                    &mut plan,
                    sym,
                    format!("funding на HTX {f} < мінімуму {} (сигнал з Binance сюди не переноситься)", cfg.min_htx_funding_per_8h),
                );
                continue;
            }
            _ => {}
        }
        // Вже в позиції (символ лишився з минулого тижня) — тримаємо, не дублюємо.
        if inputs.current_shorts.get(&code).copied().unwrap_or_default() > Decimal::ZERO {
            target_codes.insert(code.clone());
            skip(&mut plan, sym, "шорт уже відкритий — тримаємо".into());
            continue;
        }
        let Some(&(perp_bid, perp_ask)) = inputs.perp_prices.get(&code) else {
            skip(&mut plan, sym, "нема ціни перпа".into());
            continue;
        };
        let Some(&(spot_bid, spot_ask)) = inputs.spot_prices.get(&spot_sym) else {
            skip(&mut plan, sym, "нема ціни спота".into());
            continue;
        };

        let volume = contracts_for(alloc_per_symbol, perp_bid, contract.contract_size);
        if volume < 1 {
            skip(
                &mut plan,
                sym,
                format!(
                    "1 контракт ({} шт × {perp_bid}) більший за алокацію {alloc_per_symbol} USDT",
                    contract.contract_size
                ),
            );
            continue;
        }
        let perp_price = post_only_price(LegKind::PerpOpenShort, perp_bid, perp_ask, taker);
        let perp_qty = Decimal::from(volume);
        let perp_notional = perp_qty * contract.contract_size * perp_price;

        // Спот-нога дзеркалить перп у штуках базової монети.
        let spot_dp = spot_meta.tap.unwrap_or(4);
        let spot_qty = floor_to_dp(perp_qty * contract.contract_size, spot_dp);
        let spot_price = post_only_price(LegKind::SpotBuy, spot_bid, spot_ask, taker);
        let spot_notional = spot_qty * spot_price;
        if let Some(minoa) = spot_meta.minoa {
            if spot_qty < minoa {
                skip(&mut plan, sym, format!("спот-кількість {spot_qty} < мінімуму {minoa}"));
                continue;
            }
        }
        if let Some(minov) = spot_meta.minov {
            if spot_notional < minov {
                skip(&mut plan, sym, format!("спот-нотіонал {spot_notional} < мінімуму {minov} USDT"));
                continue;
            }
        }
        if spot_notional < cfg.min_leg_usdt || perp_notional < cfg.min_leg_usdt {
            skip(&mut plan, sym, format!("нога дрібніша за min_leg_usdt={}", cfg.min_leg_usdt));
            continue;
        }
        if plan.total_open_usdt + spot_notional + perp_notional > cfg.max_total_usdt {
            skip(&mut plan, sym, format!("перевищив би max_total_usdt={}", cfg.max_total_usdt));
            continue;
        }

        let spot_cid = spot_client_id(&entry.id, &spot_sym, LegKind::SpotBuy.suffix());
        let perp_cid_str = spot_client_id(&entry.id, &code, LegKind::PerpOpenShort.suffix());
        if inputs.already_sent.contains(&spot_cid) || inputs.already_sent.contains(&perp_cid_str) {
            skip(&mut plan, sym, "уже відправлялося в цьому записі журналу".into());
            target_codes.insert(code.clone());
            continue;
        }

        let residual = perp_qty * contract.contract_size - spot_qty;
        if residual > Decimal::ZERO {
            plan.warnings.push(format!(
                "{sym}: залишкова дельта {residual} {} через округлення спот-ноги",
                base_currency(sym).unwrap_or_default()
            ));
        }

        target_codes.insert(code.clone());
        plan.total_open_usdt += spot_notional + perp_notional;
        if let Some(f) = funding {
            plan.est_weekly_funding_usdt += f * dec!(21) * perp_notional;
        }
        plan.opens.push(SymbolPlan {
            binance_symbol: sym.clone(),
            htx_funding_per_8h: funding,
            legs: vec![
                PlannedLeg {
                    kind: LegKind::SpotBuy,
                    instrument: spot_sym.clone(),
                    qty: spot_qty,
                    price: spot_price,
                    notional_usdt: spot_notional,
                    client_order_id: spot_cid,
                },
                PlannedLeg {
                    kind: LegKind::PerpOpenShort,
                    instrument: code.clone(),
                    qty: perp_qty,
                    price: perp_price,
                    notional_usdt: perp_notional,
                    client_order_id: perp_cid_str,
                },
            ],
        });
    }

    // ── Закриття: наші шорти, яких немає в цільовому кошику ──
    for (code, contracts_held) in &inputs.current_shorts {
        if target_codes.contains(code) || *contracts_held <= Decimal::ZERO {
            continue;
        }
        let Some(contract) = inputs.contracts.get(code) else {
            plan.warnings.push(format!("{code}: відкритий шорт, але контракту нема в довіднику — закрий руками"));
            continue;
        };
        let Some(&(perp_bid, perp_ask)) = inputs.perp_prices.get(code) else {
            plan.warnings.push(format!("{code}: нема ціни — закриття пропущено"));
            continue;
        };
        let binance_sym = code.replace("-", "");
        let spot_sym = to_spot_symbol(&binance_sym);
        let perp_price = post_only_price(LegKind::PerpCloseShort, perp_bid, perp_ask, taker);
        let mut legs = vec![PlannedLeg {
            kind: LegKind::PerpCloseShort,
            instrument: code.clone(),
            qty: *contracts_held,
            price: perp_price,
            notional_usdt: *contracts_held * contract.contract_size * perp_price,
            client_order_id: spot_client_id(&entry.id, code, LegKind::PerpCloseShort.suffix()),
        }];

        // Продаємо відповідну кількість спота (скільки реально є на балансі).
        let base = base_currency(&binance_sym).unwrap_or_default();
        let balance = inputs.spot_balances.get(&base).copied().unwrap_or_default();
        let want = *contracts_held * contract.contract_size;
        let spot_meta = inputs.spot_meta.get(&spot_sym);
        let spot_px = inputs.spot_prices.get(&spot_sym).copied();
        if let (Some(meta), Some((sbid, sask))) = (spot_meta, spot_px) {
            let qty = floor_to_dp(want.min(balance), meta.tap.unwrap_or(4));
            let price = post_only_price(LegKind::SpotSell, sbid, sask, taker);
            let notional = qty * price;
            let big_enough = qty > Decimal::ZERO
                && meta.minoa.map(|m| qty >= m).unwrap_or(true)
                && meta.minov.map(|m| notional >= m).unwrap_or(true);
            if big_enough {
                legs.push(PlannedLeg {
                    kind: LegKind::SpotSell,
                    instrument: spot_sym.clone(),
                    qty,
                    price,
                    notional_usdt: notional,
                    client_order_id: spot_client_id(&entry.id, &spot_sym, LegKind::SpotSell.suffix()),
                });
            } else if want > Decimal::ZERO {
                plan.warnings.push(format!(
                    "{spot_sym}: спот-нога закриття ({qty}) нижча за мінімалки — лишиться пил, продай руками"
                ));
            }
        } else if want > Decimal::ZERO {
            plan.warnings.push(format!(
                "{spot_sym}: нема метаданих/ціни спота для закриття — продай руками"
            ));
        }

        let legs: Vec<PlannedLeg> = legs
            .into_iter()
            .filter(|l| !inputs.already_sent.contains(&l.client_order_id))
            .collect();
        if !legs.is_empty() {
            plan.closes.push(SymbolPlan {
                binance_symbol: binance_sym,
                htx_funding_per_8h: inputs.funding.get(code).copied(),
                legs,
            });
        }
    }

    plan
}

// ── Виконання ────────────────────────────────────────────────────────────────

/// Скільки реальних грошей потребують ВІДКРИТТЯ плану:
/// (USDT на спот-купівлі, USDT вільної крос-маржі на перп-шорти при плечі).
/// Закриття нових грошей не потребують — вони їх звільняють.
pub fn required_funds(plan: &RebalancePlan, lever_rate: u32) -> (Decimal, Decimal) {
    let mut spot = Decimal::ZERO;
    let mut margin = Decimal::ZERO;
    for s in &plan.opens {
        for l in &s.legs {
            match l.kind {
                LegKind::SpotBuy => spot += l.notional_usdt,
                LegKind::PerpOpenShort => {
                    margin += l.notional_usdt / Decimal::from(lever_rate.max(1))
                }
                _ => {}
            }
        }
    }
    (spot, margin)
}

pub struct ExecutionOutcome {
    pub placed: usize,
    pub failed: usize,
    pub lines: Vec<String>,
}

pub struct HtxCarryExecutor {
    pub client: HtxClient,
    pub cfg: LiveConfig,
    pub journal: OrderJournal,
}

impl HtxCarryExecutor {
    pub fn new(client: HtxClient, cfg: LiveConfig) -> Self {
        Self {
            client,
            cfg,
            journal: OrderJournal::new("live/orders.jsonl"),
        }
    }

    /// Збирає всі вхідні дані плану з біржі. Без ключів — публічна частина,
    /// позиції/баланси вважаються порожніми (з попередженням у плані).
    pub async fn gather_inputs(&self, entry: &LedgerBasketEntry) -> Result<(PlanInputs, Vec<String>)> {
        let mut warnings = Vec::new();
        let mut inputs = PlanInputs {
            already_sent: self.journal.sent_client_ids(),
            ..Default::default()
        };

        for c in self.client.swap_contracts().await? {
            inputs.contracts.insert(c.contract_code.clone(), c);
        }
        for f in self.client.swap_funding_rates().await? {
            if let Some(rate) = f.funding_rate.or(f.estimated_rate) {
                inputs.funding.insert(f.contract_code.clone(), rate);
            }
        }
        for m in self.client.spot_symbols().await? {
            if m.state.as_deref() != Some("online") {
                continue;
            }
            inputs.spot_meta.insert(m.sc.clone(), m);
        }

        if self.client.has_creds() {
            for p in self.client.swap_cross_positions().await? {
                if p.direction == "sell" {
                    let held = p.available.unwrap_or(p.volume);
                    *inputs.current_shorts.entry(p.contract_code.clone()).or_default() += held;
                }
            }
            let account_id = self.client.spot_account_id().await?;
            inputs.spot_balances = self.client.spot_balances(account_id).await?;
            // Реальний капітал для sizing auto: спот-USDT + вільна крос-маржа.
            let margin_free = self
                .client
                .swap_cross_account()
                .await?
                .iter()
                .filter_map(|a| a.withdraw_available)
                .fold(Decimal::ZERO, |acc, v| acc + v);
            let spot_usdt = inputs.spot_balances.get("usdt").copied().unwrap_or_default();
            inputs.available_capital = Some(spot_usdt + margin_free);
        } else {
            warnings.push(
                "БЕЗ КЛЮЧІВ: позиції й баланси невідомі — план рахує з нуля (тільки перегляд)"
                    .to_string(),
            );
        }

        // Ціни лише для потрібних інструментів: цільові + відкриті шорти.
        let mut codes: HashSet<String> = entry.symbols.iter().filter_map(|s| to_contract_code(s)).collect();
        codes.extend(inputs.current_shorts.keys().cloned());
        for code in &codes {
            if !inputs.contracts.contains_key(code) {
                continue;
            }
            match self.client.swap_best_bid_ask(code).await {
                Ok(px) => {
                    inputs.perp_prices.insert(code.clone(), px);
                }
                Err(e) => warnings.push(format!("{code}: стакан перпа недоступний ({e:#})")),
            }
            let spot_sym = to_spot_symbol(&code.replace("-", ""));
            if inputs.spot_meta.contains_key(&spot_sym) {
                match self.client.spot_best_bid_ask(&spot_sym).await {
                    Ok(px) => {
                        inputs.spot_prices.insert(spot_sym, px);
                    }
                    Err(e) => warnings.push(format!("{spot_sym}: стакан спота недоступний ({e:#})")),
                }
            }
        }

        Ok((inputs, warnings))
    }

    /// Реальна відправка ордерів плану. Обидва запобіжники перевіряються ТУТ,
    /// безпосередньо перед першим ордером.
    pub async fn execute_plan(&self, plan: &RebalancePlan) -> Result<ExecutionOutcome> {
        anyhow::ensure!(
            std::env::var("HTX_TRADING_ENABLED").as_deref() == Ok("true"),
            "HTX_TRADING_ENABLED != true — реальна відправка вимкнена (це запобіжник, план дивись через `htx-exec plan`)"
        );
        anyhow::ensure!(self.client.has_creds(), "немає ключів HTX у env");

        let account_id = self.client.spot_account_id().await?;

        // Звірка з РЕАЛЬНИМИ балансами до першого ордера: capital_usdt у
        // конфізі — обіцянка, а не факт; інакше кошик відкриється шматком
        // на відхилених «insufficient balance» ордерах.
        let (need_spot, need_margin) = required_funds(plan, self.cfg.lever_rate);
        if need_spot > Decimal::ZERO || need_margin > Decimal::ZERO {
            // Невеликий запас на комісії та рух ціни до виконання.
            let buffer = dec!(1.01);
            let spot_usdt = self
                .client
                .spot_balances(account_id)
                .await?
                .get("usdt")
                .copied()
                .unwrap_or_default();
            anyhow::ensure!(
                spot_usdt >= need_spot * buffer,
                "на спот-гаманці {spot_usdt} USDT, а купівлі плану потребують ~{:.2} — \
                 зменш capital_usdt/top_k у конфізі або поповни спот-гаманець",
                need_spot * buffer
            );
            let margin_free = self
                .client
                .swap_cross_account()
                .await?
                .iter()
                .filter_map(|a| a.withdraw_available)
                .fold(Decimal::ZERO, |acc, v| acc + v);
            anyhow::ensure!(
                margin_free >= need_margin * buffer,
                "вільна крос-маржа {margin_free} USDT, а перп-ноги потребують ~{:.2} — \
                 перекажи USDT на своп-гаманець (біржа ділить спот і своп)",
                need_margin * buffer
            );
        }

        let run_id = uuid::Uuid::new_v4().to_string();
        self.journal.append(&serde_json::json!({
            "phase": "run_start",
            "run_id": run_id,
            "ts": Utc::now(),
            "entry_id": plan.entry_id,
            "basket": plan.basket_name,
            "live_config_hash": plan.live_config_hash,
            "total_open_usdt": plan.total_open_usdt.to_string(),
        }))?;

        let mut outcome = ExecutionOutcome { placed: 0, failed: 0, lines: vec![] };
        let account_id = account_id.to_string();
        let taker = self.cfg.order_style == "taker";

        // Спершу закриття (звільняє маржу і кеш), потім відкриття.
        // Порядок ніг символа: перп → спот на закритті, спот → перп на відкритті
        // (нейтральність втрачається не більше ніж на одну ногу).
        let mut all: Vec<(&SymbolPlan, bool)> = vec![];
        all.extend(plan.closes.iter().map(|s| (s, true)));
        all.extend(plan.opens.iter().map(|s| (s, false)));

        let mut placed_legs: Vec<PlannedLeg> = vec![];
        for (symbol_plan, _closing) in all {
            for leg in &symbol_plan.legs {
                self.journal.append(&serde_json::json!({
                    "phase": "sending",
                    "run_id": run_id,
                    "ts": Utc::now(),
                    "entry_id": plan.entry_id,
                    "client_order_id": leg.client_order_id,
                    "kind": leg.kind.label(),
                    "instrument": leg.instrument,
                    "qty": leg.qty.to_string(),
                    "price": leg.price.to_string(),
                    "notional_usdt": leg.notional_usdt.to_string(),
                }))?;

                let result = self.place_leg(&account_id, leg, taker).await;
                match result {
                    Ok(exchange_id) => {
                        outcome.placed += 1;
                        outcome.lines.push(format!(
                            "✓ {} {} {} @ {} (id {exchange_id})",
                            leg.kind.label(), leg.qty, leg.instrument, leg.price
                        ));
                        self.journal.append(&serde_json::json!({
                            "phase": "placed",
                            "run_id": run_id,
                            "ts": Utc::now(),
                            "client_order_id": leg.client_order_id,
                            "exchange_order_id": exchange_id,
                        }))?;
                        placed_legs.push(leg.clone());
                    }
                    Err(e) => {
                        outcome.failed += 1;
                        outcome.lines.push(format!(
                            "✗ {} {} {}: {e:#}",
                            leg.kind.label(), leg.qty, leg.instrument
                        ));
                        self.journal.append(&serde_json::json!({
                            "phase": "error",
                            "run_id": run_id,
                            "ts": Utc::now(),
                            "client_order_id": leg.client_order_id,
                            "error": format!("{e:#}"),
                        }))?;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }

        if !placed_legs.is_empty() && self.cfg.poll_secs > 0 {
            outcome
                .lines
                .push(format!("… чекаю {} с і звіряю статуси", self.cfg.poll_secs));
            tokio::time::sleep(std::time::Duration::from_secs(self.cfg.poll_secs)).await;
            for leg in &placed_legs {
                let status = self.leg_status(leg).await;
                let line = match &status {
                    Ok(s) => format!("• {} {}: {s}", leg.instrument, leg.kind.label()),
                    Err(e) => format!("• {} {}: статус недоступний ({e:#})", leg.instrument, leg.kind.label()),
                };
                self.journal.append(&serde_json::json!({
                    "phase": "status",
                    "run_id": run_id,
                    "ts": Utc::now(),
                    "client_order_id": leg.client_order_id,
                    "status": status.as_deref().unwrap_or("недоступний"),
                }))?;
                outcome.lines.push(line);
            }
        }

        self.journal.append(&serde_json::json!({
            "phase": "run_end",
            "run_id": run_id,
            "ts": Utc::now(),
            "placed": outcome.placed,
            "failed": outcome.failed,
        }))?;
        Ok(outcome)
    }

    async fn place_leg(&self, account_id: &str, leg: &PlannedLeg, taker: bool) -> Result<String> {
        match leg.kind {
            LegKind::SpotBuy | LegKind::SpotSell => {
                let buying = leg.kind == LegKind::SpotBuy;
                // post_only на споті — окремі типи *-limit-maker.
                let order_type = match (buying, taker) {
                    (true, false) => "buy-limit-maker",
                    (false, false) => "sell-limit-maker",
                    (true, true) => "buy-limit",
                    (false, true) => "sell-limit",
                };
                self.client
                    .spot_place_order(&SpotOrderRequest {
                        account_id: account_id.to_string(),
                        symbol: leg.instrument.clone(),
                        order_type: order_type.to_string(),
                        amount: leg.qty.normalize().to_string(),
                        price: Some(leg.price.normalize().to_string()),
                        source: "spot-api".to_string(),
                        client_order_id: leg.client_order_id.clone(),
                    })
                    .await
            }
            LegKind::PerpOpenShort | LegKind::PerpCloseShort => {
                let opening = leg.kind == LegKind::PerpOpenShort;
                let ack = self
                    .client
                    .swap_place_cross_order(&SwapOrderRequest {
                        contract_code: leg.instrument.clone(),
                        price: (!taker).then(|| leg.price.normalize().to_string()),
                        volume: leg.qty.to_i64().unwrap_or(0),
                        direction: if opening { "sell" } else { "buy" }.to_string(),
                        offset: if opening { "open" } else { "close" }.to_string(),
                        lever_rate: self.cfg.lever_rate,
                        order_price_type: if taker { "opponent" } else { "post_only" }.to_string(),
                        client_order_id: Some(swap_client_id(&leg.client_order_id)),
                    })
                    .await?;
                Ok(ack.order_id_str.unwrap_or_default())
            }
        }
    }

    async fn leg_status(&self, leg: &PlannedLeg) -> Result<String> {
        match leg.kind {
            LegKind::SpotBuy | LegKind::SpotSell => {
                let info = self.client.spot_order_by_client_id(&leg.client_order_id).await?;
                Ok(format!(
                    "{} (виконано {})",
                    info.state.as_deref().unwrap_or("?"),
                    info.filled_amount.unwrap_or_default()
                ))
            }
            LegKind::PerpOpenShort | LegKind::PerpCloseShort => {
                let infos = self
                    .client
                    .swap_cross_order_info(&leg.instrument, swap_client_id(&leg.client_order_id))
                    .await?;
                let info = infos.first().context("порожня відповідь order_info")?;
                Ok(format!(
                    "{} (виконано {})",
                    info.status_label(),
                    info.trade_volume.unwrap_or_default()
                ))
            }
        }
    }
}

// ── Друк плану ────────────────────────────────────────────────────────────────

impl std::fmt::Display for RebalancePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "── ПЛАН РЕБАЛАНСУ (запис {} / кошик {}) ──", self.entry_id, self.basket_name)?;
        writeln!(f, "anchor: {}   live_config: {}", self.anchor_ts, &self.live_config_hash[..12])?;
        if let Some(note) = &self.capital_note {
            writeln!(f, "{note}")?;
        }
        if !self.closes.is_empty() {
            writeln!(f, "\nЗакрити (вибули з кошика):")?;
            for s in &self.closes {
                for l in &s.legs {
                    writeln!(f, "  {} {:>14} × {:<12} @ {} ≈ {:.2} USDT", l.kind.label(), l.qty, l.instrument, l.price, l.notional_usdt)?;
                }
            }
        }
        if !self.opens.is_empty() {
            writeln!(f, "\nВідкрити:")?;
            for s in &self.opens {
                let fr = s
                    .htx_funding_per_8h
                    .map(|r| format!("{:.4}%/8г", r * dec!(100)))
                    .unwrap_or_else(|| "?".into());
                writeln!(f, "  {} (funding HTX зараз: {fr})", s.binance_symbol)?;
                for l in &s.legs {
                    writeln!(f, "    {} {:>14} × {:<12} @ {} ≈ {:.2} USDT", l.kind.label(), l.qty, l.instrument, l.price, l.notional_usdt)?;
                }
            }
        }
        if !self.skipped.is_empty() {
            writeln!(f, "\nПропущено:")?;
            for (sym, why) in &self.skipped {
                writeln!(f, "  {sym}: {why}")?;
            }
        }
        if !self.warnings.is_empty() {
            writeln!(f, "\nПопередження:")?;
            for w in &self.warnings {
                writeln!(f, "  ⚠ {w}")?;
            }
        }
        writeln!(f, "\nСумарний нотіонал відкриттів: {:.2} USDT", self.total_open_usdt)?;
        writeln!(
            f,
            "Оцінка funding-доходу за тиждень за поточними ставками HTX: {:.2} USDT (до комісій!)",
            self.est_weekly_funding_usdt
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(code: &str, size: &str) -> SwapContractInfo {
        serde_json::from_value(serde_json::json!({
            "contract_code": code,
            "contract_size": size.parse::<f64>().unwrap(),
            "price_tick": 0.0001,
            "contract_status": 1,
            "support_margin_mode": "all",
        }))
        .unwrap()
    }

    fn spot_meta(sym: &str) -> SpotSymbolMeta {
        serde_json::from_value(serde_json::json!({
            "sc": sym, "state": "online", "tap": 2, "tpp": 4,
            "minoa": 0.1, "minov": 5,
        }))
        .unwrap()
    }

    fn entry(symbols: &[&str]) -> LedgerBasketEntry {
        LedgerBasketEntry {
            id: "test-entry-0001".into(),
            anchor_ts: "2026-07-11T08:00:00Z".into(),
            retro: false,
            basket_name: "baseline_est".into(),
            symbols: symbols.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn inputs_for(sym: &str, funding: Decimal) -> PlanInputs {
        let code = to_contract_code(sym).unwrap();
        let spot = to_spot_symbol(sym);
        let mut i = PlanInputs::default();
        i.contracts.insert(code.clone(), contract(&code, "1"));
        i.funding.insert(code.clone(), funding);
        i.spot_meta.insert(spot.clone(), spot_meta(&spot));
        i.perp_prices.insert(code, (dec!(2.00), dec!(2.01)));
        i.spot_prices.insert(spot, (dec!(1.99), dec!(2.00)));
        i
    }

    #[test]
    fn symbol_mapping() {
        assert_eq!(to_contract_code("VELVETUSDT").as_deref(), Some("VELVET-USDT"));
        assert_eq!(to_contract_code("BTC"), None);
        assert_eq!(to_spot_symbol("VELVETUSDT"), "velvetusdt");
        assert_eq!(base_currency("VELVETUSDT").as_deref(), Some("velvet"));
    }

    #[test]
    fn contracts_are_floored_integers() {
        // 50 USDT / (2 USDT × 1 шт) = 25 контрактів рівно.
        assert_eq!(contracts_for(dec!(50), dec!(2), dec!(1)), 25);
        // 50 / (3 × 1) = 16.67 → 16.
        assert_eq!(contracts_for(dec!(50), dec!(3), dec!(1)), 16);
        // Контракт дорожчий за алокацію → 0.
        assert_eq!(contracts_for(dec!(50), dec!(100000), dec!(0.001)), 0);
        assert_eq!(contracts_for(dec!(50), dec!(0), dec!(1)), 0);
    }

    #[test]
    fn swap_client_id_is_positive_and_deterministic() {
        let a = swap_client_id("dbc-test-velvetusdt-po");
        let b = swap_client_id("dbc-test-velvetusdt-po");
        assert_eq!(a, b);
        assert!(a > 0);
        assert_ne!(a, swap_client_id("dbc-test-velvetusdt-pc"));
    }

    #[test]
    fn plan_opens_both_legs_delta_neutral() {
        let e = entry(&["AAAUSDT"]);
        let i = inputs_for("AAAUSDT", dec!(0.0005));
        let cfg = LiveConfig::default();
        let plan = build_plan(&e, &i, &cfg);
        assert_eq!(plan.opens.len(), 1, "skips: {:?}", plan.skipped);
        let legs = &plan.opens[0].legs;
        assert_eq!(legs.len(), 2);
        // Ноги дзеркальні у штуках базової монети (contract_size = 1).
        assert_eq!(legs[0].qty, legs[1].qty);
        assert_eq!(legs[0].kind, LegKind::SpotBuy);
        assert_eq!(legs[1].kind, LegKind::PerpOpenShort);
        // post_only: купівля на bid, шорт на ask.
        assert_eq!(legs[0].price, dec!(1.99));
        assert_eq!(legs[1].price, dec!(2.01));
    }

    #[test]
    fn plan_skips_unlisted_and_negative_funding() {
        let e = entry(&["AAAUSDT", "BBBUSDT", "CCCUSDT"]);
        // AAA ок; BBB нема на HTX; CCC — від'ємний funding на HTX.
        let mut i = inputs_for("AAAUSDT", dec!(0.0005));
        let ccc = inputs_for("CCCUSDT", dec!(-0.0002));
        i.contracts.extend(ccc.contracts);
        i.funding.extend(ccc.funding);
        i.spot_meta.extend(ccc.spot_meta);
        i.perp_prices.extend(ccc.perp_prices);
        i.spot_prices.extend(ccc.spot_prices);
        let plan = build_plan(&e, &i, &LiveConfig::default());
        assert_eq!(plan.opens.len(), 1);
        assert!(plan.skipped.iter().any(|(s, r)| s == "BBBUSDT" && r.contains("немає")));
        assert!(plan.skipped.iter().any(|(s, r)| s == "CCCUSDT" && r.contains("funding")));
    }

    #[test]
    fn plan_respects_total_cap() {
        let e = entry(&["AAAUSDT", "CCCUSDT"]);
        let mut i = inputs_for("AAAUSDT", dec!(0.0005));
        let ccc = inputs_for("CCCUSDT", dec!(0.0005));
        i.contracts.extend(ccc.contracts);
        i.funding.extend(ccc.funding);
        i.spot_meta.extend(ccc.spot_meta);
        i.perp_prices.extend(ccc.perp_prices);
        i.spot_prices.extend(ccc.spot_prices);
        // Одна пара ніг ≈ 2×~50 USDT. Кап 150 → друга не влазить.
        let cfg = LiveConfig {
            capital_usdt: dec!(200),
            deploy_pct: dec!(0.5),
            max_total_usdt: dec!(150),
            ..LiveConfig::default()
        };
        let plan = build_plan(&e, &i, &cfg);
        assert_eq!(plan.opens.len(), 1);
        assert!(plan.skipped.iter().any(|(_, r)| r.contains("max_total_usdt")));
        assert!(plan.total_open_usdt <= cfg.max_total_usdt);
    }

    #[test]
    fn plan_keeps_existing_short_and_closes_stale() {
        let e = entry(&["AAAUSDT"]);
        let mut i = inputs_for("AAAUSDT", dec!(0.0005));
        // AAA вже в шорті — тримаємо; ZZZ у шорті, але вибув — закриваємо.
        let zzz = inputs_for("ZZZUSDT", dec!(0.0001));
        i.contracts.extend(zzz.contracts);
        i.perp_prices.extend(zzz.perp_prices);
        i.spot_prices.extend(zzz.spot_prices);
        i.spot_meta.extend(zzz.spot_meta);
        i.current_shorts.insert("AAA-USDT".into(), dec!(20));
        i.current_shorts.insert("ZZZ-USDT".into(), dec!(30));
        i.spot_balances.insert("zzz".into(), dec!(31));
        let plan = build_plan(&e, &i, &LiveConfig::default());
        assert!(plan.opens.is_empty());
        assert!(plan.skipped.iter().any(|(s, r)| s == "AAAUSDT" && r.contains("тримаємо")));
        assert_eq!(plan.closes.len(), 1);
        let close = &plan.closes[0];
        assert_eq!(close.legs[0].kind, LegKind::PerpCloseShort);
        assert_eq!(close.legs[0].qty, dec!(30));
        assert_eq!(close.legs[1].kind, LegKind::SpotSell);
        // Продаємо рівно перп-еквівалент, хоч балансу трохи більше.
        assert_eq!(close.legs[1].qty, dec!(30));
    }

    #[test]
    fn plan_is_idempotent_via_journal_ids() {
        let e = entry(&["AAAUSDT"]);
        let mut i = inputs_for("AAAUSDT", dec!(0.0005));
        i.already_sent
            .insert(spot_client_id(&e.id, "aaausdt", "sb"));
        let plan = build_plan(&e, &i, &LiveConfig::default());
        assert!(plan.opens.is_empty());
        assert!(plan.skipped.iter().any(|(_, r)| r.contains("відправлялося")));
    }

    fn auto_cfg(capital: Decimal, min_leg: Decimal) -> LiveConfig {
        LiveConfig {
            sizing_mode: "auto".into(),
            capital_usdt: capital,
            deploy_pct: dec!(0.7),
            min_leg_usdt: min_leg,
            top_k: 10,
            max_order_usdt: dec!(100),
            max_total_usdt: dec!(100000),
            ..LiveConfig::default()
        }
    }

    // Драбина: замалий → відмова; малий → мало символів з мінімальною ногою;
    // середній → повний кошик; великий → нога впирається в кап місткості.
    #[test]
    fn capital_ladder_scales_with_balance() {
        let too_small = capital_allocation(dec!(28), &auto_cfg(dec!(28), dec!(10)));
        match too_small {
            CapitalAllocation::TooSmall { min_capital_usdt } => {
                assert!(min_capital_usdt > dec!(28) && min_capital_usdt < dec!(29));
            }
            other => panic!("очікував TooSmall, отримав {other:?}"),
        }

        let CapitalAllocation::Sized(s) = capital_allocation(dec!(30), &auto_cfg(dec!(30), dec!(10)))
        else { panic!() };
        assert_eq!((s.k, s.leg_usdt), (1, dec!(10.5)));

        // Кейс Антона: $120, min_leg 13 → 3 символи × нога 14, буфер 36.
        let CapitalAllocation::Sized(s) = capital_allocation(dec!(120), &auto_cfg(dec!(120), dec!(13)))
        else { panic!() };
        assert_eq!((s.k, s.leg_usdt), (3, dec!(14)));
        assert_eq!(s.buffer_usdt, dec!(36));
        assert_eq!(s.undeployed_usdt, Decimal::ZERO);

        let CapitalAllocation::Sized(s) = capital_allocation(dec!(1000), &auto_cfg(dec!(1000), dec!(10)))
        else { panic!() };
        assert_eq!((s.k, s.leg_usdt), (10, dec!(35)));

        // Великий капітал: нога капнута max_order=100, надлишок чесно видно.
        let CapitalAllocation::Sized(s) = capital_allocation(dec!(10000), &auto_cfg(dec!(10000), dec!(10)))
        else { panic!() };
        assert_eq!((s.k, s.leg_usdt), (10, dec!(100)));
        assert_eq!(s.undeployed_usdt, dec!(5000));
    }

    // Авто-режим торгує лише перші K символів ранжування, не весь entry.
    #[test]
    fn auto_plan_trades_first_k_symbols_only() {
        let e = entry(&["AAAUSDT", "CCCUSDT"]);
        let mut i = inputs_for("AAAUSDT", dec!(0.0005));
        let ccc = inputs_for("CCCUSDT", dec!(0.0005));
        i.contracts.extend(ccc.contracts);
        i.funding.extend(ccc.funding);
        i.spot_meta.extend(ccc.spot_meta);
        i.perp_prices.extend(ccc.perp_prices);
        i.spot_prices.extend(ccc.spot_prices);
        // Капітал 40 × 0.7 = 28 у роботі → K=1 (пара ніг по 14).
        let plan = build_plan(&e, &i, &auto_cfg(dec!(40), dec!(10)));
        assert_eq!(plan.opens.len(), 1, "skips: {:?}", plan.skipped);
        assert_eq!(plan.opens[0].binance_symbol, "AAAUSDT");
        assert!(plan.capital_note.as_deref().unwrap_or("").contains("буфер"));
    }

    #[test]
    fn auto_without_capital_and_keys_refuses_honestly() {
        let e = entry(&["AAAUSDT"]);
        let i = inputs_for("AAAUSDT", dec!(0.0005));
        let plan = build_plan(&e, &i, &auto_cfg(Decimal::ZERO, dec!(10)));
        assert!(plan.is_empty());
        assert!(plan.warnings.iter().any(|w| w.contains("capital_usdt")));
    }

    #[test]
    fn journal_blocks_sending_and_placed_but_retries_errors() {
        let dir = tempfile::tempdir().unwrap();
        let j = OrderJournal::new(dir.path().join("orders.jsonl"));
        let rec = |phase: &str, id: &str| {
            serde_json::json!({"phase": phase, "client_order_id": id})
        };
        // a: краш посеред відправки (sending без розв'язки) → блокується.
        j.append(&rec("sending", "a")).unwrap();
        // b: виставлено успішно → блокується.
        j.append(&rec("sending", "b")).unwrap();
        j.append(&rec("placed", "b")).unwrap();
        // c: біржа явно відмовила → ретрай дозволено.
        j.append(&rec("sending", "c")).unwrap();
        j.append(&rec("error", "c")).unwrap();
        // d: після відмови таки виставили при перезапуску → блокується.
        j.append(&rec("sending", "d")).unwrap();
        j.append(&rec("error", "d")).unwrap();
        j.append(&rec("sending", "d")).unwrap();
        j.append(&rec("placed", "d")).unwrap();

        let sent = j.sent_client_ids();
        assert!(sent.contains("a"));
        assert!(sent.contains("b"));
        assert!(!sent.contains("c"), "явна відмова біржі має ретраїтись");
        assert!(sent.contains("d"));
    }

    #[test]
    fn required_funds_counts_only_opens() {
        let e = entry(&["AAAUSDT"]);
        let i = inputs_for("AAAUSDT", dec!(0.0005));
        let plan = build_plan(&e, &i, &LiveConfig::default());
        let (spot, margin) = required_funds(&plan, 1);
        let legs = &plan.opens[0].legs;
        assert_eq!(spot, legs[0].notional_usdt);
        assert_eq!(margin, legs[1].notional_usdt, "плече 1 → маржа = нотіонал");
        // Плече 2 вдвічі зменшує потрібну маржу, спот незмінний.
        let (spot2, margin2) = required_funds(&plan, 2);
        assert_eq!(spot2, spot);
        assert_eq!(margin2, margin / dec!(2));
    }

    #[test]
    fn last_real_entry_skips_retro() {
        let jsonl = r#"{"id":"a","anchor_ts":"t1","retro":false,"baskets":{"baseline_est":[{"symbol":"AUSDT"}]}}
{"id":"b","anchor_ts":"t2","retro":true,"baskets":{"baseline_est":[{"symbol":"BUSDT"}]}}"#;
        let e = last_real_entry(jsonl, "baseline_est").unwrap();
        assert_eq!(e.id, "a");
    }

    #[test]
    fn taker_style_crosses_spread() {
        let e = entry(&["AAAUSDT"]);
        let i = inputs_for("AAAUSDT", dec!(0.0005));
        let cfg = LiveConfig {
            order_style: "taker".into(),
            ..LiveConfig::default()
        };
        let plan = build_plan(&e, &i, &cfg);
        let legs = &plan.opens[0].legs;
        // Тейкер: купівля по ask, продаж/шорт по bid.
        assert_eq!(legs[0].price, dec!(2.00));
        assert_eq!(legs[1].price, dec!(2.00));
    }
}
