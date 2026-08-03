//! Тіньовий трейдинг carry-кошика (ex-ante, git-нотаризований).

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;

use super::funding_ml::{
    build_funding_samples, funding_feature_keys_ext, load_meta_dir, MetaRow,
    FUNDING_FEATURE_SCALE, FUNDING_HOLD, FUNDING_PAST, META_WINDOW,
};
use super::QuantArgs;
use crate::data_ingestion::adapters::funding_csv::CsvFundingAdapter;
use crate::data_ingestion::ports::funding::FundingDataPort;

// ── Тіньовий трейдинг carry-кошика (ex-ante, git-нотаризований) ──────────────
//
// Щотижня: обираємо кошик СЬОГОДНІ (тільки з даних ≤ сьогодні), пишемо в
// append-only журнал shadow/ledger.jsonl і комітимо — git-таймстемп доводить,
// що вибір зроблено ДО того, як тиждень відбувся. Наступні запуски оцінюють
// старі записи: спершу попередньо (за premium-оцінкою фандингу — свіжа
// щодня), потім фінально (за settled funding з місячного архіву).
// Ведемо ДВА кошики паралельно — A/B тест RF проти базлайну наживо.

/// Оцінка funding-ставки з premium-бару (формула Binance зі ставкою 0.01%/8h).
fn funding_estimate_from_premium(premium: Decimal) -> Decimal {
    let interest = dec!(0.0001);
    let clamp_component = (interest - premium).clamp(dec!(-0.0005), dec!(0.0005));
    premium + clamp_component
}

/// Кошики одного запису журналу: назва кошика → символи.
type LedgerBaskets = BTreeMap<String, BTreeSet<String>>;
/// Записи журналу за часом: (retro, якір, кошики).
type BasketHistory = Vec<(bool, DateTime<Utc>, LedgerBaskets)>;

/// Тижнів у році — база аннуалізації витрат. Тиждень запису рівно 7 днів
/// (якір → якір+7д), тож 365/7, а не 52.
fn weeks_per_year() -> Decimal {
    dec!(365) / dec!(7)
}

/// Скільки нотіоналу треба проторгувати, щоб із кошика `prev` дістати `cur`.
/// Одиниця виміру — частка задіяного капіталу (1.0 = увесь капітал раз).
///
/// Кошики сусідніх тижнів перетинаються, і символ, що лишився в топі, ніхто
/// не перевідкриває — його просто тримають далі. Тому витрати рахуються на
/// ОБОРОТ, а не на весь кошик щотижня. Це не дрібниця: виміряно на журналі,
/// оборот baseline_est 20–70%, а rf 0–40%, тож однакова ставка «100% щотижня»
/// зробила б A/B несправедливим — карала б кошики за розмір, а не за якість.
///
/// Кожен символ має ДВІ ноги (спот + перп), тож і вихід, і вхід коштують по
/// два перетини спреду на символ. Перший запис (немає попереднього) платить
/// лише за відкриття.
fn traded_notional(cur: &BTreeSet<String>, prev: Option<&BTreeSet<String>>) -> Decimal {
    const LEGS: Decimal = Decimal::TWO;
    if cur.is_empty() {
        return Decimal::ZERO;
    }
    let cur_n = Decimal::from(cur.len() as u64);
    let Some(prev) = prev else {
        // Перший тиждень: відкрити ввесь кошик, обидві ноги.
        return LEGS;
    };
    let entered = Decimal::from(cur.difference(prev).count() as u64) / cur_n;
    let exited = if prev.is_empty() {
        Decimal::ZERO
    } else {
        Decimal::from(prev.difference(cur).count() as u64) / Decimal::from(prev.len() as u64)
    };
    (entered + exited) * LEGS
}

/// Частка кошика, що змінилася відносно попереднього тижня (для звіту).
fn turnover(cur: &BTreeSet<String>, prev: Option<&BTreeSet<String>>) -> Decimal {
    if cur.is_empty() {
        return Decimal::ZERO;
    }
    match prev {
        None => Decimal::ONE,
        Some(p) => {
            Decimal::from(cur.difference(p).count() as u64) / Decimal::from(cur.len() as u64)
        }
    }
}

pub async fn run_shadow_carry(args: &QuantArgs) -> Result<()> {
    use crate::features::domain::models::{FeatureSet, FeatureValue};
    use crate::model::domain::models::PredictionModel;
    use crate::model::domain::random_forest_like::RandomForestLikeModel;
    use crate::trading::domain::events::SignalDirection;
    use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
    use std::io::Write;

    let dir = args
        .funding_dir
        .clone()
        .context("--funding-dir required for shadow_carry")?;
    let meta_dir = dir
        .parent()
        .map(|p| p.join("perp_meta"))
        .unwrap_or_else(|| PathBuf::from("datasets/perp_meta"));
    let meta_by_symbol = load_meta_dir(&meta_dir)?;
    anyhow::ensure!(!meta_by_symbol.is_empty(), "no meta data");

    // Якір «зараз»: останній спільний perp_meta-таймстемп (учора, свіже).
    // SHADOW_ANCHOR_DAYS_AGO зсуває якір у минуле (для тестів; retro=true).
    let retro_days: i64 = env::var("SHADOW_ANCHOR_DAYS_AGO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let anchor: DateTime<Utc> = {
        let latest = meta_by_symbol
            .values()
            .filter_map(|s| s.keys().next_back())
            .max()
            .copied()
            .context("empty meta")?;
        latest - chrono::Duration::days(retro_days)
    };
    let retro = retro_days > 0;

    // 1. Оцінка старих записів журналу.
    //
    // SHADOW_DIR зсуває журнал у інше місце. Потрібен, щоб зміни в оцінці
    // можна було перевірити НЕ чіпаючи бойовий журнал: він append-only і
    // git-нотаризований, тобто пробний прогін у ньому — це підробка запису.
    // Дефолт лишається `shadow/`, тож автоматика працює як була.
    let shadow_dir = env::var("SHADOW_DIR").unwrap_or_else(|_| "shadow".to_string());
    std::fs::create_dir_all(&shadow_dir)?;
    let ledger_path = format!("{shadow_dir}/ledger.jsonl");
    let results_path = format!("{shadow_dir}/results.jsonl");
    let (ledger_path, results_path) = (ledger_path.as_str(), results_path.as_str());
    let ledger: Vec<serde_json::Value> = std::fs::read_to_string(ledger_path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let results: Vec<serde_json::Value> = std::fs::read_to_string(results_path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let has_result = |id: &str, kind: &str| {
        results
            .iter()
            .any(|r| r["entry_id"] == id && r["kind"] == kind)
    };

    // Settled funding: BTreeMap<sym, BTreeMap<ts, rate>> для фінальної оцінки.
    let mut settled: BTreeMap<String, BTreeMap<DateTime<Utc>, Decimal>> = BTreeMap::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_uppercase();
        let adapter = CsvFundingAdapter::new(&path);
        let series = adapter
            .funding_history(&sym, Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(), Utc::now())
            .await?;
        settled.insert(sym, series.iter().map(|p| (p.timestamp, p.rate)).collect());
    }

    let annualize = |mean_per_8h: Decimal| (mean_per_8h * dec!(1095) * dec!(100)).round_dp(2);

    // Кошики журналу за часом — щоб знайти кошик ПОПЕРЕДНЬОГО тижня і взяти
    // витрати з обороту, а не з повного перевідкриття. Retro-записи живуть
    // окремою послідовністю: у них зсунутий якір, і мішати їх зі справжніми
    // означало б рахувати оборот між тижнями, що не йшли один за одним.
    let mut history: BasketHistory = ledger
        .iter()
        .filter_map(|e| {
            let anchor = e["anchor_ts"].as_str()?.parse::<DateTime<Utc>>().ok()?;
            let retro = e["retro"].as_bool().unwrap_or(false);
            let baskets = ["baseline_est", "rf"]
                .iter()
                .filter_map(|name| {
                    let syms: BTreeSet<String> = e["baskets"][name]
                        .as_array()?
                        .iter()
                        .filter_map(|s| s["symbol"].as_str().map(str::to_string))
                        .collect();
                    Some((name.to_string(), syms))
                })
                .collect();
            Some((retro, anchor, baskets))
        })
        .collect();
    history.sort_by_key(|(retro, anchor, _)| (*retro, *anchor));

    let previous_baskets = |retro: bool, anchor: DateTime<Utc>| {
        history
            .iter()
            .rfind(|(r, a, _)| *r == retro && *a < anchor)
            .map(|(_, _, b)| b)
    };

    let scenarios = super::cost_scenarios();
    let mut new_results = Vec::new();
    for e in &ledger {
        let id = e["id"].as_str().unwrap_or_default().to_string();
        let Some(entry_anchor) = e["anchor_ts"]
            .as_str()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
        else {
            continue;
        };
        let week_end = entry_anchor + chrono::Duration::days(7);
        for (kind, source_fresh) in [("provisional", true), ("final", false)] {
            if has_result(&id, kind) {
                continue;
            }
            // provisional: тиждень минув за meta-даними; final: settled funding
            // покриває тиждень.
            let coverage_ok = if source_fresh {
                anchor >= week_end
            } else {
                settled.values().any(|s| {
                    s.keys().next_back().map(|t| *t >= week_end).unwrap_or(false)
                })
            };
            if !coverage_ok {
                continue;
            }
            let entry_retro = e["retro"].as_bool().unwrap_or(false);
            let prev = previous_baskets(entry_retro, entry_anchor);
            let mut per_basket = serde_json::Map::new();
            let mut turnovers = serde_json::Map::new();
            let mut nets: BTreeMap<&str, serde_json::Map<String, serde_json::Value>> =
                scenarios.iter().map(|s| (s.key, Default::default())).collect();
            let mut cost_pcts: BTreeMap<&str, serde_json::Map<String, serde_json::Value>> =
                scenarios.iter().map(|s| (s.key, Default::default())).collect();
            for basket_name in ["baseline_est", "rf"] {
                let Some(symbols) = e["baskets"][basket_name].as_array() else { continue };
                let mut vals = Vec::new();
                for s in symbols {
                    let sym = s["symbol"].as_str().unwrap_or_default();
                    let realized: Option<Decimal> = if source_fresh {
                        meta_by_symbol.get(sym).and_then(|m| {
                            let window: Vec<Decimal> = m
                                .range((
                                    std::ops::Bound::Excluded(entry_anchor),
                                    std::ops::Bound::Included(week_end),
                                ))
                                .map(|(_, r)| funding_estimate_from_premium(r.premium))
                                .collect();
                            if window.is_empty() {
                                None
                            } else {
                                Some(window.iter().copied().sum::<Decimal>()
                                    / Decimal::from(window.len() as u64))
                            }
                        })
                    } else {
                        settled.get(sym).and_then(|m| {
                            let window: Vec<Decimal> = m
                                .range((
                                    std::ops::Bound::Excluded(entry_anchor),
                                    std::ops::Bound::Included(week_end),
                                ))
                                .map(|(_, r)| *r)
                                .collect();
                            if window.is_empty() {
                                None
                            } else {
                                Some(window.iter().copied().sum::<Decimal>()
                                    / Decimal::from(window.len() as u64))
                            }
                        })
                    };
                    if let Some(r) = realized {
                        vals.push(r);
                    }
                }
                if !vals.is_empty() {
                    let mean = vals.iter().copied().sum::<Decimal>()
                        / Decimal::from(vals.len() as u64);
                    let gross = annualize(mean);
                    per_basket.insert(
                        basket_name.to_string(),
                        serde_json::json!(gross.to_string()),
                    );

                    // Витрати: оборот кошика × дві ноги × ставка сценарію,
                    // аннуалізовано з тижня. Дає ту саму величину, що net у
                    // xs_carry, — сітки витрат спільні (super::cost_scenarios).
                    let cur: BTreeSet<String> = symbols
                        .iter()
                        .filter_map(|s| s["symbol"].as_str().map(str::to_string))
                        .collect();
                    let prev_basket = prev.and_then(|b| b.get(basket_name));
                    turnovers.insert(
                        basket_name.to_string(),
                        serde_json::json!(turnover(&cur, prev_basket).round_dp(4).to_string()),
                    );
                    let traded = traded_notional(&cur, prev_basket);
                    for sc in &scenarios {
                        let cost =
                            (traded * sc.per_notional() * weeks_per_year() * dec!(100)).round_dp(2);
                        cost_pcts
                            .get_mut(sc.key)
                            .expect("scenario key inserted above")
                            .insert(basket_name.to_string(), serde_json::json!(cost.to_string()));
                        nets.get_mut(sc.key)
                            .expect("scenario key inserted above")
                            .insert(
                                basket_name.to_string(),
                                serde_json::json!((gross - cost).to_string()),
                            );
                    }
                }
            }
            if !per_basket.is_empty() {
                new_results.push(serde_json::json!({
                    "entry_id": id,
                    "kind": kind,
                    "week": [entry_anchor, week_end],
                    // Історична назва — це ВАЛОВЕ число (фандинг без витрат).
                    // Лишається як є, щоб старі записи журналу не стали
                    // незіставними; критерій рішення v2 читає net_annualized_pct.
                    "realized_annualized_pct": per_basket,
                    "turnover": turnovers,
                    "net_annualized_pct": nets,
                    "cost_annualized_pct": cost_pcts,
                    "evaluated_at": Utc::now(),
                }));
            }
        }
    }
    if !new_results.is_empty() {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(results_path)?;
        for r in &new_results {
            writeln!(f, "{}", serde_json::to_string(r)?)?;
            println!("ОЦІНЕНО: {}", serde_json::to_string(r)?);
        }
    }

    // 2. Новий запис: два кошики станом на якір.
    //    baseline_est: трейлінг premium-оцінка funding (свіжа);
    //    rf: модель, навчена на ВСІЙ доступній історії ≤ якоря.
    let mut universe: BTreeMap<String, Vec<crate::data_ingestion::domain::funding::FundingRatePoint>> =
        BTreeMap::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_uppercase();
        let adapter = CsvFundingAdapter::new(&path);
        let series = adapter
            .funding_history(&sym, Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(), anchor)
            .await?;
        if series.len() > FUNDING_PAST + FUNDING_HOLD {
            universe.insert(sym, series);
        }
    }
    // Кошик тіньового журналу — ex-ante заявка на угоду, тож у ньому не має
    // бути перпів, які нічим хеджувати.
    super::universe::apply_spot_filter(&mut universe, &args.config.universe, "shadow_carry")?;

    let rf = RandomForestLikeModel::new_with_params_and_seed(
        funding_feature_keys_ext(),
        200_000,
        1_000,
        4_000,
        100,
        8,
        0.0,
        args.config.ensemble.seed,
    );
    let mut n_train = 0usize;
    for (sym, series) in &universe {
        let meta = meta_by_symbol.get(sym);
        for s in build_funding_samples(sym, series, FUNDING_PAST, FUNDING_HOLD, meta) {
            if s.timestamp > anchor {
                continue;
            }
            let y = (s.label * FUNDING_FEATURE_SCALE).clamp(dec!(-1), dec!(1));
            rf.learn(&s.features, y).await?;
            n_train += 1;
        }
    }

    let signed = |p: &crate::model::domain::models::Prediction| -> Decimal {
        let sign = match p.direction {
            SignalDirection::Long => Decimal::ONE,
            SignalDirection::Short => Decimal::NEGATIVE_ONE,
            SignalDirection::Exit => Decimal::ZERO,
        };
        sign * p.confidence / FUNDING_FEATURE_SCALE
    };

    // Скоринг «зараз»: f_* з останніх settled-точок, prem_*/потік — зі свіжих
    // meta-барів ≤ якоря (найкраще доступне на момент рішення, без майбутнього).
    let mut baseline_scores: Vec<(String, Decimal)> = Vec::new();
    let mut rf_scores: Vec<(String, Decimal)> = Vec::new();
    for (sym, series) in &universe {
        let Some(meta) = meta_by_symbol.get(sym) else { continue };
        let fresh: Vec<&MetaRow> = meta
            .range(..=anchor)
            .rev()
            .take(META_WINDOW)
            .map(|(_, r)| r)
            .collect();
        if fresh.len() < META_WINDOW {
            continue;
        }
        let est_mean = fresh
            .iter()
            .map(|r| funding_estimate_from_premium(r.premium))
            .sum::<Decimal>()
            / Decimal::from(META_WINDOW as u64);
        baseline_scores.push((sym.clone(), est_mean));

        let tail = &series[series.len().saturating_sub(FUNDING_PAST)..];
        let Some(tail_last) = tail.last() else {
            continue;
        };
        if tail.len() < FUNDING_PAST {
            continue;
        }
        let mean_last = |k: usize| -> Decimal {
            tail[tail.len() - k..].iter().map(|p| p.rate).sum::<Decimal>()
                / Decimal::from(k as u64)
        };
        let mean21 = mean_last(21);
        let mean63 = mean_last(63.min(tail.len()));
        let var21: Decimal = tail[tail.len() - 21..]
            .iter()
            .map(|p| (p.rate - mean21) * (p.rate - mean21))
            .sum::<Decimal>()
            / dec!(21);
        let std21 = Decimal::from_f64(var21.to_f64().unwrap_or(0.0).max(0.0).sqrt())
            .unwrap_or(Decimal::ZERO);
        let slope = tail[tail.len() - 7..].iter().map(|p| p.rate).sum::<Decimal>() / dec!(7)
            - tail[tail.len() - 21..tail.len() - 7]
                .iter()
                .map(|p| p.rate)
                .sum::<Decimal>()
                / dec!(14);
        let prem_mean = fresh.iter().map(|r| r.premium).sum::<Decimal>()
            / Decimal::from(META_WINDOW as u64);
        let prem_last = fresh[0].premium;
        let prem_slope = fresh[..7].iter().map(|r| r.premium).sum::<Decimal>() / dec!(7)
            - fresh[7..].iter().map(|r| r.premium).sum::<Decimal>()
                / Decimal::from((META_WINDOW - 7) as u64);
        let vol_sum: Decimal = fresh.iter().map(|r| r.volume).sum();
        let buy_sum: Decimal = fresh.iter().map(|r| r.taker_buy_volume).sum();
        let taker_ratio = if vol_sum > Decimal::ZERO {
            buy_sum / vol_sum
        } else {
            dec!(0.5)
        };
        let s = FUNDING_FEATURE_SCALE;
        let mut fs = FeatureSet::new(sym.clone());
        fs.insert("f_mean21".into(), FeatureValue::Scalar(mean21 * s));
        fs.insert("f_mean63".into(), FeatureValue::Scalar(mean63 * s));
        fs.insert("f_std21".into(), FeatureValue::Scalar(std21 * s));
        fs.insert(
            "f_last".into(),
            FeatureValue::Scalar(tail_last.rate * s),
        );
        fs.insert("f_slope".into(), FeatureValue::Scalar(slope * s));
        fs.insert("p_ret21".into(), FeatureValue::Scalar(Decimal::ZERO));
        fs.insert("p_vol21".into(), FeatureValue::Scalar(Decimal::ZERO));
        fs.insert("prem_mean21".into(), FeatureValue::Scalar(prem_mean * s));
        fs.insert("prem_last".into(), FeatureValue::Scalar(prem_last * s));
        fs.insert("prem_slope".into(), FeatureValue::Scalar(prem_slope * s));
        fs.insert(
            "taker_ratio21".into(),
            FeatureValue::Scalar((taker_ratio - dec!(0.5)) * dec!(10)),
        );
        fs.insert("vol_z21".into(), FeatureValue::Scalar(Decimal::ZERO));
        rf_scores.push((sym.clone(), signed(&rf.predict(&fs).await?)));
    }

    let top = |mut v: Vec<(String, Decimal)>| -> Vec<serde_json::Value> {
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.into_iter()
            .take(args.top_k)
            .map(|(sym, score)| {
                serde_json::json!({"symbol": sym, "score_per_8h": score.to_string()})
            })
            .collect()
    };
    let entry = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "created_at": Utc::now(),
        "anchor_ts": anchor,
        "retro": retro,
        "top_k": args.top_k,
        "train_samples": n_train,
        "config_hash": args.config.config_hash(),
        "baskets": {
            "baseline_est": top(baseline_scores),
            "rf": top(rf_scores),
        },
    });
    // Дубль-захист: не пишемо другий запис з тим самим якорем.
    let already = ledger.iter().any(|e| e["anchor_ts"] == entry["anchor_ts"]);
    if already {
        println!("Запис із якорем {anchor} вже є — пропускаю.");
    } else {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(ledger_path)?;
        writeln!(f, "{}", serde_json::to_string(&entry)?)?;
        println!("НОВИЙ ТІНЬОВИЙ ЗАПИС (anchor {anchor}, retro={retro}):");
        println!("{}", serde_json::to_string_pretty(&entry["baskets"])?);
    }
    println!(
        "Журнал: {} записів, {} оцінок (+{} нових).",
        ledger.len() + if already { 0 } else { 1 },
        results.len() + new_results.len(),
        new_results.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(syms: &[&str]) -> BTreeSet<String> {
        syms.iter().map(|s| s.to_string()).collect()
    }

    // Перший тиждень платить лише за відкриття: закривати ще нічого.
    #[test]
    fn first_week_pays_only_for_opening() {
        assert_eq!(traded_notional(&set(&["A", "B"]), None), dec!(2));
        assert_eq!(turnover(&set(&["A", "B"]), None), Decimal::ONE);
    }

    // Кошик не змінився — його НЕ перевідкривають, витрат нема. Це головне,
    // заради чого витрати рахуються на оборот: щотижневе перевідкриття
    // з'їдало б фандинг там, де жодної угоди не відбулось.
    #[test]
    fn unchanged_basket_costs_nothing() {
        let b = set(&["A", "B", "C"]);
        assert_eq!(traded_notional(&b, Some(&b)), Decimal::ZERO);
        assert_eq!(turnover(&b, Some(&b)), Decimal::ZERO);
    }

    // Один із двох символів замінено: вийшов 1/2, зайшов 1/2, по дві ноги
    // кожен → (0.5 + 0.5) × 2 = 2.0 нотіоналу.
    #[test]
    fn half_the_basket_replaced() {
        let prev = set(&["A", "B"]);
        let cur = set(&["A", "C"]);
        assert_eq!(traded_notional(&cur, Some(&prev)), dec!(2));
        assert_eq!(turnover(&cur, Some(&prev)), dec!(0.5));
    }

    // Повна заміна коштує вдвічі більше за половинну — витрати мусять
    // РЕАГУВАТИ на оборот, інакше A/B карав би кошики однаково незалежно
    // від того, як часто вони перетасовуються (виміряно на журналі:
    // baseline_est 20–70%, rf 0–40%).
    #[test]
    fn cost_scales_with_turnover() {
        let prev = set(&["A", "B"]);
        let half = traded_notional(&set(&["A", "C"]), Some(&prev));
        let full = traded_notional(&set(&["C", "D"]), Some(&prev));
        assert_eq!(full, half * dec!(2));
    }

    // Ставка сценарію = комісія + ПІВспреду (сторона платить свою половину).
    #[test]
    fn scenario_charges_half_spread() {
        let sc = super::super::cost_scenarios();
        let taker = sc.iter().find(|s| s.key == "taker").unwrap();
        assert_eq!(taker.per_notional(), dec!(0.00045) + dec!(0.0001));
        let maker = sc.iter().find(|s| s.key == "maker").unwrap();
        assert_eq!(maker.per_notional(), dec!(0.00018));
    }

    // Наскрізна перевірка величини: повна заміна кошика щотижня на мейкері.
    // 4 × 0.00018 × (365/7) × 100 ≈ 3.75%/рік — більше за валовий фандинг
    // хеджованого юніверсу (~1.6–1.8%), тобто оборот тут не дрібниця.
    #[test]
    fn full_weekly_turnover_costs_more_than_the_edge() {
        let traded = traded_notional(&set(&["C", "D"]), Some(&set(&["A", "B"])));
        let maker = super::super::cost_scenarios()
            .into_iter()
            .find(|s| s.key == "maker")
            .unwrap();
        let annual = traded * maker.per_notional() * weeks_per_year() * dec!(100);
        assert!(
            annual > dec!(3.7) && annual < dec!(3.8),
            "очікували ≈3.75%/рік, отримали {annual}"
        );
    }
}
