//! Тіньовий трейдинг carry-кошика (ex-ante, git-нотаризований).

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;
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
    std::fs::create_dir_all("shadow")?;
    let ledger_path = "shadow/ledger.jsonl";
    let results_path = "shadow/results.jsonl";
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
            let mut per_basket = serde_json::Map::new();
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
                    per_basket.insert(
                        basket_name.to_string(),
                        serde_json::json!(annualize(mean).to_string()),
                    );
                }
            }
            if !per_basket.is_empty() {
                new_results.push(serde_json::json!({
                    "entry_id": id,
                    "kind": kind,
                    "week": [entry_anchor, week_end],
                    "realized_annualized_pct": per_basket,
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
