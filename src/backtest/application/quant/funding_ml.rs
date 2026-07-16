//! Funding-ML: чи передбачає модель стійкість фандингу краще за наївний
//! базлайн «наступний тиждень ≈ минулий»? Датасет + оцінка якості + тести.

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{log_run, QuantArgs};
use crate::data_ingestion::adapters::funding_csv::CsvFundingAdapter;
use crate::data_ingestion::ports::funding::FundingDataPort;

// ── Funding-ML: чи передбачає модель стійкість фандингу краще за наївне
//    «наступний тиждень ≈ минулий»? (роль №1 для ML у цій системі) ──────────

/// Один навчальний приклад: фічі з минулого, лейбл — середній funding за
/// НАСТУПНІ `hold` інтервалів. Перетину минуле/майбутнє немає за побудовою.
/// (rf_ext_pred, linear_base_pred, linear_ext_pred, baseline, label, sample)
type EvalRow<'a> = (Decimal, Decimal, Decimal, Decimal, Decimal, &'a FundingSample);

pub(crate) struct FundingSample {
    pub(crate) symbol: String,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) features: crate::features::domain::models::FeatureSet,
    /// Наївний базлайн: середній funding за останні 21 інтервал (частки).
    pub(crate) baseline: Decimal,
    /// Ціль: середній funding за наступні `hold` інтервалів (частки).
    pub(crate) label: Decimal,
}

/// Мета-дані перпа з 8h-барів Binance: базис і потік агресії.
/// (OI/long-short історію біржа не віддає — лише 30 днів; це найближчі
/// повноісторичні замінники.)
#[derive(Debug, Clone)]
pub(crate) struct MetaRow {
    /// Premium index close (частка): перп проти індексу — попередник фандингу.
    pub(crate) premium: Decimal,
    /// Обсяг бару (штук).
    pub(crate) volume: Decimal,
    /// Частина обсягу, ініційована агресивними покупцями.
    pub(crate) taker_buy_volume: Decimal,
}

pub(crate) const FUNDING_FEATURE_SCALE: Decimal = dec!(1000); // 0.0001 → 0.1
pub(crate) const FUNDING_PAST: usize = 63;
pub(crate) const FUNDING_HOLD: usize = 21;
/// Вікно мета-фіч у 8h-барах.
pub(crate) const META_WINDOW: usize = 21;

pub(crate) fn funding_feature_keys() -> Vec<String> {
    vec![
        "f_mean21".into(),
        "f_mean63".into(),
        "f_std21".into(),
        "f_last".into(),
        "f_slope".into(),
        "p_ret21".into(),
        "p_vol21".into(),
    ]
}

/// Розширений набір: базові + базис/потік (роль OI-замінників).
pub(crate) fn funding_feature_keys_ext() -> Vec<String> {
    let mut keys = funding_feature_keys();
    keys.extend([
        "prem_mean21".to_string(),
        "prem_last".to_string(),
        "prem_slope".to_string(),
        "taker_ratio21".to_string(),
        "vol_z21".to_string(),
    ]);
    keys
}

pub(crate) fn load_meta_dir(dir: &Path) -> Result<BTreeMap<String, BTreeMap<DateTime<Utc>, MetaRow>>> {
    let mut out: BTreeMap<String, BTreeMap<DateTime<Utc>, MetaRow>> = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        let content = std::fs::read_to_string(&path)?;
        let mut series = BTreeMap::new();
        for (i, line) in content.lines().enumerate() {
            if i == 0 {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 5 {
                continue;
            }
            let (Ok(ts), Ok(premium), Ok(volume), Ok(taker)) = (
                parts[0].parse::<DateTime<Utc>>(),
                parts[1].parse::<Decimal>(),
                parts[3].parse::<Decimal>(),
                parts[4].parse::<Decimal>(),
            ) else {
                continue;
            };
            series.insert(
                ts,
                MetaRow {
                    premium,
                    volume,
                    taker_buy_volume: taker,
                },
            );
        }
        if series.len() > META_WINDOW {
            out.insert(sym, series);
        }
    }
    Ok(out)
}

pub(crate) fn build_funding_samples(
    symbol: &str,
    series: &[crate::data_ingestion::domain::funding::FundingRatePoint],
    past: usize,
    hold: usize,
    meta: Option<&BTreeMap<DateTime<Utc>, MetaRow>>,
) -> Vec<FundingSample> {
    use crate::features::domain::models::{FeatureSet, FeatureValue};
    use rust_decimal::prelude::FromPrimitive;

    let n = series.len();
    if n < past + hold + 1 {
        return Vec::new();
    }
    let mean = |from: usize, to_incl: usize| -> Decimal {
        let cnt = to_incl + 1 - from;
        series[from..=to_incl]
            .iter()
            .map(|p| p.rate)
            .sum::<Decimal>()
            / Decimal::from(cnt as u64)
    };

    let mut out = Vec::new();
    for t in past..(n - hold) {
        // Фічі — ТІЛЬКИ з ≤ t; лейбл — ТІЛЬКИ з (t, t+hold].
        let mean21 = mean(t - 20, t);
        let mean63 = mean(t - 62, t);
        let var21: Decimal = series[t - 20..=t]
            .iter()
            .map(|p| (p.rate - mean21) * (p.rate - mean21))
            .sum::<Decimal>()
            / dec!(21);
        let std21 = Decimal::from_f64(
            var21
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0)
                .max(0.0)
                .sqrt(),
        )
        .unwrap_or(Decimal::ZERO);
        let slope = mean(t - 6, t) - mean(t - 20, t - 7);
        let p_now = series[t].mark_price;
        let p_then = series[t - 20].mark_price;
        let ret21 = if p_then > Decimal::ZERO {
            (p_now - p_then) / p_then
        } else {
            Decimal::ZERO
        };
        let mut rets = Vec::with_capacity(20);
        for i in (t - 19)..=t {
            let a = series[i - 1].mark_price;
            let b = series[i].mark_price;
            if a > Decimal::ZERO {
                rets.push(((b - a) / a).to_string().parse::<f64>().unwrap_or(0.0));
            }
        }
        let vol21 = if rets.len() > 1 {
            let m = rets.iter().sum::<f64>() / rets.len() as f64;
            let v = rets.iter().map(|r| (r - m).powi(2)).sum::<f64>() / rets.len() as f64;
            Decimal::from_f64(v.sqrt()).unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };

        let mut fs = FeatureSet::new(symbol.to_string());
        let s = FUNDING_FEATURE_SCALE;
        fs.insert("f_mean21".into(), FeatureValue::Scalar(mean21 * s));
        fs.insert("f_mean63".into(), FeatureValue::Scalar(mean63 * s));
        fs.insert("f_std21".into(), FeatureValue::Scalar(std21 * s));
        fs.insert("f_last".into(), FeatureValue::Scalar(series[t].rate * s));
        fs.insert("f_slope".into(), FeatureValue::Scalar(slope * s));
        fs.insert("p_ret21".into(), FeatureValue::Scalar(ret21));
        fs.insert("p_vol21".into(), FeatureValue::Scalar(vol21 * dec!(10)));

        // Мета-фічі: базис (premium) і потік агресії за останні META_WINDOW
        // 8h-барів СТРОГО ≤ t. Без повного вікна семпл пропускається,
        // щоб порівняння моделей ішло на ідентичних рядках.
        if let Some(meta_series) = meta {
            let window: Vec<&MetaRow> = meta_series
                .range(..=series[t].timestamp)
                .rev()
                .take(META_WINDOW)
                .map(|(_, r)| r)
                .collect();
            if window.len() < META_WINDOW {
                continue;
            }
            let m = Decimal::from(META_WINDOW as u64);
            let prem_mean: Decimal =
                window.iter().map(|r| r.premium).sum::<Decimal>() / m;
            let prem_last = window[0].premium; // rev(): [0] — найсвіжіший
            let prem_recent: Decimal =
                window[..7].iter().map(|r| r.premium).sum::<Decimal>() / dec!(7);
            let prem_older: Decimal =
                window[7..].iter().map(|r| r.premium).sum::<Decimal>()
                    / Decimal::from((META_WINDOW - 7) as u64);
            let taker_ratio: Decimal = {
                let vol_sum: Decimal = window.iter().map(|r| r.volume).sum();
                let buy_sum: Decimal = window.iter().map(|r| r.taker_buy_volume).sum();
                if vol_sum > Decimal::ZERO {
                    buy_sum / vol_sum
                } else {
                    dec!(0.5)
                }
            };
            let vol_z = {
                let vols: Vec<f64> = window
                    .iter()
                    .map(|r| r.volume.to_string().parse::<f64>().unwrap_or(0.0))
                    .collect();
                let mu = vols.iter().sum::<f64>() / vols.len() as f64;
                let sd = (vols.iter().map(|v| (v - mu).powi(2)).sum::<f64>()
                    / vols.len() as f64)
                    .sqrt();
                if sd > 0.0 {
                    ((vols[0] - mu) / sd).clamp(-3.0, 3.0) / 3.0
                } else {
                    0.0
                }
            };
            fs.insert("prem_mean21".into(), FeatureValue::Scalar(prem_mean * s));
            fs.insert("prem_last".into(), FeatureValue::Scalar(prem_last * s));
            fs.insert(
                "prem_slope".into(),
                FeatureValue::Scalar((prem_recent - prem_older) * s),
            );
            fs.insert(
                "taker_ratio21".into(),
                FeatureValue::Scalar((taker_ratio - dec!(0.5)) * dec!(10)),
            );
            fs.insert(
                "vol_z21".into(),
                FeatureValue::Scalar(Decimal::from_f64(vol_z).unwrap_or(Decimal::ZERO)),
            );
        }

        out.push(FundingSample {
            symbol: symbol.to_string(),
            timestamp: series[t].timestamp,
            features: fs,
            baseline: mean21,
            label: mean(t + 1, t + hold),
        });
    }
    out
}

pub async fn run_funding_ml(args: &QuantArgs) -> Result<()> {
    use crate::model::domain::adaptive_linear::AdaptiveLinearModel;
    use crate::model::domain::models::PredictionModel;
    use crate::model::domain::random_forest_like::RandomForestLikeModel;
    use crate::trading::domain::events::SignalDirection;
    use rust_decimal::prelude::ToPrimitive;

    let dir = args
        .funding_dir
        .clone()
        .context("--funding-dir required for funding_ml")?;

    // Мета-дані (базис + потік): datasets/perp_meta поруч із funding-диром.
    let meta_dir = dir
        .parent()
        .map(|p| p.join("perp_meta"))
        .unwrap_or_else(|| PathBuf::from("datasets/perp_meta"));
    let meta_by_symbol = if meta_dir.exists() {
        load_meta_dir(&meta_dir)?
    } else {
        BTreeMap::new()
    };
    anyhow::ensure!(
        !meta_by_symbol.is_empty(),
        "meta dir {meta_dir:?} порожній — потрібні premium/taker дані"
    );
    println!("Мета-дані: {} символів з {:?}", meta_by_symbol.len(), meta_dir);
    let mut skipped_no_meta = 0usize;

    // 1. Датасет по всіх символах.
    let mut samples: Vec<FundingSample> = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let sym = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        let adapter = CsvFundingAdapter::new(&path);
        let series = adapter
            .funding_history(
                &sym,
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
                Utc::now(),
            )
            .await?;
        let meta = meta_by_symbol.get(&sym);
        if meta.is_none() {
            skipped_no_meta += 1;
            continue;
        }
        samples.extend(build_funding_samples(
            &sym,
            &series,
            FUNDING_PAST,
            FUNDING_HOLD,
            meta,
        ));
    }
    if skipped_no_meta > 0 {
        println!("(пропущено {skipped_no_meta} символів без мета-даних)");
    }
    anyhow::ensure!(samples.len() > 2000, "too few samples: {}", samples.len());
    samples.sort_by_key(|s| (s.timestamp, s.symbol.clone()));

    // 2. Часовий розріз 70/30 — жодних майбутніх даних у навчанні.
    let split_ts = samples[(samples.len() * 7) / 10].timestamp;
    let (train, test): (Vec<&FundingSample>, Vec<&FundingSample>) =
        samples.iter().partition(|s| s.timestamp < split_ts);
    println!(
        "Funding-ML: {} семплів ({} train / {} test), розріз {}",
        samples.len(),
        train.len(),
        test.len(),
        split_ts
    );

    // 3. Моделі користувача. Матч на ІДЕНТИЧНИХ рядках:
    //    linear_base — старі 7 фіч; linear_ext і RF — 12 (з базисом/потоком).
    //    Лейбл масштабується тим самим фактором, що фічі.
    let rf = RandomForestLikeModel::new_with_params_and_seed(
        funding_feature_keys_ext(),
        200_000, // тримати всі семпли
        1_000,
        4_000, // періодичний рефіт під час train
        100,
        8,
        0.0, // без deadzone: потрібна сира регресія
        args.config.ensemble.seed,
    );
    let linear_base = AdaptiveLinearModel::new(funding_feature_keys(), dec!(0.01));
    let linear_ext = AdaptiveLinearModel::new(funding_feature_keys_ext(), dec!(0.01));

    for s in &train {
        let y = (s.label * FUNDING_FEATURE_SCALE).clamp(dec!(-1), dec!(1));
        rf.learn(&s.features, y).await?;
        linear_base.learn(&s.features, y).await?;
        linear_ext.learn(&s.features, y).await?;
    }

    // 4. Прогнози на test (без донавчання).
    let signed = |p: &crate::model::domain::models::Prediction| -> Decimal {
        let sign = match p.direction {
            SignalDirection::Long => Decimal::ONE,
            SignalDirection::Short => Decimal::NEGATIVE_ONE,
            SignalDirection::Exit => Decimal::ZERO,
        };
        sign * p.confidence / FUNDING_FEATURE_SCALE
    };
    let mut rows: Vec<EvalRow> = Vec::new();
    for s in &test {
        let rf_pred = signed(&rf.predict(&s.features).await?);
        let lin_base_pred = signed(&linear_base.predict(&s.features).await?);
        let lin_ext_pred = signed(&linear_ext.predict(&s.features).await?);
        rows.push((rf_pred, lin_base_pred, lin_ext_pred, s.baseline, s.label, s));
    }

    // 5. Якість прогнозу: кореляція і MAE проти базлайну.
    let corr = |xs: &[f64], ys: &[f64]| -> f64 {
        let n = xs.len() as f64;
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let cov = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum::<f64>();
        let vx = xs.iter().map(|x| (x - mx).powi(2)).sum::<f64>();
        let vy = ys.iter().map(|y| (y - my).powi(2)).sum::<f64>();
        if vx <= 0.0 || vy <= 0.0 {
            0.0
        } else {
            cov / (vx.sqrt() * vy.sqrt())
        }
    };
    let f = |d: Decimal| d.to_f64().unwrap_or(0.0);
    let labels: Vec<f64> = rows.iter().map(|r| f(r.4)).collect();
    let rf_preds: Vec<f64> = rows.iter().map(|r| f(r.0)).collect();
    let lin_base_preds: Vec<f64> = rows.iter().map(|r| f(r.1)).collect();
    let lin_ext_preds: Vec<f64> = rows.iter().map(|r| f(r.2)).collect();
    let base_preds: Vec<f64> = rows.iter().map(|r| f(r.3)).collect();
    let mae = |ps: &[f64]| -> f64 {
        ps.iter()
            .zip(&labels)
            .map(|(p, l)| (p - l).abs())
            .sum::<f64>()
            / labels.len() as f64
            * 10_000.0 // у б.п. за інтервал
    };

    println!("\n=== Якість прогнозу майбутнього funding (test, {} семплів) ===", rows.len());
    println!("{:<34} {:>12} {:>16}", "Предиктор", "corr(pred,y)", "MAE (бпс/інтервал)");
    println!("{}", "-".repeat(66));
    println!("{:<34} {:>12.4} {:>16.4}", "базлайн mean21", corr(&base_preds, &labels), mae(&base_preds));
    println!("{:<34} {:>12.4} {:>16.4}", "linear (7 старих фіч)", corr(&lin_base_preds, &labels), mae(&lin_base_preds));
    println!("{:<34} {:>12.4} {:>16.4}", "linear +базис/потік (12 фіч)", corr(&lin_ext_preds, &labels), mae(&lin_ext_preds));
    println!("{:<34} {:>12.4} {:>16.4}", "RF +базис/потік (12 фіч)", corr(&rf_preds, &labels), mae(&rf_preds));

    // 6. Економічний тест: на кожному test-таймстемпі зібрати топ-5 кошик
    //    за кожним предиктором і порівняти РЕАЛЬНО зібраний майбутній funding.
    let mut by_ts: BTreeMap<DateTime<Utc>, Vec<&EvalRow>> = BTreeMap::new();
    for r in &rows {
        by_ts.entry(r.5.timestamp).or_default().push(r);
    }
    let top_k = args.top_k.clamp(2, 20);
    let mut sum_rf = Decimal::ZERO;
    let mut sum_lin_base = Decimal::ZERO;
    let mut sum_lin_ext = Decimal::ZERO;
    let mut sum_base = Decimal::ZERO;
    let mut sum_perfect = Decimal::ZERO;
    let mut n_ts = 0u64;
    for (_, group) in by_ts.iter().filter(|(_, g)| g.len() >= 10) {
        let basket_mean = |key: &dyn Fn(&&EvalRow) -> Decimal| -> Decimal {
            let mut sorted: Vec<_> = group.iter().collect();
            sorted.sort_by(|a, b| key(b).cmp(&key(a)).then_with(|| a.5.symbol.cmp(&b.5.symbol)));
            let top: Vec<_> = sorted.into_iter().take(top_k).collect();
            top.iter().map(|r| r.4).sum::<Decimal>() / Decimal::from(top_k as u64)
        };
        sum_rf += basket_mean(&|r| r.0);
        sum_lin_base += basket_mean(&|r| r.1);
        sum_lin_ext += basket_mean(&|r| r.2);
        sum_base += basket_mean(&|r| r.3);
        sum_perfect += basket_mean(&|r| r.4);
        n_ts += 1;
    }
    anyhow::ensure!(n_ts > 0, "no test timestamps with enough symbols");
    let annualize = |d: Decimal| {
        // Лейбл — середня ставка за інтервал; ~1095 інтервалів/рік (8h).
        (d / Decimal::from(n_ts) * dec!(1095) * dec!(100)).round_dp(2)
    };
    println!("\n=== Економічний тест: топ-{top_k} кошик, середній МАЙБУТНІЙ funding (≈% річних) ===");
    println!("{:<34} {:>10}", "Ранжування за", "≈%/рік");
    println!("{}", "-".repeat(46));
    println!("{:<34} {:>10}", "базлайн mean21", annualize(sum_base));
    println!("{:<34} {:>10}", "linear (7 старих фіч)", annualize(sum_lin_base));
    println!("{:<34} {:>10}", "linear +базис/потік (12 фіч)", annualize(sum_lin_ext));
    println!("{:<34} {:>10}", "RF +базис/потік (12 фіч)", annualize(sum_rf));
    println!("{:<34} {:>10}  (недосяжна стеля)", "ідеальне передбачення", annualize(sum_perfect));

    log_run(args, serde_json::json!({
        "strategy": "funding_ml_ext",
        "samples": samples.len(),
        "test_rows": rows.len(),
        "corr": {
            "baseline": corr(&base_preds, &labels),
            "linear_base": corr(&lin_base_preds, &labels),
            "linear_ext": corr(&lin_ext_preds, &labels),
            "rf_ext": corr(&rf_preds, &labels),
        },
        "mae_bps": {
            "baseline": mae(&base_preds),
            "linear_base": mae(&lin_base_preds),
            "linear_ext": mae(&lin_ext_preds),
            "rf_ext": mae(&rf_preds),
        },
        "basket_annualized_pct": {
            "baseline": annualize(sum_base).to_string(),
            "linear_base": annualize(sum_lin_base).to_string(),
            "linear_ext": annualize(sum_lin_ext).to_string(),
            "rf_ext": annualize(sum_rf).to_string(),
            "perfect": annualize(sum_perfect).to_string(),
        },
    }))
    .await;
    Ok(())
}

#[cfg(test)]
mod funding_ml_tests {
    use super::*;
    use chrono::Duration;
    use crate::data_ingestion::domain::funding::FundingRatePoint;

    /// Ступінчаста серія: до бару 100 ставка 0.0001, після — 0.0005.
    fn step_series(n: usize) -> Vec<FundingRatePoint> {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        (0..n)
            .map(|i| FundingRatePoint {
                symbol: "T".into(),
                timestamp: t0 + Duration::hours(8 * i as i64),
                rate: if i < 100 { dec!(0.0001) } else { dec!(0.0005) },
                mark_price: dec!(100),
                spot_price: Some(dec!(100)),
            })
            .collect()
    }

    // Лейбл — строго з майбутнього: семпл на t=99 (фічі бачать лише 0.0001)
    // має лейбл 0.0005 (наступні 21 інтервалів уже після стрибка).
    #[test]
    fn label_uses_only_future_features_only_past() {
        let samples = build_funding_samples("T", &step_series(200), 63, 21, None);
        let at_99 = samples.iter().find(|s| {
            s.timestamp
                == Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                    + Duration::hours(8 * 99)
        });
        let s = at_99.expect("sample at t=99 must exist");
        assert_eq!(s.baseline, dec!(0.0001), "фічі не бачать стрибка");
        assert_eq!(s.label, dec!(0.0005), "лейбл — повністю після стрибка");
        // А семпл на t=98: лейбл включає 1 інтервал старої ставки.
        let s98 = samples
            .iter()
            .find(|s| {
                s.timestamp
                    == Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                        + Duration::hours(8 * 98)
            })
            .unwrap();
        assert!(s98.label < dec!(0.0005) && s98.label > dec!(0.0004));
    }

    #[test]
    fn too_short_series_gives_no_samples() {
        assert!(build_funding_samples("T", &step_series(50), 63, 21, None).is_empty());
    }

    // Мета-фічі теж не бачать майбутнього: premium стрибає на барі 100,
    // семпл на t=99 має prem_mean зі старих значень.
    #[test]
    fn meta_features_use_only_past() {
        let series = step_series(200);
        let meta: BTreeMap<DateTime<Utc>, MetaRow> = series
            .iter()
            .enumerate()
            .map(|(i, p)| {
                (
                    p.timestamp,
                    MetaRow {
                        premium: if i < 100 { dec!(0.0001) } else { dec!(0.0009) },
                        volume: dec!(1000),
                        taker_buy_volume: dec!(500),
                    },
                )
            })
            .collect();
        let samples = build_funding_samples("T", &series, 63, 21, Some(&meta));
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let s99 = samples
            .iter()
            .find(|s| s.timestamp == t0 + chrono::Duration::hours(8 * 99))
            .unwrap();
        // prem_mean21 на t=99: усі 21 барів зі старим premium 0.0001 → ×1000 = 0.1.
        assert_eq!(s99.features.get_scalar("prem_mean21"), Some(dec!(0.1)));
        // prem_last теж старий.
        assert_eq!(s99.features.get_scalar("prem_last"), Some(dec!(0.1)));
        // А на t=101 свіжий premium уже видно.
        let s101 = samples
            .iter()
            .find(|s| s.timestamp == t0 + chrono::Duration::hours(8 * 101))
            .unwrap();
        assert_eq!(s101.features.get_scalar("prem_last"), Some(dec!(0.9)));
    }
}
