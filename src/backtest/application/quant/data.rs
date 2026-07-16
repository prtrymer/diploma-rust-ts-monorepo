//! Офлайн-джерело даних: директорія CSV (по файлу на символ).

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::trading::domain::allocation::AlignedMarketData;

/// Читає CSV-директорію: <SYMBOL>.csv з колонками timestamp/date, close, volume?
pub fn load_csv_dir(dir: &Path) -> Result<Arc<AlignedMarketData>> {
    let mut per_symbol: BTreeMap<String, BTreeMap<DateTime<Utc>, (Decimal, Decimal)>> =
        BTreeMap::new();

    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {dir:?}"))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }
        let symbol = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        let content = std::fs::read_to_string(&path)?;
        let mut lines = content.lines();
        let header = lines.next().unwrap_or_default().to_lowercase();
        let cols: Vec<&str> = header.split(',').map(|c| c.trim()).collect();
        let ts_idx = cols
            .iter()
            .position(|c| *c == "timestamp" || *c == "date")
            .context("csv must have timestamp/date column")?;
        let close_idx = cols
            .iter()
            .position(|c| *c == "adj_close" || *c == "close")
            .context("csv must have close/adj_close column")?;
        let vol_idx = cols.iter().position(|c| *c == "volume");

        let series = per_symbol.entry(symbol).or_default();
        for line in lines {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() <= close_idx {
                continue;
            }
            let raw_ts = parts[ts_idx].trim();
            let ts = raw_ts
                .parse::<DateTime<Utc>>()
                .ok()
                .or_else(|| {
                    NaiveDate::parse_from_str(raw_ts, "%Y-%m-%d")
                        .ok()
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                        .map(|dt| Utc.from_utc_datetime(&dt))
                });
            let Some(ts) = ts else { continue };
            let Ok(close) = parts[close_idx].trim().parse::<Decimal>() else {
                continue;
            };
            let volume = vol_idx
                .and_then(|vi| parts.get(vi))
                .and_then(|v| v.trim().parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            series.insert(ts, (close, volume));
        }
    }
    anyhow::ensure!(!per_symbol.is_empty(), "no csv files found in {dir:?}");

    // Вирівнювання: inner join по датах, що є в УСІХ символах.
    let mut common: Option<std::collections::BTreeSet<DateTime<Utc>>> = None;
    for series in per_symbol.values() {
        let keys: std::collections::BTreeSet<DateTime<Utc>> = series.keys().copied().collect();
        common = Some(match common {
            None => keys,
            Some(c) => c.intersection(&keys).copied().collect(),
        });
    }
    let timestamps: Vec<DateTime<Utc>> = common.unwrap_or_default().into_iter().collect();
    anyhow::ensure!(
        timestamps.len() >= 30,
        "too few common bars across symbols: {}",
        timestamps.len()
    );

    let mut closes = BTreeMap::new();
    let mut volumes = BTreeMap::new();
    for (sym, series) in &per_symbol {
        let mut c = Vec::with_capacity(timestamps.len());
        let mut v = Vec::with_capacity(timestamps.len());
        for ts in &timestamps {
            let (close, volume) = series[ts];
            c.push(close);
            v.push(volume);
        }
        closes.insert(sym.clone(), c);
        volumes.insert(sym.clone(), v);
    }

    Ok(Arc::new(AlignedMarketData::new(
        timestamps,
        closes.clone(),
        closes,
        volumes,
    )?))
}
