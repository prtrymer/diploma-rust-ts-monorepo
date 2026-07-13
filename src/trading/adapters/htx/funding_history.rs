//! Збирач історії funding з HTX у CSV-формат `datasets/funding`
//! (`timestamp,symbol,rate,mark_price,spot_price`) — рівно той, що читає
//! `CsvFundingAdapter`, тож research-пайплайн (xs_carry, funding_ml) їсть
//! файли HTX без жодних змін коду.
//!
//! Ціна на момент funding — OPEN 4h-бара, чий старт збігається з funding-часом
//! (розрахунки HTX о 00/08/16 UTC лежать на 4h-сітці); якщо бара нема —
//! CLOSE попереднього (ціна на кінець бара ≤ t, без зазирання в майбутнє).
//! spot_price пишемо = mark_price: basis свідомо не моделюємо — так само,
//! як у Binance-датасеті.

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use super::client::HtxClient;

const FOUR_H_SEC: i64 = 4 * 3600;
/// Ліміт klines ≈2000 барів/запит (перевірено живцем: 2100 → порожньо).
const KLINE_WINDOW_BARS: i64 = 1990;

/// "BTC-USDT" → "BTCUSDT" (назва файлу/символу в research-датасетах).
pub fn csv_symbol(contract_code: &str) -> String {
    contract_code.replace('-', "").to_uppercase()
}

/// Ціна на момент `ts_sec`: open бара, що стартує в `ts_sec`, інакше close
/// останнього бара перед ним. Мапа: старт бара → (open, close).
pub fn price_at(bars: &BTreeMap<i64, (Decimal, Decimal)>, ts_sec: i64) -> Option<Decimal> {
    if let Some((open, _)) = bars.get(&ts_sec) {
        return Some(*open);
    }
    bars.range(..ts_sec).next_back().map(|(_, (_, close))| *close)
}

/// Останній timestamp (мс) у наявному CSV — межа інкрементального дозбору.
pub fn last_ts_ms(path: &Path) -> Option<i64> {
    let content = std::fs::read_to_string(path).ok()?;
    content
        .lines()
        .skip(1)
        .filter_map(|l| l.split(',').next())
        .filter_map(|ts| ts.parse::<DateTime<Utc>>().ok())
        .map(|t| t.timestamp_millis())
        .max()
}

/// Мерджить нові рядки (ts_ms, rate, mark) у CSV: наявні рядки лишаються
/// побайтово як були, нові додаються, все сортується за часом. Повертає
/// кількість реально доданих.
pub fn merge_csv(
    path: &Path,
    symbol: &str,
    new_rows: &[(i64, Decimal, Decimal)],
) -> Result<usize> {
    let mut by_ts: BTreeMap<i64, String> = BTreeMap::new();
    if let Ok(content) = std::fs::read_to_string(path) {
        for line in content.lines().skip(1) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let ts = line
                .split(',')
                .next()
                .and_then(|t| t.parse::<DateTime<Utc>>().ok())
                .with_context(|| format!("{path:?}: битий рядок «{line}»"))?;
            by_ts.insert(ts.timestamp_millis(), line.to_string());
        }
    }
    let mut added = 0usize;
    for (ts_ms, rate, mark) in new_rows {
        if by_ts.contains_key(ts_ms) {
            continue;
        }
        let ts = Utc
            .timestamp_millis_opt(*ts_ms)
            .single()
            .with_context(|| format!("битий funding_time {ts_ms}"))?;
        by_ts.insert(
            *ts_ms,
            format!(
                "{},{symbol},{},{},{}",
                ts.to_rfc3339_opts(SecondsFormat::Secs, true),
                rate.normalize(),
                mark.normalize(),
                mark.normalize()
            ),
        );
        added += 1;
    }
    if added > 0 {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::File::create(path)?;
        writeln!(f, "timestamp,symbol,rate,mark_price,spot_price")?;
        for line in by_ts.values() {
            writeln!(f, "{line}")?;
        }
    }
    Ok(added)
}

pub struct ContractCollectResult {
    pub contract_code: String,
    pub added: usize,
    pub no_price: usize,
}

pub struct FundingHistoryCollector<'a> {
    pub client: &'a HtxClient,
    /// Пауза між запитами — публічні ліміти спільні, поводимось чемно.
    pub delay_ms: u64,
}

impl<'a> FundingHistoryCollector<'a> {
    pub fn new(client: &'a HtxClient) -> Self {
        Self { client, delay_ms: 120 }
    }

    async fn pause(&self) {
        tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
    }

    /// Усі settled-ставки НОВІШІ за `since_ms` (ексклюзивно), хронологічно.
    /// Пагінація йде від нових до старих, тож на дозборі зупиняємось на
    /// першому вже відомому записі.
    pub async fn funding_since(
        &self,
        contract_code: &str,
        since_ms: Option<i64>,
    ) -> Result<Vec<(i64, Decimal)>> {
        let mut out: Vec<(i64, Decimal)> = Vec::new();
        let mut page = 1u32;
        loop {
            let p = self
                .client
                .swap_historical_funding(contract_code, page, 100)
                .await?;
            let mut hit_known = false;
            for row in &p.data {
                let Some(ts) = row.time_ms() else { continue };
                if since_ms.map(|s| ts <= s).unwrap_or(false) {
                    hit_known = true;
                    break;
                }
                if let Some(rate) = row.settled_rate() {
                    out.push((ts, rate));
                }
            }
            if hit_known || p.data.is_empty() || p.current_page >= p.total_page {
                break;
            }
            page += 1;
            self.pause().await;
        }
        out.sort_by_key(|(ts, _)| *ts);
        out.dedup_by_key(|(ts, _)| *ts);
        Ok(out)
    }

    /// 4h-бари (старт → (open, close)) на діапазон, вікнами по ≤1990 барів.
    pub async fn prices_4h(
        &self,
        contract_code: &str,
        from_sec: i64,
        to_sec: i64,
    ) -> Result<BTreeMap<i64, (Decimal, Decimal)>> {
        let mut bars = BTreeMap::new();
        let mut cursor = from_sec;
        while cursor <= to_sec {
            let window_end = (cursor + KLINE_WINDOW_BARS * FOUR_H_SEC).min(to_sec);
            for k in self
                .client
                .swap_klines_4h(contract_code, cursor, window_end)
                .await?
            {
                bars.insert(k.id, (k.open, k.close));
            }
            cursor = window_end + FOUR_H_SEC;
            if cursor <= to_sec {
                self.pause().await;
            }
        }
        Ok(bars)
    }

    /// Інкрементальний дозбір одного контракту у `out_dir`.
    pub async fn collect_contract(
        &self,
        contract_code: &str,
        out_dir: &Path,
    ) -> Result<ContractCollectResult> {
        let symbol = csv_symbol(contract_code);
        let path = out_dir.join(format!("{symbol}.csv"));
        let since = last_ts_ms(&path);

        let funding = self.funding_since(contract_code, since).await?;
        if funding.is_empty() {
            return Ok(ContractCollectResult {
                contract_code: contract_code.to_string(),
                added: 0,
                no_price: 0,
            });
        }
        let from_sec = funding.first().unwrap().0 / 1000 - FOUR_H_SEC;
        let to_sec = funding.last().unwrap().0 / 1000 + FOUR_H_SEC;
        self.pause().await;
        let bars = self.prices_4h(contract_code, from_sec, to_sec).await?;

        let mut rows: Vec<(i64, Decimal, Decimal)> = Vec::new();
        let mut no_price = 0usize;
        for (ts_ms, rate) in funding {
            match price_at(&bars, ts_ms / 1000) {
                Some(mark) if mark > Decimal::ZERO => rows.push((ts_ms, rate, mark)),
                _ => no_price += 1,
            }
        }
        let added = merge_csv(&path, &symbol, &rows)?;
        Ok(ContractCollectResult {
            contract_code: contract_code.to_string(),
            added,
            no_price,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn csv_symbol_strips_dash() {
        assert_eq!(csv_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(csv_symbol("1000PEPE-USDT"), "1000PEPEUSDT");
    }

    #[test]
    fn price_at_prefers_exact_bar_open_then_prev_close() {
        let mut bars = BTreeMap::new();
        bars.insert(1000, (dec!(10), dec!(11)));
        bars.insert(1000 + FOUR_H_SEC, (dec!(12), dec!(13)));
        // Точний бар → open.
        assert_eq!(price_at(&bars, 1000 + FOUR_H_SEC), Some(dec!(12)));
        // Дірка → close попереднього (ніякого майбутнього).
        assert_eq!(price_at(&bars, 1000 + 2 * FOUR_H_SEC), Some(dec!(13)));
        // До першого бара — ціни нема.
        assert_eq!(price_at(&bars, 500), None);
    }

    #[test]
    fn merge_csv_appends_sorted_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AAAUSDT.csv");
        // Перша порція — навмисно не по порядку.
        let added = merge_csv(
            &path,
            "AAAUSDT",
            &[
                (1_700_000_000_000, dec!(0.0001), dec!(2.5)),
                (1_699_971_200_000, dec!(0.0002), dec!(2.4)),
            ],
        )
        .unwrap();
        assert_eq!(added, 2);
        // Друга порція: один дубль + один новий.
        let added = merge_csv(
            &path,
            "AAAUSDT",
            &[
                (1_700_000_000_000, dec!(0.0009), dec!(9.9)), // дубль — ігнорується
                (1_700_028_800_000, dec!(0.0003), dec!(2.6)),
            ],
        )
        .unwrap();
        assert_eq!(added, 1);

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines[0], "timestamp,symbol,rate,mark_price,spot_price");
        assert_eq!(lines.len(), 4);
        // Відсортовано і дубль не перезаписав старий рядок.
        assert!(lines[1].starts_with("2023-11-14T14:13:20Z,AAAUSDT,0.0002,2.4,2.4"));
        assert!(lines[2].contains(",0.0001,2.5,2.5"));
        assert!(lines[3].contains(",0.0003,2.6,2.6"));

        assert_eq!(last_ts_ms(&path), Some(1_700_028_800_000));
    }

    // Формат, який пише merge_csv, читається CsvFundingAdapter-ом — це
    // контракт сумісності з research-пайплайном.
    #[tokio::test]
    async fn merged_csv_is_readable_by_csv_funding_adapter() {
        use crate::data_ingestion::ports::funding::FundingDataPort;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AAAUSDT.csv");
        merge_csv(&path, "AAAUSDT", &[(1_700_000_000_000, dec!(0.00015), dec!(2.5))]).unwrap();
        let adapter = crate::data_ingestion::adapters::funding_csv::CsvFundingAdapter::new(&path);
        let points = adapter
            .funding_history(
                "AAAUSDT",
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
                Utc::now(),
            )
            .await
            .unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].rate, dec!(0.00015));
        assert_eq!(points[0].mark_price, dec!(2.5));
        assert_eq!(points[0].spot_price, Some(dec!(2.5)));
    }
}
