//! Читання маніфесту спот-пар (`datasets/spot_pairs.csv`).
//!
//! Формат: `symbol,has_spot,source,checked_at`. Пише його
//! `scripts/fetch_spot_pairs.py`, той самий скрипт має режим `--verify`,
//! що стоїть у CI.

use anyhow::{bail, Context, Result};
use std::path::Path;

use crate::data_ingestion::domain::spot_universe::SpotUniverse;

const HEADER: &str = "symbol,has_spot,source,checked_at";

pub fn load_spot_universe(path: &Path) -> Result<SpotUniverse> {
    let content = std::fs::read_to_string(path).with_context(|| {
        format!(
            "не читається маніфест спот-пар {}: згенеруй його \
             `python3 scripts/fetch_spot_pairs.py`",
            path.display()
        )
    })?;
    parse(&content).with_context(|| format!("розбір {}", path.display()))
}

fn parse(content: &str) -> Result<SpotUniverse> {
    let mut lines = content.lines();
    let header = lines.next().unwrap_or_default().trim();
    if header != HEADER {
        bail!("шапка {header:?}, очікувалась {HEADER:?}");
    }

    let mut rows: Vec<(String, bool)> = Vec::new();
    for (i, line) in lines.enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').map(|c| c.trim()).collect();
        if cols.len() != 4 {
            bail!("рядок {}: {} полів замість 4", i + 2, cols.len());
        }
        // Порожній прапорець і будь-яке інше значення — помилка, а не «ні».
        // Мовчазне трактування сміття як 0 викинуло б символ з юніверсу так,
        // що цього ніде не було б видно.
        let has_spot = match cols[1] {
            "1" => true,
            "0" => false,
            other => bail!("рядок {}: has_spot={other:?}, очікувалось 0 або 1", i + 2),
        };
        rows.push((cols[0].to_string(), has_spot));
    }

    if rows.is_empty() {
        bail!("маніфест порожній");
    }
    Ok(SpotUniverse::from_rows(rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_valid_manifest() {
        let u = parse(
            "symbol,has_spot,source,checked_at\n\
             BTCUSDT,1,exchangeInfo,2026-08-03\n\
             NVDAUSDT,0,exchangeInfo,2026-08-03\n",
        )
        .unwrap();
        assert!(u.is_hedgeable("BTCUSDT"));
        assert!(u.is_known("NVDAUSDT") && !u.is_hedgeable("NVDAUSDT"));
    }

    #[test]
    fn rejects_wrong_header() {
        let err = parse("symbol,has_spot\nBTCUSDT,1\n").unwrap_err().to_string();
        assert!(err.contains("шапка"), "{err}");
    }

    // Сміття у прапорці мусить валити розбір, а не тихо ставати нулем:
    // інакше символ зник би з юніверсу без жодного сліду.
    #[test]
    fn rejects_garbage_flag() {
        let err = parse(
            "symbol,has_spot,source,checked_at\nBTCUSDT,yes,exchangeInfo,2026-08-03\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("has_spot"), "{err}");
    }

    #[test]
    fn rejects_empty_manifest() {
        let err = parse("symbol,has_spot,source,checked_at\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("порожній"), "{err}");
    }
}
