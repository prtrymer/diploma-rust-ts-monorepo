//! Фільтр торгованості carry-юніверсу (рівень 1 роботи над базисом).
//!
//! Carry тримає лонг спота проти шорта перпа. Немає спот-ринку — немає
//! другої ноги, і угоди не існує. Юніверс `datasets/funding` цього не
//! розрізняв, а наслідок системний: у кошику тіньового журналу за
//! 2026-08-02 спот-пару мали 0 із 10 символів в обох кошиках.
//!
//! Фільтр читає закомічений маніфест (`datasets/spot_pairs.csv`) і в мережу
//! не ходить — юніверс мусить бути частиною входу прогону, а не залежати від
//! відповіді біржі в мить запуску (інваріанти 4 і 5). Керується конфігом,
//! тож входить у `config_hash`: прогони з фільтром і без нього — різні
//! гіпотези, і журнал прогонів їх розрізняє.

use anyhow::{ensure, Result};
use std::collections::BTreeMap;
use std::path::Path;

use crate::data_ingestion::adapters::spot_pairs_csv::load_spot_universe;
use crate::shared::run_config::UniverseConfig;

/// Викидає з юніверсу перпи без спот-ноги. `label` — назва стратегії у виводі.
///
/// Символи, яких немає в маніфесті, теж викидаються (перевірити хедж
/// неможливо → торгованими не вважаємо), але друкуються окремим рядком:
/// це інша ситуація, ніж перевірена відсутність спота, і мовчати про неї
/// не можна. Прогін не валиться — маніфест міг просто відстати від нового
/// лістингу; за його свіжістю стежить `fetch_spot_pairs.py --verify` у CI.
pub fn apply_spot_filter<T>(
    universe: &mut BTreeMap<String, T>,
    cfg: &UniverseConfig,
    label: &str,
) -> Result<()> {
    if !cfg.require_spot_leg {
        println!(
            "[{label}] фільтр спот-ноги ВИМКНЕНО: {} перпів, серед них можуть бути \
             нехеджовані — числа нижче не є торговими",
            universe.len()
        );
        return Ok(());
    }

    let spot = load_spot_universe(Path::new(&cfg.spot_pairs_path))?;
    let symbols: Vec<String> = universe.keys().cloned().collect();
    let outcome = spot.partition(symbols.iter().map(String::as_str));

    universe.retain(|sym, _| spot.is_hedgeable(sym));

    println!(
        "[{label}] фільтр спот-ноги: лишилось {} з {} (маніфест {}, хеджованих {})",
        outcome.kept.len(),
        symbols.len(),
        cfg.spot_pairs_path,
        spot.hedgeable_len()
    );
    if !outcome.no_spot.is_empty() {
        println!(
            "  без спот-пари, викинуто {}: {}",
            outcome.no_spot.len(),
            outcome.no_spot.join(" ")
        );
    }
    if !outcome.unknown.is_empty() {
        println!(
            "  ⚠ немає в маніфесті, викинуто {}: {} — онови \
             `python3 scripts/fetch_spot_pairs.py`",
            outcome.unknown.len(),
            outcome.unknown.join(" ")
        );
    }

    ensure!(
        !universe.is_empty(),
        "після фільтра спот-ноги не лишилось жодного символа з {}: \
         юніверс не хеджується цілком (або маніфест {} не про цю біржу)",
        symbols.len(),
        cfg.spot_pairs_path
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn manifest(rows: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "symbol,has_spot,source,checked_at\n{rows}").unwrap();
        f
    }

    fn universe(syms: &[&str]) -> BTreeMap<String, u8> {
        syms.iter().map(|s| (s.to_string(), 0u8)).collect()
    }

    fn cfg(path: &Path, require: bool) -> UniverseConfig {
        UniverseConfig {
            require_spot_leg: require,
            spot_pairs_path: path.display().to_string(),
        }
    }

    #[test]
    fn drops_symbols_without_spot_leg() {
        let m = manifest(
            "BTCUSDT,1,exchangeInfo,2026-08-03\n\
             NVDAUSDT,0,exchangeInfo,2026-08-03\n",
        );
        let mut u = universe(&["BTCUSDT", "NVDAUSDT"]);
        apply_spot_filter(&mut u, &cfg(m.path(), true), "test").unwrap();
        assert_eq!(u.keys().collect::<Vec<_>>(), vec!["BTCUSDT"]);
    }

    // Символ, якого немає в маніфесті, вважати торгованим не можна: хедж
    // не перевірений. Але й валити прогін не треба — маніфест міг відстати.
    #[test]
    fn drops_symbols_missing_from_manifest() {
        let m = manifest("BTCUSDT,1,exchangeInfo,2026-08-03\n");
        let mut u = universe(&["BTCUSDT", "NEWCOINUSDT"]);
        apply_spot_filter(&mut u, &cfg(m.path(), true), "test").unwrap();
        assert_eq!(u.keys().collect::<Vec<_>>(), vec!["BTCUSDT"]);
    }

    // Кошик тіньового журналу за 2026-08-02 був саме таким — 0 з 10. Тихо
    // віддати порожній юніверс означало б порахувати нуль угод як результат.
    #[test]
    fn empty_result_is_an_error_not_a_silent_zero() {
        let m = manifest(
            "KORUUSDT,0,exchangeInfo,2026-08-03\n\
             SOXLUSDT,0,exchangeInfo,2026-08-03\n",
        );
        let mut u = universe(&["KORUUSDT", "SOXLUSDT"]);
        let err = apply_spot_filter(&mut u, &cfg(m.path(), true), "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("не лишилось жодного символа"), "{err}");
    }

    #[test]
    fn disabled_filter_keeps_everything() {
        let m = manifest("BTCUSDT,1,exchangeInfo,2026-08-03\n");
        let mut u = universe(&["BTCUSDT", "KORUUSDT"]);
        apply_spot_filter(&mut u, &cfg(m.path(), false), "test").unwrap();
        assert_eq!(u.len(), 2);
    }

    #[test]
    fn missing_manifest_is_a_loud_error() {
        let mut u = universe(&["BTCUSDT"]);
        let err = apply_spot_filter(&mut u, &cfg(Path::new("/nope/absent.csv"), true), "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("маніфест"), "{err}");
    }
}
