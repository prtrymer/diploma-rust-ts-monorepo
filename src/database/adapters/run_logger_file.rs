//! Файловий адаптер порту RunLogger (JSONL). Fallback, коли PostgreSQL
//! недоступний: provenance зобов'язаний працювати і локально (M0.4).

use anyhow::Result;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use tokio::sync::Mutex;

use crate::database::ports::run_logger::{RunLogger, RunRecord};

pub struct FileRunLogger {
    path: PathBuf,
    lock: Mutex<()>,
}

impl FileRunLogger {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Mutex::new(()),
        }
    }

    fn read_lines(&self) -> Result<Vec<String>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = std::fs::File::open(&self.path)?;
        let reader = std::io::BufReader::new(file);
        Ok(reader.lines().collect::<std::io::Result<Vec<_>>>()?)
    }
}

#[async_trait]
impl RunLogger for FileRunLogger {
    async fn log_run(&self, record: &RunRecord) -> Result<()> {
        let _guard = self.lock.lock().await;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{}", serde_json::to_string(record)?)?;
        Ok(())
    }

    async fn count_runs(&self) -> Result<usize> {
        let _guard = self.lock.lock().await;
        Ok(self.read_lines()?.len())
    }

    async fn count_distinct_configs(&self) -> Result<usize> {
        let _guard = self.lock.lock().await;
        let mut hashes = HashSet::new();
        for line in self.read_lines()? {
            if let Ok(rec) = serde_json::from_str::<RunRecord>(&line) {
                hashes.insert(rec.config_hash);
            }
        }
        Ok(hashes.len())
    }

    async fn trial_sharpes(&self, strategy: &str) -> Result<Vec<f64>> {
        let _guard = self.lock.lock().await;
        // BTreeMap, а не HashMap: порядок спроб детермінований (інваріант 4),
        // інакше дисперсія з f64 могла б плавати від прогону до прогону.
        let mut by_config: BTreeMap<String, f64> = BTreeMap::new();
        for line in self.read_lines()? {
            let Ok(rec) = serde_json::from_str::<RunRecord>(&line) else {
                continue;
            };
            if rec.metrics.get("strategy").and_then(|s| s.as_str()) != Some(strategy) {
                continue;
            }
            let sharpe = rec
                .metrics
                .get("walk_forward")
                .and_then(|wf| wf.get("median_oos_sharpe"))
                .and_then(parse_decimal_or_number);
            if let Some(s) = sharpe {
                // Ключ — конфіг ПЛЮС юніверс: той самий tsmom по ETF і по
                // крипті має однаковий config_hash (юніверс у конфіг не
                // входить), але це дві різні гіпотези. Пізніший прогін тієї
                // самої пари витісняє ранішній.
                let dataset = rec
                    .metrics
                    .get("dataset")
                    .and_then(|d| d.as_str())
                    .unwrap_or("");
                by_config.insert(format!("{}\u{0}{dataset}", rec.config_hash), s);
            }
        }
        Ok(by_config.into_values().collect())
    }
}

/// `median_oos_sharpe` серіалізується як рядок (Decimal), але старі записи
/// могли лягти числом — приймаємо обидва, щоб історія лишалась читомою.
fn parse_decimal_or_number(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn logs_and_counts_runs() {
        let dir = tempfile::tempdir().unwrap();
        let logger = FileRunLogger::new(dir.path().join("runs.jsonl"));

        let rec1 = RunRecord::new("hash-a".into(), "{}".into(), serde_json::json!({"r": 1}));
        let rec2 = RunRecord::new("hash-a".into(), "{}".into(), serde_json::json!({"r": 2}));
        let rec3 = RunRecord::new("hash-b".into(), "{}".into(), serde_json::json!({"r": 3}));
        logger.log_run(&rec1).await.unwrap();
        logger.log_run(&rec2).await.unwrap();
        logger.log_run(&rec3).await.unwrap();

        assert_eq!(logger.count_runs().await.unwrap(), 3);
        assert_eq!(logger.count_distinct_configs().await.unwrap(), 2);
    }

    fn wf_run(config_hash: &str, dataset: &str, strategy: &str, sharpe: &str) -> RunRecord {
        RunRecord::new(
            config_hash.into(),
            "{}".into(),
            serde_json::json!({
                "strategy": strategy,
                "dataset": dataset,
                "walk_forward": {"median_oos_sharpe": sharpe},
            }),
        )
    }

    // Той самий tsmom по ETF і по крипті має ОДНАКОВИЙ config_hash (юніверс
    // у конфіг не входить), але це дві різні гіпотези. Без юніверсу в ключі
    // пізніший прогін затирав би ранішній і N trials занижувався б.
    #[tokio::test]
    async fn same_config_on_different_universes_counts_as_two_trials() {
        let dir = tempfile::tempdir().unwrap();
        let logger = FileRunLogger::new(dir.path().join("runs.jsonl"));

        logger
            .log_run(&wf_run("hash-a", "datasets/etf", "tsmom", "0.371"))
            .await
            .unwrap();
        logger
            .log_run(&wf_run("hash-a", "datasets/daily", "tsmom", "1.037"))
            .await
            .unwrap();

        let mut got = logger.trial_sharpes("tsmom").await.unwrap();
        got.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(got, vec![0.371, 1.037]);
    }

    // Повтор тієї самої пари (конфіг, юніверс) — не нова гіпотеза: береться
    // останній результат.
    #[tokio::test]
    async fn repeated_run_of_same_pair_is_one_trial() {
        let dir = tempfile::tempdir().unwrap();
        let logger = FileRunLogger::new(dir.path().join("runs.jsonl"));

        logger
            .log_run(&wf_run("hash-a", "datasets/etf", "tsmom", "0.371"))
            .await
            .unwrap();
        logger
            .log_run(&wf_run("hash-a", "datasets/etf", "tsmom", "0.402"))
            .await
            .unwrap();
        // Інша стратегія в тому ж журналі не має потрапляти у вибірку.
        logger
            .log_run(&wf_run("hash-b", "datasets/etf", "xsmom", "1.14"))
            .await
            .unwrap();

        assert_eq!(logger.trial_sharpes("tsmom").await.unwrap(), vec![0.402]);
    }
}
