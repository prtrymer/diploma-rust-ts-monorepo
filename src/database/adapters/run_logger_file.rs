//! Файловий адаптер порту RunLogger (JSONL). Fallback, коли PostgreSQL
//! недоступний: provenance зобов'язаний працювати і локально (M0.4).

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
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
}
