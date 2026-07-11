//! PostgreSQL-адаптер порту RunLogger (M0.4): таблиця
//! runs(run_id, config_hash, config_json, metrics, created_at).

use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::database::ports::run_logger::{RunLogger, RunRecord};

pub struct PostgresRunLogger {
    pool: PgPool,
}

impl PostgresRunLogger {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS runs (
                run_id UUID PRIMARY KEY,
                config_hash TEXT NOT NULL,
                config_json JSONB NOT NULL,
                metrics JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS runs_config_hash_idx ON runs (config_hash);
            "#,
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl RunLogger for PostgresRunLogger {
    async fn log_run(&self, record: &RunRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO runs (run_id, config_hash, config_json, metrics, created_at)
             VALUES ($1, $2, $3::jsonb, $4, $5)",
        )
        .bind(record.run_id)
        .bind(&record.config_hash)
        .bind(&record.config_json)
        .bind(&record.metrics)
        .bind(record.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn count_runs(&self) -> Result<usize> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM runs")
            .fetch_one(&self.pool)
            .await?;
        let n: i64 = row.try_get("n")?;
        Ok(n as usize)
    }

    async fn count_distinct_configs(&self) -> Result<usize> {
        let row = sqlx::query("SELECT COUNT(DISTINCT config_hash) AS n FROM runs")
            .fetch_one(&self.pool)
            .await?;
        let n: i64 = row.try_get("n")?;
        Ok(n as usize)
    }
}
