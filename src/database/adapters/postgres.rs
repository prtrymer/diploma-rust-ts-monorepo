use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::database::domain::models::User;
use crate::database::ports::repository::UserRepository;

/// Row type for `SELECT` queries against the `users` table.
/// Uses `DateTime<Utc>` to match the `TIMESTAMPTZ` column type.
#[derive(sqlx::FromRow)]
struct UserRow {
    username: String,
    password_hash: String,
    created_at: DateTime<Utc>,
}

/// PostgreSQL-backed repository for user authentication data.
pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    /// Connect to PostgreSQL and auto-apply migrations from `migrations/postgres/`.
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url)
            .await
            .context("Failed to connect to PostgreSQL")?;

        sqlx::migrate!("migrations/postgres")
            .run(&pool)
            .await
            .context("Failed to run PostgreSQL migrations")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl UserRepository for PostgresRepository {
    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>> {
        let row: Option<UserRow> = sqlx::query_as(
            "SELECT username, password_hash, created_at FROM users WHERE username = $1",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to query user by username")?;

        Ok(row.map(|r| User {
            username: r.username,
            password_hash: r.password_hash,
            created_at: r.created_at,
        }))
    }

    async fn create_user(&self, user: &User) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (username, password_hash, created_at) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (username) DO NOTHING",
        )
        .bind(&user.username)
        .bind(&user.password_hash)
        .bind(user.created_at)   // DateTime<Utc> binds directly as TIMESTAMPTZ
        .execute(&self.pool)
        .await
        .context("Failed to insert user into PostgreSQL")?;

        Ok(())
    }

    async fn get_active_symbols(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT symbol FROM active_symbols ORDER BY added_at ASC")
            .fetch_all(&self.pool)
            .await
            .context("Failed to query active symbols")?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn add_active_symbol(&self, symbol: &str) -> Result<()> {
        sqlx::query("INSERT INTO active_symbols (symbol) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(symbol)
            .execute(&self.pool)
            .await
            .context("Failed to insert active symbol")?;
        Ok(())
    }

    async fn remove_active_symbol(&self, symbol: &str) -> Result<()> {
        sqlx::query("DELETE FROM active_symbols WHERE symbol = $1")
            .bind(symbol)
            .execute(&self.pool)
            .await
            .context("Failed to remove active symbol")?;
        Ok(())
    }
}
