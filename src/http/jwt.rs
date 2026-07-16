//! Єдине джерело JWT-секрету.
//!
//! Секрет читається з env рівно один раз, на старті сервера (`init_from_env`).
//! Фолбеку немає: токени, підписані публічно відомим ключем, — це відсутність
//! автентифікації, тому без JWT_SECRET сервер не стартує взагалі.

use anyhow::{Context, Result};
use std::sync::OnceLock;

static SECRET: OnceLock<String> = OnceLock::new();

/// Fail-fast читання JWT_SECRET. Викликати на старті сервера,
/// до прийому першого запиту.
pub fn init_from_env() -> Result<()> {
    let secret = std::env::var("JWT_SECRET")
        .context("JWT_SECRET не задано — сервер не стартує без секрету (згенеруй: openssl rand -hex 32)")?;
    anyhow::ensure!(
        !secret.trim().is_empty(),
        "JWT_SECRET порожній — сервер не стартує без секрету"
    );
    if secret.len() < 32 {
        tracing::warn!("JWT_SECRET коротший за 32 символи — для HS256 бажано openssl rand -hex 32");
    }
    let _ = SECRET.set(secret);
    Ok(())
}

/// Секрет для підпису й перевірки токенів.
///
/// Панікує, якщо `init_from_env` не викликано: це помилка wiring'а сервера,
/// а не рантайм-стан, який можна обробити.
pub fn secret() -> &'static [u8] {
    SECRET
        .get()
        .expect("JWT-секрет не ініціалізовано — виклич http::jwt::init_from_env() на старті сервера")
        .as_bytes()
}
