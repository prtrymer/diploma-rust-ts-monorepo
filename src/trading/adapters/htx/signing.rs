//! Підпис запитів HTX API (SignatureVersion=2, HmacSHA256, base64).
//!
//! Канонічний рядок: `МЕТОД\nхост\nшлях\nвідсортовані_параметри`.
//! Параметри сортуються за ASCII, значення URL-енкодяться (двокрапки
//! Timestamp — теж). ВАЖЛИВО: для POST підписуються лише query-параметри
//! авторизації — JSON-тіло у підпис не входить, так влаштований протокол.
//! Хост у канонічному рядку має побайтово збігатися з хостом запиту.

use base64::Engine;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Ключі API. Secret навмисно недоступний зовні і не потрапляє в Debug.
#[derive(Clone)]
pub struct ApiCredentials {
    pub access_key: String,
    secret_key: String,
}

impl ApiCredentials {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
        }
    }
}

impl std::fmt::Debug for ApiCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shown: String = self.access_key.chars().take(4).collect();
        write!(f, "ApiCredentials({shown}…, secret=***)")
    }
}

/// Час підпису у форматі HTX: UTC `yyyy-MM-ddTHH:mm:ss` (без мілісекунд).
/// Вікно валідності на боці біржі — 5 хвилин; годинник має бути в NTP-синку.
pub fn signature_timestamp(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn encode_pairs(params: &[(String, String)]) -> String {
    let mut sorted: Vec<&(String, String)> = params.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    sorted
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Канонічний рядок, який підписується.
pub fn canonical_string(
    method: &str,
    host: &str,
    path: &str,
    params: &[(String, String)],
) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        method,
        host.to_lowercase(),
        path,
        encode_pairs(params)
    )
}

/// HMAC-SHA256 канонічного рядка → base64.
pub fn sign(secret_key: &str, canonical: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes())
        .expect("HMAC приймає ключ будь-якої довжини");
    mac.update(canonical.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// Повний query string підписаного запиту: авторизаційні параметри +
/// `extra` (GET-параметри, що входять у підпис) + `Signature`.
pub fn signed_query(
    creds: &ApiCredentials,
    method: &str,
    host: &str,
    path: &str,
    extra: &[(String, String)],
    timestamp: &str,
) -> String {
    let mut params: Vec<(String, String)> = vec![
        ("AccessKeyId".into(), creds.access_key.clone()),
        ("SignatureMethod".into(), "HmacSHA256".into()),
        ("SignatureVersion".into(), "2".into()),
        ("Timestamp".into(), timestamp.to_string()),
    ];
    params.extend_from_slice(extra);
    let signature = sign(
        &creds.secret_key,
        &canonical_string(method, host, path, &params),
    );
    format!(
        "{}&Signature={}",
        encode_pairs(&params),
        urlencoding::encode(&signature)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn auth_params(ts: &str) -> Vec<(String, String)> {
        vec![
            ("AccessKeyId".into(), "test-access".into()),
            ("SignatureMethod".into(), "HmacSHA256".into()),
            ("SignatureVersion".into(), "2".into()),
            ("Timestamp".into(), ts.into()),
        ]
    }

    // Формат канонічного рядка — точно за протоколом: сортування ASCII,
    // URL-енкодинг значень (двокрапки Timestamp → %3A).
    #[test]
    fn canonical_string_is_sorted_and_encoded() {
        let mut params = auth_params("2026-01-02T03:04:05");
        params.push(("symbol".into(), "btcusdt".into()));
        let s = canonical_string("GET", "API.huobi.PRO", "/v1/account/accounts", &params);
        assert_eq!(
            s,
            "GET\napi.huobi.pro\n/v1/account/accounts\n\
             AccessKeyId=test-access&SignatureMethod=HmacSHA256&SignatureVersion=2\
             &Timestamp=2026-01-02T03%3A04%3A05&symbol=btcusdt"
        );
    }

    // Незалежний вектор: openssl dgst -sha256 -hmac "test-secret" | base64
    // на тому самому канонічному рядку.
    #[test]
    fn signature_matches_openssl_vector() {
        let s = canonical_string(
            "GET",
            "api.huobi.pro",
            "/v1/account/accounts",
            &auth_params("2026-01-02T03:04:05"),
        );
        assert_eq!(sign("test-secret", &s), "gw9IyI8v6itatfRyfUmeihsvsqKkBV5BWupToueqGcQ=");
    }

    #[test]
    fn signed_query_appends_encoded_signature() {
        let creds = ApiCredentials::new("test-access", "test-secret");
        let q = signed_query(
            &creds,
            "GET",
            "api.huobi.pro",
            "/v1/account/accounts",
            &[],
            "2026-01-02T03:04:05",
        );
        assert!(q.starts_with(
            "AccessKeyId=test-access&SignatureMethod=HmacSHA256&SignatureVersion=2\
             &Timestamp=2026-01-02T03%3A04%3A05&Signature="
        ));
        // HMAC-SHA256 → 32 байти → base64 завжди з одним падінгом «=»,
        // який у query енкодиться у %3D.
        assert!(q.ends_with("%3D"));
    }

    #[test]
    fn timestamp_format_has_no_millis() {
        let t = Utc.with_ymd_and_hms(2026, 7, 12, 9, 5, 1).unwrap();
        assert_eq!(signature_timestamp(t), "2026-07-12T09:05:01");
    }

    #[test]
    fn debug_never_leaks_secret() {
        let creds = ApiCredentials::new("AK1234567", "SUPER-SECRET");
        let dbg = format!("{creds:?}");
        assert!(!dbg.contains("SUPER-SECRET"));
    }
}
