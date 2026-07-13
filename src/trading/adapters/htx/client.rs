//! Тонкий REST-клієнт HTX. Ключі — з env (`HTX_ACCESS_KEY`/`HTX_SECRET_KEY`),
//! хости конфігуруються (`HTX_SPOT_HOST`/`HTX_SWAP_HOST`); підпис включає
//! хост, тому підміняти хост можна тільки разом із запитом. Без ключів
//! працюють лише публічні ендпоінти (план у dry-run цього достатньо).

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

use super::signing::{signature_timestamp, signed_query, ApiCredentials};
use super::types::Envelope;

/// Дефолтні хости. Спот також доступний як api.htx.com — обидва живі
/// після ребрендингу; деривативи лишилися на api.hbdm.com.
pub const DEFAULT_SPOT_HOST: &str = "api.huobi.pro";
pub const DEFAULT_SWAP_HOST: &str = "api.hbdm.com";

pub struct HtxClient {
    http: reqwest::Client,
    pub spot_host: String,
    pub swap_host: String,
    creds: Option<ApiCredentials>,
}

impl HtxClient {
    pub fn new(
        spot_host: impl Into<String>,
        swap_host: impl Into<String>,
        creds: Option<ApiCredentials>,
    ) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .context("не вдалося зібрати HTTP-клієнт")?,
            spot_host: spot_host.into(),
            swap_host: swap_host.into(),
            creds,
        })
    }

    /// Клієнт із env. Порожні/відсутні ключі — це ок: публічні ендпоінти
    /// працюють, приватні повернуть зрозумілу помилку.
    pub fn from_env() -> Result<Self> {
        let access = std::env::var("HTX_ACCESS_KEY").unwrap_or_default();
        let secret = std::env::var("HTX_SECRET_KEY").unwrap_or_default();
        let creds = if access.is_empty() || secret.is_empty() {
            None
        } else {
            Some(ApiCredentials::new(access, secret))
        };
        Self::new(
            std::env::var("HTX_SPOT_HOST").unwrap_or_else(|_| DEFAULT_SPOT_HOST.into()),
            std::env::var("HTX_SWAP_HOST").unwrap_or_else(|_| DEFAULT_SWAP_HOST.into()),
            creds,
        )
    }

    pub fn has_creds(&self) -> bool {
        self.creds.is_some()
    }

    /// Access key — потрібен параметром у /v2/user/api-key. Secret не віддаємо.
    pub fn access_key(&self) -> Option<&str> {
        self.creds.as_ref().map(|c| c.access_key.as_str())
    }

    fn creds(&self) -> Result<&ApiCredentials> {
        self.creds.as_ref().context(
            "потрібні ключі HTX: задай HTX_ACCESS_KEY і HTX_SECRET_KEY у .env \
             (ключ створюй БЕЗ права на вивід коштів)",
        )
    }

    async fn parse<T: DeserializeOwned>(resp: reqwest::Response, ctx: &str) -> Result<T> {
        let http_status = resp.status();
        let text = resp.text().await.with_context(|| format!("HTX {ctx}: читання тіла"))?;
        let envelope: Envelope<T> = serde_json::from_str(&text).with_context(|| {
            // Тіло обрізаємо: у ньому не буває секретів, але буває багато.
            let head: String = text.chars().take(300).collect();
            format!("HTX {ctx}: не JSON-конверт (HTTP {http_status}): {head}")
        })?;
        envelope.into_data(ctx)
    }

    pub(crate) async fn get_public<T: DeserializeOwned>(
        &self,
        host: &str,
        path: &str,
        query: &[(String, String)],
    ) -> Result<T> {
        let mut url = format!("https://{host}{path}");
        if !query.is_empty() {
            let qs: Vec<String> = query
                .iter()
                .map(|(k, v)| format!("{k}={}", urlencoding::encode(v)))
                .collect();
            url = format!("{url}?{}", qs.join("&"));
        }
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("HTX GET {path}: мережа"))?;
        Self::parse(resp, path).await
    }

    pub(crate) async fn get_signed<T: DeserializeOwned>(
        &self,
        host: &str,
        path: &str,
        extra: &[(String, String)],
    ) -> Result<T> {
        let ts = signature_timestamp(chrono::Utc::now());
        let query = signed_query(self.creds()?, "GET", host, path, extra, &ts);
        let resp = self
            .http
            .get(format!("https://{host}{path}?{query}"))
            .send()
            .await
            .with_context(|| format!("HTX GET {path}: мережа"))?;
        Self::parse(resp, path).await
    }

    /// POST із JSON-тілом. Підписуються лише auth-параметри в query —
    /// тіло в підпис не входить (протокол HTX).
    pub(crate) async fn post_signed<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        host: &str,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let ts = signature_timestamp(chrono::Utc::now());
        let query = signed_query(self.creds()?, "POST", host, path, &[], &ts);
        let resp = self
            .http
            .post(format!("https://{host}{path}?{query}"))
            .json(body)
            .send()
            .await
            .with_context(|| format!("HTX POST {path}: мережа"))?;
        Self::parse(resp, path).await
    }
}
