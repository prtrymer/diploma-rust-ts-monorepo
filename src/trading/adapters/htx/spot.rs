//! Спот-ендпоінти HTX (хост `spot_host`).

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::collections::HashMap;

use super::client::HtxClient;
use super::types::*;

impl HtxClient {
    /// Час сервера (мс) — для контролю розсинхрону годинника перед підписом.
    pub async fn spot_server_time(&self) -> Result<i64> {
        self.get_public(&self.spot_host.clone(), "/v1/common/timestamp", &[])
            .await
    }

    pub async fn spot_accounts(&self) -> Result<Vec<SpotAccount>> {
        self.get_signed(&self.spot_host.clone(), "/v1/account/accounts", &[])
            .await
    }

    /// id робочого спот-акаунта (потрібен у кожному ордері).
    pub async fn spot_account_id(&self) -> Result<i64> {
        let accounts = self.spot_accounts().await?;
        accounts
            .iter()
            .find(|a| a.account_type == "spot" && a.state == "working")
            .map(|a| a.id)
            .context("немає робочого spot-акаунта на HTX")
    }

    /// Доступні (type=trade) баланси > 0: валюта (lowercase) → кількість.
    pub async fn spot_balances(&self, account_id: i64) -> Result<HashMap<String, Decimal>> {
        let data: SpotBalanceData = self
            .get_signed(
                &self.spot_host.clone(),
                &format!("/v1/account/accounts/{account_id}/balance"),
                &[],
            )
            .await?;
        Ok(data
            .list
            .into_iter()
            .filter(|e| e.balance_type == "trade" && e.balance > Decimal::ZERO)
            .map(|e| (e.currency.to_lowercase(), e.balance))
            .collect())
    }

    /// Метадані всіх спот-пар: точності, мінімальні розміри.
    pub async fn spot_symbols(&self) -> Result<Vec<SpotSymbolMeta>> {
        self.get_public(&self.spot_host.clone(), "/v2/settings/common/symbols", &[])
            .await
    }

    /// UID користувача (потрібен для запиту стану API-ключів).
    pub async fn user_uid(&self) -> Result<i64> {
        self.get_signed(&self.spot_host.clone(), "/v2/user/uid", &[])
            .await
    }

    /// Стан API-ключів: права, IP-прив'язка, validDays (лічильник
    /// деактивації за неактивності; -1 = безстроковий).
    pub async fn api_key_info(&self, uid: i64, access_key: &str) -> Result<Vec<ApiKeyInfo>> {
        self.get_signed(
            &self.spot_host.clone(),
            "/v2/user/api-key",
            &[
                ("uid".to_string(), uid.to_string()),
                ("accessKey".to_string(), access_key.to_string()),
            ],
        )
        .await
    }

    /// Фактичні комісії користувача по парах (maker/taker).
    pub async fn spot_fee_rates(&self, symbols: &[String]) -> Result<Vec<SpotFeeRate>> {
        self.get_signed(
            &self.spot_host.clone(),
            "/v2/reference/transact-fee-rate",
            &[("symbols".to_string(), symbols.join(","))],
        )
        .await
    }

    /// Верх стакана: (bid, ask).
    pub async fn spot_best_bid_ask(&self, symbol: &str) -> Result<(Decimal, Decimal)> {
        let tick: SpotMergedTick = self
            .get_public(
                &self.spot_host.clone(),
                "/market/detail/merged",
                &[("symbol".to_string(), symbol.to_string())],
            )
            .await?;
        Ok((tick.bid.0, tick.ask.0))
    }

    /// Виставити ордер; повертає біржовий order-id.
    pub async fn spot_place_order(&self, req: &SpotOrderRequest) -> Result<String> {
        self.post_signed(&self.spot_host.clone(), "/v1/order/orders/place", req)
            .await
    }

    /// Стан ордера за нашим client-order-id (шлях звірки після таймаута:
    /// НІКОЛИ не перевиставляємо наосліп — спочатку питаємо цей ендпоінт).
    pub async fn spot_order_by_client_id(&self, client_order_id: &str) -> Result<SpotOrderInfo> {
        self.get_signed(
            &self.spot_host.clone(),
            "/v1/order/orders/getClientOrder",
            &[("clientOrderId".to_string(), client_order_id.to_string())],
        )
        .await
    }

    pub async fn spot_cancel_by_client_id(&self, client_order_id: &str) -> Result<()> {
        let body = serde_json::json!({ "client-order-id": client_order_id });
        let _: serde_json::Value = self
            .post_signed(
                &self.spot_host.clone(),
                "/v1/order/orders/submitCancelClientOrder",
                &body,
            )
            .await?;
        Ok(())
    }
}
