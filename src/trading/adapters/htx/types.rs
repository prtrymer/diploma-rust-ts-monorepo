//! Типи запитів/відповідей HTX. Гроші — `Decimal` (інваріант 6): спот-API
//! віддає суми рядками, своп-API — JSON-числами; `Decimal` десеріалізує
//! обидва варіанти. Поля, яких може не бути в старих/нових версіях
//! відповіді, — `Option` з `default`, щоб зміна схеми не валила розбір.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ── Спільний конверт відповіді ────────────────────────────────────────────────

/// HTX повертає HTTP 200 навіть на логічні помилки; справжній статус — у тілі.
/// v1: `{"status":"ok"|"error", "err-code", "err-msg", "data"}` (спот — з
/// дефісами, своп — з підкресленнями); v2: `{"code":200, "message", "data"}`;
/// маркет-дані: `{"status":"ok", "tick": {...}}`.
#[derive(Debug, Deserialize)]
pub struct Envelope<T> {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub code: Option<i64>,
    #[serde(rename = "err-code", alias = "err_code", default)]
    pub err_code: Option<serde_json::Value>,
    #[serde(rename = "err-msg", alias = "err_msg", default)]
    pub err_msg: Option<serde_json::Value>,
    #[serde(default)]
    pub message: Option<String>,
    // Option-поля без default: serde сам ставить None за відсутності,
    // а явний default тягнув би зайвий бонд T: Default.
    pub data: Option<T>,
    pub tick: Option<T>,
}

impl<T> Envelope<T> {
    pub fn into_data(self, ctx: &str) -> anyhow::Result<T> {
        let ok = self.status.as_deref() == Some("ok") || self.code == Some(200);
        if !ok {
            anyhow::bail!(
                "HTX {}: err-code={} err-msg={}",
                ctx,
                self.err_code
                    .or(self.code.map(Into::into))
                    .unwrap_or_default(),
                self.err_msg
                    .or(self.message.map(Into::into))
                    .unwrap_or_default()
            );
        }
        self.data
            .or(self.tick)
            .ok_or_else(|| anyhow::anyhow!("HTX {ctx}: порожнє data у відповіді"))
    }
}

// ── Спот ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SpotAccount {
    pub id: i64,
    #[serde(rename = "type")]
    pub account_type: String,
    pub state: String,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalanceData {
    pub list: Vec<SpotBalanceEntry>,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalanceEntry {
    pub currency: String,
    #[serde(rename = "type")]
    pub balance_type: String,
    pub balance: Decimal,
}

/// Метадані спот-пари з /v2/settings/common/symbols. Поля в API — абревіатури.
#[derive(Debug, Clone, Deserialize)]
pub struct SpotSymbolMeta {
    /// Код пари, напр. "btcusdt".
    pub sc: String,
    #[serde(default)]
    pub state: Option<String>,
    /// Точність кількості (знаків після коми).
    #[serde(default)]
    pub tap: Option<u32>,
    /// Точність ціни.
    #[serde(default)]
    pub tpp: Option<u32>,
    /// Мінімальна кількість базової монети в ордері.
    #[serde(default)]
    pub minoa: Option<Decimal>,
    /// Мінімальний нотіонал ордера (в котирувальній валюті).
    #[serde(default)]
    pub minov: Option<Decimal>,
    /// Мінімальна кількість для sell-market.
    #[serde(default)]
    pub smminoa: Option<Decimal>,
    /// Максимальний нотіонал для buy-market.
    #[serde(default)]
    pub bmmaxov: Option<Decimal>,
}

/// Стан API-ключа з /v2/user/api-key: права, IP-прив'язка, термін дії.
#[derive(Debug, Deserialize)]
pub struct ApiKeyInfo {
    #[serde(rename = "accessKey", default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
    /// Кома-розділений список: readOnly, trade, withdraw.
    #[serde(default)]
    pub permission: Option<String>,
    /// Кома-розділені прив'язані IP; порожньо = без прив'язки.
    #[serde(rename = "ipAddresses", default)]
    pub ip_addresses: Option<String>,
    /// Днів до деактивації за НЕАКТИВНОСТІ; -1 = безстроковий (IP-прив'язаний).
    /// Використання ключа скидає лічильник на 90.
    #[serde(rename = "validDays", default)]
    pub valid_days: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
}

impl ApiKeyInfo {
    pub fn has_withdraw(&self) -> bool {
        self.permission
            .as_deref()
            .map(|p| p.to_lowercase().contains("withdraw"))
            .unwrap_or(false)
    }
    pub fn is_ip_bound(&self) -> bool {
        self.ip_addresses
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }
}

#[derive(Debug, Deserialize)]
pub struct SpotFeeRate {
    pub symbol: String,
    #[serde(rename = "actualMakerRate", default)]
    pub actual_maker_rate: Option<Decimal>,
    #[serde(rename = "actualTakerRate", default)]
    pub actual_taker_rate: Option<Decimal>,
}

/// Верх стакана з /market/detail/merged: bid/ask = [ціна, кількість].
#[derive(Debug, Deserialize)]
pub struct SpotMergedTick {
    pub bid: (Decimal, Decimal),
    pub ask: (Decimal, Decimal),
}

/// POST /v1/order/orders/place. Спот v1 чекає числа рядками.
#[derive(Debug, Serialize)]
pub struct SpotOrderRequest {
    #[serde(rename = "account-id")]
    pub account_id: String,
    pub symbol: String,
    /// buy-limit | sell-limit | buy-limit-maker (post-only) |
    /// sell-limit-maker | buy-market | sell-market.
    /// УВАГА: для buy-market `amount` — сума в КОТИРУВАЛЬНІЙ валюті (USDT),
    /// для sell-market — кількість БАЗОВОЇ монети.
    #[serde(rename = "type")]
    pub order_type: String,
    pub amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub source: String,
    #[serde(rename = "client-order-id")]
    pub client_order_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SpotOrderInfo {
    pub id: i64,
    #[serde(default)]
    pub state: Option<String>,
    /// Так, в API історично "field-", а не "filled-" (у частині
    /// ендпоінтів уже виправлено — приймаємо обидва написання).
    #[serde(rename = "field-amount", alias = "filled-amount", default)]
    pub filled_amount: Option<Decimal>,
    #[serde(rename = "field-cash-amount", alias = "filled-cash-amount", default)]
    pub filled_cash_amount: Option<Decimal>,
    #[serde(rename = "field-fees", alias = "filled-fees", default)]
    pub filled_fees: Option<Decimal>,
}

impl SpotOrderInfo {
    pub fn is_filled(&self) -> bool {
        self.state.as_deref() == Some("filled")
    }
    pub fn is_open(&self) -> bool {
        matches!(
            self.state.as_deref(),
            Some("created" | "submitted" | "partial-filled")
        )
    }
}

// ── USDT-M своп (linear swap) ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct SwapContractInfo {
    pub contract_code: String,
    /// Скільки базової монети в 1 контракті (обсяг ордера — ЦІЛІ контракти).
    pub contract_size: Decimal,
    pub price_tick: Decimal,
    /// 1 = торгується.
    #[serde(default)]
    pub contract_status: Option<i32>,
    #[serde(default)]
    pub support_margin_mode: Option<String>,
}

impl SwapContractInfo {
    pub fn is_trading(&self) -> bool {
        self.contract_status.unwrap_or(0) == 1
    }
    pub fn supports_cross(&self) -> bool {
        matches!(self.support_margin_mode.as_deref(), Some("all" | "cross") | None)
    }
}

#[derive(Debug, Deserialize)]
pub struct SwapFundingRate {
    pub contract_code: String,
    /// Поточна ставка за 8г-інтервал; біржа інколи віддає null.
    #[serde(default)]
    pub funding_rate: Option<Decimal>,
    #[serde(default)]
    pub estimated_rate: Option<Decimal>,
    #[serde(default)]
    pub funding_time: Option<String>,
}

/// Стакан свопу: рівні = [ціна, контракти].
#[derive(Debug, Deserialize)]
pub struct SwapDepthTick {
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

/// Сторінка історії funding (новіші записи першими; page_size максимум 100).
#[derive(Debug, Deserialize)]
pub struct SwapHistoricalFundingPage {
    pub total_page: u32,
    pub current_page: u32,
    pub total_size: u32,
    pub data: Vec<SwapHistoricalFundingRow>,
}

#[derive(Debug, Deserialize)]
pub struct SwapHistoricalFundingRow {
    pub contract_code: String,
    /// Ставка розрахунку; live-перевірка показала, що realized_rate — null,
    /// тож джерело правди — funding_rate.
    #[serde(default)]
    pub funding_rate: Option<Decimal>,
    #[serde(default)]
    pub realized_rate: Option<Decimal>,
    /// Час розрахунку, мс епохи, рядком.
    pub funding_time: String,
}

impl SwapHistoricalFundingRow {
    pub fn settled_rate(&self) -> Option<Decimal> {
        self.funding_rate.or(self.realized_rate)
    }
    pub fn time_ms(&self) -> Option<i64> {
        self.funding_time.parse().ok()
    }
}

/// Свічка свопу; `id` — час СТАРТУ бара в секундах епохи.
#[derive(Debug, Deserialize)]
pub struct SwapKline {
    pub id: i64,
    pub open: Decimal,
    pub close: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct SwapCrossAccountEntry {
    #[serde(default)]
    pub margin_account: Option<String>,
    #[serde(default)]
    pub margin_balance: Option<Decimal>,
    #[serde(default)]
    pub margin_static: Option<Decimal>,
    #[serde(default)]
    pub withdraw_available: Option<Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SwapCrossPosition {
    pub contract_code: String,
    /// Обсяг у КОНТРАКТАХ.
    pub volume: Decimal,
    /// Доступно до закриття (контракти).
    #[serde(default)]
    pub available: Option<Decimal>,
    /// "buy" (лонг) | "sell" (шорт).
    pub direction: String,
    #[serde(default)]
    pub cost_hold: Option<Decimal>,
    #[serde(default)]
    pub profit_unreal: Option<Decimal>,
    #[serde(default)]
    pub lever_rate: Option<u32>,
}

/// POST /linear-swap-api/v1/swap_cross_order.
#[derive(Debug, Serialize)]
pub struct SwapOrderRequest {
    pub contract_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// ЦІЛА кількість контрактів (мінімум 1).
    pub volume: i64,
    /// buy | sell.
    pub direction: String,
    /// open | close (hedge-режим — дефолт HTX). В one-way режимі — "both".
    pub offset: String,
    pub lever_rate: u32,
    /// limit | post_only | opponent (тейкер по стакану) | ...
    pub order_price_type: String,
    /// У свопах client_order_id — ЧИСЛО (i64), не рядок; живе ~8 годин.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SwapOrderAck {
    #[serde(default)]
    pub order_id_str: Option<String>,
    #[serde(default)]
    pub client_order_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SwapOrderInfo {
    #[serde(default)]
    pub order_id_str: Option<String>,
    /// 3 submitted, 4 partial, 5 partial+cancelled, 6 filled, 7 cancelled, 11 cancelling.
    #[serde(default)]
    pub status: Option<i32>,
    #[serde(default)]
    pub trade_volume: Option<Decimal>,
    #[serde(default)]
    pub trade_avg_price: Option<Decimal>,
    #[serde(default)]
    pub fee: Option<Decimal>,
}

impl SwapOrderInfo {
    pub fn is_filled(&self) -> bool {
        self.status == Some(6)
    }
    pub fn is_open(&self) -> bool {
        matches!(self.status, Some(3 | 4 | 11))
    }
    pub fn status_label(&self) -> &'static str {
        match self.status {
            Some(3) => "виставлено",
            Some(4) => "частково виконано",
            Some(5) => "частково виконано і скасовано",
            Some(6) => "виконано",
            Some(7) => "скасовано",
            Some(11) => "скасовується",
            _ => "невідомо",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Своп віддає числа JSON-числами, спот — рядками; Decimal розбирає обидва.
    #[test]
    fn decimal_parses_numbers_and_strings() {
        let c: SwapContractInfo = serde_json::from_str(
            r#"{"contract_code":"BTC-USDT","contract_size":0.001,"price_tick":0.1,"contract_status":1}"#,
        )
        .unwrap();
        assert_eq!(c.contract_size, dec!(0.001));
        assert!(c.is_trading());

        let b: SpotBalanceEntry =
            serde_json::from_str(r#"{"currency":"usdt","type":"trade","balance":"12.34"}"#)
                .unwrap();
        assert_eq!(b.balance, dec!(12.34));
    }

    #[test]
    fn envelope_v1_error_is_error() {
        let e: Envelope<serde_json::Value> = serde_json::from_str(
            r#"{"status":"error","err-code":"order-value-min-error","err-msg":"too small"}"#,
        )
        .unwrap();
        assert!(e.into_data("test").is_err());
    }

    #[test]
    fn envelope_swap_error_uses_underscores() {
        let e: Envelope<serde_json::Value> =
            serde_json::from_str(r#"{"status":"error","err_code":1047,"err_msg":"margin"}"#)
                .unwrap();
        let msg = format!("{:#}", e.into_data("swap").unwrap_err());
        assert!(msg.contains("1047"));
    }

    #[test]
    fn envelope_v2_code_200_is_ok() {
        let e: Envelope<Vec<i32>> = serde_json::from_str(r#"{"code":200,"data":[1]}"#).unwrap();
        assert_eq!(e.into_data("test").unwrap(), vec![1]);
    }

    #[test]
    fn envelope_market_tick_is_data() {
        let e: Envelope<SpotMergedTick> = serde_json::from_str(
            r#"{"status":"ok","tick":{"bid":[100.5,2.0],"ask":[100.6,1.5]}}"#,
        )
        .unwrap();
        let t = e.into_data("merged").unwrap();
        assert_eq!(t.bid.0, dec!(100.5));
    }
}
