//! USDT-M своп-ендпоінти HTX (хост `swap_host`, крос-маржа).

use anyhow::Result;
use rust_decimal::Decimal;

use super::client::HtxClient;
use super::types::*;

impl HtxClient {
    /// Усі USDT-M свопи: contract_size, price_tick, статус.
    pub async fn swap_contracts(&self) -> Result<Vec<SwapContractInfo>> {
        self.get_public(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_contract_info",
            &[("business_type".to_string(), "swap".to_string())],
        )
        .await
    }

    /// Поточні funding-ставки всіх контрактів одним запитом.
    pub async fn swap_funding_rates(&self) -> Result<Vec<SwapFundingRate>> {
        self.get_public(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_batch_funding_rate",
            &[],
        )
        .await
    }

    /// Верх стакана свопу: (bid, ask).
    pub async fn swap_best_bid_ask(&self, contract_code: &str) -> Result<(Decimal, Decimal)> {
        let tick: SwapDepthTick = self
            .get_public(
                &self.swap_host.clone(),
                "/linear-swap-ex/market/depth",
                &[
                    ("contract_code".to_string(), contract_code.to_string()),
                    ("type".to_string(), "step0".to_string()),
                ],
            )
            .await?;
        let bid = tick.bids.first().map(|l| l.0).unwrap_or_default();
        let ask = tick.asks.first().map(|l| l.0).unwrap_or_default();
        anyhow::ensure!(
            bid > Decimal::ZERO && ask > Decimal::ZERO,
            "порожній стакан {contract_code}"
        );
        Ok((bid, ask))
    }

    /// Сторінка історії settled funding (новіші першими). page_size ≤ 100 —
    /// більше біржа мовчки обрізає до 100.
    pub async fn swap_historical_funding(
        &self,
        contract_code: &str,
        page_index: u32,
        page_size: u32,
    ) -> Result<SwapHistoricalFundingPage> {
        self.get_public(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_historical_funding_rate",
            &[
                ("contract_code".to_string(), contract_code.to_string()),
                ("page_index".to_string(), page_index.to_string()),
                ("page_size".to_string(), page_size.to_string()),
            ],
        )
        .await
    }

    /// 4h-свічки за [from, to] у секундах. Ліміт біржі ≈2000 барів на запит:
    /// ширший діапазон повертає ПОРОЖНЬО (не помилку) — вікна ріже викликач.
    pub async fn swap_klines_4h(
        &self,
        contract_code: &str,
        from_sec: i64,
        to_sec: i64,
    ) -> Result<Vec<SwapKline>> {
        self.get_public(
            &self.swap_host.clone(),
            "/linear-swap-ex/market/history/kline",
            &[
                ("contract_code".to_string(), contract_code.to_string()),
                ("period".to_string(), "4hour".to_string()),
                ("from".to_string(), from_sec.to_string()),
                ("to".to_string(), to_sec.to_string()),
            ],
        )
        .await
    }

    /// Стан крос-маржинального рахунку (USDT).
    pub async fn swap_cross_account(&self) -> Result<Vec<SwapCrossAccountEntry>> {
        let body = serde_json::json!({ "margin_account": "USDT" });
        self.post_signed(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_cross_account_info",
            &body,
        )
        .await
    }

    /// Відкриті крос-позиції (обсяги в контрактах).
    pub async fn swap_cross_positions(&self) -> Result<Vec<SwapCrossPosition>> {
        let body = serde_json::json!({});
        self.post_signed(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_cross_position_info",
            &body,
        )
        .await
    }

    pub async fn swap_place_cross_order(&self, req: &SwapOrderRequest) -> Result<SwapOrderAck> {
        self.post_signed(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_cross_order",
            req,
        )
        .await
    }

    /// Стан ордера за client_order_id (звірка перед будь-яким ретраєм).
    pub async fn swap_cross_order_info(
        &self,
        contract_code: &str,
        client_order_id: i64,
    ) -> Result<Vec<SwapOrderInfo>> {
        let body = serde_json::json!({
            "contract_code": contract_code,
            "client_order_id": client_order_id.to_string(),
        });
        self.post_signed(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_cross_order_info",
            &body,
        )
        .await
    }

    pub async fn swap_cross_cancel(
        &self,
        contract_code: &str,
        client_order_id: i64,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "contract_code": contract_code,
            "client_order_id": client_order_id.to_string(),
        });
        self.post_signed(
            &self.swap_host.clone(),
            "/linear-swap-api/v1/swap_cross_cancel",
            &body,
        )
        .await
    }
}
