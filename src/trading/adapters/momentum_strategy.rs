use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use uuid::Uuid;

use crate::database::domain::models::{Candle, StockTick, Timeframe};
use crate::features::domain::registry::FeatureRegistry;
use crate::model::domain::models::PredictionModel;
use crate::trading::domain::events::SignalEvent;
use crate::trading::ports::StrategyPort;

pub struct MomentumStrategy {
    name: String,
    feature_registry: Arc<FeatureRegistry>,
    model: Arc<dyn PredictionModel>,
    lookback: HashMap<String, VecDeque<Candle>>,
    last_features: HashMap<String, crate::features::domain::models::FeatureSet>,
    last_price: HashMap<String, Decimal>,
    last_signal_direction: HashMap<String, crate::trading::domain::events::SignalDirection>,
    last_signal_time: HashMap<String, chrono::DateTime<Utc>>,
    lookback_size: usize,
    min_confidence: Decimal,
    min_signal_gap_secs: i64,
    emit_signals: bool,
    learning_enabled: bool,
    long_only: bool,
}

impl MomentumStrategy {
    pub fn new(
        feature_registry: Arc<FeatureRegistry>,
        model: Arc<dyn PredictionModel>,
        lookback_size: usize,
        min_confidence: Decimal,
        min_signal_gap_secs: i64,
    ) -> Self {
        Self {
            name: "momentum".to_string(),
            feature_registry,
            model,
            lookback: HashMap::new(),
            last_features: HashMap::new(),
            last_price: HashMap::new(),
            last_signal_direction: HashMap::new(),
            last_signal_time: HashMap::new(),
            lookback_size,
            min_confidence,
            min_signal_gap_secs,
            emit_signals: true,
            learning_enabled: true,
            long_only: true,
        }
    }

    pub fn reset_state(&mut self) {
        self.lookback.clear();
        self.last_features.clear();
        self.last_price.clear();
        self.last_signal_direction.clear();
        self.last_signal_time.clear();
    }

    pub fn set_emit_signals(&mut self, emit: bool) {
        self.emit_signals = emit;
    }

    pub fn set_long_only(&mut self, long_only: bool) {
        self.long_only = long_only;
    }

    pub fn set_learning_enabled(&mut self, enabled: bool) {
        self.learning_enabled = enabled;
    }
}

#[async_trait]
impl StrategyPort for MomentumStrategy {
    async fn on_market_event(&mut self, tick: &StockTick) -> Result<Option<SignalEvent>> {
        let candle = Candle {
            symbol: tick.symbol.clone(),
            timestamp: tick.timestamp,
            timeframe: Timeframe::Tick,
            open: tick.price,
            high: tick.price,
            low: tick.price,
            close: tick.price,
            volume: tick.volume,
            trades_count: Some(1),
            vwap: None,
        };

        self.add_candle(candle).await;

        let buffer = self.lookback.get(&tick.symbol).unwrap();
        if buffer.len() < self.lookback_size {
            return Ok(None);
        }

        let data: Vec<Candle> = buffer.iter().cloned().collect();
        let features = self.feature_registry.calculate_all(&tick.symbol, &data)?;

        if self.learning_enabled {
            if let (Some(prev_features), Some(prev_price)) = (
                self.last_features.get(&tick.symbol),
                self.last_price.get(&tick.symbol),
            ) {
                if *prev_price > Decimal::ZERO {
                    let realized_return = (tick.price - *prev_price) / *prev_price;
                    let _ = self.model.learn(prev_features, realized_return).await;
                }
            }
        }
        self.last_features
            .insert(tick.symbol.clone(), features.clone());
        self.last_price.insert(tick.symbol.clone(), tick.price);

        let prediction = self.model.predict(&features).await?;

        if !self.emit_signals {
            return Ok(None);
        }

        println!(
            "📈 {} — {:?} confidence={:.4} (min={:.4})",
            tick.symbol, prediction.direction, prediction.confidence, self.min_confidence
        );

        if prediction.confidence < self.min_confidence {
            return Ok(None);
        }

        let direction = if self.long_only
            && matches!(
                prediction.direction,
                crate::trading::domain::events::SignalDirection::Short
            ) {
            crate::trading::domain::events::SignalDirection::Exit
        } else {
            prediction.direction
        };
        if matches!(
            self.last_signal_direction.get(&tick.symbol),
            Some(prev) if *prev == direction
        ) {
            return Ok(None);
        }

        if let Some(last_ts) = self.last_signal_time.get(&tick.symbol) {
            let gap = tick.timestamp.signed_duration_since(*last_ts).num_seconds();
            if gap < self.min_signal_gap_secs {
                return Ok(None);
            }
        }

        self.last_signal_direction
            .insert(tick.symbol.clone(), direction);
        self.last_signal_time
            .insert(tick.symbol.clone(), tick.timestamp);

        // Ринковий контекст для моделі витрат (M0.1): середній обсяг з
        // lookback-буфера і волатильність із уже порахованих фіч.
        let avg_volume = {
            let buf = self.lookback.get(&tick.symbol).unwrap();
            if buf.is_empty() {
                None
            } else {
                let total: i64 = buf.iter().map(|c| c.volume).sum();
                Some(Decimal::from(total) / Decimal::from(buf.len() as u64))
            }
        };
        let volatility = features.get_scalar("volatility_20");
        let market_context = Some(crate::trading::domain::costs::MarketContext {
            avg_volume,
            volatility,
            spread_pct: features.get_scalar("bid_ask_spread_proxy"),
        });

        // Детермінований id: похідна від символу+часу+напряму (інваріант 4).
        let id_seed = format!(
            "{}|{}|{:?}|{}",
            tick.symbol,
            tick.timestamp.timestamp_nanos_opt().unwrap_or_default(),
            direction,
            self.name
        );
        Ok(Some(SignalEvent {
            id: Uuid::new_v5(&Uuid::NAMESPACE_OID, id_seed.as_bytes()),
            timestamp: tick.timestamp,
            symbol: tick.symbol.clone(),
            direction,
            strength: prediction.confidence,
            strategy_name: self.name.clone(),
            metadata: Some(serde_json::to_string(&serde_json::json!({
                "last_price": tick.price.to_string()
            }))?),
            market_context,
        }))
    }

    async fn warmup(&mut self, candles: Vec<Candle>) -> Result<()> {
        for candle in candles {
            self.add_candle(candle).await;
        }
        Ok(())
    }

    async fn is_warmed_up(&self, symbol: &str) -> bool {
        if let Some(buffer) = self.lookback.get(symbol) {
            buffer.len() >= self.lookback_size
        } else {
            false
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl MomentumStrategy {
    async fn add_candle(&mut self, candle: Candle) {
        let buffer = self
            .lookback
            .entry(candle.symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(self.lookback_size));

        if buffer.len() >= self.lookback_size {
            buffer.pop_front();
        }
        buffer.push_back(candle);
    }
}
