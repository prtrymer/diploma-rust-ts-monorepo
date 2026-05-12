use anyhow::Result;
use async_trait::async_trait;
use candle_core::{Device, Tensor};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::path::Path;
use tokio::sync::RwLock;

use super::models::{Prediction, PredictionModel};
use crate::features::domain::models::FeatureSet;
use crate::trading::domain::events::SignalDirection;

pub struct CandleLinearModel {
    feature_keys: Vec<String>,
    learning_rate: f32,
    decision_threshold: f32,
    target_gain: f32,
    weights: RwLock<Vec<f32>>,
    bias: RwLock<f32>,
}

impl CandleLinearModel {
    pub fn new(feature_keys: Vec<String>, learning_rate: f32, decision_threshold: f32) -> Self {
        let weights = vec![0.0; feature_keys.len()];
        Self {
            feature_keys,
            learning_rate,
            decision_threshold,
            target_gain: 150.0,
            weights: RwLock::new(weights),
            bias: RwLock::new(0.0),
        }
    }

    fn feature_vec(&self, features: &FeatureSet) -> Vec<f32> {
        self.feature_keys
            .iter()
            .map(|k| {
                features
                    .get_scalar(k)
                    .unwrap_or(Decimal::ZERO)
                    .to_f32()
                    .unwrap_or(0.0)
                    .tanh()
            })
            .collect()
    }

    fn compute_score_with_candle(&self, x: &[f32], w: &[f32], b: f32) -> Result<f32> {
        if x.is_empty() || w.is_empty() || x.len() != w.len() {
            return Ok(0.0);
        }
        let device = Device::Cpu;
        let x_t = Tensor::from_vec(x.to_vec(), (1, x.len()), &device)?;
        let w_t = Tensor::from_vec(w.to_vec(), (x.len(), 1), &device)?;
        let out = x_t.matmul(&w_t)?.to_vec2::<f32>()?;
        let raw = out
            .first()
            .and_then(|row| row.first())
            .copied()
            .unwrap_or(0.0)
            + b;
        Ok(raw.tanh().clamp(-1.0, 1.0))
    }

    async fn raw_score(&self, features: &FeatureSet) -> Result<f32> {
        let x = self.feature_vec(features);
        let w = self.weights.read().await.clone();
        let b = *self.bias.read().await;
        self.compute_score_with_candle(&x, &w, b)
    }
}

#[async_trait]
impl PredictionModel for CandleLinearModel {
    fn name(&self) -> &str {
        "candle_linear"
    }

    async fn predict(&self, features: &FeatureSet) -> Result<Prediction> {
        let score = self.raw_score(features).await?;
        let abs = score.abs();
        let direction = if abs < self.decision_threshold {
            SignalDirection::Exit
        } else if score > 0.0 {
            SignalDirection::Long
        } else {
            SignalDirection::Short
        };
        Ok(Prediction {
            direction,
            confidence: Decimal::from_f32_retain(abs).unwrap_or(Decimal::ZERO),
            model_name: self.name().to_string(),
        })
    }

    async fn learn(&self, features: &FeatureSet, realized_return: Decimal) -> Result<()> {
        let x = self.feature_vec(features);
        if x.is_empty() {
            return Ok(());
        }

        let pred = self.raw_score(features).await?;
        let target = (realized_return.to_f32().unwrap_or(0.0) * self.target_gain)
            .tanh()
            .clamp(-1.0, 1.0);
        let err = target - pred;

        let mut weights = self.weights.write().await;
        for (i, xi) in x.iter().enumerate() {
            weights[i] = (weights[i] + self.learning_rate * err * *xi).clamp(-3.0, 3.0);
        }
        let mut bias = self.bias.write().await;
        *bias = (*bias + self.learning_rate * err).clamp(-1.0, 1.0);
        Ok(())
    }

    async fn save_checkpoint(&self, path: &str) -> Result<()> {
        let weights = self.weights.read().await.clone();
        let bias = *self.bias.read().await;
        let payload = serde_json::json!({
            "feature_keys": self.feature_keys,
            "learning_rate": self.learning_rate,
            "decision_threshold": self.decision_threshold,
            "bias": bias,
            "weights": weights
        });
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(&payload)?)?;
        Ok(())
    }

    async fn load_checkpoint(&self, path: &str) -> Result<()> {
        let data = std::fs::read_to_string(path)?;
        let v: serde_json::Value = serde_json::from_str(&data)?;
        let parsed_weights = v
            .get("weights")
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|it| it.as_f64().map(|f| f as f32))
                    .collect::<Vec<f32>>()
            })
            .unwrap_or_default();
        if !parsed_weights.is_empty() {
            *self.weights.write().await = parsed_weights;
        }
        if let Some(b) = v.get("bias").and_then(|x| x.as_f64()) {
            *self.bias.write().await = b as f32;
        }
        Ok(())
    }
}
