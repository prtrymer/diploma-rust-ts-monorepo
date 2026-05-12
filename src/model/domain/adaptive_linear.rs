use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::RwLock;

use super::models::{Prediction, PredictionModel};
use crate::features::domain::models::FeatureSet;
use crate::trading::domain::events::SignalDirection;

pub struct AdaptiveLinearModel {
    feature_keys: Vec<String>,
    learning_rate: Decimal,
    weights: RwLock<HashMap<String, Decimal>>,
    bias: RwLock<Decimal>,
}

impl AdaptiveLinearModel {
    pub fn new(feature_keys: Vec<String>, learning_rate: Decimal) -> Self {
        let mut weights = HashMap::new();
        for k in &feature_keys {
            weights.insert(k.clone(), Decimal::ZERO);
        }
        Self {
            feature_keys,
            learning_rate,
            weights: RwLock::new(weights),
            bias: RwLock::new(Decimal::ZERO),
        }
    }

    async fn raw_score(&self, features: &FeatureSet) -> Decimal {
        let weights = self.weights.read().await;
        let bias = *self.bias.read().await;
        let mut score = bias;
        for key in &self.feature_keys {
            if let Some(v) = features.get_scalar(key) {
                let w = *weights.get(key).unwrap_or(&Decimal::ZERO);
                score += w * normalize(v);
            }
        }
        score.clamp(dec!(-1), dec!(1))
    }
}

#[async_trait]
impl PredictionModel for AdaptiveLinearModel {
    fn name(&self) -> &str {
        "adaptive_linear"
    }

    async fn predict(&self, features: &FeatureSet) -> Result<Prediction> {
        let score = self.raw_score(features).await;
        let abs = score.abs();
        let direction = if abs < dec!(0.03) {
            SignalDirection::Exit
        } else if score > Decimal::ZERO {
            SignalDirection::Long
        } else {
            SignalDirection::Short
        };
        Ok(Prediction {
            direction,
            confidence: abs.min(Decimal::ONE),
            model_name: self.name().to_string(),
        })
    }

    async fn learn(&self, features: &FeatureSet, realized_return: Decimal) -> Result<()> {
        let pred = self.raw_score(features).await;
        let target = realized_return.clamp(dec!(-1), dec!(1));
        let err = target - pred;

        let mut weights = self.weights.write().await;
        for key in &self.feature_keys {
            if let Some(v) = features.get_scalar(key) {
                let x = normalize(v);
                let w = weights.entry(key.clone()).or_insert(Decimal::ZERO);
                *w += self.learning_rate * err * x;
                *w = (*w).clamp(dec!(-2), dec!(2));
            }
        }
        let mut b = self.bias.write().await;
        *b += self.learning_rate * err;
        *b = b.clamp(dec!(-1), dec!(1));
        Ok(())
    }

    async fn save_checkpoint(&self, path: &str) -> Result<()> {
        let weights = self.weights.read().await.clone();
        let bias = *self.bias.read().await;
        let payload = serde_json::json!({
            "feature_keys": self.feature_keys,
            "learning_rate": self.learning_rate.to_string(),
            "bias": bias.to_string(),
            "weights": weights.into_iter().map(|(k,v)| (k, v.to_string())).collect::<HashMap<_,_>>()
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
        let mut weights_new = HashMap::new();
        if let Some(weights_obj) = v.get("weights").and_then(|x| x.as_object()) {
            for (k, vv) in weights_obj {
                if let Some(s) = vv.as_str() {
                    if let Ok(d) = s.parse::<Decimal>() {
                        weights_new.insert(k.clone(), d);
                    }
                }
            }
        }
        let bias = v
            .get("bias")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        *self.weights.write().await = weights_new;
        *self.bias.write().await = bias;
        Ok(())
    }
}

fn normalize(v: Decimal) -> Decimal {
    let x = v.to_f64().unwrap_or(0.0);
    let y = x.tanh();
    Decimal::from_f64_retain(y).unwrap_or(Decimal::ZERO)
}
