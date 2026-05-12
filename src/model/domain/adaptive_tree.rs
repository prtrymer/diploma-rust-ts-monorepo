use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::RwLock;

use super::models::{Prediction, PredictionModel};
use crate::features::domain::models::FeatureSet;
use crate::trading::domain::events::SignalDirection;

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct BucketStats {
    sum: f64,
    count: u64,
}

pub struct AdaptiveTreeModel {
    feature_keys: Vec<String>,
    buckets: usize,
    learning_rate: f64,
    stats: RwLock<HashMap<String, Vec<BucketStats>>>,
}

impl AdaptiveTreeModel {
    pub fn new(feature_keys: Vec<String>, buckets: usize, learning_rate: f64) -> Self {
        let mut stats = HashMap::new();
        for k in &feature_keys {
            stats.insert(k.clone(), vec![BucketStats::default(); buckets]);
        }
        Self {
            feature_keys,
            buckets,
            learning_rate,
            stats: RwLock::new(stats),
        }
    }

    fn to_bucket(&self, v: Decimal) -> usize {
        let x = v.to_f64().unwrap_or(0.0).tanh();
        let normalized = ((x + 1.0) / 2.0).clamp(0.0, 0.999_999);
        (normalized * self.buckets as f64) as usize
    }
}

#[async_trait]
impl PredictionModel for AdaptiveTreeModel {
    fn name(&self) -> &str {
        "adaptive_tree"
    }

    async fn predict(&self, features: &FeatureSet) -> Result<Prediction> {
        let stats = self.stats.read().await;
        let mut score = 0.0;
        let mut used = 0.0;
        for k in &self.feature_keys {
            if let Some(v) = features.get_scalar(k) {
                let b = self.to_bucket(v);
                if let Some(vecs) = stats.get(k) {
                    let s = &vecs[b];
                    if s.count > 5 {
                        score += s.sum / s.count as f64;
                        used += 1.0;
                    }
                }
            }
        }
        if used > 0.0 {
            score /= used;
        }
        score = score.clamp(-1.0, 1.0);
        let conf = score.abs();
        let direction = if conf < 0.025 {
            SignalDirection::Exit
        } else if score > 0.0 {
            SignalDirection::Long
        } else {
            SignalDirection::Short
        };
        Ok(Prediction {
            direction,
            confidence: Decimal::from_f64_retain(conf).unwrap_or(Decimal::ZERO),
            model_name: self.name().to_string(),
        })
    }

    async fn learn(&self, features: &FeatureSet, realized_return: Decimal) -> Result<()> {
        let target = realized_return.to_f64().unwrap_or(0.0).clamp(-1.0, 1.0);
        let mut stats = self.stats.write().await;
        for k in &self.feature_keys {
            if let Some(v) = features.get_scalar(k) {
                let b = self.to_bucket(v);
                if let Some(vecs) = stats.get_mut(k) {
                    let cell = &mut vecs[b];
                    let prev_mean = if cell.count > 0 {
                        cell.sum / cell.count as f64
                    } else {
                        0.0
                    };
                    let updated = prev_mean + self.learning_rate * (target - prev_mean);
                    cell.sum += updated;
                    cell.count += 1;
                }
            }
        }
        Ok(())
    }

    async fn save_checkpoint(&self, path: &str) -> Result<()> {
        let stats = self.stats.read().await.clone();
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(&stats)?)?;
        Ok(())
    }

    async fn load_checkpoint(&self, path: &str) -> Result<()> {
        let data = std::fs::read_to_string(path)?;
        let parsed: HashMap<String, Vec<BucketStats>> = serde_json::from_str(&data)?;
        *self.stats.write().await = parsed;
        Ok(())
    }
}
