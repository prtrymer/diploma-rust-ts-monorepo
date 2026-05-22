use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use smartcore::ensemble::random_forest_regressor::{
    RandomForestRegressor, RandomForestRegressorParameters,
};
use smartcore::linalg::basic::matrix::DenseMatrix;
use std::collections::VecDeque;
use tokio::sync::RwLock;

use super::models::{Prediction, PredictionModel};
use crate::features::domain::models::FeatureSet;
use crate::trading::domain::events::SignalDirection;

pub struct RandomForestLikeModel {
    feature_keys: Vec<String>,
    window: usize,
    min_samples: usize,
    train_every: usize,
    n_trees: usize,
    max_depth: u16,
    decision_threshold: f64,
    samples: RwLock<VecDeque<(Vec<f64>, f64)>>,
    updates: RwLock<usize>,
    model: RwLock<Option<RandomForestRegressor<f64, f64, DenseMatrix<f64>, Vec<f64>>>>,
}

impl RandomForestLikeModel {
    pub fn new(feature_keys: Vec<String>, window: usize, min_samples: usize) -> Self {
        Self::new_with_params(feature_keys, window, min_samples, 512, 10, 5, 0.006)
    }

    pub fn new_with_params(
        feature_keys: Vec<String>,
        window: usize,
        min_samples: usize,
        train_every: usize,
        n_trees: usize,
        max_depth: u16,
        decision_threshold: f64,
    ) -> Self {
        Self {
            feature_keys,
            window,
            min_samples,
            train_every,
            n_trees,
            max_depth,
            decision_threshold,
            samples: RwLock::new(VecDeque::with_capacity(window)),
            updates: RwLock::new(0),
            model: RwLock::new(None),
        }
    }

    fn feature_vec(&self, features: &FeatureSet) -> Vec<f64> {
        self.feature_keys
            .iter()
            .map(|k| {
                features
                    .get_scalar(k)
                    .unwrap_or(Decimal::ZERO)
                    .to_f64()
                    .unwrap_or(0.0)
                    .tanh()
            })
            .collect()
    }
}

#[async_trait]
impl PredictionModel for RandomForestLikeModel {
    fn name(&self) -> &str {
        "random_forest_like"
    }

    async fn predict(&self, features: &FeatureSet) -> Result<Prediction> {
        let x = self.feature_vec(features);
        let maybe_model = self.model.read().await;
        let score = if let Some(model) = maybe_model.as_ref() {
            let m = DenseMatrix::new(1, x.len(), x.clone(), false)
                .map_err(|e| anyhow::anyhow!("matrix build failed: {}", e))?;
            model
                .predict(&m)?
                .first()
                .copied()
                .unwrap_or(0.0)
                .clamp(-1.0, 1.0)
        } else {
            0.0
        };
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
            confidence: Decimal::from_f64_retain(abs).unwrap_or(dec!(0)),
            model_name: self.name().to_string(),
        })
    }

    async fn learn(&self, features: &FeatureSet, realized_return: Decimal) -> Result<()> {
        let x = self.feature_vec(features);
        let y = realized_return.to_f64().unwrap_or(0.0).clamp(-1.0, 1.0);

        let mut s = self.samples.write().await;
        if s.len() >= self.window {
            s.pop_front();
        }
        s.push_back((x, y));
        if s.len() < self.min_samples {
            return Ok(());
        }
        drop(s);

        let mut updates = self.updates.write().await;
        *updates += 1;
        
        let needs_initial_train = self.model.read().await.is_none();
        if *updates % self.train_every != 0 && !needs_initial_train {
            return Ok(());
        }
        drop(updates);

        let s = self.samples.read().await;
        let rows = s.len();
        let cols = self.feature_keys.len();
        let mut x_all = Vec::with_capacity(rows * cols);
        let mut y_all = Vec::with_capacity(rows);
        for (fx, fy) in s.iter() {
            x_all.extend_from_slice(fx);
            y_all.push(*fy);
        }
        drop(s);

        let x_m = DenseMatrix::new(rows, cols, x_all, false)
            .map_err(|e| anyhow::anyhow!("matrix build failed: {}", e))?;
        let params = RandomForestRegressorParameters::default()
            .with_n_trees(self.n_trees)
            .with_max_depth(self.max_depth);
        if let Ok(rf) = RandomForestRegressor::fit(&x_m, &y_all, params) {
            *self.model.write().await = Some(rf);
        }
        Ok(())
    }
}
