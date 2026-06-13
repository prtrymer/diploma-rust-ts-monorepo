use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::models::{Prediction, PredictionModel};
use crate::features::domain::models::FeatureSet;
use crate::trading::domain::events::SignalDirection;

pub struct EnsembleFeatureModel;

#[async_trait]
impl PredictionModel for EnsembleFeatureModel {
    fn name(&self) -> &str {
        "ensemble_feature_model"
    }

    async fn predict(&self, features: &FeatureSet) -> Result<Prediction> {
        let score = ensemble_score(features);
        let abs = score.abs();

        let direction = if abs < dec!(0.01) {
            SignalDirection::Exit
        } else if score > Decimal::ZERO {
            SignalDirection::Long
        } else {
            SignalDirection::Short
        };

        let confidence = abs.min(Decimal::ONE);
        Ok(Prediction {
            direction,
            confidence,
            model_name: self.name().to_string(),
        })
    }
}

fn ensemble_score(features: &FeatureSet) -> Decimal {
    let trend = trend_component(features);
    let mean_rev = mean_reversion_component(features);
    let momentum = momentum_component(features);
    let micro = microstructure_component(features);
    let volatility_penalty = volatility_penalty(features);

    // Weighted average approximation of an ensemble/meta-model.
    let mut score =
        dec!(0.32) * trend + dec!(0.23) * mean_rev + dec!(0.25) * momentum + dec!(0.20) * micro;
    score *= Decimal::ONE - volatility_penalty;
    score.clamp(dec!(-1), dec!(1))
}

fn trend_component(f: &FeatureSet) -> Decimal {
    let ema12 = f.get_scalar("ema_12");
    let ema26 = f.get_scalar("ema_26");
    let macd = f.get_scalar("macd");

    let mut out = Decimal::ZERO;
    if let (Some(s), Some(l)) = (ema12, ema26) {
        if l > Decimal::ZERO {
            out += ((s - l) / l).clamp(dec!(-0.1), dec!(0.1)) * dec!(6);
        }
    }
    if let Some(m) = macd {
        out += m.clamp(dec!(-1), dec!(1)) * dec!(0.25);
    }
    out.clamp(dec!(-1), dec!(1))
}

fn mean_reversion_component(f: &FeatureSet) -> Decimal {
    let rsi = f.get_scalar("rsi_14");
    let z = f.get_scalar("mean_reversion_20");
    let mut out = Decimal::ZERO;

    if let Some(r) = rsi {
        // Oversold => bullish; overbought => bearish.
        out += ((dec!(50) - r) / dec!(50)).clamp(dec!(-1), dec!(1)) * dec!(0.7);
    }
    if let Some(zs) = z {
        out += (-zs).clamp(dec!(-1), dec!(1)) * dec!(0.3);
    }
    out.clamp(dec!(-1), dec!(1))
}

fn momentum_component(f: &FeatureSet) -> Decimal {
    let m5 = f.get_scalar("momentum_5").unwrap_or(Decimal::ZERO);
    let m20 = f.get_scalar("momentum_20").unwrap_or(Decimal::ZERO);
    (m5 * dec!(15) + m20 * dec!(6)).clamp(dec!(-1), dec!(1))
}

fn microstructure_component(f: &FeatureSet) -> Decimal {
    let of = f.get_scalar("order_flow_proxy_20").unwrap_or(Decimal::ZERO);
    let liq = f
        .get_scalar("liquidity_imbalance_proxy_20")
        .unwrap_or(Decimal::ZERO);
    let spread = f
        .get_scalar("bid_ask_spread_proxy")
        .unwrap_or(Decimal::ZERO)
        .clamp(Decimal::ZERO, dec!(0.05));
    (of.clamp(dec!(-1), dec!(1)) * dec!(0.65) + liq.clamp(dec!(-1), dec!(1)) * dec!(0.35))
        * (Decimal::ONE - spread * dec!(10))
}

fn volatility_penalty(f: &FeatureSet) -> Decimal {
    let vol = f.get_scalar("volatility_20").unwrap_or(Decimal::ZERO);
    let cluster = f.get_scalar("vol_cluster_5_20").unwrap_or(Decimal::ONE);
    let mut p = (vol * dec!(12)).clamp(Decimal::ZERO, dec!(0.4));
    if cluster > dec!(1.5) {
        p += dec!(0.1);
    }
    p.clamp(Decimal::ZERO, dec!(0.6))
}

#[allow(dead_code)]
fn _to_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

pub struct WeightedEnsembleModel {
    pub models: Vec<(Arc<dyn PredictionModel>, Decimal)>,
    pub adaptive_weights: RwLock<Vec<Decimal>>,
    adaptive_learning_enabled: RwLock<bool>,
    signal_deadzone: Decimal,
    pub learning_rate: Decimal,
}

impl WeightedEnsembleModel {
    pub fn new(models: Vec<(Arc<dyn PredictionModel>, Decimal)>, learning_rate: Decimal) -> Self {
        Self::new_with_signal_deadzone(models, learning_rate, dec!(0.001))
    }

    pub fn new_with_signal_deadzone(
        models: Vec<(Arc<dyn PredictionModel>, Decimal)>,
        learning_rate: Decimal,
        signal_deadzone: Decimal,
    ) -> Self {
        let adaptive_weights = models.iter().map(|(_, w)| *w).collect();
        Self {
            models,
            adaptive_weights: RwLock::new(adaptive_weights),
            adaptive_learning_enabled: RwLock::new(true),
            signal_deadzone: signal_deadzone.clamp(Decimal::ZERO, Decimal::ONE),
            learning_rate,
        }
    }

    async fn to_score(&self, p: &Prediction) -> Decimal {
        let sign = match p.direction {
            SignalDirection::Long => Decimal::ONE,
            SignalDirection::Short => -Decimal::ONE,
            SignalDirection::Exit => Decimal::ZERO,
        };
        sign * p.confidence.clamp(Decimal::ZERO, Decimal::ONE)
    }

    pub async fn set_weights(&self, new_weights: Vec<Decimal>) {
        let mut w = self.adaptive_weights.write().await;
        if w.len() != new_weights.len() {
            return;
        }
        let sum: Decimal = new_weights.iter().copied().sum();
        if sum <= Decimal::ZERO {
            return;
        }
        for (i, v) in new_weights.iter().enumerate() {
            w[i] = *v / sum;
        }
    }

    pub async fn set_adaptive_learning_enabled(&self, enabled: bool) {
        *self.adaptive_learning_enabled.write().await = enabled;
    }
}

#[async_trait]
impl PredictionModel for WeightedEnsembleModel {
    fn name(&self) -> &str {
        "weighted_ensemble"
    }

    async fn predict(&self, features: &FeatureSet) -> Result<Prediction> {
        let weights = self.adaptive_weights.read().await;
        let mut combined = Decimal::ZERO;
        let mut total_w = Decimal::ZERO;

        for (idx, (model, _)) in self.models.iter().enumerate() {
            let p = model.predict(features).await?;
            let s = self.to_score(&p).await;
            let w = *weights.get(idx).unwrap_or(&Decimal::ZERO);
            if w > Decimal::ZERO {
                combined += s * w;
                total_w += w;
            }
        }
        let score = if total_w > Decimal::ZERO {
            (combined / total_w).clamp(dec!(-1), dec!(1))
        } else {
            Decimal::ZERO
        };
        let abs = score.abs();
        let direction = if abs < self.signal_deadzone {
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
        let adaptive_learning_enabled = *self.adaptive_learning_enabled.read().await;
        if !adaptive_learning_enabled {
            for (model, _) in self.models.iter() {
                model.learn(features, realized_return).await?;
            }
            return Ok(());
        }

        let target = realized_return.clamp(dec!(-1), dec!(1));
        let mut weights = self.adaptive_weights.write().await;
        for (idx, (model, _)) in self.models.iter().enumerate() {
            let p = model.predict(features).await?;
            let pred_score = self.to_score(&p).await;
            let err = (target - pred_score).abs();
            let mut w = *weights.get(idx).unwrap_or(&Decimal::ZERO);
            w += self.learning_rate * (Decimal::ONE - err);
            w = w.clamp(dec!(0.01), dec!(0.95));
            weights[idx] = w;

            model.learn(features, realized_return).await?;
        }

        let sum: Decimal = weights.iter().copied().sum();
        if sum > Decimal::ZERO {
            for w in weights.iter_mut() {
                *w /= sum;
            }
        }
        Ok(())
    }

    async fn save_checkpoint(&self, path: &str) -> Result<()> {
        let weights = self.adaptive_weights.read().await.clone();
        let payload = serde_json::json!({
            "weights": weights.iter().map(|w| w.to_string()).collect::<Vec<_>>()
        });
        std::fs::create_dir_all(path)?;
        std::fs::write(
            format!("{}/ensemble.json", path),
            serde_json::to_string_pretty(&payload)?,
        )?;
        for (idx, (model, _)) in self.models.iter().enumerate() {
            model
                .save_checkpoint(&format!("{}/model_{}.json", path, idx))
                .await?;
        }
        Ok(())
    }

    async fn load_checkpoint(&self, path: &str) -> Result<()> {
        let p = format!("{}/ensemble.json", path);
        if let Ok(data) = std::fs::read_to_string(&p) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) {
                if let Some(arr) = v.get("weights").and_then(|x| x.as_array()) {
                    let mut w = self.adaptive_weights.write().await;
                    for (idx, val) in arr.iter().enumerate() {
                        if let Some(s) = val.as_str() {
                            if let Ok(d) = s.parse::<Decimal>() {
                                if idx < w.len() {
                                    w[idx] = d;
                                }
                            }
                        }
                    }
                }
            }
        }
        for (idx, (model, _)) in self.models.iter().enumerate() {
            let _ = model
                .load_checkpoint(&format!("{}/model_{}.json", path, idx))
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::domain::models::FeatureValue;
    use rust_decimal_macros::dec;

    struct ConstModel {
        label: String,
        direction: SignalDirection,
        confidence: Decimal,
    }

    #[async_trait]
    impl PredictionModel for ConstModel {
        fn name(&self) -> &str {
            &self.label
        }

        async fn predict(&self, _features: &FeatureSet) -> Result<Prediction> {
            Ok(Prediction {
                direction: self.direction,
                confidence: self.confidence,
                model_name: self.label.clone(),
            })
        }
    }

    fn const_model(direction: SignalDirection, confidence: Decimal) -> Arc<dyn PredictionModel> {
        Arc::new(ConstModel {
            label: "const".to_string(),
            direction,
            confidence,
        })
    }

    fn features() -> FeatureSet {
        FeatureSet::new("TEST".to_string())
    }

    #[tokio::test]
    async fn ensemble_combines_submodel_scores_by_weight() {
        let model = WeightedEnsembleModel::new(
            vec![
                (const_model(SignalDirection::Long, dec!(1)), dec!(0.75)),
                (const_model(SignalDirection::Short, dec!(1)), dec!(0.25)),
            ],
            dec!(0.01),
        );
        let p = model.predict(&features()).await.unwrap();
        assert_eq!(p.direction, SignalDirection::Long);
        assert_eq!(p.confidence, dec!(0.5));
    }

    #[tokio::test]
    async fn ensemble_emits_exit_within_signal_deadzone() {
        let model = WeightedEnsembleModel::new(
            vec![
                (const_model(SignalDirection::Long, dec!(1)), dec!(0.5)),
                (const_model(SignalDirection::Short, dec!(1)), dec!(0.5)),
            ],
            dec!(0.01),
        );
        let p = model.predict(&features()).await.unwrap();
        assert_eq!(p.direction, SignalDirection::Exit);
        assert_eq!(p.confidence, Decimal::ZERO);
    }

    #[tokio::test]
    async fn set_weights_normalizes_to_unit_sum() {
        let model = WeightedEnsembleModel::new(
            vec![
                (const_model(SignalDirection::Long, dec!(1)), dec!(1)),
                (const_model(SignalDirection::Short, dec!(1)), dec!(1)),
            ],
            dec!(0.01),
        );
        model.set_weights(vec![dec!(2), dec!(6)]).await;
        let w = model.adaptive_weights.read().await;
        assert_eq!(w[0], dec!(0.25));
        assert_eq!(w[1], dec!(0.75));
    }

    #[tokio::test]
    async fn learn_shifts_weight_toward_accurate_model() {
        let model = WeightedEnsembleModel::new(
            vec![
                (const_model(SignalDirection::Long, dec!(1)), dec!(0.5)),
                (const_model(SignalDirection::Short, dec!(1)), dec!(0.5)),
            ],
            dec!(0.05),
        );
        // realized return is positive: the Long submodel was right
        model.learn(&features(), dec!(1)).await.unwrap();
        let w = model.adaptive_weights.read().await;
        assert!(w[0] > w[1]);
    }

    #[tokio::test]
    async fn weights_remain_normalized_after_learning() {
        let model = WeightedEnsembleModel::new(
            vec![
                (const_model(SignalDirection::Long, dec!(1)), dec!(0.5)),
                (const_model(SignalDirection::Short, dec!(1)), dec!(0.5)),
            ],
            dec!(0.05),
        );
        model.learn(&features(), dec!(1)).await.unwrap();
        let w = model.adaptive_weights.read().await;
        let sum: Decimal = w.iter().copied().sum();
        assert_eq!(sum, dec!(1));
    }

    #[tokio::test]
    async fn heuristic_model_exits_on_empty_features() {
        let model = EnsembleFeatureModel;
        let p = model.predict(&features()).await.unwrap();
        assert_eq!(p.direction, SignalDirection::Exit);
    }

    #[tokio::test]
    async fn heuristic_model_goes_long_on_bullish_features() {
        let mut f = features();
        f.insert("ema_12".to_string(), FeatureValue::Scalar(dec!(105)));
        f.insert("ema_26".to_string(), FeatureValue::Scalar(dec!(100)));
        f.insert("macd".to_string(), FeatureValue::Scalar(dec!(0.5)));
        f.insert("momentum_5".to_string(), FeatureValue::Scalar(dec!(0.05)));
        f.insert("momentum_20".to_string(), FeatureValue::Scalar(dec!(0.08)));
        let p = EnsembleFeatureModel.predict(&f).await.unwrap();
        assert_eq!(p.direction, SignalDirection::Long);
        assert!(p.confidence > Decimal::ZERO);
    }

    #[tokio::test]
    async fn heuristic_model_goes_short_on_bearish_features() {
        let mut f = features();
        f.insert("ema_12".to_string(), FeatureValue::Scalar(dec!(95)));
        f.insert("ema_26".to_string(), FeatureValue::Scalar(dec!(100)));
        f.insert("macd".to_string(), FeatureValue::Scalar(dec!(-0.5)));
        f.insert("momentum_5".to_string(), FeatureValue::Scalar(dec!(-0.05)));
        f.insert("momentum_20".to_string(), FeatureValue::Scalar(dec!(-0.08)));
        let p = EnsembleFeatureModel.predict(&f).await.unwrap();
        assert_eq!(p.direction, SignalDirection::Short);
    }
}
