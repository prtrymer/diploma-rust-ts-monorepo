use std::sync::Arc;
use rust_decimal_macros::dec;
use crate::model::domain::ensemble::WeightedEnsembleModel;
use crate::model::domain::models::PredictionModel;
use crate::model::domain::random_forest_like::RandomForestLikeModel;

pub fn init_models(is_simulated: bool) -> Arc<dyn PredictionModel> {
    let feature_keys = vec![
        "ema_12".into(),
        "ema_26".into(),
        "rsi_14".into(),
        "macd".into(),
        "momentum_5".into(),
        "momentum_20".into(),
        "mean_reversion_20".into(),
        "volatility_20".into(),
        "vol_cluster_5_20".into(),
        "bid_ask_spread_proxy".into(),
        "liquidity_imbalance_proxy_20".into(),
        "order_flow_proxy_20".into(),
    ];
    let fast_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys.clone(),
        100,
        if is_simulated { 5 } else { 20 },
        128,
        8,
        4,
        0.005,
    )) as Arc<dyn PredictionModel>;
    let medium_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys.clone(),
        500,
        if is_simulated { 10 } else { 100 },
        256,
        12,
        6,
        0.006,
    )) as Arc<dyn PredictionModel>;
    let slow_rf = Arc::new(RandomForestLikeModel::new_with_params(
        feature_keys,
        2600,
        if is_simulated { 20 } else { 520 },
        1024,
        24,
        8,
        0.009,
    )) as Arc<dyn PredictionModel>;
    
    let heuristic_model = Arc::new(crate::model::domain::ensemble::EnsembleFeatureModel) as Arc<dyn PredictionModel>;

    let mut model_weights = vec![
        (fast_rf, dec!(0.30)),
        (medium_rf, dec!(0.25)),
        (slow_rf, dec!(0.20)),
    ];
    
    if is_simulated {
        model_weights.push((heuristic_model, dec!(0.25)));
    } else {
        model_weights.push((heuristic_model, dec!(0.05)));
    }

    Arc::new(WeightedEnsembleModel::new(
        model_weights,
        dec!(0.01),
    )) as Arc<dyn PredictionModel>
}
