use std::sync::Arc;
use crate::features::domain::indicators::{
    BidAskSpreadProxyFeature, EmaFeature, LiquidityImbalanceProxyFeature, MacdFeature,
    MeanReversionFeature, MomentumFeature, OrderFlowProxyFeature, RsiFeature,
    VolatilityClusteringFeature, VolatilityFeature,
};
use crate::features::domain::registry::FeatureRegistry;

pub fn init_features() -> Arc<FeatureRegistry> {
    let mut feature_registry = FeatureRegistry::new();
    feature_registry.register("rsi_14".into(), Arc::new(RsiFeature::new(14)));
    feature_registry.register("ema_12".into(), Arc::new(EmaFeature::new(12)));
    feature_registry.register("ema_26".into(), Arc::new(EmaFeature::new(26)));
    feature_registry.register("macd".into(), Arc::new(MacdFeature::new()));
    feature_registry.register("momentum_5".into(), Arc::new(MomentumFeature::new(5)));
    feature_registry.register("momentum_20".into(), Arc::new(MomentumFeature::new(20)));
    feature_registry.register(
        "mean_reversion_20".into(),
        Arc::new(MeanReversionFeature::new(20)),
    );
    feature_registry.register("volatility_20".into(), Arc::new(VolatilityFeature::new(20)));
    feature_registry.register(
        "vol_cluster_5_20".into(),
        Arc::new(VolatilityClusteringFeature::new(5, 20)),
    );
    feature_registry.register(
        "bid_ask_spread_proxy".into(),
        Arc::new(BidAskSpreadProxyFeature::new()),
    );
    feature_registry.register(
        "liquidity_imbalance_proxy_20".into(),
        Arc::new(LiquidityImbalanceProxyFeature::new(20)),
    );
    feature_registry.register(
        "order_flow_proxy_20".into(),
        Arc::new(OrderFlowProxyFeature::new(20)),
    );
    Arc::new(feature_registry)
}
