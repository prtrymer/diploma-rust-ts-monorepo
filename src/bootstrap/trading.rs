use std::sync::Arc;
use tokio::sync::RwLock;
use rust_decimal_macros::dec;
use crate::database::ports::repository::Repository;
use crate::features::domain::registry::FeatureRegistry;
use crate::model::domain::models::PredictionModel;
use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
use crate::trading::adapters::execution_handler::SimpleExecutionHandler;
use crate::trading::adapters::momentum_strategy::MomentumStrategy;
use crate::trading::adapters::portfolio_manager::PortfolioManager;
use crate::trading::ports::{BrokerSimulatorPort, ExecutionHandlerPort, PortfolioPort, StrategyPort};
use crate::data_ingestion::ports::DataSourcePort;
use rust_decimal::Decimal;

pub async fn init_trading_engine(
    feature_registry: Arc<FeatureRegistry>,
    model: Arc<dyn PredictionModel>,
    repository: &Arc<dyn Repository>,
    symbols_vec: &[String],
    is_simulated: bool,
    _data_source: Arc<dyn DataSourcePort>,
) -> (
    Arc<RwLock<dyn StrategyPort>>,
    Arc<dyn BrokerSimulatorPort>,
    Arc<dyn PortfolioPort>,
    Arc<dyn ExecutionHandlerPort>,
    usize, // lookback_size
) {
    // Єдине джерело правди для параметрів — RunConfig (M0.4). ENV може
    // переозначити min_confidence для live-деплою, але дефолт — з конфіга.
    let run_config = crate::shared::run_config::RunConfig::default();
    let lookback_size = if is_simulated { 10 } else { 30 };

    let min_confidence = std::env::var("MIN_CONFIDENCE")
        .ok()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(run_config.strategy.min_confidence);
    println!(
        "Initializing MomentumStrategy with min_confidence: {} (config hash {})",
        min_confidence,
        run_config.config_hash()
    );

    let strategy = Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry,
        model,
        lookback_size,
        min_confidence,
        if is_simulated { 2 } else { 10 },
    ))) as Arc<RwLock<dyn StrategyPort>>;

    // Warmup
    println!("Warming up strategy with historical data (lookback_size={})...", lookback_size);
    for symbol in symbols_vec {
        match repository.get_latest_candles(symbol, "1min", lookback_size).await {
            Ok(candles) => {
                if !candles.is_empty() {
                    println!("  Warming up {} with {} candles from ScyllaDB", symbol, candles.len());
                    let mut strat = strategy.write().await;
                    if let Err(e) = strat.warmup(candles).await {
                        eprintln!("  Failed to warm up {}: {}", symbol, e);
                    }
                } else {
                    println!("  No historical candles found for {} in ScyllaDB. Skipping direct API fetch (will be processed via Kafka startup warmup).", symbol);
                }
            }
            Err(e) => eprintln!("  Error fetching candles for {}: {}", symbol, e),
        }
    }

    let broker = Arc::new(SimpleBrokerSimulator {
        slippage_pct: dec!(0.001),
        cost_model: crate::trading::domain::costs::cost_model_from_config(&run_config.costs),
    }) as Arc<dyn BrokerSimulatorPort>;

    let portfolio_manager = Arc::new(PortfolioManager::new(dec!(100000))) as Arc<dyn PortfolioPort>;

    let execution_handler = Arc::new(SimpleExecutionHandler {
        sizer: crate::trading::domain::sizing::PositionSizer::from_config(
            run_config.sizing.clone(),
        ),
        portfolio: Some(portfolio_manager.clone()),
    }) as Arc<dyn ExecutionHandlerPort>;

    (strategy, broker, portfolio_manager, execution_handler, lookback_size)
}
