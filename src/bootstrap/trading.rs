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
use crate::database::domain::models::{Candle, Timeframe};
use rust_decimal::Decimal;
use std::convert::TryFrom;

pub async fn init_trading_engine(
    feature_registry: Arc<FeatureRegistry>,
    model: Arc<dyn PredictionModel>,
    repository: &Arc<dyn Repository>,
    symbols_vec: &[String],
    is_simulated: bool,
    data_source: Arc<dyn DataSourcePort>,
) -> (
    Arc<RwLock<dyn StrategyPort>>,
    Arc<dyn BrokerSimulatorPort>,
    Arc<dyn PortfolioPort>,
    Arc<dyn ExecutionHandlerPort>,
    usize, // lookback_size
) {
    let lookback_size = if is_simulated { 10 } else { 30 };
    
    let min_confidence_str = std::env::var("MIN_CONFIDENCE").unwrap_or_else(|_| "0.05".to_string());
    let min_confidence = min_confidence_str.parse::<Decimal>().unwrap_or(dec!(0.05));
    println!("Initializing MomentumStrategy with min_confidence: {}", min_confidence);

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
        commission: dec!(1.00),
    }) as Arc<dyn BrokerSimulatorPort>;

    let portfolio_manager = Arc::new(PortfolioManager::new(dec!(100000))) as Arc<dyn PortfolioPort>;

    let execution_handler = Arc::new(SimpleExecutionHandler {
        default_quantity: dec!(0),
        max_position_pct: dec!(0.65),
        min_trade_quantity: dec!(1),
        stop_loss_pct: dec!(0.03),
        take_profit_pct: dec!(0.02),
        reserve_cash_pct: dec!(0.02),
        portfolio: Some(portfolio_manager.clone()),
    }) as Arc<dyn ExecutionHandlerPort>;

    (strategy, broker, portfolio_manager, execution_handler, lookback_size)
}
