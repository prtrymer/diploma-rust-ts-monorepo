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
    let strategy = Arc::new(RwLock::new(MomentumStrategy::new(
        feature_registry,
        model,
        lookback_size,
        dec!(0.001),
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
                    println!("  No historical candles found for {} in ScyllaDB. Fetching from API/Data Source...", symbol);
                    let now = chrono::Utc::now();
                    // Fetch lookback_size * 3 minutes to ensure we have enough candles to satisfy lookback
                    let start = now - chrono::Duration::minutes(lookback_size as i64 * 3);
                    match data_source.fetch_historical_quotes(symbol, start, now, "1m").await {
                        Ok(quotes) if !quotes.is_empty() => {
                            println!("  Fetched {} historical quotes from API for {}", quotes.len(), symbol);
                            let mut converted_candles = Vec::new();
                            for q in quotes {
                                let candle = Candle {
                                    symbol: symbol.clone(),
                                    timestamp: q.timestamp,
                                    timeframe: Timeframe::OneMin,
                                    open: Decimal::try_from(q.open).unwrap_or_default(),
                                    high: Decimal::try_from(q.high).unwrap_or_default(),
                                    low: Decimal::try_from(q.low).unwrap_or_default(),
                                    close: Decimal::try_from(q.close).unwrap_or_default(),
                                    volume: q.volume as i64,
                                    trades_count: None,
                                    vwap: None,
                                };
                                // Persist to ScyllaDB
                                if let Err(err) = repository.insert_candle_1min(&candle).await {
                                    eprintln!("  Failed to persist candle to stock_1min: {}", err);
                                }
                                if let Err(err) = repository.insert_candle(&candle).await {
                                    eprintln!("  Failed to persist candle to unified candles table: {}", err);
                                }
                                converted_candles.push(candle);
                            }
                            let mut strat = strategy.write().await;
                            if let Err(e) = strat.warmup(converted_candles).await {
                                eprintln!("  Failed to warm up {}: {}", symbol, e);
                            }
                        }
                        Ok(_) => {
                            eprintln!("  No real historical quotes returned from API for {}", symbol);
                        }
                        Err(e) => {
                            eprintln!("  Failed to fetch historical quotes from API for {}: {}", symbol, e);
                        }
                    }
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
