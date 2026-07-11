//! Application-сервіс порівняння з бенчмарками (M0.2).
//!
//! Будь-який портфельний прогін проходить тут: основна стратегія + всі три
//! бенчмарки (BuyAndHold, EqualWeight, 60/40) ганяються ОДНИМ движком,
//! з ОДНІЄЮ моделлю витрат, на тих самих даних і періоді. Звіт завжди
//! містить їх поруч — PnL без baseline більше не існує.

use anyhow::Result;
use rust_decimal::Decimal;
use std::sync::Arc;

use crate::backtest::domain::multi_report::{ComparativeReport, StrategySummaryRow};
use crate::backtest::domain::portfolio_engine::{PortfolioBacktester, PortfolioRunResult};
use crate::trading::adapters::benchmark_strategies::{BuyAndHold, EqualWeight, SixtyForty};
use crate::trading::adapters::portfolio_manager::PortfolioManager;
use crate::trading::domain::allocation::{AllocationStrategy, AlignedMarketData};
use crate::trading::ports::{BrokerSimulatorPort, PortfolioPort};

/// Місячний ребаланс для бенчмарків із періодичним ребалансом.
const BENCHMARK_REBALANCE_BARS: usize = 21;

pub struct BenchmarkRunner {
    data: Arc<AlignedMarketData>,
    broker: Arc<dyn BrokerSimulatorPort>,
    initial_capital: Decimal,
}

impl BenchmarkRunner {
    pub fn new(
        data: Arc<AlignedMarketData>,
        broker: Arc<dyn BrokerSimulatorPort>,
        initial_capital: Decimal,
    ) -> Self {
        Self {
            data,
            broker,
            initial_capital,
        }
    }

    /// Прогін стратегії у вікні [start_idx, end_idx) + бенчмарки за ТОЙ САМИЙ
    /// період і символи, тим самим движком і моделлю витрат.
    pub async fn run_with_benchmarks(
        &self,
        strategy: &mut dyn AllocationStrategy,
        allow_short: bool,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<ComparativeReport> {
        let main = self
            .run_one(strategy, allow_short, start_idx, end_idx)
            .await?;

        let universe: Vec<String> = self.data.symbols().cloned().collect();
        let mut benchmarks: Vec<StrategySummaryRow> = Vec::new();
        let mut bh = BuyAndHold::new();
        benchmarks.push((&self.run_one(&mut bh, false, start_idx, end_idx).await?).into());
        let mut ew = EqualWeight::new(BENCHMARK_REBALANCE_BARS);
        benchmarks.push((&self.run_one(&mut ew, false, start_idx, end_idx).await?).into());
        let mut sf = SixtyForty::all_equity(universe, BENCHMARK_REBALANCE_BARS);
        benchmarks.push((&self.run_one(&mut sf, false, start_idx, end_idx).await?).into());

        Ok(ComparativeReport {
            strategy: (&main).into(),
            benchmarks,
        })
    }

    async fn run_one(
        &self,
        strategy: &mut dyn AllocationStrategy,
        allow_short: bool,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<PortfolioRunResult> {
        // Кожен прогін — свіжий портфель; брокер (і його CostModel) СПІЛЬНИЙ.
        let portfolio: Arc<dyn PortfolioPort> = if allow_short {
            Arc::new(PortfolioManager::new_allowing_short(self.initial_capital))
        } else {
            Arc::new(PortfolioManager::new(self.initial_capital))
        };
        let engine = PortfolioBacktester::new(
            self.data.clone(),
            self.broker.clone(),
            portfolio,
            self.initial_capital,
        );
        engine.run_window(strategy, start_idx, end_idx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::run_config::CostConfig;
    use crate::trading::adapters::broker_simulator::SimpleBrokerSimulator;
    use crate::trading::domain::costs::cost_model_from_config;
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    fn single_symbol_data(prices: Vec<Decimal>) -> Arc<AlignedMarketData> {
        let t0 = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let timestamps: Vec<_> = (0..prices.len())
            .map(|i| t0 + Duration::days(i as i64))
            .collect();
        let mut closes = BTreeMap::new();
        closes.insert("ONLY".to_string(), prices);
        Arc::new(AlignedMarketData::from_closes(timestamps, closes).unwrap())
    }

    fn broker(costs: &CostConfig) -> Arc<dyn BrokerSimulatorPort> {
        Arc::new(SimpleBrokerSimulator {
            slippage_pct: Decimal::ZERO,
            cost_model: cost_model_from_config(costs),
        })
    }

    // M0.2: BuyAndHold на одному активі без ребалансу ≈ проста зміна ціни
    // мінус one-off витрати (вхід + фінальне закриття).
    #[tokio::test]
    async fn buy_and_hold_equals_price_change_minus_oneoff_costs() {
        // 100 → 120: +20%.
        let n = 40;
        let prices: Vec<Decimal> = (0..n)
            .map(|i| dec!(100) + Decimal::from(i as u64) * dec!(20) / Decimal::from((n - 1) as u64))
            .collect();
        let data = single_symbol_data(prices);
        let costs = CostConfig {
            commission_fixed: dec!(1),
            commission_pct: Decimal::ZERO,
            spread_pct: Decimal::ZERO,
            impact: None,
        };
        let runner = BenchmarkRunner::new(data, broker(&costs), dec!(100000));
        let mut bh = BuyAndHold::new();
        let result = runner.run_one(&mut bh, false, 0, 40).await.unwrap();

        // Очікування: ~+20% від вкладеного, мінус 2 фіксовані комісії.
        // Вкладено ~100000 (округлення кількості вниз): net ≈ 20000 − 2.
        let net = result.report.total_return;
        assert!(
            net > dec!(19900) && net < dec!(20010),
            "BuyAndHold net {net} має бути ≈ 20000 − one-off costs"
        );
        assert_eq!(result.report.total_costs, dec!(2), "рівно 2 угоди: вхід і вихід");
        // Угод рівно 2 → turnover мінімальний.
        assert_eq!(result.fills.len(), 2);
    }

    // M0.2: звіт містить усі три бенчмарки поруч зі стратегією.
    #[tokio::test]
    async fn comparative_report_contains_all_benchmarks() {
        let prices: Vec<Decimal> = (0..60).map(|i| dec!(100) + Decimal::from(i as u64)).collect();
        let data = single_symbol_data(prices);
        let runner = BenchmarkRunner::new(data, broker(&CostConfig::default()), dec!(100000));
        let mut strategy = EqualWeight::new(10); // будь-яка стратегія
        let report = runner
            .run_with_benchmarks(&mut strategy, false, 0, 60)
            .await
            .unwrap();
        let names: Vec<&str> = report.benchmarks.iter().map(|b| b.name.as_str()).collect();
        assert!(names.contains(&"buy_and_hold"));
        assert!(names.contains(&"equal_weight"));
        assert!(names.contains(&"sixty_forty"));
        let rendered = report.render();
        assert!(rendered.contains("buy_and_hold"));
        assert!(rendered.contains("NetRet%"));
        assert!(rendered.contains("GrossRet%"));
    }
}
