//! Фабрика allocation-стратегій за назвою з конфіга.

use rust_decimal_macros::dec;

use super::QuantArgs;
use crate::trading::adapters::benchmark_strategies::{BuyAndHold, EqualWeight, SixtyForty};
use crate::trading::adapters::cross_sectional_momentum::CrossSectionalMomentum;
use crate::trading::adapters::tsmom_strategy::TsmomStrategy;
use crate::trading::domain::allocation::AllocationStrategy;

pub fn build_strategy(args: &QuantArgs, universe: Vec<String>) -> (Box<dyn AllocationStrategy>, bool) {
    let s = &args.config.strategy;
    match args.strategy.as_str() {
        "tsmom" => (
            Box::new(TsmomStrategy::new(
                s.momentum_lookback,
                20,
                s.vol_target_annual,
                21,
                !s.long_only,
            )),
            !s.long_only,
        ),
        "xsmom" => (
            Box::new(CrossSectionalMomentum::new(
                s.momentum_lookback,
                21,
                s.xs_quantile,
                21,
                dec!(0.02),
                dec!(1.0),
            )),
            true, // dollar-neutral потребує шортів
        ),
        "equal_weight" => (Box::new(EqualWeight::new(21)), false),
        "sixty_forty" => (Box::new(SixtyForty::all_equity(universe, 21)), false),
        _ => (Box::new(BuyAndHold::new()), false),
    }
}
