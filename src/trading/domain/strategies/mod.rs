//! Стратегії — доменна логіка (інваріант 1: спільна для бектесту і live).
//! Усі чисті: без I/O, залежать лише від domain-типів і портів.

pub mod benchmark_strategies;
pub mod cross_sectional_momentum;
pub mod momentum_strategy;
pub mod tsmom_strategy;
