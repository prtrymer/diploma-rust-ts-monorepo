//! Adapter біржі HTX (ex-Huobi): REST-підпис v2, спот + USDT-M своп,
//! і виконавець тижневого carry-кошика зі shadow-журналу.
//!
//! Запуск — бінарник `htx-exec` (check | plan | positions | execute | config).
//! Реальна відправка ордерів подвійно закрита: підкоманда `execute` +
//! `HTX_TRADING_ENABLED=true` в env. Ключі — `HTX_ACCESS_KEY`/`HTX_SECRET_KEY`
//! (створювати БЕЗ права на вивід коштів і з прив'язкою до IP).

pub mod client;
pub mod executor;
pub mod funding_history;
pub mod selector;
pub mod signing;
pub mod spot;
pub mod swap;
pub mod types;

pub use client::HtxClient;
pub use executor::{build_plan, HtxCarryExecutor, LedgerBasketEntry, PlanInputs, RebalancePlan};
