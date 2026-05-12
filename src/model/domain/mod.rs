pub mod adaptive_linear;
pub mod adaptive_tree;
#[cfg(feature = "candle-ml")]
pub mod candle_linear;
pub mod ensemble;
pub mod models;
pub mod random_forest_like;
pub mod registry;
