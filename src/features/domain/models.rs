use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeatureValue {
    Scalar(Decimal),
    Vector(Vec<Decimal>),
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSet {
    pub symbol: String,
    pub features: HashMap<String, FeatureValue>,
}

impl FeatureSet {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            features: HashMap::new(),
        }
    }

    pub fn insert(&mut self, name: String, value: FeatureValue) {
        self.features.insert(name, value);
    }

    pub fn get_scalar(&self, name: &str) -> Option<Decimal> {
        match self.features.get(name) {
            Some(FeatureValue::Scalar(v)) => Some(*v),
            _ => None,
        }
    }

    pub fn get_vector(&self, name: &str) -> Option<&[Decimal]> {
        match self.features.get(name) {
            Some(FeatureValue::Vector(v)) => Some(v.as_slice()),
            _ => None,
        }
    }
}
