use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use super::indicators::Feature;
use super::models::{FeatureSet, FeatureValue};
use crate::database::domain::models::Candle;

pub struct FeatureRegistry {
    features: HashMap<String, Arc<dyn Feature>>,
}

impl FeatureRegistry {
    pub fn new() -> Self {
        Self {
            features: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, feature: Arc<dyn Feature>) {
        self.features.insert(name, feature);
    }

    pub fn calculate_all(&self, symbol: &str, data: &[Candle]) -> Result<FeatureSet> {
        let mut set = FeatureSet::new(symbol.to_string());
        for (name, feature) in &self.features {
            let value = feature.calculate(data)?;
            set.insert(name.clone(), value);
        }
        Ok(set)
    }

    pub fn calculate_one(&self, name: &str, data: &[Candle]) -> Result<Option<FeatureValue>> {
        match self.features.get(name) {
            Some(feature) => Ok(Some(feature.calculate(data)?)),
            None => Ok(None),
        }
    }
}
