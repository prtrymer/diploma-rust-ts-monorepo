use std::collections::HashMap;
use std::sync::Arc;

use super::models::PredictionModel;

pub struct ModelRegistry {
    models: HashMap<String, Arc<dyn PredictionModel>>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, model: Arc<dyn PredictionModel>) {
        self.models.insert(name, model);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn PredictionModel>> {
        self.models.get(name).cloned()
    }
}
