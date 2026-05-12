use anyhow::Result;
use std::sync::Arc;

use crate::database::domain::models::DailyCandle;
use crate::database::ports::repository::Repository;

pub struct DatabaseService {
    repository: Arc<dyn Repository>,
}

impl DatabaseService {
    pub fn new(repository: Arc<dyn Repository>) -> Self {
        Self { repository }
    }

    pub async fn save_daily_candle(&self, candle: &DailyCandle) -> Result<()> {
        self.repository.insert_daily_candle(candle).await
    }
}
