use anyhow::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub scylla_nodes: Vec<String>,
    pub scylla_keyspace: String,
    pub kafka_brokers: String,
    pub kafka_topic: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenv::dotenv().ok();

        Ok(Self {
            database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgresql://postgres:postgres@localhost:5432/testdb".to_string()
            }),
            scylla_nodes: env::var("SCYLLA_NODES")
                .unwrap_or_else(|_| "127.0.0.1:9042".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            scylla_keyspace: env::var("SCYLLA_KEYSPACE")
                .unwrap_or_else(|_| "market_data".to_string()),
            kafka_brokers: env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            kafka_topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "market-data-raw".to_string()),
        })
    }
}
