use chrono::{DateTime, Utc};

/// Message wrapper for the broker service
#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

/// Raw consumed message from Kafka
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub timestamp: DateTime<Utc>,
}

impl ConsumedMessage {
    /// Convert key bytes to string if valid UTF-8
    pub fn key_as_string(&self) -> Option<String> {
        self.key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string())
    }

    /// Convert payload bytes to string if valid UTF-8
    pub fn payload_as_string(&self) -> Option<String> {
        self.payload
            .as_ref()
            .map(|p| String::from_utf8_lossy(p).to_string())
    }
}
