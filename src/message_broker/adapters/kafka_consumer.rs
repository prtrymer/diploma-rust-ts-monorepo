use crate::message_broker::ports::{Consumer, Message};
use anyhow::Result;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer as KafkaConsumer, StreamConsumer};
use rdkafka::Message as KafkaMessage;

pub struct KafkaConsumerAdapter {
    consumer: StreamConsumer,
}

impl KafkaConsumerAdapter {
    pub fn new(brokers: &str, group_id: &str, topics: &[String]) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create()?;

        consumer.subscribe(&topics.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;

        Ok(Self { consumer })
    }
}

#[async_trait]
impl Consumer for KafkaConsumerAdapter {
    async fn consume(&self) -> Result<Option<Message>> {
        match self.consumer.recv().await {
            Ok(message) => {
                let payload = match message.payload_view::<str>() {
                    Some(Ok(s)) => s.to_string(),
                    Some(Err(e)) => {
                        tracing::error!(error = ?e, "failed to deserialize message payload");
                        return Ok(None);
                    }
                    None => {
                        tracing::warn!("empty message payload");
                        return Ok(None);
                    }
                };

                let key = message
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok().map(|s| s.to_string()));

                Ok(Some(Message {
                    topic: message.topic().to_string(),
                    partition: message.partition(),
                    offset: message.offset(),
                    payload,
                    key,
                }))
            }
            Err(e) => {
                tracing::error!(error = %e, "kafka consume error");
                Ok(None)
            }
        }
    }

    async fn commit(&self) -> Result<()> {
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }
}
