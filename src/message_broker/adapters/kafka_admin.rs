use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::time::Duration;

pub async fn ensure_topics(brokers: &str, topics: &[String]) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("Failed to create Kafka admin client")?;

    let new_topics: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1)))
        .collect();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    let _ = admin
        .create_topics(&new_topics, &opts)
        .await
        .context("Failed to ensure Kafka topics")?;
    Ok(())
}
