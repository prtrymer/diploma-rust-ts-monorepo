use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    db_con::bootstrap::run().await
}
