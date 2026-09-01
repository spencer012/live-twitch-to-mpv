#[tokio::main]
async fn main() -> anyhow::Result<()> {
    twitchpipe_core::cli::run().await
}
