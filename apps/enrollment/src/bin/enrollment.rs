use anyhow::{Context, Result};
#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::env::args()
        .nth(1)
        .context("Usage: meshmellow-enrollment PRIVATE_STATE_DIRECTORY")?;
    llm_cluster::mesh::cli::run(dir.into()).await
}
