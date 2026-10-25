//! The packaged executable runs the core and its dashboard in one process.
use clap::Parser;
#[derive(Parser)]
#[command(about = "Run the Layercake mesh, OpenAI API, and browser dashboard")]
struct Cli {
    #[command(flatten)]
    options: llm_cluster::ui::Options,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    llm_cluster::ui::run(Cli::parse().options).await
}
