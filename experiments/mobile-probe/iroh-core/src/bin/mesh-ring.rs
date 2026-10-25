use anyhow::Context;
use llm_cluster::network;
use meshthing_ios_iroh_probe::ring;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        args.len() >= 6,
        "gateway KEY ENTRY TERMINAL PORT | worker KEY UPSTREAM LEADER NEXT_OR_DASH PORT"
    );
    let endpoint =
        network::bind_endpoint(std::path::Path::new(&args[2]), vec![ring::ALPN.to_vec()]).await?;
    eprintln!("ring node id: {}", endpoint.id());
    match args[1].as_str() {
        "gateway" => {
            ring::gateway(
                endpoint,
                network::parse_node_id(&args[3])?,
                network::parse_node_id(&args[4])?,
                args[5].parse()?,
            )
            .await
        }
        "worker" => {
            let next = if args[5] == "-" {
                None
            } else {
                Some(network::parse_node_id(&args[5])?)
            };
            let port: u16 = args.get(6).context("missing native port")?.parse()?;
            ring::worker(
                endpoint,
                network::parse_node_id(&args[3])?,
                network::parse_node_id(&args[4])?,
                next,
                ([127, 0, 0, 1], port).into(),
            )
            .await
        }
        _ => anyhow::bail!("unknown mode"),
    }
}
