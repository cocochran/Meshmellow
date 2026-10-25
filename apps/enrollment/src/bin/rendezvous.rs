use anyhow::{Context, Result};
use meshthing_enrollment::rendezvous;

#[tokio::main]
async fn main() -> Result<()> {
    let directory = std::path::PathBuf::from(
        std::env::args()
            .nth(1)
            .context("Usage: layercake-rendezvous PRIVATE_STATE_DIRECTORY")?,
    );
    std::fs::create_dir_all(&directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let endpoint = llm_cluster::network::bind_endpoint(
        &directory.join("identity.key"),
        vec![rendezvous::ALPN.to_vec()],
    )
    .await?;
    println!("LAYERCAKE_RENDEZVOUS_ID={}", endpoint.id());
    endpoint.online().await;
    println!("Invitation lookup online through Iroh. Keep this process running.");
    rendezvous::serve(endpoint).await;
    Ok(())
}
