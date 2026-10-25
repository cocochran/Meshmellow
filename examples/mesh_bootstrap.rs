//! Operator bootstrap for two explicitly authorized devices, retaining their identities.
use anyhow::{Context, Result, ensure};
use iroh::SecretKey;
use llm_cluster::mesh::membership::Ledger;
use std::{fs, io::Write, path::Path};

fn private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)?;
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 4,
        "usage: mesh_bootstrap ORIGIN_HEX_KEY WORKER_HEX_KEY OUTPUT_DIRECTORY"
    );
    let origin: SecretKey = fs::read_to_string(&args[1])?
        .trim()
        .parse()
        .context("origin key")?;
    let worker: SecretKey = fs::read_to_string(&args[2])?
        .trim()
        .parse()
        .context("worker key")?;
    ensure!(
        origin.public() != worker.public(),
        "devices must have different identities"
    );
    let mut ledger = Ledger::create(&origin, "Layercake shared inference", "MacBook")?;
    ledger.admit(
        &origin,
        &worker.public().to_string(),
        "Taxpayers CUDA",
        &uuid::Uuid::new_v4().simple().to_string(),
    )?;
    let record = serde_json::to_vec_pretty(&serde_json::json!({
        "ledger": ledger, "invite": null, "requests": [], "joining": null, "notice": ""
    }))?;
    for (name, key) in [("mac-state", &origin), ("linux-state", &worker)] {
        let directory = Path::new(&args[3]).join(name);
        fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        private_file(&directory.join("identity.key"), &key.to_bytes())?;
        private_file(&directory.join("membership.json"), &record)?;
    }
    println!("Origin: {}\nWorker: {}", origin.public(), worker.public());
    Ok(())
}
