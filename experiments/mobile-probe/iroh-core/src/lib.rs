use std::ffi::{CStr, c_char};
use std::path::Path;

use llm_cluster::network;
pub mod ring;

/// Blocks on a dedicated app thread. All strings must remain valid for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn meshthing_iroh_run(
    key_path: *const c_char,
    allowed_peer: *const c_char,
    leader_peer: *const c_char,
    next_peer: *const c_char,
    status_path: *const c_char,
    target_port: u16,
) -> i32 {
    if key_path.is_null()
        || allowed_peer.is_null()
        || leader_peer.is_null()
        || next_peer.is_null()
        || status_path.is_null()
    {
        return 2;
    }
    let result = std::panic::catch_unwind(|| -> anyhow::Result<()> {
        let key = unsafe { CStr::from_ptr(key_path) }.to_str()?;
        let allowed = unsafe { CStr::from_ptr(allowed_peer) }.to_str()?;
        let leader = unsafe { CStr::from_ptr(leader_peer) }.to_str()?;
        let next = unsafe { CStr::from_ptr(next_peer) }.to_str()?;
        let status = unsafe { CStr::from_ptr(status_path) }.to_str()?;
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_writer(std::io::stderr)
            .try_init();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let peer = network::parse_node_id(allowed)?;
            let leader = network::parse_node_id(leader)?;
            let next = network::parse_node_id(next)?;
            let endpoint =
                network::bind_endpoint(Path::new(key), vec![ring::ALPN.to_vec()]).await?;
            std::fs::write(status, format!("{}\n", endpoint.id()))?;
            ring::worker(
                endpoint,
                peer,
                leader,
                Some(next),
                ([127, 0, 0, 1], target_port).into(),
            )
            .await
        })
    });
    match result {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            eprintln!("iOS Iroh probe: {error:#}");
            1
        }
        Err(_) => {
            eprintln!("iOS Iroh probe panicked");
            1
        }
    }
}
