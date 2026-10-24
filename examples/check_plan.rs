//! Inspect a saved plan before starting any workers or model processes.
use llm_cluster::planning::{PlanningOutput, PlanningRequest, validate_output};

fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    anyhow::ensure!(
        args.len() == 2,
        "usage: check_plan <plan.json> <profile.json>"
    );
    let output: PlanningOutput = serde_json::from_str(&std::fs::read_to_string(&args[0])?)?;
    let request: PlanningRequest = serde_json::from_str(&std::fs::read_to_string(&args[1])?)?;
    validate_output(&request, &output, 2)
        .map_err(|error| anyhow::anyhow!("preflight failed: {error:?}"))?;
    anyhow::ensure!(
        !output.runtime_devices.is_empty(),
        "legacy plan: regenerate plan to include runtime_devices"
    );
    for device in &output.runtime_devices {
        if let Some(endpoint) = device.rpc_endpoint {
            llm_cluster::network::parse_node_id(&device.peer_id)?;
            println!(
                "remote {} -> {} ({})",
                device.peer_id, endpoint, device.device_name
            );
        } else {
            println!("local {} ({})", device.peer_id, device.device_name);
        }
    }
    println!("Preflight passed; no processes launched or runtime evidence verified.");
    Ok(())
}
