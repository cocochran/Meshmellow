use llm_cluster::{placement::*, rpc_plan::*};

fn main() -> Result<(), PlanError> {
    // Synthetic numbers for API demonstration only, not hardware measurements.
    let model = ModelProfile {
        model_hash: "synthetic-model".into(),
        runtime_id: "synthetic-runtime".into(),
        context_tokens: 8192,
        layers: vec![
            LayerMemory {
                weight_bytes: 100_000_000,
                kv_bytes_per_token: 1024
            };
            4
        ],
    };
    let peers = ["worker", "leader"].map(|id| PeerProfile {
        id: id.into(),
        model_hash: model.model_hash.clone(),
        runtime_id: model.runtime_id.clone(),
        context_tokens: model.context_tokens,
        authorized: true,
        healthy: true,
        memory_budget_bytes: 300_000_000,
        reserved_bytes: 50_000_000,
        decode_us_per_layer: vec![1000; 4],
    });
    let links = [LinkProfile {
        from: "worker".into(),
        to: "leader".into(),
        transfer_us: 500,
    }];
    let placement = plan(&model, &peers, &links, 2)?;
    println!("Synthetic placement (not executed): {placement:#?}");
    // Replace both names with --list-devices output from the actual runtime.
    let devices = [
        RuntimeDevice {
            peer_id: "worker".into(),
            device_name: "RPC_DEVICE_FROM_DISCOVERY".into(),
            rpc_endpoint: Some("127.0.0.1:50053".parse().unwrap()),
        },
        RuntimeDevice {
            peer_id: "leader".into(),
            device_name: "LOCAL_DEVICE_FROM_DISCOVERY".into(),
            rpc_endpoint: None,
        },
    ];
    println!(
        "Advisory argv (not executed): {:?}",
        server_args(&placement, &devices, "/path/to/verified.gguf")?
    );
    Ok(())
}
