use llm_cluster::{placement::*, rpc_plan::*};

fn fixture() -> (Placement, Vec<RuntimeDevice>) {
    (
        Placement {
            model_hash: "hash".into(),
            runtime_id: "commit".into(),
            context_tokens: 8192,
            assignments: vec![
                Assignment {
                    peer_id: "remote".into(),
                    layers: 0..2,
                    estimated_bytes: 100,
                },
                Assignment {
                    peer_id: "local".into(),
                    layers: 2..6,
                    estimated_bytes: 200,
                },
            ],
            predicted_decode_us: 10,
        },
        vec![
            RuntimeDevice {
                peer_id: "remote".into(),
                device_name: "RPC[127.0.0.1:50053]Metal0".into(),
                rpc_endpoint: Some("127.0.0.1:50053".parse().unwrap()),
            },
            RuntimeDevice {
                peer_id: "local".into(),
                device_name: "Metal0".into(),
                rpc_endpoint: None,
            },
        ],
    )
}

#[test]
fn explicit_order_and_ratios_with_shell_safe_path() {
    let (placement, devices) = fixture();
    let path = "/models/a model $(touch nope).gguf";
    let args = server_args(&placement, &devices, path).unwrap();
    let option = |name: &str| args[args.iter().position(|a| a == name).unwrap() + 1].as_str();
    assert_eq!(option("--model"), path);
    assert_eq!(option("--rpc"), "127.0.0.1:50053");
    assert_eq!(option("--tensor-split"), "2,4");
    assert_eq!(option("--device"), "RPC[127.0.0.1:50053]Metal0,Metal0");
    assert_eq!(option("--parallel"), "1");
}

#[test]
fn refuses_order_mismatch_and_public_rpc_endpoints() {
    let (placement, mut devices) = fixture();
    devices.swap(0, 1);
    assert!(server_args(&placement, &devices, "model").is_err());
    devices.swap(0, 1);
    devices[0].rpc_endpoint = Some("192.168.1.2:50052".parse().unwrap());
    assert!(server_args(&placement, &devices, "model").is_err());
}

#[test]
fn rejects_gaps_duplicates_and_invalid_context() {
    let (mut placement, devices) = fixture();
    placement.assignments[1].layers = 3..6;
    assert!(server_args(&placement, &devices, "model").is_err());
    placement.assignments[1].layers = 2..6;
    placement.context_tokens = u64::MAX;
    assert!(server_args(&placement, &devices, "model").is_err());
    let (placement, mut devices) = fixture();
    devices[1].device_name = devices[0].device_name.clone();
    assert!(server_args(&placement, &devices, "model").is_err());
}
