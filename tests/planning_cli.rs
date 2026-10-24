use std::process::Command;

#[test]
fn final_peer_constraint_prevents_skipping_local_device() {
    let mut request: llm_cluster::planning::PlanningRequest =
        serde_json::from_str(include_str!("../examples/placement-profile.json")).unwrap();
    for peer in &mut request.peers {
        peer.memory_budget_bytes = 1_000_000_000;
    }
    request.peers[0].decode_us_per_layer = vec![1; 4];
    request.peers[1].decode_us_per_layer = vec![100; 4];
    request.links[0].transfer_us = 0;
    request.final_peer_id = None;
    let unconstrained = llm_cluster::planning::build_plan(&request, 1).unwrap();
    assert_eq!(unconstrained.placement.assignments.len(), 1);
    assert_eq!(unconstrained.placement.assignments[0].peer_id, "worker");
    request.final_peer_id = Some("leader".into());
    let constrained = llm_cluster::planning::build_plan(&request, 1).unwrap();
    assert_eq!(
        constrained.placement.assignments.last().unwrap().peer_id,
        "leader"
    );
    assert_eq!(constrained.placement.predicted_decode_us, 103);
    request.peers[1].healthy = false;
    assert_eq!(
        llm_cluster::planning::build_plan(&request, 1).unwrap_err(),
        llm_cluster::placement::PlanError::NoFeasiblePlacement
    );
    request.final_peer_id = Some("unknown".into());
    assert!(matches!(
        llm_cluster::planning::build_plan(&request, 1),
        Err(llm_cluster::placement::PlanError::InvalidInput(_))
    ));
}

#[test]
fn verifies_collected_evidence_and_rejects_budget_overrun() {
    let directory = std::env::temp_dir().join(format!("mesh-verify-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let plan_path = directory.join("plan.json");
    let evidence_path = directory.join("evidence.json");
    let plan = Command::new(env!("CARGO_BIN_EXE_llm-cluster"))
        .args(["plan", "examples/placement-profile.json"])
        .output()
        .unwrap();
    assert!(plan.status.success());
    std::fs::write(&plan_path, plan.stdout).unwrap();
    let mut evidence = serde_json::json!({
        "model_hash": "synthetic-model",
        "runtime_id": "synthetic-runtime",
        "context_tokens": 8192,
        "devices": [
            {"peer_id": "worker", "peak_bytes": 280000000, "compute_events": 10},
            {"peer_id": "leader", "peak_bytes": 280000000, "compute_events": 10}
        ]
    });
    for valid in [true, false] {
        if !valid {
            evidence["devices"][0]["peak_bytes"] = 300000001u64.into();
        }
        std::fs::write(&evidence_path, serde_json::to_vec(&evidence).unwrap()).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_llm-cluster"))
            .arg("verify")
            .arg(&plan_path)
            .arg("examples/placement-profile.json")
            .arg(&evidence_path)
            .output()
            .unwrap();
        assert_eq!(output.status.success(), valid);
        if valid {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                serde_json::json!({"verified": true})
            );
        } else {
            assert!(output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).contains("BudgetExceeded"));
        }
    }
    std::fs::remove_file(plan_path).unwrap();
    std::fs::remove_file(evidence_path).unwrap();
    std::fs::remove_dir(directory).unwrap();
}

#[test]
fn plans_from_json_and_reorders_device_mappings() {
    let output = Command::new(env!("CARGO_BIN_EXE_llm-cluster"))
        .args(["plan", "examples/placement-profile.json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["advisory"], true);
    assert_eq!(
        result["placement"]["assignments"].as_array().unwrap().len(),
        2
    );
    let args = result["server_args"].as_array().unwrap();
    let position = args.iter().position(|arg| arg == "--device").unwrap();
    assert_eq!(args[position + 1], "RPC0,MTL0");
}

#[test]
fn infeasible_request_exits_unsuccessfully_without_a_plan() {
    let output = Command::new(env!("CARGO_BIN_EXE_llm-cluster"))
        .args([
            "plan",
            "examples/placement-profile.json",
            "--min-peers",
            "3",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("NoFeasiblePlacement"));
}

#[test]
fn rejects_profile_typos_and_duplicate_device_mappings() {
    let mut input: serde_json::Value =
        serde_json::from_str(include_str!("../examples/placement-profile.json")).unwrap();
    input["peers"][0]["memory_buget_bytes"] = 1.into();
    assert!(
        serde_json::from_value::<llm_cluster::planning::PlanningRequest>(input.clone()).is_err()
    );
    input["peers"][0]
        .as_object_mut()
        .unwrap()
        .remove("memory_buget_bytes");
    input["devices"][0] = input["devices"][1].clone();
    let request = serde_json::from_value(input).unwrap();
    assert!(llm_cluster::planning::build_plan(&request, 2).is_err());
}
