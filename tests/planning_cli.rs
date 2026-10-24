use std::process::Command;

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
