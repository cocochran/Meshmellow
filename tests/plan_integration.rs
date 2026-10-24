use llm_cluster::{planning::*, verification::*};

#[test]
fn launcher_fixture_passes_preflight_with_parseable_remote_identity() {
    let request: PlanningRequest =
        serde_json::from_str(include_str!("../examples/launcher-profile.json")).unwrap();
    let output: PlanningOutput =
        serde_json::from_str(include_str!("../examples/launcher-plan.json")).unwrap();
    assert_eq!(validate_output(&request, &output, 2), Ok(()));
    for device in output
        .runtime_devices
        .iter()
        .filter(|d| d.rpc_endpoint.is_some())
    {
        assert!(llm_cluster::network::parse_node_id(&device.peer_id).is_ok());
    }
}

fn fixture() -> (PlanningRequest, PlanningOutput, RunEvidence) {
    let request: PlanningRequest =
        serde_json::from_str(include_str!("../examples/placement-profile.json")).unwrap();
    let output = build_plan(&request, 2).unwrap();
    let evidence = RunEvidence {
        model_hash: request.model.model_hash.clone(),
        runtime_id: request.model.runtime_id.clone(),
        context_tokens: request.model.context_tokens,
        devices: request
            .peers
            .iter()
            .map(|p| DeviceEvidence {
                peer_id: p.id.clone(),
                peak_bytes: p.memory_budget_bytes,
                compute_events: 10,
            })
            .collect(),
    };
    (request, output, evidence)
}

#[test]
fn verifies_saved_plan_against_current_inputs() {
    let (request, output, evidence) = fixture();
    assert_eq!(verify_output(&request, &output, &evidence, 2), Ok(()));
}

#[test]
fn preflight_needs_no_runtime_evidence() {
    let (request, output, _) = fixture();
    assert_eq!(validate_output(&request, &output, 2), Ok(()));
    let mut request = request;
    request.peers[0].healthy = false;
    assert_eq!(
        validate_output(&request, &output, 2),
        Err(VerificationError::IneligiblePeer("worker".into()))
    );
}

#[test]
fn preflight_rejects_stale_topology_and_peer_order() {
    let (mut request, output, _) = fixture();
    request.links.clear();
    assert_eq!(
        validate_output(&request, &output, 2),
        Err(VerificationError::InvalidPlan)
    );
    let (mut request, output, _) = fixture();
    request.peers.reverse();
    assert_eq!(
        validate_output(&request, &output, 2),
        Err(VerificationError::InvalidPlan)
    );
}

#[test]
fn device_bindings_round_trip_in_assignment_order() {
    let (request, output, evidence) = fixture();
    assert_eq!(output.runtime_devices[0].peer_id, "worker");
    assert_eq!(output.runtime_devices[1].peer_id, "leader");
    assert_eq!(
        output.runtime_devices[0].rpc_endpoint.unwrap().port(),
        50053
    );
    assert_eq!(output.runtime_devices[1].rpc_endpoint, None);
    let saved = serde_json::to_value(&output).unwrap();
    let restored: PlanningOutput = serde_json::from_value(saved).unwrap();
    assert_eq!(verify_output(&request, &restored, &evidence, 2), Ok(()));
}

#[test]
fn rejects_changed_bindings_but_loads_legacy_plans() {
    let (request, mut output, evidence) = fixture();
    let mut saved = serde_json::to_value(&output).unwrap();
    saved.as_object_mut().unwrap().remove("runtime_devices");
    let legacy: PlanningOutput = serde_json::from_value(saved).unwrap();
    assert!(legacy.runtime_devices.is_empty());
    assert_eq!(verify_output(&request, &legacy, &evidence, 2), Ok(()));
    output.runtime_devices[0].rpc_endpoint = Some("127.0.0.1:50054".parse().unwrap());
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::InvalidPlan)
    );
}

#[test]
fn refuses_truncated_model_and_altered_launch_arguments() {
    let (request, mut output, evidence) = fixture();
    output.placement.assignments.last_mut().unwrap().layers.end -= 1;
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::InvalidPlan)
    );
    let (request, mut output, evidence) = fixture();
    output.server_args.push("--unexpected".into());
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::InvalidPlan)
    );
}

#[test]
fn rejects_changed_model_memory_endpoint_and_final_peer() {
    let (mut request, output, evidence) = fixture();
    request.model.runtime_id = "patched-runtime".into();
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::IdentityMismatch)
    );
    let (mut request, output, evidence) = fixture();
    request.model.layers[0].weight_bytes += 1;
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::InvalidPlan)
    );
    let (mut request, output, evidence) = fixture();
    request.devices[1].rpc_endpoint = Some("127.0.0.1:50054".parse().unwrap());
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::InvalidPlan)
    );
    let (mut request, output, evidence) = fixture();
    request.final_peer_id = Some("worker".into());
    assert_eq!(
        verify_output(&request, &output, &evidence, 2),
        Err(VerificationError::InvalidPlan)
    );
}
