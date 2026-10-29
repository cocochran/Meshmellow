//! File-driven advisory planning without network or process side effects.

pub mod evidence;
pub mod launch;
pub mod measured_placement;
pub mod placement;
pub mod ring_deployment;
pub mod rpc_plan;
pub mod verification;

use placement::{LinkProfile, ModelProfile, PeerProfile, Placement, PlanError};
use rpc_plan::RuntimeDevice;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use verification::{RunEvidence, VerificationError};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningRequest {
    pub model: ModelProfile,
    pub peers: Vec<PeerProfile>,
    pub links: Vec<LinkProfile>,
    /// One discovered device for every candidate peer; order is immaterial.
    pub devices: Vec<RuntimeDevice>,
    pub model_path: String,
    /// Objective for candidate selection; older profiles default to decode latency.
    #[serde(default)]
    pub objective: PlanningObjective,
    /// Require the final block group on this peer (for example, the local device).
    #[serde(default)]
    pub final_peer_id: Option<String>,
}

/// Optimization target requested from the planner.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningObjective {
    /// Minimize measured single-request decode time.
    #[default]
    SequentialDecodeLatency,
    /// Maximize measured steady-state throughput across concurrent requests.
    MultiRequestThroughput,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningOutput {
    pub advisory: bool,
    /// Objective used to score the selected placement.
    #[serde(default)]
    pub objective: PlanningObjective,
    pub placement: Placement,
    /// Pass as individual process arguments, not as a shell command.
    pub server_args: Vec<String>,
    /// Selected devices in assignment order. Empty when loading legacy plans.
    #[serde(default)]
    pub runtime_devices: Vec<RuntimeDevice>,
}

/// Verify a saved launch plan against current model/profile inputs and evidence.
/// This validates the proposed assignment and supplied observations, not actual
/// runtime layer/output ownership or the authenticity of collected telemetry.
pub fn verify_output(
    request: &PlanningRequest,
    output: &PlanningOutput,
    evidence: &RunEvidence,
    min_peers: usize,
) -> Result<(), VerificationError> {
    validate_output(request, output, min_peers)?;
    verification::verify_run(&output.placement, &request.peers, evidence, min_peers)
}

/// Validate a saved plan before launch, without asserting runtime execution.
pub fn validate_output(
    request: &PlanningRequest,
    output: &PlanningOutput,
    min_peers: usize,
) -> Result<(), VerificationError> {
    use VerificationError::{IdentityMismatch, InvalidPlan};
    let placement = &output.placement;
    if request.objective != PlanningObjective::SequentialDecodeLatency
        || output.objective != request.objective
    {
        return Err(InvalidPlan);
    }
    if placement.model_hash != request.model.model_hash
        || placement.runtime_id != request.model.runtime_id
        || placement.context_tokens != request.model.context_tokens
    {
        return Err(IdentityMismatch);
    }
    if !output.advisory
        || request.model.layers.is_empty()
        || placement.assignments.last().map(|a| a.layers.end) != Some(request.model.layers.len())
        || request.final_peer_id.as_ref().is_some_and(|id| {
            placement
                .assignments
                .last()
                .is_none_or(|a| &a.peer_id != id)
        })
    {
        return Err(InvalidPlan);
    }
    verification::validate_placement(placement, &request.peers, min_peers)?;
    let mut previous_peer: Option<&str> = None;
    for assignment in &placement.assignments {
        if !request
            .peers
            .iter()
            .any(|peer| peer.id == assignment.peer_id)
        {
            return Err(InvalidPlan);
        }
        if let Some(last_id) = previous_peer
            && !request
                .links
                .iter()
                .any(|link| link.from == last_id && link.to == assignment.peer_id)
        {
            return Err(InvalidPlan);
        }
        previous_peer = Some(&assignment.peer_id);
    }
    let mut devices = Vec::new();
    for assignment in &placement.assignments {
        let peer = request
            .peers
            .iter()
            .find(|p| p.id == assignment.peer_id)
            .ok_or(InvalidPlan)?;
        let layers = request
            .model
            .layers
            .get(assignment.layers.clone())
            .ok_or(InvalidPlan)?;
        let mut bytes = peer.reserved_bytes;
        for layer in layers {
            if layer.weight_bytes == 0 {
                return Err(InvalidPlan);
            }
            bytes = layer
                .kv_bytes_per_token
                .checked_mul(placement.context_tokens)
                .and_then(|kv| kv.checked_add(layer.weight_bytes))
                .and_then(|memory| bytes.checked_add(memory))
                .ok_or(InvalidPlan)?;
        }
        if bytes != assignment.estimated_bytes || bytes > peer.memory_budget_bytes {
            return Err(InvalidPlan);
        }
        let mut matches = request
            .devices
            .iter()
            .filter(|d| d.peer_id == assignment.peer_id);
        devices.push(matches.next().ok_or(InvalidPlan)?.clone());
        if matches.next().is_some() {
            return Err(InvalidPlan);
        }
    }
    let argv =
        rpc_plan::server_args(placement, &devices, &request.model_path).map_err(|_| InvalidPlan)?;
    if !output.runtime_devices.is_empty() && output.runtime_devices != devices {
        return Err(InvalidPlan);
    }
    if argv != output.server_args {
        return Err(InvalidPlan);
    }
    Ok(())
}

pub fn build_plan(
    request: &PlanningRequest,
    min_peers: usize,
) -> Result<PlanningOutput, PlanError> {
    if request.objective == PlanningObjective::MultiRequestThroughput {
        return Err(PlanError::InvalidInput(
            "multi-request throughput needs concurrent execution and measured stage/link throughput"
                .to_owned(),
        ));
    }
    let mut ids = HashSet::new();
    for device in &request.devices {
        if !ids.insert(&device.peer_id)
            || !request.peers.iter().any(|peer| peer.id == device.peer_id)
        {
            return Err(PlanError::InvalidInput(
                "device mappings must reference unique candidate peers".into(),
            ));
        }
    }
    if request.devices.len() != request.peers.len() {
        return Err(PlanError::InvalidInput(
            "provide one device mapping for every candidate peer".into(),
        ));
    }
    let placement = placement::plan_with_final_peer(
        &request.model,
        &request.peers,
        &request.links,
        min_peers,
        request.final_peer_id.as_deref(),
    )?;
    let devices = placement
        .assignments
        .iter()
        .map(|assignment| {
            request
                .devices
                .iter()
                .find(|device| device.peer_id == assignment.peer_id)
                .expect("candidate device mappings validated above")
                .clone()
        })
        .collect::<Vec<_>>();
    let server_args = rpc_plan::server_args(&placement, &devices, &request.model_path)?;
    Ok(PlanningOutput {
        advisory: true,
        objective: request.objective,
        placement,
        server_args,
        runtime_devices: devices,
    })
}
