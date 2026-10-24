//! File-driven advisory planning without network or process side effects.
use crate::placement::{self, LinkProfile, ModelProfile, PeerProfile, Placement, PlanError};
use crate::rpc_plan::{self, RuntimeDevice};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningRequest {
    pub model: ModelProfile,
    pub peers: Vec<PeerProfile>,
    pub links: Vec<LinkProfile>,
    /// One discovered device for every candidate peer; order is immaterial.
    pub devices: Vec<RuntimeDevice>,
    pub model_path: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningOutput {
    pub advisory: bool,
    pub placement: Placement,
    /// Pass as individual process arguments, not as a shell command.
    pub server_args: Vec<String>,
}

pub fn build_plan(
    request: &PlanningRequest,
    min_peers: usize,
) -> Result<PlanningOutput, PlanError> {
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
    let placement = placement::plan(&request.model, &request.peers, &request.links, min_peers)?;
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
        placement,
        server_args,
    })
}
