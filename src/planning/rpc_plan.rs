//! Advisory llama.cpp configuration; never starts processes or enforces caps.
use crate::planning::placement::{Placement, PlanError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDevice {
    pub peer_id: String,
    /// Exact device name discovered from the pinned runtime, after adding RPC.
    pub device_name: String,
    /// Leader-local listener supplied by the authenticated Iroh/SSH bridge.
    pub rpc_endpoint: Option<SocketAddr>,
}

/// Return argv items, suitable for Command::args (never a shell command).
/// `devices` must be in the SAME order as the placement assignments. Each
/// assignment maps to exactly one discovered non-CPU runtime device.
/// Tensor split proportions request an approximation; llama.cpp may assign
/// output/embedding tensors differently. Verify logs and memory before use.
pub fn server_args(
    placement: &Placement,
    devices: &[RuntimeDevice],
    model_path: &str,
) -> Result<Vec<String>, PlanError> {
    let invalid = |s: &str| PlanError::InvalidInput(s.to_owned());
    if devices.is_empty()
        || devices.len() != placement.assignments.len()
        || model_path.is_empty()
        || placement.context_tokens == 0
        || placement.context_tokens > i32::MAX as u64
    {
        return Err(invalid(
            "provide a model path, valid context and one device per assignment",
        ));
    }
    let mut names = HashSet::new();
    let mut peers = HashSet::new();
    let mut endpoints = Vec::new();
    let mut splits = Vec::new();
    let mut next_layer = 0;
    for (assignment, device) in placement.assignments.iter().zip(devices) {
        if assignment.peer_id != device.peer_id
            || !peers.insert(&device.peer_id)
            || device.device_name.is_empty()
            || device.device_name == "none"
            || device.device_name.contains([',', '\n', '\r'])
            || !names.insert(&device.device_name)
            || assignment.layers.start != next_layer
            || assignment.layers.end <= assignment.layers.start
        {
            return Err(invalid(
                "device IDs/order must match unique contiguous assignments",
            ));
        }
        if let Some(endpoint) = device.rpc_endpoint {
            if !endpoint.ip().is_loopback() || endpoint.port() == 0 {
                return Err(invalid(
                    "RPC bridge endpoints must be loopback with nonzero ports",
                ));
            }
            let endpoint = endpoint.to_string();
            if !endpoints.contains(&endpoint) {
                endpoints.push(endpoint);
            }
        }
        splits.push(assignment.layers.len().to_string());
        next_layer = assignment.layers.end;
    }
    let offload_layers = next_layer
        .checked_add(1)
        .filter(|n| *n <= i32::MAX as usize)
        .ok_or_else(|| invalid("layer count exceeds runtime range"))?;
    let mut args = Vec::new();
    if !endpoints.is_empty() {
        args.extend(["--rpc".into(), endpoints.join(",")]);
    }
    args.extend([
        "--model".into(),
        model_path.into(),
        "--device".into(),
        devices
            .iter()
            .map(|d| d.device_name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        "--split-mode".into(),
        "layer".into(),
        "--tensor-split".into(),
        splits.join(","),
        "--n-gpu-layers".into(),
        offload_layers.to_string(),
        "--ctx-size".into(),
        placement.context_tokens.to_string(),
        "--parallel".into(),
        "1".into(),
        "--host".into(),
        "127.0.0.1".into(),
    ]);
    Ok(args)
}
