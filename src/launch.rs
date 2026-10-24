//! Launch plan: load, validate before launch, and map planned workers to RPC bridges.
//!
//! [`LaunchPlan::from_parts`] is the leader's pre-launch gate: nothing binds a port or starts a
//! process until the saved plan has passed [`planning::validate_output`] against the profile it
//! was built from.

use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, anyhow, bail, ensure};
use iroh::EndpointId;
use serde::de::DeserializeOwned;

use crate::network;
use crate::planning::{self, PlanningOutput, PlanningRequest};

/// Default minimum number of contributing peers, matching the `plan` and `verify` commands.
pub const DEFAULT_MIN_PEERS: usize = 2;

/// What an operator must do with a plan saved before `runtime_devices` existed.
const LEGACY_PLAN_FIX: &str = "regenerate the plan with `llm-cluster plan`";

/// A planned peer on another machine, reached through an RPC bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteWorker {
    /// The worker's Iroh node ID, which is its `peer_id` in the plan.
    pub id: EndpointId,
    /// Leader-local loopback address `llama-server` reaches the worker's RPC server on.
    pub rpc_endpoint: SocketAddr,
}

/// Everything the leader launches, validated before launch.
#[derive(Debug)]
pub struct LaunchPlan {
    /// The saved `llm-cluster plan` output.
    pub plan: PlanningOutput,
    /// The planning profile the plan was built from; needed again for post-run verification.
    pub profile: PlanningRequest,
    /// Minimum number of contributing peers the plan must use.
    pub min_peers: usize,
    /// Remote workers in assignment order, one RPC bridge each.
    pub workers: Vec<RemoteWorker>,
}

impl LaunchPlan {
    /// Reads the plan and profile JSON files (errors name the file), then validates them with
    /// [`Self::from_parts`].
    pub fn load(plan_path: &Path, profile_path: &Path, min_peers: usize) -> anyhow::Result<Self> {
        let plan = read_json(plan_path, "plan")?;
        let profile = read_json(profile_path, "profile")?;
        Self::from_parts(plan, profile, min_peers)
    }

    /// Validates a plan against its profile and derives the remote workers.
    ///
    /// Checks, in order: `server_args` is non-empty; `runtime_devices` is non-empty (legacy plans
    /// without it must be regenerated); [`planning::validate_output`] passes; every runtime device
    /// with an `rpc_endpoint` has a `peer_id` that is a node ID.
    pub fn from_parts(
        plan: PlanningOutput,
        profile: PlanningRequest,
        min_peers: usize,
    ) -> anyhow::Result<Self> {
        ensure!(!plan.server_args.is_empty(), "plan has no server_args");
        ensure!(
            !plan.runtime_devices.is_empty(),
            "plan has no runtime_devices (legacy plan format): {LEGACY_PLAN_FIX}"
        );
        planning::validate_output(&profile, &plan, min_peers)
            .map_err(|error| anyhow!("plan failed pre-launch validation: {error:?}"))?;
        let workers = remote_workers(&plan)?;
        Ok(Self {
            plan,
            profile,
            min_peers,
            workers,
        })
    }

    /// The `llama-server` arguments from the plan, to pass as individual process arguments.
    pub fn server_args(&self) -> &[String] {
        &self.plan.server_args
    }
}

/// Maps every runtime device with an RPC endpoint to the remote worker behind it.
///
/// Devices without an endpoint run on the leader itself and need no bridge.
fn remote_workers(plan: &PlanningOutput) -> anyhow::Result<Vec<RemoteWorker>> {
    plan.runtime_devices
        .iter()
        .filter_map(|device| device.rpc_endpoint.map(|endpoint| (device, endpoint)))
        .map(|(device, rpc_endpoint)| {
            let peer_id = &device.peer_id;
            let id = network::parse_node_id(peer_id)
                .with_context(|| format!("remote peer {peer_id:?} must be the worker's node ID"))?;
            Ok(RemoteWorker { id, rpc_endpoint })
        })
        .collect()
}

fn read_json<T: DeserializeOwned>(path: &Path, what: &str) -> anyhow::Result<T> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {what} {}", path.display()))?;
    match serde_json::from_str(&text) {
        Ok(value) => Ok(value),
        Err(error) => bail!("invalid {what} JSON in {}: {error}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAN: &str = include_str!("../examples/launcher-plan.json");
    const PROFILE: &str = include_str!("../examples/launcher-profile.json");
    const REMOTE_ID: &str = "0c668f7dd608a97bde84ae7758ac63a9775e6cb5b3e663ce9c3bd3371139a4a9";

    fn fixtures() -> (PlanningOutput, PlanningRequest) {
        (
            serde_json::from_str(PLAN).expect("fixture plan"),
            serde_json::from_str(PROFILE).expect("fixture profile"),
        )
    }

    fn rejection(plan: PlanningOutput, profile: PlanningRequest) -> String {
        let error = LaunchPlan::from_parts(plan, profile, DEFAULT_MIN_PEERS).unwrap_err();
        format!("{error:#}")
    }

    #[test]
    fn fixture_plan_yields_its_one_remote_worker() {
        let (plan, profile) = fixtures();

        let launch = LaunchPlan::from_parts(plan, profile, DEFAULT_MIN_PEERS).expect("valid plan");

        assert_eq!(
            launch.workers,
            [RemoteWorker {
                id: network::parse_node_id(REMOTE_ID).unwrap(),
                rpc_endpoint: "127.0.0.1:50053".parse().unwrap(),
            }]
        );
        assert_eq!(launch.server_args(), launch.plan.server_args.as_slice());
    }

    #[test]
    fn empty_server_args_are_checked_first() {
        let (mut plan, profile) = fixtures();
        plan.server_args.clear();
        plan.runtime_devices.clear();

        assert!(rejection(plan, profile).contains("server_args"));
    }

    #[test]
    fn legacy_plan_is_rejected_before_validation() {
        let (mut plan, mut profile) = fixtures();
        plan.runtime_devices.clear();
        profile.model.model_hash = "other".into();

        assert!(rejection(plan, profile).contains("regenerate the plan"));
    }

    #[test]
    fn validation_error_variant_is_named() {
        let (mut plan, profile) = fixtures();
        plan.advisory = false;

        assert!(rejection(plan, profile).contains("InvalidPlan"));
    }

    #[test]
    fn requested_min_peers_is_enforced() {
        let (plan, profile) = fixtures();

        let error = LaunchPlan::from_parts(plan, profile, 3).unwrap_err();

        assert!(error.to_string().contains("InvalidPlan"), "{error}");
    }

    #[test]
    fn unreadable_and_invalid_files_are_named() {
        let dir = std::env::temp_dir().join(format!("meshthing-launch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing.json");
        let garbage = dir.join("garbage.json");
        std::fs::write(&garbage, "{not json").unwrap();

        let unread = LaunchPlan::load(&missing, &garbage, DEFAULT_MIN_PEERS).unwrap_err();
        let invalid = read_json::<PlanningRequest>(&garbage, "profile").unwrap_err();

        assert!(unread.to_string().contains("missing.json"), "{unread}");
        assert!(invalid.to_string().contains("garbage.json"), "{invalid}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
