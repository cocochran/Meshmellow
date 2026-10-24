//! Validate caller-collected evidence from one fresh inference run.
//! This module neither collects telemetry nor stops the runtime.
use crate::placement::{PeerProfile, Placement};
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceEvidence {
    pub peer_id: String,
    /// Peak total contribution memory, including weights, KV, and scratch.
    pub peak_bytes: u64,
    /// Fresh compute events observed during this run, excluding model loading.
    pub compute_events: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvidence {
    pub model_hash: String,
    pub runtime_id: String,
    pub context_tokens: u64,
    pub devices: Vec<DeviceEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerificationError {
    InvalidPlan,
    IdentityMismatch,
    DuplicatePeer(String),
    MissingPeer(String),
    UnexpectedPeer(String),
    IneligiblePeer(String),
    MissingTelemetry(String),
    NoCompute(String),
    BudgetExceeded {
        peer_id: String,
        peak_bytes: u64,
        budget_bytes: u64,
    },
}

/// Validate placement shape and current contributor eligibility before launch.
/// Does not require or assert runtime telemetry.
pub fn validate_placement(
    placement: &Placement,
    profiles: &[PeerProfile],
    min_peers: usize,
) -> Result<(), VerificationError> {
    use VerificationError::*;
    if min_peers == 0
        || placement.assignments.len() < min_peers
        || placement.model_hash.is_empty()
        || placement.runtime_id.is_empty()
        || placement.context_tokens == 0
    {
        return Err(InvalidPlan);
    }
    let mut selected = HashSet::new();
    let mut next = 0;
    for assignment in &placement.assignments {
        if assignment.peer_id.is_empty()
            || !selected.insert(assignment.peer_id.as_str())
            || assignment.layers.start != next
            || assignment.layers.end <= next
        {
            return Err(InvalidPlan);
        }
        next = assignment.layers.end;
    }
    let mut profile_ids = HashSet::new();
    for profile in profiles {
        if !profile_ids.insert(profile.id.as_str()) {
            return Err(DuplicatePeer(profile.id.clone()));
        }
    }
    for assignment in &placement.assignments {
        let id = &assignment.peer_id;
        let profile = profiles
            .iter()
            .find(|p| p.id == *id)
            .ok_or_else(|| MissingPeer(id.clone()))?;
        if !profile.authorized
            || !profile.healthy
            || profile.model_hash != placement.model_hash
            || profile.runtime_id != placement.runtime_id
            || profile.context_tokens != placement.context_tokens
        {
            return Err(IneligiblePeer(id.clone()));
        }
    }
    Ok(())
}

/// Require evidence for every selected peer and no unplanned contributors.
/// Profiles must be current; the caller must associate observations with this
/// exact run and collect peaks over load AND inference. Success validates these
/// observations only: it is neither a hard cap nor proof of future behavior.
pub fn verify_run(
    placement: &Placement,
    profiles: &[PeerProfile],
    evidence: &RunEvidence,
    min_peers: usize,
) -> Result<(), VerificationError> {
    use VerificationError::*;
    validate_placement(placement, profiles, min_peers)?;
    if evidence.model_hash != placement.model_hash
        || evidence.runtime_id != placement.runtime_id
        || evidence.context_tokens != placement.context_tokens
    {
        return Err(IdentityMismatch);
    }
    let selected: HashSet<_> = placement
        .assignments
        .iter()
        .map(|a| a.peer_id.as_str())
        .collect();
    let mut observed = HashSet::new();
    for device in &evidence.devices {
        if !observed.insert(device.peer_id.as_str()) {
            return Err(DuplicatePeer(device.peer_id.clone()));
        }
        if !selected.contains(device.peer_id.as_str()) {
            return Err(UnexpectedPeer(device.peer_id.clone()));
        }
    }
    for assignment in &placement.assignments {
        let id = &assignment.peer_id;
        let profile = profiles
            .iter()
            .find(|p| p.id == *id)
            .ok_or_else(|| MissingPeer(id.clone()))?;
        let device = evidence
            .devices
            .iter()
            .find(|d| d.peer_id == *id)
            .ok_or_else(|| MissingPeer(id.clone()))?;
        if device.peak_bytes == 0 {
            return Err(MissingTelemetry(id.clone()));
        }
        if device.peak_bytes > profile.memory_budget_bytes {
            return Err(BudgetExceeded {
                peer_id: id.clone(),
                peak_bytes: device.peak_bytes,
                budget_bytes: profile.memory_budget_bytes,
            });
        }
        if device.compute_events == 0 {
            return Err(NoCompute(id.clone()));
        }
    }
    Ok(())
}
