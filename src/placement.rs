//! Plan contiguous layers across an explicitly ordered set of trusted devices.
//! All costs must come from measurements for the same model/runtime/context.
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ops::Range;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfile {
    pub model_hash: String,
    pub runtime_id: String,
    pub context_tokens: u64,
    pub layers: Vec<LayerMemory>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayerMemory {
    pub weight_bytes: u64,
    pub kv_bytes_per_token: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerProfile {
    pub id: String,
    pub model_hash: String,
    pub runtime_id: String,
    pub context_tokens: u64,
    pub authorized: bool,
    pub healthy: bool,
    /// Opt-in application budget, not total physical/free memory.
    pub memory_budget_bytes: u64,
    /// Conservative non-layer allocation: runtime scratch, headroom, and any
    /// embedding/output tensors owned by this device. Reserve even if unused.
    pub reserved_bytes: u64,
    pub decode_us_per_layer: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LinkProfile {
    pub from: String,
    pub to: String,
    /// Measured per-token transfer overhead for this ordered pair. For RPC,
    /// use observed controller-mediated overhead, not a fictitious direct link.
    pub transfer_us: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Assignment {
    pub peer_id: String,
    pub layers: Range<usize>,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Placement {
    pub model_hash: String,
    pub runtime_id: String,
    pub context_tokens: u64,
    pub assignments: Vec<Assignment>,
    pub predicted_decode_us: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    InvalidInput(String),
    Overflow,
    NoFeasiblePlacement,
}

#[derive(Clone)]
struct Candidate {
    cost: u64,
    assignments: Vec<Assignment>,
}

/// Dynamic programming over layer boundaries, last peer and active peer count.
/// Input order is the allowed stage order; peers may be skipped. A missing link
/// forbids that transition. `min_peers = 2` requires real multi-device placement.
/// Returns estimates only; a backend must verify actual allocation after load.
pub fn plan(
    model: &ModelProfile,
    peers: &[PeerProfile],
    links: &[LinkProfile],
    min_peers: usize,
) -> Result<Placement, PlanError> {
    let invalid = |s: &str| PlanError::InvalidInput(s.to_owned());
    let n = model.layers.len();
    if n == 0
        || model.model_hash.is_empty()
        || model.runtime_id.is_empty()
        || model.context_tokens == 0
        || min_peers == 0
    {
        return Err(invalid(
            "model identity, context, layers and min_peers must be nonempty",
        ));
    }
    let mut ids = HashSet::new();
    for p in peers {
        if p.id.is_empty() || !ids.insert(p.id.as_str()) {
            return Err(invalid("peer IDs must be nonempty and unique"));
        }
        if p.decode_us_per_layer.len() != n || p.decode_us_per_layer.contains(&0) {
            return Err(invalid(
                "each peer needs positive measured cost for every layer",
            ));
        }
    }
    let mut link_ids = HashSet::new();
    for l in links {
        if l.from == l.to
            || !ids.contains(l.from.as_str())
            || !ids.contains(l.to.as_str())
            || !link_ids.insert((l.from.as_str(), l.to.as_str()))
        {
            return Err(invalid(
                "links must reference distinct known peers without duplicates",
            ));
        }
    }
    if min_peers > peers.len() || min_peers > n {
        return Err(PlanError::NoFeasiblePlacement);
    }
    let add = |a: u64, b: u64| a.checked_add(b).ok_or(PlanError::Overflow);
    let mut memory = vec![0u64];
    for layer in &model.layers {
        if layer.weight_bytes == 0 {
            return Err(invalid("layer weight bytes must be positive"));
        }
        let kv = layer
            .kv_bytes_per_token
            .checked_mul(model.context_tokens)
            .ok_or(PlanError::Overflow)?;
        memory.push(add(*memory.last().unwrap(), add(layer.weight_bytes, kv)?)?);
    }
    // Each state retains the cheapest prefix ending at this peer with this many
    // layers and contributors. Future cost depends only on those three values.
    let mut states = vec![vec![vec![None::<Candidate>; min_peers + 1]; n + 1]; peers.len()];
    for (i, peer) in peers.iter().enumerate() {
        if !peer.authorized
            || !peer.healthy
            || peer.model_hash != model.model_hash
            || peer.runtime_id != model.runtime_id
            || peer.context_tokens != model.context_tokens
        {
            continue;
        }
        let mut costs = vec![0u64];
        for c in &peer.decode_us_per_layer {
            costs.push(add(*costs.last().unwrap(), *c)?);
        }
        for start in 0..n {
            let mut predecessors = Vec::new();
            if start == 0 {
                predecessors.push((
                    0,
                    Candidate {
                        cost: 0,
                        assignments: Vec::new(),
                    },
                ));
            } else {
                for j in 0..i {
                    let Some(link) = links
                        .iter()
                        .find(|l| l.from == peers[j].id && l.to == peer.id)
                    else {
                        continue;
                    };
                    for (count, state) in states[j][start].iter().enumerate().skip(1) {
                        if let Some(prev) = state {
                            let mut prev = prev.clone();
                            prev.cost = add(prev.cost, link.transfer_us)?;
                            predecessors.push((count, prev));
                        }
                    }
                }
            }
            for end in start + 1..=n {
                let bytes = add(memory[end] - memory[start], peer.reserved_bytes)?;
                if bytes > peer.memory_budget_bytes {
                    break;
                }
                for (count, prev) in &predecessors {
                    let count = (count + 1).min(min_peers);
                    let cost = add(prev.cost, costs[end] - costs[start])?;
                    let slot = &mut states[i][end][count];
                    if slot.as_ref().is_none_or(|best| cost < best.cost) {
                        let mut assignments = prev.assignments.clone();
                        assignments.push(Assignment {
                            peer_id: peer.id.clone(),
                            layers: start..end,
                            estimated_bytes: bytes,
                        });
                        *slot = Some(Candidate { cost, assignments });
                    }
                }
            }
        }
    }
    let best = states
        .iter()
        .filter_map(|s| s[n][min_peers].as_ref())
        .min_by_key(|c| c.cost)
        .ok_or(PlanError::NoFeasiblePlacement)?;
    Ok(Placement {
        model_hash: model.model_hash.clone(),
        runtime_id: model.runtime_id.clone(),
        context_tokens: model.context_tokens,
        assignments: best.assignments.clone(),
        predicted_decode_us: best.cost,
    })
}
