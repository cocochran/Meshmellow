use crate::history_sync::history::{Epoch, NodeId, Seq};

/// What a surviving peer reports when the main process is lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub id: NodeId,
    pub epoch: Epoch,
    pub len: Seq,
    /// When this peer last connected to the mesh, from `PeerInfo::connected_at_unix_ms`.
    /// Earlier means connected longer.
    pub connected_at_unix_ms: u64,
}

/// Outcome of a failover election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elected {
    pub id: NodeId,
    pub epoch: Epoch,
}

/// Picks the replacement main process: the peer with the most up-to-date history
/// (highest epoch, then most entries), then whichever peer has been connected the
/// longest, then the lowest node ID. Every peer that sees the same candidate set
/// picks the same winner.
///
/// History comes before connection time so the new leader never has fewer
/// entries than a peer that could have won instead. Connection time is a
/// wall-clock reading and devices' clocks can differ, so it only breaks ties.
///
/// Prototype only: assumes survivors agree on the candidate set. A real mesh
/// needs a consensus round or a quorum to rule out two simultaneous winners.
pub fn elect(candidates: &[Candidate]) -> Option<Elected> {
    let max_epoch = candidates.iter().map(|c| c.epoch).max()?;
    let winner = candidates.iter().max_by(|a, b| {
        (a.epoch, a.len)
            .cmp(&(b.epoch, b.len))
            .then(b.connected_at_unix_ms.cmp(&a.connected_at_unix_ms))
            .then(b.id.cmp(&a.id))
    })?;
    Some(Elected {
        id: winner.id,
        epoch: max_epoch + 1,
    })
}
