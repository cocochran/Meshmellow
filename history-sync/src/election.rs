use crate::history::{Epoch, NodeId, Seq};

/// What a surviving peer reports when the main process is lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub id: NodeId,
    pub epoch: Epoch,
    pub len: Seq,
}

/// Outcome of a failover election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elected {
    pub id: NodeId,
    pub epoch: Epoch,
}

/// Picks the replacement main process: the peer with the most up-to-date history
/// (highest epoch, then most entries), ties broken by lowest node ID. Every peer
/// that sees the same candidate set picks the same winner.
///
/// Prototype only: assumes survivors agree on the candidate set. A real mesh
/// needs a consensus round or a quorum to rule out two simultaneous winners.
pub fn elect(candidates: &[Candidate]) -> Option<Elected> {
    let max_epoch = candidates.iter().map(|c| c.epoch).max()?;
    let winner = candidates
        .iter()
        .max_by(|a, b| (a.epoch, a.len).cmp(&(b.epoch, b.len)).then(b.id.cmp(&a.id)))?;
    Some(Elected { id: winner.id, epoch: max_epoch + 1 })
}
