use crate::history::{History, NodeId};
use crate::leader::Leader;
use crate::message::{Outgoing, SyncMessage};
use crate::replica::Replica;

/// A peer's history-sync role. Handles stepping down when a newer leader shows up.
#[derive(Debug)]
pub enum Node {
    Leader(Leader),
    Replica(Replica),
}

impl Node {
    pub fn id(&self) -> NodeId {
        match self {
            Self::Leader(l) => l.id(),
            Self::Replica(r) => r.id(),
        }
    }

    pub fn history(&self) -> &History {
        match self {
            Self::Leader(l) => l.history(),
            Self::Replica(r) => r.history(),
        }
    }

    pub fn as_leader_mut(&mut self) -> Option<&mut Leader> {
        match self {
            Self::Leader(l) => Some(l),
            Self::Replica(_) => None,
        }
    }

    pub fn handle(&mut self, from: NodeId, msg: SyncMessage) -> Vec<Outgoing> {
        let newer_leader = match self {
            Self::Leader(l) => {
                let out = l.handle(from, msg.clone());
                if l.deposed_by().is_none() && !Self::from_newer_leader(l, &msg) {
                    return out;
                }
                true
            }
            Self::Replica(_) => false,
        };
        if newer_leader {
            self.step_down();
            if matches!(msg, SyncMessage::Reject { .. }) {
                return Vec::new();
            }
        }
        match self {
            Self::Replica(r) => r.handle(msg).map(|reply| vec![(from, reply)]).unwrap_or_default(),
            Self::Leader(_) => unreachable!(),
        }
    }

    fn from_newer_leader(leader: &Leader, msg: &SyncMessage) -> bool {
        matches!(msg, SyncMessage::Snapshot { .. } | SyncMessage::Append { .. }) && msg.epoch() > leader.epoch()
    }

    /// Converts a leader into a replica that keeps its history.
    fn step_down(&mut self) {
        let placeholder = Self::Replica(Replica::new(self.id()));
        if let Self::Leader(leader) = std::mem::replace(self, placeholder) {
            let (id, epoch) = (leader.id(), leader.deposed_by().unwrap_or(leader.epoch()));
            *self = Self::Replica(Replica::with_history(id, epoch, leader.into_history()));
        }
    }
}
