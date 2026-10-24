use crate::history_sync::history::{Epoch, History, NodeId};
use crate::history_sync::leader::Leader;
use crate::history_sync::message::{Outgoing, SyncMessage};
use crate::history_sync::replica::Replica;
use crate::history_sync::store::{HistoryStore, StoreError};
use crate::node::PeerTable;

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

    pub fn epoch(&self) -> Epoch {
        match self {
            Self::Leader(l) => l.epoch(),
            Self::Replica(r) => r.epoch(),
        }
    }

    /// Restores a node from disk after a restart. It comes back as a replica: the
    /// current leader's next message brings it up to date, or an operator promotes it.
    pub fn from_store(id: NodeId, store: &HistoryStore) -> Self {
        Self::Replica(Replica::with_history(
            id,
            store.epoch(),
            store.history().clone(),
        ))
    }

    /// Saves any new entries and a newer epoch to `store`.
    ///
    /// On the main process, call this after recording a job (e.g. `complete_task`) and
    /// before sending the returned messages, so workers never hold entries the leader lost.
    pub fn persist(&self, store: &mut HistoryStore) -> Result<(), StoreError> {
        let history = self.history();
        if history.len() < store.len() {
            return store.replace(history, self.epoch());
        }
        store.append(history.since(store.len()))?;
        if self.epoch() > store.epoch() {
            store.set_epoch(self.epoch())?;
        }
        Ok(())
    }

    /// Like [`Node::handle`], but saves the result to `store` before returning the replies,
    /// so an `Ack` is only ever sent for entries already on disk.
    pub fn handle_durable(
        &mut self,
        from: NodeId,
        msg: SyncMessage,
        store: &mut HistoryStore,
    ) -> Result<Vec<Outgoing>, StoreError> {
        let is_snapshot = matches!(msg, SyncMessage::Snapshot { .. });
        let out = self.handle(from, msg);
        // An Ack after a Snapshot means it was applied: the history was replaced, not extended.
        let replaced = is_snapshot
            && out
                .iter()
                .any(|(_, reply)| matches!(reply, SyncMessage::Ack { .. }));
        if replaced {
            store.replace(self.history(), self.epoch())?;
        } else {
            self.persist(store)?;
        }
        Ok(out)
    }

    /// Makes the main process sync with exactly the peers in `table`.
    /// Workers don't track peers, so this does nothing on a replica.
    pub fn set_peers(&mut self, table: &PeerTable) {
        if let Self::Leader(leader) = self {
            leader.set_peers(table.ids());
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
            Self::Replica(r) => r
                .handle(msg)
                .map(|reply| vec![(from, reply)])
                .unwrap_or_default(),
            Self::Leader(_) => unreachable!(),
        }
    }

    fn from_newer_leader(leader: &Leader, msg: &SyncMessage) -> bool {
        matches!(
            msg,
            SyncMessage::Snapshot { .. } | SyncMessage::Append { .. }
        ) && msg.epoch() > leader.epoch()
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
