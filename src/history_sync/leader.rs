use std::collections::{BTreeMap, BTreeSet};

use crate::history_sync::history::{Entry, Epoch, History, NodeId, Role, Seq};
use crate::history_sync::message::{Outgoing, SyncMessage};

/// Per-peer replication state tracked by the main process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerState {
    /// Peer hasn't confirmed a snapshot in this epoch yet.
    NeedsSnapshot,
    /// Peer has acknowledged this many entries.
    Acked(Seq),
}

/// History-sync side of the main process (the prompted device).
///
/// Owns the authoritative history and pushes it to every worker after each task.
/// Messages are idempotent, so `sync` can be called repeatedly to retransmit.
#[derive(Debug)]
pub struct Leader {
    id: NodeId,
    epoch: Epoch,
    history: History,
    peers: BTreeMap<NodeId, PeerState>,
    deposed_by: Option<Epoch>,
}

impl Leader {
    pub fn new(
        id: NodeId,
        epoch: Epoch,
        history: History,
        peers: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        let mut leader = Self {
            id,
            epoch,
            history,
            peers: BTreeMap::new(),
            deposed_by: None,
        };
        for peer in peers {
            leader.add_peer(peer);
        }
        leader
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn into_history(self) -> History {
        self.history
    }

    /// The epoch of the leader that replaced this one, if it has been deposed.
    pub fn deposed_by(&self) -> Option<Epoch> {
        self.deposed_by
    }

    pub fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.keys().copied()
    }

    pub fn add_peer(&mut self, peer: NodeId) {
        if peer != self.id {
            self.peers.insert(peer, PeerState::NeedsSnapshot);
        }
    }

    pub fn remove_peer(&mut self, peer: NodeId) {
        self.peers.remove(&peer);
    }

    /// Replaces the peer list with `peers`, e.g. the IDs in the node's `PeerTable`.
    ///
    /// Peers already tracked keep their progress; new ones start with a snapshot,
    /// and peers that are no longer listed stop being synced.
    pub fn set_peers(&mut self, peers: impl IntoIterator<Item = NodeId>) {
        let current: BTreeSet<NodeId> = peers.into_iter().filter(|&peer| peer != self.id).collect();
        self.peers.retain(|peer, _| current.contains(peer));
        for peer in current {
            self.peers.entry(peer).or_insert(PeerState::NeedsSnapshot);
        }
    }

    /// Peers that hold the full current history.
    pub fn up_to_date_peers(&self) -> Vec<NodeId> {
        let len = self.history.len();
        self.peers
            .iter()
            .filter(|(_, state)| **state == PeerState::Acked(len))
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn append(&mut self, entries: impl IntoIterator<Item = Entry>) {
        self.history.extend(entries);
    }

    /// Records a completed task (prompt and response) and returns the sync
    /// messages to send to all workers.
    pub fn complete_task(&mut self, task_id: u64, prompt: &str, response: &str) -> Vec<Outgoing> {
        self.append([
            Entry::new(task_id, Role::User, prompt),
            Entry::new(task_id, Role::Assistant, response),
        ]);
        self.sync()
    }

    /// Messages that bring every peer up to date. Peers already up to date are skipped.
    pub fn sync(&self) -> Vec<Outgoing> {
        if self.deposed_by.is_some() {
            return Vec::new();
        }
        self.peers
            .iter()
            .filter_map(|(&peer, &state)| self.message_for(state).map(|msg| (peer, msg)))
            .collect()
    }

    fn message_for(&self, state: PeerState) -> Option<SyncMessage> {
        match state {
            PeerState::NeedsSnapshot => Some(self.snapshot()),
            PeerState::Acked(len) if len < self.history.len() => Some(SyncMessage::Append {
                epoch: self.epoch,
                leader: self.id,
                start: len,
                entries: self.history.since(len).to_vec(),
            }),
            PeerState::Acked(_) => None,
        }
    }

    fn snapshot(&self) -> SyncMessage {
        SyncMessage::Snapshot {
            epoch: self.epoch,
            leader: self.id,
            entries: self.history.entries().to_vec(),
        }
    }

    /// Handles a reply from a worker and returns any immediate follow-up messages.
    pub fn handle(&mut self, from: NodeId, msg: SyncMessage) -> Vec<Outgoing> {
        if self.deposed_by.is_some() {
            return Vec::new();
        }
        match msg {
            SyncMessage::Reject { epoch } if epoch > self.epoch => {
                self.deposed_by = Some(epoch);
                Vec::new()
            }
            SyncMessage::Ack { epoch, len } if epoch == self.epoch => {
                if let Some(state) = self.peers.get_mut(&from) {
                    // Acks may arrive out of order, so never move backwards.
                    let len = len.min(self.history.len());
                    *state = match *state {
                        PeerState::Acked(prev) => PeerState::Acked(prev.max(len)),
                        PeerState::NeedsSnapshot => PeerState::Acked(len),
                    };
                }
                Vec::new()
            }
            SyncMessage::Resync { epoch, .. } if epoch == self.epoch => {
                match self.peers.get_mut(&from) {
                    Some(state) => {
                        *state = PeerState::NeedsSnapshot;
                        vec![(from, self.snapshot())]
                    }
                    None => Vec::new(),
                }
            }
            // A peer still on an older epoch sent us something: tell it who's in charge.
            msg if msg.epoch() < self.epoch && !matches!(msg, SyncMessage::Reject { .. }) => {
                vec![(from, SyncMessage::Reject { epoch: self.epoch })]
            }
            _ => Vec::new(),
        }
    }
}
