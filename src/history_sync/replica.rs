use crate::history_sync::election::Candidate;
use crate::history_sync::history::{Epoch, History, NodeId, Seq};
use crate::history_sync::leader::Leader;
use crate::history_sync::message::SyncMessage;

/// History-sync side of a worker on a non-prompted device. Holds a copy of the
/// history so it can take over as the main process if the leader disappears.
#[derive(Clone, Debug)]
pub struct Replica {
    id: NodeId,
    epoch: Epoch,
    leader: Option<NodeId>,
    history: History,
    /// True once a snapshot from the current epoch's leader has been applied.
    in_sync_epoch: bool,
}

impl Replica {
    pub fn new(id: NodeId) -> Self {
        Self::with_history(id, 0, History::new())
    }

    /// A replica that keeps existing history, e.g. a main process that stepped down.
    pub fn with_history(id: NodeId, epoch: Epoch, history: History) -> Self {
        Self {
            id,
            epoch,
            leader: None,
            history,
            in_sync_epoch: false,
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn len(&self) -> Seq {
        self.history.len()
    }

    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// This replica's entry for a failover election. `connected_at_unix_ms` is when
    /// it joined the mesh, as the other peers' `PeerTable`s record it.
    pub fn candidate(&self, connected_at_unix_ms: u64) -> Candidate {
        Candidate {
            id: self.id,
            epoch: self.epoch,
            len: self.len(),
            connected_at_unix_ms,
        }
    }

    /// Applies a message from the main process and returns the reply to send back.
    pub fn handle(&mut self, msg: SyncMessage) -> Option<SyncMessage> {
        if msg.epoch() < self.epoch {
            return match msg {
                SyncMessage::Reject { .. } => None,
                _ => Some(SyncMessage::Reject { epoch: self.epoch }),
            };
        }
        match msg {
            SyncMessage::Snapshot {
                epoch,
                leader,
                entries,
            } => {
                self.epoch = epoch;
                self.leader = Some(leader);
                self.history = History::from_entries(entries);
                self.in_sync_epoch = true;
                Some(self.ack())
            }
            SyncMessage::Append {
                epoch,
                leader,
                start,
                entries,
            } => {
                // A new leader's history may diverge from ours, so only accept
                // appends after its snapshot, and only without gaps.
                if epoch > self.epoch || !self.in_sync_epoch || start > self.len() {
                    self.epoch = epoch;
                    self.leader = Some(leader);
                    self.in_sync_epoch = false;
                    return Some(SyncMessage::Resync {
                        epoch,
                        len: self.len(),
                    });
                }
                // Skip entries we already have from a retransmission.
                let skip = (self.len() - start) as usize;
                self.history.extend(entries.into_iter().skip(skip));
                Some(self.ack())
            }
            SyncMessage::Reject { epoch } => {
                if epoch > self.epoch {
                    self.epoch = epoch;
                    self.in_sync_epoch = false;
                }
                None
            }
            SyncMessage::Ack { .. } | SyncMessage::Resync { .. } => None,
        }
    }

    fn ack(&self) -> SyncMessage {
        SyncMessage::Ack {
            epoch: self.epoch,
            len: self.len(),
        }
    }

    /// Becomes the new main process after failover, seeded from this replica's history.
    pub fn promote(self, new_epoch: Epoch, peers: impl IntoIterator<Item = NodeId>) -> Leader {
        assert!(new_epoch > self.epoch, "a new leader needs a newer epoch");
        Leader::new(self.id, new_epoch, self.history, peers)
    }
}
