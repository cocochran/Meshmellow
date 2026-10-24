use serde::{Deserialize, Serialize};

use crate::history_sync::history::{Entry, Epoch, NodeId, Seq};

/// Wire messages for history sync. Carried over whatever transport the P2P layer
/// provides (e.g. an Iroh QUIC stream); see [`SyncMessage::to_bytes`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMessage {
    /// Main -> worker: replace your history with this one. Sent the first time a
    /// leader talks to a peer in its epoch, and whenever a peer asks to resync.
    Snapshot {
        epoch: Epoch,
        leader: NodeId,
        entries: Vec<Entry>,
    },
    /// Main -> worker: entries starting at index `start`. Only valid once the
    /// worker has taken a snapshot in the same epoch.
    Append {
        epoch: Epoch,
        leader: NodeId,
        start: Seq,
        entries: Vec<Entry>,
    },
    /// Worker -> main: I hold the first `len` entries of your history.
    Ack { epoch: Epoch, len: Seq },
    /// Worker -> main: I can't apply your append (gap or new epoch); send a snapshot.
    Resync { epoch: Epoch, len: Seq },
    /// Any -> sender: your epoch is stale; the current one is `epoch`.
    Reject { epoch: Epoch },
}

impl SyncMessage {
    pub fn epoch(&self) -> Epoch {
        match self {
            Self::Snapshot { epoch, .. }
            | Self::Append { epoch, .. }
            | Self::Ack { epoch, .. }
            | Self::Resync { epoch, .. }
            | Self::Reject { epoch } => *epoch,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("SyncMessage is always serializable")
    }

    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// A message addressed to a peer.
pub type Outgoing = (NodeId, SyncMessage);
