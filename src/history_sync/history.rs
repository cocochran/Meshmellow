use serde::{Deserialize, Serialize};

/// Identifies a peer in the mesh: its Iroh endpoint ID, the same type the peer table uses.
pub use crate::node::NodeId;
/// Leadership term. Bumped on every failover so stale main processes can be rejected.
pub type Epoch = u64;
/// Number of history entries, i.e. the index of the next entry to be appended.
pub type Seq = u64;

/// One event in the conversation log.
///
/// A job is committed once its `JobCompleted` is in the log; a `JobStarted` with no
/// `JobCompleted` after it was interrupted, and its `OutputCommitted` events say how
/// much of the response was saved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Entry {
    /// A prompt was accepted for inference.
    JobStarted { request_id: String, prompt: String },
    /// Part of a job's response, starting at byte `offset` of the full response.
    OutputCommitted {
        request_id: String,
        offset: u64,
        text: String,
    },
    /// The job's response is complete.
    JobCompleted { request_id: String },
    /// A failover made `leader` the main process in `epoch`.
    EpochAdvanced { epoch: Epoch, leader: NodeId },
}

impl Entry {
    /// The request this event belongs to, if it is a job event.
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::JobStarted { request_id, .. }
            | Self::OutputCommitted { request_id, .. }
            | Self::JobCompleted { request_id } => Some(request_id),
            Self::EpochAdvanced { .. } => None,
        }
    }
}

/// Append-only conversation history (the LLM context).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct History {
    entries: Vec<Entry>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: Vec<Entry>) -> Self {
        Self { entries }
    }

    pub fn len(&self) -> Seq {
        self.entries.len() as Seq
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Entries from `start` onward; empty if `start` is past the end.
    pub fn since(&self, start: Seq) -> &[Entry] {
        let start = (start as usize).min(self.entries.len());
        &self.entries[start..]
    }

    pub fn extend(&mut self, entries: impl IntoIterator<Item = Entry>) {
        self.entries.extend(entries);
    }
}
