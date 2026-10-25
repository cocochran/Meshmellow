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
        // A sequence number can come from a peer and may not fit `usize` on 32-bit builds.
        // Treat an unrepresentable index as past the end instead of letting it wrap.
        let start = usize::try_from(start)
            .unwrap_or(usize::MAX)
            .min(self.entries.len());
        &self.entries[start..]
    }

    pub fn extend(&mut self, entries: impl IntoIterator<Item = Entry>) {
        self.entries.extend(entries);
    }

    /// The epoch this history was written under: the newest `EpochAdvanced`, or 0 if none.
    ///
    /// Elections compare this, not the highest epoch a node has heard of, so a node
    /// that learned of a new epoch but never received that leader's entries doesn't
    /// look more up to date than it is.
    pub fn last_epoch(&self) -> Epoch {
        self.entries
            .iter()
            .rev()
            .find_map(|entry| match entry {
                Entry::EpochAdvanced { epoch, .. } => Some(*epoch),
                _ => None,
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn since_returns_empty_for_unrepresentable_or_past_end_sequence() {
        let history = History::from_entries(vec![Entry::JobCompleted {
            request_id: "request".into(),
        }]);

        assert!(history.since(u64::MAX).is_empty());
        assert!(history.since(history.len()).is_empty());
    }
}
