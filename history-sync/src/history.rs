use serde::{Deserialize, Serialize};

/// Identifies a peer in the mesh. With Iroh this would map to a `NodeId`/public key.
pub type NodeId = u64;
/// Leadership term. Bumped on every failover so stale main processes can be rejected.
pub type Epoch = u64;
/// Number of history entries, i.e. the index of the next entry to be appended.
pub type Seq = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    System,
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub task_id: u64,
    pub role: Role,
    pub content: String,
}

impl Entry {
    pub fn new(task_id: u64, role: Role, content: impl Into<String>) -> Self {
        Self { task_id, role, content: content.into() }
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
