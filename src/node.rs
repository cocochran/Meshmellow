//! Node state: identity and the table of known peers.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::protocol::Hello;

/// What this node knows about one peer, taken from that peer's most recent [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// Peer's Iroh node ID as text.
    pub node_id: String,
    /// Protocol version the peer announced.
    pub protocol_version: u16,
    /// Wall-clock UTC time (Unix ms) of this peer's most recent connection; overwritten on reconnect.
    /// Logging only — never use for leader ordering (architecture §9). Requested by connor-claude-003.
    pub connected_at_unix_ms: u64,
}

/// Peers this node has completed a hello exchange with, keyed by node ID.
#[derive(Debug, Default)]
pub struct PeerTable {
    peers: HashMap<String, PeerInfo>,
}

impl PeerTable {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a connection from the peer that sent `hello`, stamped with the current wall-clock time.
    pub fn record_connection(&mut self, hello: &Hello) -> &PeerInfo {
        self.record_connection_at(hello, now_unix_ms())
    }

    /// Records a connection at an explicit Unix-ms timestamp, replacing any earlier entry for that peer.
    pub fn record_connection_at(&mut self, hello: &Hello, connected_at_unix_ms: u64) -> &PeerInfo {
        let info = PeerInfo {
            node_id: hello.node_id.clone(),
            protocol_version: hello.protocol_version,
            connected_at_unix_ms,
        };
        match self.peers.entry(hello.node_id.clone()) {
            Entry::Occupied(mut existing) => {
                existing.insert(info);
                existing.into_mut()
            }
            Entry::Vacant(slot) => slot.insert(info),
        }
    }

    /// Looks up a peer by node ID.
    pub fn get(&self, node_id: &str) -> Option<&PeerInfo> {
        self.peers.get(node_id)
    }

    /// Iterates over every known peer in no particular order.
    pub fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.peers.values()
    }
}

/// Current wall-clock time as Unix milliseconds; clocks before 1970 read as 0.
fn now_unix_ms() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION;

    fn hello(node_id: &str) -> Hello {
        Hello {
            node_id: node_id.to_owned(),
            protocol_version: PROTOCOL_VERSION,
            cluster_id: "test".to_owned(),
            last_log_index: 0,
        }
    }

    #[test]
    fn record_stores_hello_fields() {
        let mut table = PeerTable::new();
        let info = table.record_connection_at(&hello("a"), 1_000).clone();

        assert_eq!(info.node_id, "a");
        assert_eq!(info.protocol_version, PROTOCOL_VERSION);
        assert_eq!(info.connected_at_unix_ms, 1_000);
    }

    #[test]
    fn record_uses_current_time() {
        let before = now_unix_ms();
        let mut table = PeerTable::new();
        let stamped = table.record_connection(&hello("a")).connected_at_unix_ms;

        assert!(stamped >= before);
        assert!(stamped <= now_unix_ms());
    }

    #[test]
    fn reconnect_overwrites_connected_at() {
        let mut table = PeerTable::new();
        table.record_connection_at(&hello("a"), 1_000);
        table.record_connection_at(&hello("a"), 2_000);

        assert_eq!(table.peers().count(), 1);
        assert_eq!(table.get("a").map(|p| p.connected_at_unix_ms), Some(2_000));
    }

    #[test]
    fn lookup_finds_only_recorded_peers() {
        let mut table = PeerTable::new();
        table.record_connection_at(&hello("a"), 1_000);
        table.record_connection_at(&hello("b"), 1_500);

        assert_eq!(table.get("b").map(|p| p.node_id.as_str()), Some("b"));
        assert!(table.get("c").is_none());
        assert_eq!(table.peers().count(), 2);
    }
}
