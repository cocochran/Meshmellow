//! Node state: identity and the table of known peers.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::protocol::{Heartbeat, Hello};

/// Identifies a node everywhere in the crate: its Iroh endpoint ID (an Ed25519 public key).
pub type NodeId = iroh::EndpointId;

/// What this node knows about one peer, taken from that peer's most recent [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    /// Peer's Iroh node ID, as authenticated by the connection.
    pub node_id: NodeId,
    /// Protocol version the peer announced.
    pub protocol_version: u16,
    /// Wall-clock UTC time (Unix ms) of this peer's most recent connection; overwritten on reconnect.
    /// Logging only — never use for leader ordering (architecture §9). Requested by connor-claude-003.
    pub connected_at_unix_ms: u64,
    /// Wall-clock UTC time (Unix ms) the latest accepted heartbeat arrived; `None` until one does.
    /// Reset on reconnect. Logging and diagnostics only; liveness tracking is M-11.
    pub last_heartbeat_unix_ms: Option<u64>,
    /// Sequence number of the latest accepted heartbeat; `None` until one arrives. Reset on reconnect,
    /// so a restarted peer may start counting from 1 again after its new hello.
    pub last_heartbeat_sequence: Option<u64>,
}

/// Peers this node currently has a control connection with, keyed by node ID.
#[derive(Debug, Default)]
pub struct PeerTable {
    peers: HashMap<NodeId, PeerInfo>,
}

impl PeerTable {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a connection from `node_id`, which sent `hello`, stamped with the current wall-clock time.
    pub fn record_connection(&mut self, node_id: NodeId, hello: &Hello) -> &PeerInfo {
        self.record_connection_at(node_id, hello, now_unix_ms())
    }

    /// Records a connection at an explicit Unix-ms timestamp, replacing any earlier entry for that peer
    /// (including its heartbeat state).
    pub fn record_connection_at(&mut self, hello: &Hello, connected_at_unix_ms: u64) -> &PeerInfo {
        let info = PeerInfo {
            node_id,
            protocol_version: hello.protocol_version,
            connected_at_unix_ms,
            last_heartbeat_unix_ms: None,
            last_heartbeat_sequence: None,
        };
        match self.peers.entry(node_id) {
            Entry::Occupied(mut existing) => {
                existing.insert(info);
                existing.into_mut()
            }
            Entry::Vacant(slot) => slot.insert(info),
        }
    }


    /// Removes a peer whose connection closed, unless it has reconnected since.
    ///
    /// `connected_at_unix_ms` identifies the closed connection: if the entry carries a different
    /// timestamp, a newer connection replaced it and the entry is kept. Returns whether it was removed.
    pub fn remove_connection(&mut self, node_id: &NodeId, connected_at_unix_ms: u64) -> bool {
        match self.peers.entry(*node_id) {
            Entry::Occupied(entry) if entry.get().connected_at_unix_ms == connected_at_unix_ms => {
                entry.remove();
                true
            }
            _ => false,
        }
    /// Records `heartbeat`, stamped with the current wall-clock time.
    ///
    /// Returns `false`, leaving the table unchanged, if the sender has not said hello or the
    /// sequence is not greater than the last accepted one.
    pub fn record_heartbeat(&mut self, heartbeat: &Heartbeat) -> bool {
        self.record_heartbeat_at(heartbeat, now_unix_ms())
    }

    /// Like [`Self::record_heartbeat`] with an explicit Unix-ms arrival time.
    ///
    /// Only the sequence is compared; epoch-aware ordering belongs to the heartbeat tracker (M-11).
    pub fn record_heartbeat_at(&mut self, heartbeat: &Heartbeat, received_at_unix_ms: u64) -> bool {
        let Some(info) = self.peers.get_mut(&heartbeat.node_id) else {
            return false;
        };
        if info
            .last_heartbeat_sequence
            .is_some_and(|last| heartbeat.sequence <= last)
        {
            return false;
        }
        info.last_heartbeat_sequence = Some(heartbeat.sequence);
        info.last_heartbeat_unix_ms = Some(received_at_unix_ms);
        true
    }

    /// Looks up a peer by node ID.
    pub fn get(&self, node_id: &NodeId) -> Option<&PeerInfo> {
        self.peers.get(node_id)
    }

    /// Iterates over every known peer in no particular order.
    pub fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.peers.values()
    }

    /// Iterates over the node IDs of every known peer in no particular order.
    pub fn ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.keys().copied()
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

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn hello(node_id: NodeId) -> Hello {
    fn heartbeat(node_id: &str, sequence: u64) -> Heartbeat {
        Heartbeat {
            node_id: node_id.to_owned(),
            epoch: 0,
            sequence,
        }
    }

    fn hello(node_id: &str) -> Hello {
        Hello {
            node_id: node_id.to_string(),
            protocol_version: PROTOCOL_VERSION,
            cluster_id: "test".to_owned(),
            last_log_index: 0,
        }
    }

    #[test]
    fn record_stores_hello_fields() {
        let a = node(1);
        let mut table = PeerTable::new();
        let info = table.record_connection_at(a, &hello(a), 1_000).clone();

        assert_eq!(info.node_id, a);
        assert_eq!(info.protocol_version, PROTOCOL_VERSION);
        assert_eq!(info.connected_at_unix_ms, 1_000);
    }

    #[test]
    fn record_uses_current_time() {
        let a = node(1);
        let before = now_unix_ms();
        let mut table = PeerTable::new();
        let stamped = table.record_connection(a, &hello(a)).connected_at_unix_ms;

        assert!(stamped >= before);
        assert!(stamped <= now_unix_ms());
    }

    #[test]
    fn reconnect_overwrites_connected_at() {
        let a = node(1);
        let mut table = PeerTable::new();
        table.record_connection_at(a, &hello(a), 1_000);
        table.record_connection_at(a, &hello(a), 2_000);

        assert_eq!(table.peers().count(), 1);
        assert_eq!(table.get(&a).map(|p| p.connected_at_unix_ms), Some(2_000));
    }

    #[test]
    fn lookup_finds_only_recorded_peers() {
        let (a, b, c) = (node(1), node(2), node(3));
        let mut table = PeerTable::new();
        table.record_connection_at(a, &hello(a), 1_000);
        table.record_connection_at(b, &hello(b), 1_500);

        assert_eq!(table.get(&b).map(|p| p.node_id), Some(b));
        assert!(table.get(&c).is_none());
        assert_eq!(table.peers().count(), 2);
        let mut ids: Vec<_> = table.ids().collect();
        ids.sort();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn closing_a_connection_removes_the_peer() {
        let a = node(1);
        let mut table = PeerTable::new();
        table.record_connection_at(a, &hello(a), 1_000);

        assert!(table.remove_connection(&a, 1_000));
        assert!(table.get(&a).is_none());
    }

    #[test]
    fn closing_an_old_connection_keeps_the_reconnected_peer() {
        let a = node(1);
        let mut table = PeerTable::new();
        table.record_connection_at(a, &hello(a), 1_000);
        table.record_connection_at(a, &hello(a), 2_000);

        assert!(!table.remove_connection(&a, 1_000));
        assert_eq!(table.get(&a).map(|p| p.connected_at_unix_ms), Some(2_000));
    }

    #[test]
    fn heartbeat_from_unknown_peer_is_rejected() {
        let mut table = PeerTable::new();

        assert!(!table.record_heartbeat_at(&heartbeat("a", 1), 1_000));
        assert!(table.get("a").is_none());
    }

    #[test]
    fn heartbeat_updates_sequence_and_time() {
        let mut table = PeerTable::new();
        table.record_connection_at(&hello("a"), 1_000);

        assert!(table.record_heartbeat_at(&heartbeat("a", 1), 1_100));
        assert!(table.record_heartbeat_at(&heartbeat("a", 5), 1_200));

        let info = table.get("a").expect("peer recorded");
        assert_eq!(info.last_heartbeat_sequence, Some(5));
        assert_eq!(info.last_heartbeat_unix_ms, Some(1_200));
    }

    #[test]
    fn repeated_or_older_sequence_is_rejected() {
        let mut table = PeerTable::new();
        table.record_connection_at(&hello("a"), 1_000);
        assert!(table.record_heartbeat_at(&heartbeat("a", 3), 1_100));

        assert!(!table.record_heartbeat_at(&heartbeat("a", 3), 1_200));
        assert!(!table.record_heartbeat_at(&heartbeat("a", 2), 1_300));

        let info = table.get("a").expect("peer recorded");
        assert_eq!(info.last_heartbeat_sequence, Some(3));
        assert_eq!(info.last_heartbeat_unix_ms, Some(1_100));
    }

    #[test]
    fn reconnect_resets_heartbeat_state() {
        let mut table = PeerTable::new();
        table.record_connection_at(&hello("a"), 1_000);
        assert!(table.record_heartbeat_at(&heartbeat("a", 7), 1_100));

        table.record_connection_at(&hello("a"), 2_000);
        let info = table.get("a").expect("peer recorded");
        assert_eq!(info.last_heartbeat_sequence, None);
        assert_eq!(info.last_heartbeat_unix_ms, None);
        assert!(table.record_heartbeat_at(&heartbeat("a", 1), 2_100));
    }

    #[test]
    fn record_heartbeat_uses_current_time() {
        let mut table = PeerTable::new();
        table.record_connection(&hello("a"));
        let before = now_unix_ms();

        assert!(table.record_heartbeat(&heartbeat("a", 1)));
        let stamped = table
            .get("a")
            .and_then(|info| info.last_heartbeat_unix_ms)
            .expect("heartbeat stamped");
        assert!(stamped >= before);
        assert!(stamped <= now_unix_ms());
    }
}
