//! In-memory cluster for tests and the demo. Stands in for the Iroh transport:
//! every message is serialized to bytes and back, and links can be cut to
//! simulate dropped packets and offline peers. Membership is kept in a real
//! [`PeerTable`], which drives the leader's peer list and election candidates.

use std::collections::{BTreeMap, VecDeque};

use crate::history_sync::election::{self, Candidate, Elected};
use crate::history_sync::history::{History, NodeId};
use crate::history_sync::leader::Leader;
use crate::history_sync::message::{Outgoing, SyncMessage};
use crate::history_sync::node::Node;
use crate::history_sync::replica::Replica;
use crate::node::PeerTable;
use crate::protocol::{Hello, PROTOCOL_VERSION};

/// A deterministic node ID for simulated node number `index`.
pub fn node_id(index: u64) -> NodeId {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&(index + 1).to_le_bytes());
    iroh::SecretKey::from_bytes(&seed).public()
}

struct Envelope {
    from: NodeId,
    to: NodeId,
    bytes: Vec<u8>,
}

pub struct Cluster {
    /// Node IDs in creation order; index 0 is the first main process.
    ids: Vec<NodeId>,
    nodes: BTreeMap<NodeId, Node>,
    queue: VecDeque<Envelope>,
    /// Online nodes and when each connected, on a logical clock rather than wall time.
    /// One table stands in for every node's own view of the mesh.
    peers: PeerTable,
    clock: u64,
    /// Number of upcoming messages to each node that will be silently dropped.
    drop_next: BTreeMap<NodeId, usize>,
    pub delivered: usize,
    pub dropped: usize,
}

impl Cluster {
    /// `n` nodes; node 0 starts as the main process in epoch 1.
    pub fn new(n: u64) -> Self {
        assert!(n >= 1);
        let ids: Vec<NodeId> = (0..n).map(node_id).collect();
        let mut nodes = BTreeMap::new();
        let leader = Leader::new(ids[0], 1, History::new(), ids.iter().copied());
        nodes.insert(ids[0], Node::Leader(leader));
        for &id in &ids[1..] {
            nodes.insert(id, Node::Replica(Replica::new(id)));
        }
        let mut cluster = Self {
            ids,
            nodes,
            queue: VecDeque::new(),
            peers: PeerTable::new(),
            clock: 0,
            drop_next: BTreeMap::new(),
            delivered: 0,
            dropped: 0,
        };
        // Nodes join in index order, so node 0 has been connected the longest.
        for index in 0..n {
            cluster.connect(cluster.id(index));
        }
        cluster
    }

    /// The node ID of simulated node number `index`.
    pub fn id(&self, index: u64) -> NodeId {
        self.ids[index as usize]
    }

    /// The creation index of `id`, for printing.
    pub fn index(&self, id: NodeId) -> usize {
        self.ids
            .iter()
            .position(|&i| i == id)
            .expect("unknown node")
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[&id]
    }

    /// Node IDs in creation order.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.ids.iter().copied()
    }

    fn online(&self, id: &NodeId) -> bool {
        self.peers.get(id).is_some()
    }

    /// The current, non-deposed leader that is online.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.nodes.iter().find_map(|(&id, node)| match node {
            Node::Leader(l) if l.deposed_by().is_none() && self.online(&id) => Some(id),
            _ => None,
        })
    }

    pub fn leader_mut(&mut self, id: NodeId) -> &mut Leader {
        self.nodes
            .get_mut(&id)
            .and_then(Node::as_leader_mut)
            .expect("node is not a leader")
    }

    /// Takes a node off the mesh: it leaves the peer table and the leader stops syncing it.
    pub fn disconnect(&mut self, id: NodeId) {
        if let Some(connected_at) = self.peers.get(&id).map(|p| p.connected_at_unix_ms) {
            self.peers.remove_connection(&id, connected_at);
        }
        self.update_leader_peers();
    }

    /// Puts a node back on the mesh with a fresh connection time, without telling the
    /// leader. A returning stale leader can act on this before it is resynced.
    pub fn reconnect(&mut self, id: NodeId) {
        self.connect(id);
    }

    fn connect(&mut self, id: NodeId) {
        let hello = Hello {
            node_id: id.to_string(),
            protocol_version: PROTOCOL_VERSION,
            cluster_id: "sim".to_owned(),
            last_log_index: 0,
        };
        self.peers.record_connection_at(id, &hello, self.clock);
        self.clock += 1;
    }

    /// Points the current leader at exactly the online peers.
    fn update_leader_peers(&mut self) {
        if let Some(leader) = self.leader_id() {
            let node = self.nodes.get_mut(&leader).expect("leader exists");
            node.set_peers(&self.peers);
        }
    }

    pub fn drop_next_to(&mut self, id: NodeId, count: usize) {
        self.drop_next.insert(id, count);
    }

    /// Runs a task on the given leader and queues its sync messages.
    pub fn complete_task(&mut self, leader: NodeId, task_id: u64, prompt: &str, response: &str) {
        let out = self
            .leader_mut(leader)
            .complete_task(task_id, prompt, response);
        self.send(leader, out);
    }

    /// Asks the given leader to retransmit to any peer that isn't up to date.
    pub fn resync(&mut self, leader: NodeId) {
        let out = self.leader_mut(leader).sync();
        self.send(leader, out);
    }

    pub fn send(&mut self, from: NodeId, out: Vec<Outgoing>) {
        for (to, msg) in out {
            self.queue.push_back(Envelope {
                from,
                to,
                bytes: msg.to_bytes(),
            });
        }
    }

    /// Delivers queued messages (and their replies) until the network is quiet.
    pub fn run(&mut self) {
        while let Some(Envelope { from, to, bytes }) = self.queue.pop_front() {
            if !self.online(&to) || !self.online(&from) || self.take_drop(to) {
                self.dropped += 1;
                continue;
            }
            let Some(node) = self.nodes.get_mut(&to) else {
                continue;
            };
            let msg = SyncMessage::from_bytes(&bytes).expect("valid wire message");
            self.delivered += 1;
            let out = node.handle(from, msg);
            self.send(to, out);
        }
    }

    fn take_drop(&mut self, to: NodeId) -> bool {
        match self.drop_next.get_mut(&to) {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }

    /// Failover: the online replicas elect a new main process, which then
    /// snapshots its history to every other online peer.
    pub fn failover(&mut self) -> Option<Elected> {
        let candidates: Vec<Candidate> = self
            .nodes
            .values()
            .filter_map(|node| match node {
                Node::Replica(r) => {
                    let info = self.peers.get(&r.id())?;
                    Some(r.candidate(info.connected_at_unix_ms))
                }
                Node::Leader(_) => None,
            })
            .collect();
        let elected = election::elect(&candidates)?;
        let Some(Node::Replica(replica)) = self.nodes.remove(&elected.id) else {
            unreachable!()
        };
        let leader = replica.promote(elected.epoch, self.peers.ids());
        let out = leader.sync();
        self.nodes.insert(elected.id, Node::Leader(leader));
        self.send(elected.id, out);
        Some(elected)
    }

    /// A node reconnects and the current leader picks it up from the peer table and
    /// snapshots it; if it was a stale leader it gets rejected.
    pub fn rejoin(&mut self, id: NodeId) {
        if !self.online(&id) {
            self.connect(id);
        }
        self.update_leader_peers();
        if let Some(leader) = self.leader_id() {
            self.resync(leader);
        }
    }

    /// True if every online node holds exactly the leader's history.
    pub fn converged(&self) -> bool {
        let Some(leader) = self.leader_id() else {
            return false;
        };
        let expected = self.nodes[&leader].history();
        self.nodes
            .iter()
            .filter(|(id, _)| self.online(id))
            .all(|(_, node)| node.history() == expected)
    }
}
