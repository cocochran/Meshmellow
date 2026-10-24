//! In-memory cluster for tests and the demo. Stands in for the Iroh transport:
//! every message is serialized to bytes and back, and links can be cut to
//! simulate dropped packets and offline peers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::election::{self, Candidate, Elected};
use crate::history::{History, NodeId};
use crate::leader::Leader;
use crate::message::{Outgoing, SyncMessage};
use crate::node::Node;
use crate::replica::Replica;

struct Envelope {
    from: NodeId,
    to: NodeId,
    bytes: Vec<u8>,
}

pub struct Cluster {
    nodes: BTreeMap<NodeId, Node>,
    queue: VecDeque<Envelope>,
    offline: BTreeSet<NodeId>,
    /// Number of upcoming messages to each node that will be silently dropped.
    drop_next: BTreeMap<NodeId, usize>,
    pub delivered: usize,
    pub dropped: usize,
}

impl Cluster {
    /// `n` nodes; node 0 starts as the main process in epoch 1.
    pub fn new(n: u64) -> Self {
        assert!(n >= 1);
        let mut nodes = BTreeMap::new();
        nodes.insert(0, Node::Leader(Leader::new(0, 1, History::new(), 1..n)));
        for id in 1..n {
            nodes.insert(id, Node::Replica(Replica::new(id)));
        }
        Self { nodes, queue: VecDeque::new(), offline: BTreeSet::new(), drop_next: BTreeMap::new(), delivered: 0, dropped: 0 }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[&id]
    }

    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.keys().copied()
    }

    /// The current, non-deposed leader that is online.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.nodes.iter().find_map(|(&id, node)| match node {
            Node::Leader(l) if l.deposed_by().is_none() && !self.offline.contains(&id) => Some(id),
            _ => None,
        })
    }

    pub fn leader_mut(&mut self, id: NodeId) -> &mut Leader {
        self.nodes.get_mut(&id).and_then(Node::as_leader_mut).expect("node is not a leader")
    }

    pub fn set_offline(&mut self, id: NodeId, offline: bool) {
        if offline {
            self.offline.insert(id);
        } else {
            self.offline.remove(&id);
        }
    }

    pub fn drop_next_to(&mut self, id: NodeId, count: usize) {
        self.drop_next.insert(id, count);
    }

    /// Runs a task on the given leader and queues its sync messages.
    pub fn complete_task(&mut self, leader: NodeId, task_id: u64, prompt: &str, response: &str) {
        let out = self.leader_mut(leader).complete_task(task_id, prompt, response);
        self.send(leader, out);
    }

    /// Asks the given leader to retransmit to any peer that isn't up to date.
    pub fn resync(&mut self, leader: NodeId) {
        let out = self.leader_mut(leader).sync();
        self.send(leader, out);
    }

    pub fn send(&mut self, from: NodeId, out: Vec<Outgoing>) {
        for (to, msg) in out {
            self.queue.push_back(Envelope { from, to, bytes: msg.to_bytes() });
        }
    }

    /// Delivers queued messages (and their replies) until the network is quiet.
    pub fn run(&mut self) {
        while let Some(Envelope { from, to, bytes }) = self.queue.pop_front() {
            if self.offline.contains(&to) || self.offline.contains(&from) || self.take_drop(to) {
                self.dropped += 1;
                continue;
            }
            let Some(node) = self.nodes.get_mut(&to) else { continue };
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
            .iter()
            .filter(|(id, _)| !self.offline.contains(id))
            .filter_map(|(_, node)| match node {
                Node::Replica(r) => Some(Candidate { id: r.id(), epoch: r.epoch(), len: r.len() }),
                Node::Leader(_) => None,
            })
            .collect();
        let elected = election::elect(&candidates)?;
        let peers: Vec<NodeId> = candidates.iter().map(|c| c.id).collect();
        let Some(Node::Replica(replica)) = self.nodes.remove(&elected.id) else { unreachable!() };
        let leader = replica.promote(elected.epoch, peers);
        let out = leader.sync();
        self.nodes.insert(elected.id, Node::Leader(leader));
        self.send(elected.id, out);
        Some(elected)
    }

    /// A previously offline peer reconnects to the mesh. The current leader adds
    /// it as a peer and snapshots it; if it was a stale leader it gets rejected.
    pub fn rejoin(&mut self, id: NodeId) {
        self.set_offline(id, false);
        if let Some(leader) = self.leader_id().filter(|&l| l != id) {
            self.leader_mut(leader).add_peer(id);
            self.resync(leader);
        }
    }

    /// True if every online node holds exactly the leader's history.
    pub fn converged(&self) -> bool {
        let Some(leader) = self.leader_id() else { return false };
        let expected = self.nodes[&leader].history();
        self.nodes
            .iter()
            .filter(|(id, _)| !self.offline.contains(id))
            .all(|(_, node)| node.history() == expected)
    }
}
