//! Live history replication over control links, and electing a new leader when it is lost.
//!
//! The leader runs [`LeaderReplication`]: it records every completed turn in its
//! [`HistoryStore`] (saving before sending) and, every [`SYNC_INTERVAL`], sends each connected
//! worker a [`Roster`] and whatever history it lacks, as [`crate::history_sync`] sync messages
//! inside [`Message::History`] frames. Workers keep a [`ReplicaState`]: the roster plus their
//! own on-disk copy of the conversation, acknowledged only once saved.
//!
//! When a worker loses its leader, [`elect_successor`] picks a replacement from the last roster:
//! only workers that offered to take over ([`Message::Standby`]) qualify, ranked by
//! [`crate::history_sync::election::elect`]. Every worker ranks the same roster, so they agree on
//! the winner as long as they saw the same roster. That holds unless the leader died between
//! sending it to some workers and not others; a real quorum is future work (see
//! `docs/history-sync.md`).

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, warn};

use crate::history::{CompletedTurn, HistorySink, LoggingHistorySink};
use crate::history_sync::election::{Candidate, Elected, elect};
use crate::history_sync::history::Epoch;
use crate::history_sync::leader::Leader;
use crate::history_sync::message::{Outgoing, SyncMessage};
use crate::history_sync::node::Node;
use crate::history_sync::store::{HistoryStore, ModelManifest, StoreMeta};
use crate::network::{Outboxes, SharedPeerTable, parse_node_id};
use crate::node::NodeId;
use crate::protocol::{History, Message, Roster, RosterMember};

/// Conversation the cluster replicates unless configured otherwise.
pub const DEFAULT_CONVERSATION_ID: &str = "main";

/// How often the leader sends every worker the roster and any history it lacks. Sync messages
/// are idempotent, so resending them is also how lost messages and new workers are caught up.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(1);

/// Request field holding the chat messages; recorded as the job's prompt.
const MESSAGES_FIELD: &str = "messages";

/// Where a leader keeps the replicated conversation.
#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    /// Data directory holding `history/<conversation_id>/`.
    pub data_dir: PathBuf,
    /// Conversation the cluster replicates.
    pub conversation_id: String,
    /// Lowest epoch to lead in. The leader leads one epoch above the stored one, or in this
    /// epoch if it is higher, as when a worker takes over in the epoch it was elected in.
    pub min_epoch: Epoch,
}

/// The leader's side of replication; see the module docs.
pub struct LeaderReplication {
    node: Node,
    store: HistoryStore,
    peers: SharedPeerTable,
    outboxes: Outboxes,
    /// Workers that offered to take over.
    standbys: BTreeSet<NodeId>,
    conversation_id: String,
    model_hash: String,
}

impl LeaderReplication {
    /// Opens (or creates) the stored conversation and starts leading it in the next epoch.
    ///
    /// Fails if the stored conversation belongs to another cluster or model.
    pub fn open(
        id: NodeId,
        config: &ReplicationConfig,
        cluster_id: &str,
        model_hash: &str,
        peers: SharedPeerTable,
        outboxes: Outboxes,
    ) -> anyhow::Result<Self> {
        let model = ModelManifest {
            model_hash: model_hash.to_owned(),
            generation: Default::default(),
        };
        let meta = StoreMeta::new(&config.conversation_id, cluster_id, model);
        let mut store = HistoryStore::open(&config.data_dir, meta)
            .context("failed to open the replicated conversation")?;
        let epoch = (store.epoch() + 1).max(config.min_epoch);
        let node = Node::Leader(Leader::new(id, epoch, store.history().clone(), []));
        node.persist(&mut store)
            .context("failed to save the new epoch")?;
        info!(
            conversation = %config.conversation_id,
            epoch,
            entries = store.len(),
            "leading the replicated conversation"
        );
        Ok(Self {
            node,
            store,
            peers,
            outboxes,
            standbys: BTreeSet::new(),
            conversation_id: config.conversation_id.clone(),
            model_hash: model_hash.to_owned(),
        })
    }

    /// The epoch this leader leads in.
    pub fn epoch(&self) -> Epoch {
        self.node.epoch()
    }

    /// Records turns from `turns`, answers workers' messages from `inbox`, and refreshes every
    /// worker each [`SYNC_INTERVAL`], until both channels close.
    pub async fn run(
        mut self,
        mut turns: UnboundedReceiver<CompletedTurn>,
        mut inbox: UnboundedReceiver<(NodeId, Message)>,
    ) {
        let mut ticker = tokio::time::interval(SYNC_INTERVAL);
        loop {
            tokio::select! {
                Some(turn) = turns.recv() => self.record(&turn),
                Some((from, message)) = inbox.recv() => self.receive(from, message),
                _ = ticker.tick() => self.refresh(),
                else => return,
            }
        }
    }

    /// Records a completed turn, saves it, then sends it to every worker.
    fn record(&mut self, turn: &CompletedTurn) {
        let Some(leader) = self.node.as_leader_mut() else {
            warn!(request_id = %turn.request_id, "a newer leader took over; turn not replicated");
            return;
        };
        let out = leader.complete_task(&turn.request_id, &prompt_text(turn), &turn.response_text);
        // Saved before sending, so no worker ever holds an entry the leader lost.
        if let Err(error) = self.node.persist(&mut self.store) {
            error!(%error, request_id = %turn.request_id, "failed to save turn; not replicated");
            return;
        }
        self.send(out);
    }

    /// Handles a worker's sync reply or standby offer.
    fn receive(&mut self, from: NodeId, message: Message) {
        match message {
            Message::History(history) => {
                let Some(sync) = self.sync_message(from, &history) else {
                    return;
                };
                let was_leader = self.node.as_leader_mut().is_some();
                let out = self.node.handle(from, sync);
                if was_leader && self.node.as_leader_mut().is_none() {
                    error!(
                        epoch = self.node.epoch(),
                        "a newer leader took over this conversation; stopped replicating"
                    );
                }
                self.send(out);
            }
            Message::Standby(standby) => {
                if parse_node_id(&standby.node_id).ok() == Some(from) {
                    if self.standbys.insert(from) {
                        info!(peer = %from, "worker can take over as leader");
                    }
                } else {
                    warn!(peer = %from, claimed = %standby.node_id, "ignoring standby for another node");
                }
            }
            _ => debug!(peer = %from, "ignoring unexpected control message"),
        }
    }

    /// Decodes a worker's sync message for this conversation.
    fn sync_message(&self, from: NodeId, history: &History) -> Option<SyncMessage> {
        if history.conversation_id != self.conversation_id {
            warn!(peer = %from, conversation = %history.conversation_id, "ignoring history for another conversation");
            return None;
        }
        SyncMessage::from_bytes(&history.payload)
            .inspect_err(|error| warn!(peer = %from, %error, "ignoring malformed history message"))
            .ok()
    }

    /// Follows the peer table, then sends every worker the roster and any history it lacks.
    fn refresh(&mut self) {
        match self.peers.lock() {
            Ok(table) => self.node.set_peers(&table),
            Err(_) => warn!("peer table lock poisoned; peers not refreshed"),
        }
        let Some(leader) = self.node.as_leader_mut() else {
            return;
        };
        let out = leader.sync();
        let roster = self.roster();
        // The roster goes first: a worker opens its copy from it before applying history.
        for member in &roster.members {
            if let Ok(peer) = parse_node_id(&member.node_id) {
                self.outboxes.send(&peer, Message::Roster(roster.clone()));
            }
        }
        self.send(out);
    }

    /// Every connected worker, with how far it has synced.
    fn roster(&self) -> Roster {
        let leader = self.node.as_leader();
        let members = match self.peers.lock() {
            Ok(table) => table
                .peers()
                .map(|info| {
                    let acked = leader.and_then(|leader| leader.acked_len(&info.node_id));
                    RosterMember {
                        node_id: info.node_id.to_string(),
                        connected_at_unix_ms: info.connected_at_unix_ms,
                        standby: self.standbys.contains(&info.node_id),
                        history_epoch: acked.map_or(0, |_| self.node.epoch()),
                        history_len: acked.unwrap_or(0),
                    }
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        Roster {
            leader: self.node.id().to_string(),
            epoch: self.node.epoch(),
            conversation_id: self.conversation_id.clone(),
            model_hash: self.model_hash.clone(),
            members,
        }
    }

    /// Queues sync messages on their peers' connections; a disconnected peer is caught up later.
    fn send(&self, out: Vec<Outgoing>) {
        for (peer, sync) in out {
            let message = history_frame(&self.conversation_id, &sync);
            if !self.outboxes.send(&peer, message) {
                debug!(%peer, "peer not connected; history sent on its next sync");
            }
        }
    }
}

/// History sink that logs each turn and hands it to [`LeaderReplication::run`].
pub struct ReplicatingSink {
    turns: UnboundedSender<CompletedTurn>,
}

impl ReplicatingSink {
    /// A sink feeding the replication task that reads `turns`.
    pub fn new(turns: UnboundedSender<CompletedTurn>) -> Self {
        Self { turns }
    }
}

impl HistorySink for ReplicatingSink {
    fn record(&self, turn: CompletedTurn) {
        LoggingHistorySink.record(turn.clone());
        if self.turns.send(turn).is_err() {
            warn!("history replication stopped; turn not replicated");
        }
    }
}

/// A worker's side of replication: the last roster from its leader and, with a data
/// directory, its own saved copy of the conversation.
pub struct ReplicaState {
    id: NodeId,
    cluster_id: String,
    data_dir: Option<PathBuf>,
    copy: Option<StoredCopy>,
    roster: Option<Roster>,
}

/// A worker's open copy of the conversation.
struct StoredCopy {
    conversation_id: String,
    store: HistoryStore,
    node: Node,
}

impl ReplicaState {
    /// A worker with no roster yet; without `data_dir` it tracks rosters but keeps no history.
    pub fn new(id: NodeId, cluster_id: &str, data_dir: Option<PathBuf>) -> Self {
        Self {
            id,
            cluster_id: cluster_id.to_owned(),
            data_dir,
            copy: None,
            roster: None,
        }
    }

    /// The last roster a leader sent.
    pub fn roster(&self) -> Option<&Roster> {
        self.roster.as_ref()
    }

    /// Closes the saved copy, e.g. before this node leads from it; the next roster reopens it.
    pub fn close_copy(&mut self) {
        self.copy = None;
    }

    /// History entries saved on this worker.
    pub fn history_len(&self) -> u64 {
        self.copy.as_ref().map_or(0, |copy| copy.store.len())
    }

    /// Handles a message from the leader `from`; returns the replies to send back.
    pub fn receive(&mut self, from: NodeId, message: Message) -> Vec<Message> {
        match message {
            Message::Roster(roster) => {
                self.on_roster(from, roster);
                Vec::new()
            }
            Message::History(history) => self.on_history(from, &history),
            _ => {
                debug!(peer = %from, "ignoring unexpected control message");
                Vec::new()
            }
        }
    }

    fn on_roster(&mut self, from: NodeId, roster: Roster) {
        if parse_node_id(&roster.leader).ok() != Some(from) {
            warn!(peer = %from, claimed = %roster.leader, "ignoring roster sent for another leader");
            return;
        }
        let reopen = self
            .copy
            .as_ref()
            .is_none_or(|copy| copy.conversation_id != roster.conversation_id);
        if reopen && let Some(data_dir) = &self.data_dir {
            self.copy = open_copy(self.id, data_dir, &self.cluster_id, &roster)
                .inspect_err(|error| {
                    error!(error = %format!("{error:#}"), "cannot keep a copy of the conversation");
                })
                .ok();
        }
        self.roster = Some(roster);
    }

    fn on_history(&mut self, from: NodeId, history: &History) -> Vec<Message> {
        let Some(copy) = &mut self.copy else {
            debug!(peer = %from, "no copy of the conversation open yet; history dropped");
            return Vec::new();
        };
        if history.conversation_id != copy.conversation_id {
            warn!(peer = %from, conversation = %history.conversation_id, "ignoring history for another conversation");
            return Vec::new();
        }
        let sync = match SyncMessage::from_bytes(&history.payload) {
            Ok(sync) => sync,
            Err(error) => {
                warn!(peer = %from, %error, "ignoring malformed history message");
                return Vec::new();
            }
        };
        match copy.node.handle_durable(from, sync, &mut copy.store) {
            Ok(out) => out
                .into_iter()
                .map(|(_, reply)| history_frame(&copy.conversation_id, &reply))
                .collect(),
            // Without the save there is no ack, so the leader resends.
            Err(error) => {
                error!(%error, "failed to save replicated history");
                Vec::new()
            }
        }
    }
}

/// Opens this worker's copy of the roster's conversation, keeping any history already saved.
fn open_copy(
    id: NodeId,
    data_dir: &std::path::Path,
    cluster_id: &str,
    roster: &Roster,
) -> anyhow::Result<StoredCopy> {
    let model = ModelManifest {
        model_hash: roster.model_hash.clone(),
        generation: Default::default(),
    };
    let meta = StoreMeta::new(&roster.conversation_id, cluster_id, model);
    let store = HistoryStore::open(data_dir, meta)?;
    info!(
        conversation = %roster.conversation_id,
        entries = store.len(),
        "keeping a copy of the conversation"
    );
    Ok(StoredCopy {
        conversation_id: roster.conversation_id.clone(),
        node: Node::from_store(id, &store),
        store,
    })
}

/// Elects a new leader from `roster`, leaving out every node in `lost`: the leader that went
/// away and any winner that never took over. Only standbys qualify. `None` if none is left.
pub fn elect_successor(roster: &Roster, lost: &BTreeSet<NodeId>) -> Option<Elected> {
    elect(&candidates(roster, lost))
}

/// Tiebreaker after an election failed: the standby with the lowest node ID, leaving out `lost`,
/// in the epoch [`elect_successor`] would give. It ignores how far each worker synced, which is
/// what differs between rosters and splits votes; node IDs are the same in every copy.
pub fn tiebreak_successor(roster: &Roster, lost: &BTreeSet<NodeId>) -> Option<Elected> {
    let candidates = candidates(roster, lost);
    let epoch = elect(&candidates)?.epoch;
    let id = candidates.iter().map(|candidate| candidate.id).min()?;
    Some(Elected { id, epoch })
}

/// The roster's standbys not in `lost`, as election candidates.
fn candidates(roster: &Roster, lost: &BTreeSet<NodeId>) -> Vec<Candidate> {
    roster
        .members
        .iter()
        .filter(|member| member.standby)
        .filter_map(|member| {
            let id = parse_node_id(&member.node_id).ok()?;
            (!lost.contains(&id)).then_some(Candidate {
                id,
                epoch: roster.epoch,
                history_epoch: member.history_epoch,
                len: member.history_len,
                connected_at_unix_ms: member.connected_at_unix_ms,
            })
        })
        .collect()
}

/// Wraps a sync message for the wire.
fn history_frame(conversation_id: &str, sync: &SyncMessage) -> Message {
    Message::History(History {
        conversation_id: conversation_id.to_owned(),
        payload: sync.to_bytes(),
    })
}

/// The prompt recorded for a turn: its chat messages as JSON, or the whole request without them.
fn prompt_text(turn: &CompletedTurn) -> String {
    turn.request
        .get(MESSAGES_FIELD)
        .unwrap_or(&turn.request)
        .to_string()
}
