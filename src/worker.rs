//! Worker process: supervised ggml-rpc-server, RPC bridge, heartbeats to the leader, a copy of
//! the conversation, and taking over when the leader is lost.
//!
//! A worker binds one Iroh endpoint (one identity per process). With an RPC server configured it
//! keeps llama.cpp's `ggml-rpc-server` running on a loopback port and exposes it to the leader
//! through [`rpc_bridge::serve_worker`]. When a leader is configured it also holds a control
//! link: it dials the leader, exchanges hellos, and writes [`Heartbeat`] frames on one
//! unidirectional stream, redialling with backoff whenever the link drops. Heartbeat sequence
//! numbers keep counting across reconnects, so every heartbeat this process sends has a larger
//! sequence than the one before it.
//!
//! The worker also watches the leader's own heartbeats on the link (see
//! [`network::watch_control`]). Once the leader has sent one, a gap longer than
//! [`WorkerConfig::leader_heartbeat_timeout`] makes the worker close the link with
//! [`network::HEARTBEAT_TIMEOUT_CODE`] and redial, instead of waiting for QUIC's idle timeout to
//! notice a crashed or suspended leader. A leader that never heartbeats is never timed out.
//!
//! Over the same link the leader replicates the conversation and sends its roster
//! ([`crate::replication`]); with a data directory the worker saves its own copy. If the leader
//! stays unreachable for [`WorkerConfig::failover_after`], the worker elects a replacement from
//! the last roster. A worker with a [`TakeoverConfig`] offers itself as a standby; if it wins, it
//! stops being a worker and runs the leader on its own endpoint, from its copy of the
//! conversation and in the new epoch. Every other worker follows the winner, and if the winner
//! never comes up they leave it out and elect again.
//!
//! The winner only leads once a majority of the cluster it was elected in has joined it (see
//! [`leader::Quorum`]). If they don't, it stands down to a worker, leaves itself out of the next
//! election and follows whoever wins it. Two workers that each think they won therefore can't
//! both serve, and neither can a standby cut off from a leader that is still running.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use iroh::endpoint::ConnectionError;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::history_sync::history::Epoch;
use crate::leader::{self, BackendConfig, LeaderConfig, NoQuorum, Quorum, ShardBackendConfig};
use crate::network::{self, HEARTBEAT_EPOCH, Inbox, SharedPeerTable};
use crate::node::{NodeId, PeerTable};
use crate::process::{self, ChildSpec, Readiness, RestartPolicy, Supervised};
use crate::protocol::{CONTROL_ALPN, Heartbeat, Hello, Message, RPC_BRIDGE_ALPN, Roster, Standby};
use crate::replication::{ReplicaState, ReplicationConfig, elect_successor, tiebreak_successor};
use crate::rpc_bridge;
use crate::shard::manifest::ShardManifest;

/// How often a worker sends a heartbeat to its leader unless configured otherwise.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How long a worker waits for the leader's next heartbeat before dropping the link and
/// redialling, unless configured otherwise: three missed beats at the leader's 2s default
/// (`leader::DEFAULT_HEARTBEAT_INTERVAL`) plus a second of slack.
pub const DEFAULT_LEADER_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(7);

/// How long the leader must stay unreachable before workers elect a replacement, unless
/// configured otherwise.
pub const DEFAULT_FAILOVER_AFTER: Duration = Duration::from_secs(10);

/// How long a worker that won an election waits for a majority to join it, unless configured
/// otherwise: the other workers need their own failover window, plus slack, to follow it.
pub const DEFAULT_QUORUM_WAIT: Duration = Duration::from_secs(30);

/// Log label for the supervised RPC server.
const RPC_SERVER_NAME: &str = "ggml-rpc-server";

/// Longest the RPC server may take to accept TCP connections after each (re)start.
const RPC_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Workers keep no replicated log yet, so their hello always reports an empty one.
const LAST_LOG_INDEX: u64 = 0;

/// Longest one attempt to dial the leader and finish the hello may take.
const LEADER_DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// First pause before redialling a leader whose link dropped.
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);

/// Longest pause between redial attempts.
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Everything `llm-cluster worker` needs to run.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Secret key file holding this node's identity; created if absent.
    pub key_path: PathBuf,
    /// Cluster this worker belongs to; the leader rejects hellos from other clusters.
    pub cluster_id: String,
    /// The `ggml-rpc-server` binary to supervise; `None` runs no RPC server and no bridge, so
    /// the worker only holds the control link (a copy of the conversation, and a standby).
    pub rpc_server: Option<PathBuf>,
    /// Loopback port the RPC server listens on, usually [`rpc_bridge::DEFAULT_RPC_PORT`].
    pub rpc_port: u16,
    /// Extra RPC server arguments, appended after `-H 127.0.0.1 -p <rpc_port>`.
    pub rpc_args: Vec<String>,
    /// Leader to keep a control link to; `None` runs the bridge only.
    pub leader: Option<EndpointId>,
    /// Peers besides the leader allowed to use this worker's RPC bridge (`--allow-peer`).
    pub allowed_peers: Vec<EndpointId>,
    /// Time between heartbeats on the control link.
    pub heartbeat_interval: Duration,
    /// How long the worker waits for the leader's next heartbeat before closing the control link
    /// with [`network::HEARTBEAT_TIMEOUT_CODE`] and redialling, usually
    /// [`DEFAULT_LEADER_HEARTBEAT_TIMEOUT`]. It arms only once the leader has sent a heartbeat on
    /// the current link, so a leader that never heartbeats is never timed out. Must exceed the
    /// leader's heartbeat interval and must not be zero; `None` disables it, leaving a lost
    /// leader to QUIC's idle timeout.
    pub leader_heartbeat_timeout: Option<Duration>,
    /// Data directory for this worker's copy of the conversation; `None` keeps no copy.
    pub data_dir: Option<PathBuf>,
    /// How to lead if this worker is elected to replace a lost leader; `None` never takes over.
    /// Needs a leader and a data directory.
    pub takeover: Option<TakeoverConfig>,
    /// How long the leader must stay unreachable before the workers elect a replacement.
    pub failover_after: Duration,
}

/// How a worker leads after taking over: the shard backend, from a manifest written for this
/// machine (its own local stages, and remote stages that allow this worker's node ID).
#[derive(Debug, Clone)]
pub struct TakeoverConfig {
    /// Shard manifest the new leader serves; checked when the worker starts.
    pub manifest_path: PathBuf,
    /// Loopback address the new leader's job API listens on.
    pub api_addr: SocketAddr,
    /// Internal loopback port of the new leader's shard front end.
    pub upstream_port: u16,
    /// How long the new leader retries its readiness probe.
    pub worker_wait: Duration,
    /// The new leader's timeout for its workers' heartbeats; `None` disables it.
    pub heartbeat_timeout: Option<Duration>,
    /// How often the new leader heartbeats to its workers; `None` sends none.
    pub heartbeat_interval: Option<Duration>,
    /// How long the new leader waits for a majority to join before standing down.
    pub quorum_wait: Duration,
}

impl TakeoverConfig {
    /// Leads from `manifest_path` with the job API on `api_addr` and the leader's defaults for
    /// everything else.
    pub fn new(manifest_path: PathBuf, api_addr: SocketAddr) -> Self {
        Self {
            manifest_path,
            api_addr,
            upstream_port: leader::DEFAULT_LLAMA_PORT,
            worker_wait: leader::DEFAULT_WORKER_WAIT,
            heartbeat_timeout: Some(leader::DEFAULT_HEARTBEAT_TIMEOUT),
            heartbeat_interval: Some(leader::DEFAULT_HEARTBEAT_INTERVAL),
            quorum_wait: DEFAULT_QUORUM_WAIT,
        }
    }
}

/// Runs a worker until the RPC server fails for good or the future is dropped.
///
/// Binds one endpoint accepting [`CONTROL_ALPN`] and [`RPC_BRIDGE_ALPN`], prints `node id: <hex>`,
/// then behaves as [`run_worker_on`], dialling the leader by node ID alone.
pub async fn run_worker(config: WorkerConfig) -> anyhow::Result<()> {
    let endpoint = network::bind_endpoint(&config.key_path, worker_alpns()).await?;
    println!("node id: {}", endpoint.id());
    let leader = config.leader.map(EndpointAddr::from);
    run_worker_on(endpoint, leader, &config).await
}

/// Runs a worker on an endpoint the caller has already bound, dialling `leader` for the control link.
///
/// `config.key_path` and `config.leader` are ignored: the endpoint carries the identity, and
/// `leader` may carry direct addresses (as tests on isolated endpoints need). With an RPC server
/// configured, supervises it on `127.0.0.1:<rpc_port>` and bridges [`RPC_BRIDGE_ALPN`] streams to
/// it; only `leader` and `config.allowed_peers` may use the bridge, and with neither this fails
/// before starting anything. If `leader` is set, holds the control link, and if this worker is
/// elected leader, runs the leader on the endpoint from then on (see the module docs). Returns an
/// error once the RPC server's restarts are exhausted. Dropping the future kills the RPC server
/// and closes the endpoint.
pub async fn run_worker_on(
    endpoint: Endpoint,
    leader: Option<EndpointAddr>,
    config: &WorkerConfig,
) -> anyhow::Result<()> {
    let _close_on_drop = CloseOnDrop(endpoint.clone());
    let outcome = serve(&endpoint, leader, config).await;
    endpoint.close().await;
    outcome
}

/// ALPNs a worker endpoint accepts.
fn worker_alpns() -> Vec<Vec<u8>> {
    vec![CONTROL_ALPN.to_vec(), RPC_BRIDGE_ALPN.to_vec()]
}

/// Starts the RPC server, if any, then runs the bridge and control link until one of them ends.
/// Each time this worker is elected it leads; if no majority joins it, it stands down and
/// carries on as a worker.
async fn serve(
    endpoint: &Endpoint,
    leader: Option<EndpointAddr>,
    config: &WorkerConfig,
) -> anyhow::Result<()> {
    check_takeover(config, leader.is_some())?;
    let leader_id = leader.as_ref().map(|addr| addr.id);
    let mut rpc = match &config.rpc_server {
        Some(program) => Some(RpcService::start(config, program, leader_id).await?),
        None if leader.is_none() => {
            bail!("worker has nothing to do: pass --rpc-server, --leader, or both")
        }
        None => None,
    };
    let mut link = leader.map(|leader| ControlLink::new(endpoint, leader, config));
    let outcome = loop {
        let promotion = tokio::select! {
            ended = run_rpc(endpoint, rpc.as_mut()) => break ended,
            promoted = follow(link.as_mut()) => match promoted {
                Some(promotion) => promotion,
                None => break Ok(()),
            },
        };
        // Only a standby is elected, and a standby runs no RPC server, so the leader has the
        // endpoint to itself.
        let Some(link) = link.as_mut() else {
            break Ok(());
        };
        // The leader opens the conversation itself; the worker's copy reopens on its next roster.
        link.replica.close_copy();
        match take_over(endpoint, config, &promotion).await {
            Err(error) if error.downcast_ref::<NoQuorum>().is_some() => {
                warn!(error = %format!("{error:#}"), "no majority joined; standing down to a worker");
                link.failover.stood_down(Instant::now());
            }
            led => break led,
        }
    };
    if let Some(rpc) = rpc {
        rpc.stop().await;
    }
    outcome
}

/// Runs the control link until the endpoint closes (`None`) or this worker is elected; never
/// resolves without a link.
async fn follow(link: Option<&mut ControlLink<'_>>) -> Option<Promotion> {
    match link {
        Some(link) => link.run().await,
        None => std::future::pending().await,
    }
}

/// Checks a standby's settings before anything starts, including its takeover manifest.
fn check_takeover(config: &WorkerConfig, has_leader: bool) -> anyhow::Result<()> {
    let Some(takeover) = &config.takeover else {
        return Ok(());
    };
    ensure!(
        has_leader,
        "a standby needs --leader: it takes over from that leader"
    );
    ensure!(
        config.rpc_server.is_none(),
        "a standby runs no RPC server: it leads on the same endpoint after taking over"
    );
    ensure!(
        config.data_dir.is_some(),
        "a standby needs a data directory to keep the conversation in"
    );
    ShardManifest::load(&takeover.manifest_path).context("invalid takeover manifest")?;
    Ok(())
}

/// The supervised RPC server and who may reach it through the bridge.
struct RpcService {
    server: Supervised,
    addr: SocketAddr,
    allowed: rpc_bridge::PeerAllowlist,
}

impl RpcService {
    async fn start(
        config: &WorkerConfig,
        program: &Path,
        leader: Option<EndpointId>,
    ) -> anyhow::Result<Self> {
        let allowed = bridge_allowlist(config, leader)?;
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.rpc_port));
        let server = process::spawn_supervised(rpc_server_spec(config, program, addr))
            .await
            .with_context(|| format!("failed to start {RPC_SERVER_NAME}"))?;
        info!(rpc_addr = %addr, "{RPC_SERVER_NAME} ready");
        Ok(Self {
            server,
            addr,
            allowed,
        })
    }

    async fn stop(self) {
        if let Err(error) = self.server.shutdown().await {
            warn!(error = %format!("{error:#}"), "failed to stop {RPC_SERVER_NAME} cleanly");
        }
    }
}

/// Bridges to the RPC server until the endpoint closes (`Ok`) or the server fails for good;
/// never resolves without an RPC server.
async fn run_rpc(endpoint: &Endpoint, rpc: Option<&mut RpcService>) -> anyhow::Result<()> {
    let Some(rpc) = rpc else {
        return std::future::pending().await;
    };
    tokio::select! {
        exited = rpc.server.wait() => match exited {
            Ok(()) => Err(anyhow!("{RPC_SERVER_NAME} stopped")),
            Err(error) => Err(error.context(format!("{RPC_SERVER_NAME} exhausted its restarts"))),
        },
        served = rpc_bridge::serve_worker(endpoint.clone(), rpc.addr, rpc.allowed.clone()) => served,
    }
}

/// Peers allowed to use the RPC bridge: the configured extras plus the leader this worker serves.
fn bridge_allowlist(
    config: &WorkerConfig,
    leader: Option<EndpointId>,
) -> anyhow::Result<rpc_bridge::PeerAllowlist> {
    let peers = config.allowed_peers.iter().copied().chain(leader);
    rpc_bridge::PeerAllowlist::new(peers)
        .context("worker has no approved peers: pass --leader or --allow-peer")
}

/// Command line and supervision policy for the RPC server; it never listens beyond loopback.
fn rpc_server_spec(config: &WorkerConfig, program: &Path, rpc_addr: SocketAddr) -> ChildSpec {
    let mut args = vec![
        "-H".to_owned(),
        rpc_addr.ip().to_string(),
        "-p".to_owned(),
        rpc_addr.port().to_string(),
    ];
    args.extend(config.rpc_args.iter().cloned());
    ChildSpec {
        name: RPC_SERVER_NAME.to_owned(),
        program: program.to_path_buf(),
        args,
        readiness: Readiness::TcpPort(rpc_addr),
        ready_timeout: RPC_READY_TIMEOUT,
        restart: RestartPolicy::default(),
    }
}

/// This worker won an election: it leads `conversation_id` in `epoch` once `needed` of
/// `members` (the rest of the cluster it was elected in) have joined it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Promotion {
    epoch: Epoch,
    conversation_id: String,
    members: BTreeSet<NodeId>,
    needed: usize,
}

impl Promotion {
    /// Winning `epoch` among `roster`'s workers. The cluster is the lost leader plus every worker
    /// in the roster, this one included; a majority of it is more than half, and this worker
    /// counts itself.
    fn from_roster(roster: &Roster, me: NodeId, epoch: Epoch) -> Self {
        let members: BTreeSet<NodeId> = roster
            .members
            .iter()
            .filter_map(|member| network::parse_node_id(&member.node_id).ok())
            .filter(|id| *id != me)
            .collect();
        let cluster = roster.members.len() + 1;
        Self {
            epoch,
            conversation_id: roster.conversation_id.clone(),
            members,
            needed: cluster / 2,
        }
    }
}

/// Stops being a worker and runs the leader on `endpoint` from this worker's copy of the
/// conversation, until the leader stops or fails.
async fn take_over(
    endpoint: &Endpoint,
    config: &WorkerConfig,
    promotion: &Promotion,
) -> anyhow::Result<()> {
    let takeover = config
        .takeover
        .as_ref()
        .context("elected leader without a takeover configuration")?;
    let data_dir = config
        .data_dir
        .clone()
        .context("elected leader without a data directory")?;
    info!(
        epoch = promotion.epoch,
        needed = promotion.needed,
        "elected leader; taking over once a majority joins"
    );
    let leader_config = LeaderConfig {
        key_path: config.key_path.clone(),
        cluster_id: config.cluster_id.clone(),
        backend: BackendConfig::Shard(ShardBackendConfig {
            manifest_path: takeover.manifest_path.clone(),
        }),
        upstream_port: takeover.upstream_port,
        api_addr: takeover.api_addr,
        worker_wait: takeover.worker_wait,
        heartbeat_timeout: takeover.heartbeat_timeout,
        heartbeat_interval: takeover.heartbeat_interval,
        replication: Some(ReplicationConfig {
            data_dir,
            conversation_id: promotion.conversation_id.clone(),
            min_epoch: promotion.epoch,
        }),
        quorum: Some(Quorum {
            members: promotion.members.clone(),
            needed: promotion.needed,
            wait: takeover.quorum_wait,
        }),
    };
    // The leader stops when the worker future is dropped, as the worker itself does.
    leader::lead_on(endpoint, &leader_config, std::future::pending()).await
}

/// State of the worker's control link that survives reconnects.
struct ControlLink<'a> {
    endpoint: &'a Endpoint,
    /// The leader this worker dials: the configured one, or the replacement it follows.
    target: EndpointAddr,
    local: Hello,
    /// Holds the leader while a link is up, with the last heartbeat the leader sent on it.
    peers: SharedPeerTable,
    heartbeats: HeartbeatSequence,
    backoff: ReconnectBackoff,
    interval: Duration,
    leader_heartbeat_timeout: Option<Duration>,
    /// The roster and this worker's copy of the conversation.
    replica: ReplicaState,
    /// Where the link delivers the leader's rosters and history.
    inbox: Inbox,
    received: UnboundedReceiver<(NodeId, Message)>,
    /// Whether this worker offers to take over.
    standby: bool,
    failover: Failover,
}

impl<'a> ControlLink<'a> {
    /// A link to `leader` that has not dialled yet.
    fn new(endpoint: &'a Endpoint, leader: EndpointAddr, config: &WorkerConfig) -> Self {
        let (inbox, received) = unbounded_channel();
        Self {
            endpoint,
            target: leader,
            local: network::local_hello(endpoint, &config.cluster_id, LAST_LOG_INDEX),
            peers: Arc::new(Mutex::new(PeerTable::new())),
            heartbeats: HeartbeatSequence::new(endpoint.id().to_string()),
            backoff: ReconnectBackoff::default(),
            interval: config.heartbeat_interval,
            leader_heartbeat_timeout: config.leader_heartbeat_timeout,
            replica: ReplicaState::new(endpoint.id(), &config.cluster_id, config.data_dir.clone()),
            inbox,
            received,
            standby: config.takeover.is_some(),
            failover: Failover::new(config.failover_after),
        }
    }

    /// Runs sessions back to back, pausing with backoff between them, until the endpoint closes
    /// (`None`) or this worker is elected leader.
    async fn run(&mut self) -> Option<Promotion> {
        loop {
            let Err(error) = self.session().await;
            if self.endpoint.is_closed() {
                return None;
            }
            let leader = self.target.id;
            warn!(
                leader = %leader.fmt_short(),
                error = %format!("{error:#}"),
                "control link to leader lost"
            );
            let step = self.failover.link_lost(
                leader,
                self.endpoint.id(),
                self.replica.roster(),
                Instant::now(),
            );
            match step {
                FailoverStep::Redial => {}
                FailoverStep::Follow(next) => {
                    info!(
                        lost = %leader.fmt_short(),
                        next = %next.fmt_short(),
                        "following the elected replacement"
                    );
                    self.target = EndpointAddr::from(next);
                    self.backoff.reset();
                }
                FailoverStep::Lead(promotion) => return Some(promotion),
            }
            let delay = self.backoff.next_delay();
            info!(retry_in_ms = delay.as_millis(), "redialling the leader");
            tokio::time::sleep(delay).await;
        }
    }

    /// Dials the leader, then for as long as the link lasts: sends heartbeats, watches the
    /// leader's, applies its rosters and history, and sends the replies.
    async fn session(&mut self) -> anyhow::Result<Infallible> {
        self.failover.dialling();
        let dial = network::dial_peer(self.endpoint, self.target.clone(), &self.local, &self.peers);
        let session = tokio::time::timeout(self.failover.dial_timeout(), dial)
            .await
            .context("timed out dialling the leader")??;
        self.backoff.reset();
        self.failover.connected();
        info!(leader = %session.hello().node_id, "control link to leader established");

        let (outbound, mut queue) = unbounded_channel();
        if self.standby {
            let offer = Standby {
                node_id: self.endpoint.id().to_string(),
            };
            // The queue is read below, so it is open.
            let _ = outbound.send(Message::Standby(offer));
        }
        let watch = network::watch_control(
            &session,
            &self.peers,
            self.leader_heartbeat_timeout,
            Some(&self.inbox),
        );
        // These run for the whole session: the send loops only end on a failed write, the watch
        // only when the connection closes (whether the leader closed it or the watch did), and
        // replication never.
        let heartbeats = &mut self.heartbeats;
        let send = network::write_heartbeats(session.connection(), self.interval, || {
            heartbeats.next_heartbeat()
        });
        let write = network::write_messages(session.connection(), &mut queue);
        let replicate = replicate(&mut self.replica, &mut self.received, &outbound);
        tokio::select! {
            sent = send => sent,
            written = write => written,
            reason = watch => Err(link_lost(reason)),
            never = replicate => match never {},
        }
    }
}

/// Applies the leader's rosters and history as they arrive, queueing replies on `outbound`.
async fn replicate(
    replica: &mut ReplicaState,
    received: &mut UnboundedReceiver<(NodeId, Message)>,
    outbound: &UnboundedSender<Message>,
) -> Infallible {
    // The link holds the inbox's sender, so the channel never closes while this runs.
    while let Some((from, message)) = received.recv().await {
        for reply in replica.receive(from, message) {
            // A closed queue means the session is ending; the leader resends what matters.
            let _ = outbound.send(reply);
        }
    }
    std::future::pending().await
}

/// What a worker does after its link to the leader fails.
#[derive(Debug, PartialEq, Eq)]
enum FailoverStep {
    /// Keep redialling the current leader.
    Redial,
    /// Dial this node instead: the elected replacement, or the original leader again once no
    /// replacement is left.
    Follow(NodeId),
    /// This worker won the election.
    Lead(Promotion),
}

/// Decides when a worker that lost its leader elects a replacement, and whom it follows.
///
/// The first election after losing a leader ranks standbys by [`elect_successor`]. If it fails
/// (the winner never came up, or let workers in and then stood down for lack of a majority),
/// every later election uses [`tiebreak_successor`], the lowest node ID, until a leader takes.
/// Votes split because rosters disagree on sync progress; node IDs never differ, so the
/// tiebreak settles on one node. Only replacements this worker could not reach are left out: a
/// winner that stood down is alive and stays eligible, or two of them would each follow the
/// other.
struct Failover {
    /// How long the leader must stay unreachable first.
    after: Duration,
    /// When the current leader was first found unreachable; `None` while connected.
    lost_since: Option<Instant>,
    /// Elected replacements this worker could not reach; left out of later elections.
    lost: BTreeSet<NodeId>,
    /// Whether an election since the leader was lost has failed, so the tiebreak decides.
    tiebreak: bool,
    /// Whether the latest dial got through.
    reached: bool,
}

impl Failover {
    fn new(after: Duration) -> Self {
        Self {
            after,
            lost_since: None,
            lost: BTreeSet::new(),
            tiebreak: false,
            reached: false,
        }
    }

    /// This worker won but no majority joined it at `now`. The next election, after a full
    /// window, uses the tiebreak; this worker stays eligible in it.
    fn stood_down(&mut self, now: Instant) {
        self.tiebreak = true;
        self.lost_since = Some(now);
    }

    /// A dial is starting.
    fn dialling(&mut self) {
        self.reached = false;
    }

    /// A dial got through; the next loss of this link starts the clock again.
    fn connected(&mut self) {
        self.reached = true;
        self.lost_since = None;
    }

    /// How long one dial may take: while the leader is lost, no longer than the failover
    /// window, so an unreachable leader cannot delay the election.
    fn dial_timeout(&self) -> Duration {
        match self.lost_since {
            Some(_) => self.after.min(LEADER_DIAL_TIMEOUT),
            None => LEADER_DIAL_TIMEOUT,
        }
    }

    /// The link to `leader` failed at `now`. Once it has been down for the whole window, elects
    /// a replacement for it from `roster`, the last one this worker (`me`) received.
    fn link_lost(
        &mut self,
        leader: NodeId,
        me: NodeId,
        roster: Option<&Roster>,
        now: Instant,
    ) -> FailoverStep {
        let original = roster.and_then(|roster| network::parse_node_id(&roster.leader).ok());
        if self.lost_since.is_none() {
            // A link that was up just dropped. Losing the roster's own leader starts a fresh
            // failover; a replacement that let this worker in and then dropped it stood down.
            if original == Some(leader) {
                self.lost.clear();
                self.tiebreak = false;
            } else {
                self.tiebreak = true;
            }
        }
        let since = *self.lost_since.get_or_insert(now);
        if now.duration_since(since) < self.after {
            return FailoverStep::Redial;
        }
        let Some(roster) = roster else {
            return FailoverStep::Redial;
        };
        // Whatever happens next gets a full window before the next decision.
        self.lost_since = Some(now);
        if original != Some(leader) && !self.reached {
            // An elected replacement that never answered: presumed gone.
            self.lost.insert(leader);
            self.tiebreak = true;
        }
        let elected = if self.tiebreak {
            tiebreak_successor(roster, &self.lost)
        } else {
            elect_successor(roster, &self.lost)
        };
        match elected {
            Some(elected) if elected.id == me => {
                FailoverStep::Lead(Promotion::from_roster(roster, me, elected.epoch))
            }
            Some(elected) => FailoverStep::Follow(elected.id),
            None => {
                warn!("leader lost and no standby can take over");
                match network::parse_node_id(&roster.leader) {
                    Ok(original) if original != leader => FailoverStep::Follow(original),
                    _ => FailoverStep::Redial,
                }
            }
        }
    }
}

/// Explains why [`network::watch_control`] saw the control link close.
///
/// Only the watch closes the connection locally during a session (closing the endpoint ends
/// the worker instead of redialling), so a local close means the leader went quiet.
fn link_lost(reason: ConnectionError) -> anyhow::Error {
    match reason {
        ConnectionError::LocallyClosed => anyhow!("leader stopped heartbeating"),
        other => anyhow!("leader closed the control link: {other}"),
    }
}

/// Produces this node's heartbeats with strictly increasing sequence numbers, starting at 1.
struct HeartbeatSequence {
    node_id: String,
    last: u64,
}

impl HeartbeatSequence {
    fn new(node_id: String) -> Self {
        Self { node_id, last: 0 }
    }

    fn next_heartbeat(&mut self) -> Heartbeat {
        self.last += 1;
        Heartbeat {
            node_id: self.node_id.clone(),
            epoch: HEARTBEAT_EPOCH,
            sequence: self.last,
        }
    }
}

/// Delay between redials of the leader: exponential per consecutive failure, restarting after a
/// successful hello.
#[derive(Default)]
struct ReconnectBackoff {
    consecutive_failures: u32,
}

impl ReconnectBackoff {
    /// The worker redials for as long as it runs, so only the delays of this policy are used.
    const POLICY: RestartPolicy = RestartPolicy {
        max_restarts: u32::MAX,
        initial_backoff: RECONNECT_INITIAL_BACKOFF,
        max_backoff: RECONNECT_MAX_BACKOFF,
    };

    fn next_delay(&mut self) -> Duration {
        let delay = Self::POLICY.backoff(self.consecutive_failures);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        delay
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
    }
}

/// Closes the endpoint in the background if the worker future is dropped before it could.
struct CloseOnDrop(Endpoint);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if self.0.is_closed() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let endpoint = self.0.clone();
            runtime.spawn(async move { endpoint.close().await });
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn config() -> WorkerConfig {
        WorkerConfig {
            key_path: PathBuf::from("worker.key"),
            cluster_id: "test".to_owned(),
            rpc_server: Some(PathBuf::from("ggml-rpc-server")),
            rpc_port: rpc_bridge::DEFAULT_RPC_PORT,
            rpc_args: vec!["--threads".to_owned(), "4".to_owned()],
            leader: None,
            allowed_peers: Vec::new(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            leader_heartbeat_timeout: Some(DEFAULT_LEADER_HEARTBEAT_TIMEOUT),
            data_dir: None,
            takeover: None,
            failover_after: DEFAULT_FAILOVER_AFTER,
        }
    }

    #[test]
    fn a_locally_closed_link_means_the_leader_went_quiet() {
        let quiet = link_lost(ConnectionError::LocallyClosed);
        let timed_out = link_lost(ConnectionError::TimedOut);

        assert_eq!(quiet.to_string(), "leader stopped heartbeating");
        assert!(
            timed_out
                .to_string()
                .starts_with("leader closed the control link"),
            "{timed_out}"
        );
    }

    #[test]
    fn bridge_allowlist_approves_the_leader_and_extra_peers() {
        let leader = SecretKey::generate().public();
        let extra = SecretKey::generate().public();
        let stranger = SecretKey::generate().public();
        let mut config = config();
        config.allowed_peers = vec![extra];

        let allowed = bridge_allowlist(&config, Some(leader)).expect("allowlist");

        assert!(allowed.contains(&leader));
        assert!(allowed.contains(&extra));
        assert!(!allowed.contains(&stranger));
    }

    #[test]
    fn bridge_allowlist_fails_closed_without_leader_or_peers() {
        let error = bridge_allowlist(&config(), None).expect_err("no approved peers");
        assert!(format!("{error:#}").contains("--allow-peer"), "{error:#}");
    }

    #[test]
    fn rpc_server_listens_on_loopback_with_extra_args_last() {
        let rpc_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 50_100));
        let program = PathBuf::from("ggml-rpc-server");
        let spec = rpc_server_spec(&config(), &program, rpc_addr);

        assert_eq!(
            spec.args,
            ["-H", "127.0.0.1", "-p", "50100", "--threads", "4"]
        );
        assert_eq!(spec.program, PathBuf::from("ggml-rpc-server"));
        assert!(matches!(spec.readiness, Readiness::TcpPort(addr) if addr == rpc_addr));
    }

    #[test]
    fn worker_accepts_control_and_bridge_alpns() {
        assert_eq!(
            worker_alpns(),
            [CONTROL_ALPN.to_vec(), RPC_BRIDGE_ALPN.to_vec()]
        );
    }

    #[test]
    fn heartbeats_count_up_from_one_in_epoch_zero() {
        let node_id = SecretKey::generate().public().to_string();
        let mut sequence = HeartbeatSequence::new(node_id.clone());

        let sent: Vec<Heartbeat> = (0..3).map(|_| sequence.next_heartbeat()).collect();

        assert_eq!(
            sent.iter().map(|beat| beat.sequence).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert!(
            sent.iter()
                .all(|beat| beat.epoch == 0 && beat.node_id == node_id)
        );
    }

    const WINDOW: Duration = Duration::from_secs(10);

    fn node(seed: u8) -> NodeId {
        SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// A roster from `leader` in epoch 3 listing `members` as `(node, standby, history_len)`;
    /// earlier members connected earlier.
    fn roster_of(leader: NodeId, members: &[(NodeId, bool, u64)]) -> Roster {
        Roster {
            leader: leader.to_string(),
            epoch: 3,
            conversation_id: "main".to_owned(),
            model_hash: "sha256:00".to_owned(),
            members: members
                .iter()
                .zip(0..)
                .map(
                    |(&(id, standby, len), order)| crate::protocol::RosterMember {
                        node_id: id.to_string(),
                        connected_at_unix_ms: 1_000 + order,
                        standby,
                        history_epoch: 3,
                        history_len: len,
                    },
                )
                .collect(),
        }
    }

    #[test]
    fn a_lost_leader_is_redialled_until_the_window_passes() {
        let (leader, me, other) = (node(1), node(2), node(3));
        let roster = roster_of(leader, &[(me, true, 4), (other, true, 4)]);
        let mut failover = Failover::new(WINDOW);
        let start = Instant::now();

        let early = failover.link_lost(leader, me, Some(&roster), start);
        let still_early = failover.link_lost(leader, me, Some(&roster), start + WINDOW / 2);

        assert_eq!(
            (early, still_early),
            (FailoverStep::Redial, FailoverStep::Redial)
        );
        assert!(failover.dial_timeout() <= WINDOW);
    }

    #[test]
    fn the_longest_connected_standby_with_the_most_history_takes_over() {
        let (leader, me, other) = (node(1), node(2), node(3));
        let roster = roster_of(leader, &[(other, true, 4), (me, true, 4)]);
        let start = Instant::now();

        // Equal history: `other` connected first, so it wins and `me` follows it.
        let mut follower = Failover::new(WINDOW);
        follower.link_lost(leader, me, Some(&roster), start);
        let step = follower.link_lost(leader, me, Some(&roster), start + WINDOW);
        assert_eq!(step, FailoverStep::Follow(other));

        // The same roster makes `other` lead, in the epoch after the roster's.
        let mut winner = Failover::new(WINDOW);
        winner.link_lost(leader, other, Some(&roster), start);
        let step = winner.link_lost(leader, other, Some(&roster), start + WINDOW);
        // A majority of {leader, other, me} is two: `other` itself plus `me`.
        let promotion = Promotion {
            epoch: 4,
            conversation_id: "main".to_owned(),
            members: BTreeSet::from([me]),
            needed: 1,
        };
        assert_eq!(step, FailoverStep::Lead(promotion));

        // More history beats connecting first.
        let ahead = roster_of(leader, &[(other, true, 4), (me, true, 5)]);
        let mut ahead_failover = Failover::new(WINDOW);
        ahead_failover.link_lost(leader, me, Some(&ahead), start);
        let step = ahead_failover.link_lost(leader, me, Some(&ahead), start + WINDOW);
        assert!(matches!(step, FailoverStep::Lead(_)), "{step:?}");
    }

    #[test]
    fn a_winner_that_never_comes_up_is_left_out_of_the_next_election() {
        let (leader, me, first, second) = (node(1), node(2), node(3), node(4));
        let roster = roster_of(
            leader,
            &[(first, true, 4), (second, true, 4), (me, false, 4)],
        );
        let mut failover = Failover::new(WINDOW);
        let start = Instant::now();
        failover.link_lost(leader, me, Some(&roster), start);

        let step = failover.link_lost(leader, me, Some(&roster), start + WINDOW);
        assert_eq!(step, FailoverStep::Follow(first));
        let step = failover.link_lost(first, me, Some(&roster), start + WINDOW * 2);
        assert_eq!(step, FailoverStep::Follow(second));
        // With every standby gone, go back to the original leader.
        let step = failover.link_lost(second, me, Some(&roster), start + WINDOW * 3);
        assert_eq!(step, FailoverStep::Follow(leader));

        // A fresh connection starts the next failover from scratch.
        failover.connected();
        assert_eq!(failover.dial_timeout(), LEADER_DIAL_TIMEOUT);
        failover.link_lost(leader, me, Some(&roster), start + WINDOW * 4);
        let step = failover.link_lost(leader, me, Some(&roster), start + WINDOW * 5);
        assert_eq!(step, FailoverStep::Follow(first));
    }

    #[test]
    fn a_winner_needs_more_than_half_the_cluster_counting_the_lost_leader() {
        let (leader, me) = (node(1), node(2));
        let others: Vec<NodeId> = (3..=6).map(node).collect();
        // (workers in the roster, followers the winner needs)
        for (workers, needed) in [(1, 1), (2, 1), (3, 2), (4, 2), (5, 3)] {
            let mut members = vec![(me, true, 4)];
            members.extend(others[..workers - 1].iter().map(|&id| (id, false, 4)));
            let promotion = Promotion::from_roster(&roster_of(leader, &members), me, 4);
            assert_eq!(promotion.needed, needed, "{workers} workers");
            assert_eq!(promotion.members.len(), workers - 1);
            assert!(!promotion.members.contains(&me));
        }
    }

    #[test]
    fn a_split_vote_settles_on_the_lowest_node_id_after_both_winners_stand_down() {
        let (leader, a, b, follower) = (node(1), node(2), node(3), node(4));
        let lowest = a.min(b);
        // Each winner's last roster put its own history ahead, so each elected itself.
        let roster_a = roster_of(leader, &[(a, true, 5), (b, true, 4), (follower, false, 4)]);
        let roster_b = roster_of(leader, &[(a, true, 4), (b, true, 5), (follower, false, 4)]);
        let start = Instant::now();
        let decide = |me: NodeId, roster: &Roster, failover: &mut Failover| {
            failover.link_lost(leader, me, Some(roster), start);
            failover.link_lost(leader, me, Some(roster), start + WINDOW)
        };
        let (mut at_a, mut at_b) = (Failover::new(WINDOW), Failover::new(WINDOW));
        assert!(matches!(
            decide(a, &roster_a, &mut at_a),
            FailoverStep::Lead(_)
        ));
        assert!(matches!(
            decide(b, &roster_b, &mut at_b),
            FailoverStep::Lead(_)
        ));

        // Neither gets a majority, so both stand down; after a window both pick the same node.
        let retry = |me: NodeId, roster: &Roster, failover: &mut Failover| {
            failover.stood_down(start + WINDOW * 2);
            failover.link_lost(leader, me, Some(roster), start + WINDOW * 3)
        };
        let step_a = retry(a, &roster_a, &mut at_a);
        let step_b = retry(b, &roster_b, &mut at_b);
        for (me, step) in [(a, &step_a), (b, &step_b)] {
            if me == lowest {
                assert!(matches!(step, FailoverStep::Lead(_)), "{step:?}");
            } else {
                assert_eq!(*step, FailoverStep::Follow(lowest));
            }
        }

        // A worker let in by a winner that then stood down keeps it eligible and agrees too.
        let mut at_follower = Failover::new(WINDOW);
        at_follower.dialling();
        at_follower.connected();
        at_follower.link_lost(b, follower, Some(&roster_a), start + WINDOW * 2);
        let step = at_follower.link_lost(b, follower, Some(&roster_a), start + WINDOW * 3);
        assert_eq!(step, FailoverStep::Follow(lowest));
    }

    #[test]
    fn the_tiebreak_skips_an_unreachable_replacement_and_resets_once_a_leader_takes() {
        let (leader, me, other) = (node(1), node(2), node(3));
        let (low, high) = (me.min(other), me.max(other));
        let roster = roster_of(leader, &[(low, true, 4), (high, true, 4)]);
        let observer = node(4);
        let mut failover = Failover::new(WINDOW);
        let start = Instant::now();
        failover.link_lost(leader, observer, Some(&roster), start);
        assert_eq!(
            failover.link_lost(leader, observer, Some(&roster), start + WINDOW),
            FailoverStep::Follow(low)
        );
        // `low` never answers, so the tiebreak leaves it out.
        failover.dialling();
        assert_eq!(
            failover.link_lost(low, observer, Some(&roster), start + WINDOW * 2),
            FailoverStep::Follow(high)
        );

        // `high` takes over and sends its own roster; losing it later is a fresh failover.
        failover.dialling();
        failover.connected();
        let next = roster_of(high, &[(low, true, 9), (observer, true, 9)]);
        failover.link_lost(high, observer, Some(&next), start + WINDOW * 3);
        let step = failover.link_lost(high, observer, Some(&next), start + WINDOW * 4);
        let ranked = elect_successor(&next, &BTreeSet::new()).expect("winner").id;
        let expected = if ranked == observer {
            matches!(step, FailoverStep::Lead(_))
        } else {
            step == FailoverStep::Follow(ranked)
        };
        assert!(expected, "{step:?}");
        assert!(
            !failover.tiebreak,
            "a fresh failover ranks by sync progress again"
        );
    }

    #[test]
    fn without_a_roster_or_a_standby_the_leader_is_redialled() {
        let (leader, me) = (node(1), node(2));
        let start = Instant::now();
        let mut no_roster = Failover::new(WINDOW);
        no_roster.link_lost(leader, me, None, start);
        assert_eq!(
            no_roster.link_lost(leader, me, None, start + WINDOW),
            FailoverStep::Redial
        );

        let not_standby = roster_of(leader, &[(me, false, 4)]);
        let mut no_standby = Failover::new(WINDOW);
        no_standby.link_lost(leader, me, Some(&not_standby), start);
        assert_eq!(
            no_standby.link_lost(leader, me, Some(&not_standby), start + WINDOW),
            FailoverStep::Redial
        );
    }

    #[test]
    fn a_standby_needs_a_leader_a_data_directory_and_a_valid_manifest() {
        let mut standby = config();
        standby.takeover = Some(TakeoverConfig::new(
            PathBuf::from("missing-manifest.json"),
            leader::DEFAULT_API_ADDR,
        ));
        let no_leader = check_takeover(&standby, false).unwrap_err();
        assert!(
            format!("{no_leader:#}").contains("--leader"),
            "{no_leader:#}"
        );
        let with_rpc = check_takeover(&standby, true).unwrap_err();
        assert!(
            format!("{with_rpc:#}").contains("RPC server"),
            "{with_rpc:#}"
        );
        standby.rpc_server = None;
        let no_data = check_takeover(&standby, true).unwrap_err();
        assert!(
            format!("{no_data:#}").contains("data directory"),
            "{no_data:#}"
        );
        standby.data_dir = Some(PathBuf::from("data"));
        let bad_manifest = check_takeover(&standby, true).unwrap_err();
        assert!(
            format!("{bad_manifest:#}").contains("takeover manifest"),
            "{bad_manifest:#}"
        );
        assert!(check_takeover(&config(), false).is_ok());
    }

    #[test]
    fn reconnect_backoff_doubles_up_to_the_cap_and_resets() {
        let mut backoff = ReconnectBackoff::default();

        let delays: Vec<Duration> = (0..7).map(|_| backoff.next_delay()).collect();
        assert_eq!(delays[0], RECONNECT_INITIAL_BACKOFF);
        assert_eq!(delays[1], RECONNECT_INITIAL_BACKOFF * 2);
        assert!(delays.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(delays[6], RECONNECT_MAX_BACKOFF);

        backoff.reset();
        assert_eq!(backoff.next_delay(), RECONNECT_INITIAL_BACKOFF);
    }
}
