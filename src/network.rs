//! Iroh endpoint setup, peer connections, and the hello handshake.
//!
//! A control connection starts with a hello exchange on one bidirectional stream: the dialer
//! opens the stream and sends its [`Hello`], the acceptor validates it, records the peer, and
//! replies with its own. Both sides reject peers whose hello does not match the authenticated
//! Iroh identity, protocol version, or cluster.
//!
//! After the hello, the dialer may open unidirectional streams of framed messages. The acceptor
//! records every [`Heartbeat`] on them in its [`PeerTable`] and logs and ignores anything else.
//!
//! [`serve_control_with`] can also enforce a heartbeat timeout: if a peer goes longer than
//! [`ControlOptions::heartbeat_timeout`] without a heartbeat the table accepts (measured on the
//! monotonic clock from the hello onward), the acceptor closes the connection with
//! [`HEARTBEAT_TIMEOUT_CODE`] and the session's usual cleanup forgets the peer. [`serve_control`]
//! never times peers out.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use iroh::endpoint::presets::{self, Preset};
use iroh::endpoint::{Connection, ConnectionError, Incoming, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::node::{NodeId, PeerTable};
use crate::protocol::{self, CONTROL_ALPN, Heartbeat, Hello, Message, PROTOCOL_VERSION};

/// Peer table shared between the accept loop and the code that dials out.
pub type SharedPeerTable = Arc<Mutex<PeerTable>>;

/// Longest a peer may take to complete the hello exchange before the connection is dropped.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

/// QUIC application close code for a connection dropped because its hello exchange failed.
const HELLO_REJECTED_CODE: u32 = 1;

/// QUIC close reason sent with [`HELLO_REJECTED_CODE`].
const HELLO_REJECTED_REASON: &[u8] = b"hello rejected";

/// QUIC application close code for a connection dropped because the peer stopped heartbeating.
pub const HEARTBEAT_TIMEOUT_CODE: u32 = 4;

/// QUIC close reason sent with [`HEARTBEAT_TIMEOUT_CODE`].
const HEARTBEAT_TIMEOUT_REASON: &[u8] = b"heartbeat timeout";

/// Options for [`serve_control_with`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ControlOptions {
    /// Longest an accepted peer may go without a heartbeat the [`PeerTable`] accepts before its
    /// connection is closed with [`HEARTBEAT_TIMEOUT_CODE`]; `None` (the default) never times out.
    ///
    /// The clock starts when the peer's hello is accepted. Stale heartbeats and heartbeats sent
    /// for another node do not reset it. `Some(Duration::ZERO)` is not meaningful and is treated
    /// as `None`; callers should map a configured `0` to `None` themselves.
    pub heartbeat_timeout: Option<Duration>,
}

/// Length of an Ed25519 secret key in bytes.
const SECRET_KEY_LEN: usize = 32;

/// Unix permissions for a new key file: read/write for the owner only.
#[cfg(unix)]
const OWNER_ONLY_MODE: u32 = 0o600;

/// Loads the secret key at `key_path` (creating and saving it if absent) and binds an endpoint accepting `alpns`.
///
/// Uses the n0 defaults: public relays plus DNS address lookup, so peers can dial by node ID alone.
pub async fn bind_endpoint(key_path: &Path, alpns: Vec<Vec<u8>>) -> anyhow::Result<Endpoint> {
    bind_with_preset(key_path, alpns, presets::N0).await
}

/// Like [`bind_endpoint`] but with no relays and no address lookup, for tests and isolated LANs.
///
/// Peers of a local endpoint must be dialed with a full [`EndpointAddr`] that carries direct
/// addresses, such as the one returned by [`Endpoint::addr`].
pub async fn bind_local_endpoint(key_path: &Path, alpns: Vec<Vec<u8>>) -> anyhow::Result<Endpoint> {
    bind_with_preset(key_path, alpns, presets::Minimal).await
}

/// Parses a node ID string as printed by `listen` (hex, or base32).
pub fn parse_node_id(text: &str) -> anyhow::Result<EndpointId> {
    text.trim()
        .parse()
        .with_context(|| format!("invalid node ID {text:?}"))
}

/// Builds the hello this node sends on every new control connection.
pub fn local_hello(endpoint: &Endpoint, cluster_id: &str, last_log_index: u64) -> Hello {
    Hello {
        node_id: endpoint.id().to_string(),
        protocol_version: PROTOCOL_VERSION,
        cluster_id: cluster_id.to_owned(),
        last_log_index,
    }
}

/// An open control connection to a peer that has completed the hello exchange.
///
/// The peer stays in the [`PeerTable`] for as long as this connection is its newest one. It is
/// removed when the connection closes, from either side, or when the session is dropped, which
/// closes the connection. A newer connection from the same peer is never removed, because cleanup
/// matches the connection token assigned during this session's handshake.
pub struct PeerSession {
    connection: Connection,
    hello: Hello,
    node_id: NodeId,
    connection_token: u64,
    peers: SharedPeerTable,
}

impl PeerSession {
    /// Wraps a handshaken connection and starts watching it, so a close by either side
    /// removes the peer from `peers`.
    fn start(
        connection: Connection,
        hello: Hello,
        connection_token: u64,
        peers: &SharedPeerTable,
    ) -> Self {
        let session = Self {
            node_id: connection.remote_id(),
            connection,
            hello,
            connection_token,
            peers: Arc::clone(peers),
        };
        session.forget_peer_when_closed();
        session
    }

    /// The open connection, for opening or accepting streams and closing it with a reason.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// The hello the peer sent during the handshake.
    pub fn hello(&self) -> &Hello {
        &self.hello
    }

    /// The peer's node ID, as authenticated by the connection.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The token this connection holds in the [`PeerTable`].
    pub fn connection_token(&self) -> u64 {
        self.connection_token
    }

    /// Spawns a task that removes this connection's entry once the connection closes.
    ///
    /// It waits on a weak handle, so the watch does not keep the connection open.
    fn forget_peer_when_closed(&self) {
        let closed = self.connection.weak_handle().closed();
        let peers = Arc::clone(&self.peers);
        let (node_id, token) = (self.node_id, self.connection_token);
        tokio::spawn(async move {
            closed.await;
            forget_peer(&peers, &node_id, token);
        });
    }
}

impl Drop for PeerSession {
    /// Removes the entry immediately, so a caller that drops a session never sees a stale peer.
    fn drop(&mut self) {
        forget_peer(&self.peers, &self.node_id, self.connection_token);
    }
}

impl std::fmt::Debug for PeerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerSession")
            .field("node_id", &self.node_id)
            .field("connection_token", &self.connection_token)
            .field("hello", &self.hello)
            .finish_non_exhaustive()
    }
}

/// Dials `addr` on [`CONTROL_ALPN`], exchanges hellos, and records the peer in `peers`.
pub async fn dial_peer(
    endpoint: &Endpoint,
    addr: impl Into<EndpointAddr>,
    local: &Hello,
    peers: &SharedPeerTable,
) -> anyhow::Result<PeerSession> {
    let connection = endpoint
        .connect(addr, CONTROL_ALPN)
        .await
        .context("failed to connect to peer")?;
    let hello = finish_handshake(&connection, dialer_handshake(&connection, local)).await?;
    let token = record_peer(peers, connection.remote_id(), &hello)?;
    Ok(PeerSession::start(connection, hello, token, peers))
}

/// Completes an incoming control connection: exchanges hellos and records the peer in `peers`.
pub async fn accept_peer(
    incoming: Incoming,
    local: &Hello,
    peers: &SharedPeerTable,
) -> anyhow::Result<PeerSession> {
    let connection = incoming.await.context("incoming connection failed")?;
    let (hello, token) =
        finish_handshake(&connection, acceptor_handshake(&connection, local, peers)).await?;
    Ok(PeerSession::start(connection, hello, token, peers))
}

/// Accepts control connections on `endpoint` until it is closed, keeping each one open until the peer leaves.
///
/// While a connection is open, heartbeats the peer sends on unidirectional streams are recorded in
/// `peers`. Idle peers are never timed out; use [`serve_control_with`] for that.
pub async fn serve_control(
    endpoint: Endpoint,
    local: Hello,
    peers: SharedPeerTable,
) -> anyhow::Result<()> {
    serve_control_with(endpoint, local, peers, ControlOptions::default()).await
}

/// Like [`serve_control`], but applies `options`, such as closing connections whose peer stops
/// heartbeating.
pub async fn serve_control_with(
    endpoint: Endpoint,
    local: Hello,
    peers: SharedPeerTable,
    options: ControlOptions,
) -> anyhow::Result<()> {
    let local = Arc::new(local);
    let heartbeat_timeout = options
        .heartbeat_timeout
        .filter(|timeout| !timeout.is_zero());
    let mut sessions = JoinSet::new();
    while let Some(incoming) = endpoint.accept().await {
        let local = Arc::clone(&local);
        let peers = Arc::clone(&peers);
        sessions.spawn(async move {
            run_control_session(incoming, &local, &peers, heartbeat_timeout).await;
        });
        // Reap finished sessions so a long-running listener does not accumulate them.
        while sessions.try_join_next().is_some() {}
    }
    sessions.shutdown().await;
    Ok(())
}

/// Handles one incoming control connection from hello to disconnect, recording the peer's
/// heartbeats while it is open and logging the outcome.
///
/// When the connection closes the session removes the peer from `peers`, unless it has already
/// reconnected on a newer connection. With a `heartbeat_timeout`, the session closes the
/// connection itself once the peer stops heartbeating.
async fn run_control_session(
    incoming: Incoming,
    local: &Hello,
    peers: &SharedPeerTable,
    heartbeat_timeout: Option<Duration>,
) {
    match accept_peer(incoming, local, peers).await {
        Ok(session) => {
            let watchdog = HeartbeatWatchdog::start(heartbeat_timeout);
            let reason = receive_control_streams(&session, peers, watchdog).await;
            info!(peer = %session.hello().node_id, %reason, "peer disconnected");
        }
        Err(error) => warn!("rejected incoming connection: {error:#}"),
    }
}

/// Removes `node_id` from `peers` if its entry still belongs to the connection with
/// `connection_token`; a newer connection from the same peer is kept.
fn forget_peer(peers: &SharedPeerTable, node_id: &NodeId, connection_token: u64) {
    match peers.lock() {
        Ok(mut table) => {
            if table.remove_connection(node_id, connection_token) {
                info!(peer = %node_id, "peer removed from table");
            }
        }
        Err(_) => warn!(peer = %node_id, "peer table lock poisoned; peer not removed"),
    }
}

/// Tracks when a session's peer must next heartbeat, on the monotonic clock.
struct HeartbeatWatchdog {
    timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl HeartbeatWatchdog {
    /// Arms the watchdog from now; with no `timeout` it never expires.
    fn start(timeout: Option<Duration>) -> Self {
        let mut watchdog = Self {
            timeout,
            deadline: None,
        };
        watchdog.reset();
        watchdog
    }

    /// Moves the deadline a full timeout past now, as after an accepted heartbeat.
    fn reset(&mut self) {
        self.deadline = self.timeout.map(|timeout| Instant::now() + timeout);
    }

    /// Completes when the deadline passes; never completes without a timeout.
    async fn expired(&self) {
        match self.deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    }
}

/// Reads every unidirectional stream the session's peer opens until the connection closes, or
/// closes it when `watchdog` expires; returns why it closed.
async fn receive_control_streams(
    session: &PeerSession,
    peers: &SharedPeerTable,
    mut watchdog: HeartbeatWatchdog,
) -> ConnectionError {
    let accepted_heartbeat = Arc::new(Notify::new());
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            accepted = session.connection().accept_uni() => match accepted {
                Ok(recv) => {
                    let reader = StreamReader {
                        peer: session.node_id(),
                        peers: Arc::clone(peers),
                        accepted_heartbeat: Arc::clone(&accepted_heartbeat),
                    };
                    streams.spawn(reader.run(recv));
                }
                Err(reason) => return reason,
            },
            Some(_) = streams.join_next(), if !streams.is_empty() => {}
            () = accepted_heartbeat.notified() => watchdog.reset(),
            () = watchdog.expired() => {
                return close_for_heartbeat_timeout(session, watchdog.timeout).await;
            }
        }
    }
}

/// Closes `session`'s connection because its peer stopped heartbeating; returns the close reason.
///
/// The peer is then removed by the session's own token-matched cleanup, so a newer connection
/// from the same peer is unaffected.
async fn close_for_heartbeat_timeout(
    session: &PeerSession,
    timeout: Option<Duration>,
) -> ConnectionError {
    warn!(peer = %session.node_id(), ?timeout, "peer stopped heartbeating; closing connection");
    let connection = session.connection();
    connection.close(HEARTBEAT_TIMEOUT_CODE.into(), HEARTBEAT_TIMEOUT_REASON);
    connection.closed().await
}

/// Reads one control stream from `peer`, signalling `accepted_heartbeat` for each heartbeat the
/// peer table accepts.
struct StreamReader {
    peer: NodeId,
    peers: SharedPeerTable,
    accepted_heartbeat: Arc<Notify>,
}

impl StreamReader {
    /// Reads framed messages until the stream ends, recording heartbeats and ignoring the rest.
    async fn run(self, mut recv: RecvStream) {
        let peer = self.peer;
        loop {
            match protocol::read_frame(&mut recv).await {
                Ok(Some(Message::Heartbeat(heartbeat))) => self.on_heartbeat(&heartbeat),
                Ok(Some(other)) => {
                    warn!(%peer, kind = message_kind(&other), "ignoring unexpected control message");
                }
                Ok(None) => return,
                Err(error) => {
                    warn!(%peer, %error, "control stream failed");
                    return;
                }
            }
        }
    }

    /// Records `heartbeat` and, if the table accepts it, resets the session's watchdog.
    fn on_heartbeat(&self, heartbeat: &Heartbeat) {
        if record_heartbeat(&self.peers, self.peer, heartbeat) {
            self.accepted_heartbeat.notify_one();
        }
    }
}

/// Records a heartbeat from `peer`, dropping it if it names another node or is stale; returns
/// whether the table accepted it.
fn record_heartbeat(peers: &SharedPeerTable, peer: NodeId, heartbeat: &Heartbeat) -> bool {
    if parse_node_id(&heartbeat.node_id).ok() != Some(peer) {
        warn!(%peer, claimed = %heartbeat.node_id, "ignoring heartbeat sent for another node");
        return false;
    }
    let Ok(mut table) = peers.lock() else {
        warn!(%peer, "peer table lock poisoned; dropping heartbeat");
        return false;
    };
    let accepted = table.record_heartbeat(&peer, heartbeat);
    if accepted {
        debug!(%peer, epoch = heartbeat.epoch, sequence = heartbeat.sequence, "heartbeat");
    } else {
        warn!(%peer, sequence = heartbeat.sequence, "ignoring stale heartbeat");
    }
    accepted
}

/// Names a message's variant for logs without dumping its (possibly large) payload.
fn message_kind(message: &Message) -> &'static str {
    match message {
        Message::Hello(_) => "hello",
        Message::Heartbeat(_) => "heartbeat",
        Message::Prompt(_) => "prompt",
        Message::Activations(_) => "activations",
        Message::History(_) => "history",
    }
}

/// Runs one side of the hello exchange under [`HELLO_TIMEOUT`], closing the connection with
/// [`HELLO_REJECTED_CODE`] on failure so the peer learns why it was dropped.
async fn finish_handshake<T>(
    connection: &Connection,
    handshake: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    let remote = connection.remote_id();
    let outcome = match tokio::time::timeout(HELLO_TIMEOUT, handshake).await {
        Ok(outcome) => outcome,
        Err(_) => Err(anyhow!("peer {remote} did not complete the hello in time")),
    };
    if outcome.is_err() {
        connection.close(HELLO_REJECTED_CODE.into(), HELLO_REJECTED_REASON);
    }
    outcome
}

/// Dialer side of the hello exchange: send ours, then read and validate theirs.
async fn dialer_handshake(connection: &Connection, local: &Hello) -> anyhow::Result<Hello> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("failed to open hello stream")?;
    protocol::write_frame(&mut send, &Message::Hello(local.clone())).await?;
    let hello = read_hello(&mut recv).await?;
    validate_hello(&hello, connection.remote_id(), local)?;
    finish_stream(&mut send)?;
    Ok(hello)
}

/// Acceptor side of the hello exchange: read and validate theirs, record the peer, then reply.
///
/// The peer is recorded before replying so that a dialer which has received our hello can rely on
/// being present in our table.
async fn acceptor_handshake(
    connection: &Connection,
    local: &Hello,
    peers: &SharedPeerTable,
) -> anyhow::Result<(Hello, u64)> {
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .context("peer never opened the hello stream")?;
    let hello = read_hello(&mut recv).await?;
    validate_hello(&hello, connection.remote_id(), local)?;
    let token = record_peer(peers, connection.remote_id(), &hello)?;
    let mut pending = PendingPeerRegistration {
        peers,
        remote: connection.remote_id(),
        token,
        active: true,
    };
    protocol::write_frame(&mut send, &Message::Hello(local.clone())).await?;
    finish_stream(&mut send)?;
    pending.active = false;
    Ok((hello, token))
}

/// Cleans up a peer recorded during a handshake if that handshake fails or times out, before a
/// [`PeerSession`] exists to do it.
struct PendingPeerRegistration<'a> {
    peers: &'a SharedPeerTable,
    remote: NodeId,
    token: u64,
    active: bool,
}

impl Drop for PendingPeerRegistration<'_> {
    fn drop(&mut self) {
        if self.active {
            forget_peer(self.peers, &self.remote, self.token);
        }
    }
}

async fn read_hello(recv: &mut RecvStream) -> anyhow::Result<Hello> {
    match protocol::read_frame(recv).await? {
        Some(Message::Hello(hello)) => Ok(hello),
        Some(other) => bail!("expected Hello, got {other:?}"),
        None => bail!("peer closed the stream before sending Hello"),
    }
}

/// Rejects a hello whose node ID differs from the authenticated `remote`, or whose version or cluster differ from ours.
fn validate_hello(hello: &Hello, remote: EndpointId, local: &Hello) -> anyhow::Result<()> {
    let claimed = parse_node_id(&hello.node_id)?;
    ensure!(
        claimed == remote,
        "peer {remote} claimed node ID {}",
        hello.node_id
    );
    ensure!(
        hello.protocol_version == local.protocol_version,
        "peer {remote} speaks protocol version {}, we speak {}",
        hello.protocol_version,
        local.protocol_version
    );
    ensure!(
        hello.cluster_id == local.cluster_id,
        "peer {remote} belongs to cluster {:?}, we belong to {:?}",
        hello.cluster_id,
        local.cluster_id
    );
    Ok(())
}

/// Records the validated peer under its authenticated node ID.
fn record_peer(peers: &SharedPeerTable, remote: NodeId, hello: &Hello) -> anyhow::Result<u64> {
    let mut table = peers
        .lock()
        .map_err(|_| anyhow!("peer table lock poisoned"))?;
    let info = table.record_connection(remote, hello);
    let token = info.connection_token;
    info!(
        peer = %info.node_id,
        protocol_version = info.protocol_version,
        connected_at_unix_ms = info.connected_at_unix_ms,
        "peer connected"
    );
    Ok(token)
}

fn finish_stream(send: &mut SendStream) -> anyhow::Result<()> {
    send.finish().context("failed to finish hello stream")
}

async fn bind_with_preset(
    key_path: &Path,
    alpns: Vec<Vec<u8>>,
    preset: impl Preset,
) -> anyhow::Result<Endpoint> {
    let secret_key = load_or_create_secret_key(key_path)?;
    let endpoint = Endpoint::builder(preset)
        .secret_key(secret_key)
        .alpns(alpns)
        .bind()
        .await
        .context("failed to bind Iroh endpoint")?;
    Ok(endpoint)
}

/// Reads the hex-encoded secret key at `path`, or generates one and writes it there.
fn load_or_create_secret_key(path: &Path) -> anyhow::Result<SecretKey> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .trim()
            .parse()
            .with_context(|| format!("invalid secret key in {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let secret_key = SecretKey::generate();
            save_secret_key(path, &secret_key)
                .with_context(|| format!("failed to save secret key to {}", path.display()))?;
            info!(path = %path.display(), "generated new node identity");
            Ok(secret_key)
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to read secret key {}", path.display()))
        }
    }
}

/// Writes a new key file, refusing to overwrite an existing one; owner-only on Unix.
fn save_secret_key(path: &Path, secret_key: &SecretKey) -> std::io::Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(OWNER_ONLY_MODE);
    }
    let mut file = options.open(path)?;
    file.write_all(encode_hex(&secret_key.to_bytes()).as_bytes())
}

fn encode_hex(bytes: &[u8; SECRET_KEY_LEN]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_key_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("meshthing-{}-{name}.key", std::process::id()))
    }

    #[test]
    fn secret_key_persists_across_loads() {
        let path = temp_key_path("persist");
        // A leftover file from an aborted run may or may not exist; either way we start fresh.
        let _ = std::fs::remove_file(&path);

        let created = load_or_create_secret_key(&path).expect("create key");
        let loaded = load_or_create_secret_key(&path).expect("load key");
        std::fs::remove_file(&path).expect("remove key");

        assert_eq!(created.public(), loaded.public());
    }

    #[test]
    fn corrupt_key_file_is_an_error() {
        let path = temp_key_path("corrupt");
        std::fs::write(&path, "not a key").expect("write key");

        let result = load_or_create_secret_key(&path);
        std::fs::remove_file(&path).expect("remove key");

        assert!(result.is_err());
    }

    #[test]
    fn node_id_round_trips_through_display() {
        let id = SecretKey::generate().public();

        assert_eq!(parse_node_id(&format!(" {id}\n")).expect("parse"), id);
        assert!(parse_node_id("nonsense").is_err());
    }

    #[test]
    fn hello_from_wrong_identity_or_cluster_is_rejected() {
        let remote = SecretKey::generate().public();
        let local = Hello {
            node_id: SecretKey::generate().public().to_string(),
            protocol_version: PROTOCOL_VERSION,
            cluster_id: "alpha".to_owned(),
            last_log_index: 0,
        };
        let honest = Hello {
            node_id: remote.to_string(),
            ..local.clone()
        };
        let impostor = Hello {
            node_id: SecretKey::generate().public().to_string(),
            ..honest.clone()
        };
        let other_cluster = Hello {
            cluster_id: "beta".to_owned(),
            ..honest.clone()
        };
        let other_version = Hello {
            protocol_version: PROTOCOL_VERSION + 1,
            ..honest.clone()
        };

        assert!(validate_hello(&honest, remote, &local).is_ok());
        assert!(validate_hello(&impostor, remote, &local).is_err());
        assert!(validate_hello(&other_cluster, remote, &local).is_err());
        assert!(validate_hello(&other_version, remote, &local).is_err());
    }
}
