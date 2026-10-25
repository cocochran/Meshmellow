//! Opaque TCP-over-Iroh bridge for llama.cpp RPC traffic.
//!
//! On the leader, llama.cpp connects to a loopback TCP port served by [`run_leader`], which
//! carries every TCP connection over its own bidirectional QUIC stream (on [`RPC_BRIDGE_ALPN`])
//! to the worker. There [`serve_worker`] dials the local `ggml-rpc-server` for each stream.
//! Bytes are forwarded untouched: the bridge never frames or inspects RPC traffic.
//!
//! # Bridging several workers
//!
//! One leader process can bridge several workers over one shared endpoint. [`LeaderBridges::bind`]
//! takes one [`BridgeMapping`] (worker node ID → leader-local loopback address) per worker and
//! binds every listener before returning; port 0 picks a free port. [`LeaderBridges::addrs`] then
//! reports the address each worker is actually reachable on, in input order, which is what
//! llama.cpp's `--rpc` list needs. [`run_leader_multi`] binds and serves in one call, and
//! [`run_leader`] is its single-worker form. On the command line the same mapping is
//! `llm-cluster bridge-connect --worker <NODE_ID>=<ADDR> [--worker ...]`, which prints the bound
//! mapping as one JSON line: `{"bridges":[{"worker":"<id>","listen":"127.0.0.1:50053"},...]}`.
//!
//! `ggml-rpc-server` has no authentication of its own, so the worker only serves peers on a
//! [`PeerAllowlist`]: an authenticated Iroh identity proves who a peer is, not that it may use
//! this machine. Unapproved peers are closed before any byte reaches the RPC server.
//!
//! Every entry point is cancel-safe: dropping its future stops accepting and aborts every
//! in-flight stream, and each returns `Ok(())` once its endpoint is closed.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use iroh::endpoint::{Connection, Incoming, RecvStream, SendStream, VarInt};
use iroh::{Endpoint, EndpointId};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{self, JoinError, JoinSet};
use tracing::{debug, error, info, warn};

use crate::protocol::RPC_BRIDGE_ALPN;

/// Port llama.cpp's `ggml-rpc-server` listens on by default.
pub const DEFAULT_RPC_PORT: u16 = 50052;

/// Per-direction copy buffer; QUIC and TCP flow control bound everything beyond it.
const COPY_BUFFER_LEN: usize = 64 * 1024;

/// Application error code used to reset a bridged stream that failed part-way.
const STREAM_ABORTED: VarInt = VarInt::from_u32(1);

/// Application error code used to close a connection that negotiated another protocol.
const WRONG_ALPN: VarInt = VarInt::from_u32(2);

/// Application error code used to close a connection from a peer that is not on the allowlist.
pub const PEER_NOT_APPROVED: VarInt = VarInt::from_u32(3);

/// Close reason sent with [`PEER_NOT_APPROVED`].
pub const PEER_NOT_APPROVED_REASON: &[u8] = b"peer not approved";

/// Pause after a failed TCP accept so a persistent error (such as fd exhaustion) cannot spin.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Maximum active local TCP clients across all leader-side worker bridges.
const MAX_BRIDGE_CLIENTS: usize = 128;

/// Maximum incoming worker-side connections and streams per connection.
const MAX_BRIDGE_CONNECTIONS: usize = 128;
const MAX_BRIDGE_STREAMS_PER_CONNECTION: usize = 64;

/// Maximum active worker-side streams across all remote connections.
const MAX_BRIDGE_STREAMS_TOTAL: usize = 256;

/// Port that asks the OS for any free port; any number of mappings may use it at once.
const ANY_PORT: u16 = 0;

/// Leader: accept TCP on `listen` (loopback only — refuse non-loopback addresses); for each TCP
/// connection open one bidirectional Iroh stream to `worker` on [`RPC_BRIDGE_ALPN`] and copy
/// bytes both ways until both directions finish.
///
/// All streams share one Iroh connection, which is re-dialled if it closes. A failing stream is
/// torn down on its own without affecting the listener or other streams. This is
/// [`run_leader_multi`] with a single mapping.
pub async fn run_leader(
    endpoint: Endpoint,
    worker: EndpointId,
    listen: SocketAddr,
) -> anyhow::Result<()> {
    run_leader_multi(endpoint, &[BridgeMapping { worker, listen }]).await
}

/// Leader: bridges every worker in `mappings` over the shared `endpoint`, as [`run_leader`] does
/// for one. Binds every listener first ([`LeaderBridges::bind`]), then serves until the endpoint
/// closes ([`LeaderBridges::serve`]).
pub async fn run_leader_multi(
    endpoint: Endpoint,
    mappings: &[BridgeMapping],
) -> anyhow::Result<()> {
    LeaderBridges::bind(endpoint, mappings).await?.serve().await
}

/// One worker's leader-side bridge: RPC clients connecting to `listen` reach `worker`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BridgeMapping {
    /// The worker's Iroh node ID, as printed by its `bridge-serve` or `worker` command.
    pub worker: EndpointId,
    /// Leader-local loopback address llama.cpp connects to; port 0 picks a free port on bind.
    pub listen: SocketAddr,
}

/// Bound leader-side bridges for several workers sharing one endpoint.
///
/// Every listener is already bound, so RPC clients can connect (and queue) as soon as
/// [`LeaderBridges::bind`] returns, even before [`LeaderBridges::serve`] runs.
#[derive(Debug)]
pub struct LeaderBridges {
    endpoint: Endpoint,
    bridges: Vec<BoundBridge>,
}

/// One worker's bound listener and the address it actually got.
#[derive(Debug)]
struct BoundBridge {
    worker: EndpointId,
    listener: TcpListener,
    local: SocketAddr,
}

impl LeaderBridges {
    /// Binds every listener in `mappings` before returning (loopback only; port 0 picks a free
    /// port). Refuses an empty list, a worker mapped twice, the same listen address twice and
    /// any non-loopback address, all before binding anything.
    pub async fn bind(endpoint: Endpoint, mappings: &[BridgeMapping]) -> anyhow::Result<Self> {
        validate_mappings(mappings)?;
        let mut bridges = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            bridges.push(BoundBridge::bind(*mapping).await?);
        }
        Ok(Self { endpoint, bridges })
    }

    /// The address each worker is actually reachable on, in input order.
    pub fn addrs(&self) -> Vec<BridgeMapping> {
        self.bridges
            .iter()
            .map(|bridge| BridgeMapping {
                worker: bridge.worker,
                listen: bridge.local,
            })
            .collect()
    }

    /// Serves every bridge until the endpoint closes (`Ok(())`), or returns an error naming the
    /// worker whose bridge stopped. Cancel-safe: dropping the future aborts every bridge and
    /// every in-flight stream.
    pub async fn serve(self) -> anyhow::Result<()> {
        let Self { endpoint, bridges } = self;
        let mut workers = HashMap::with_capacity(bridges.len());
        let mut accept_loops = JoinSet::new();
        let client_slots = Arc::new(Semaphore::new(MAX_BRIDGE_CLIENTS));
        for bridge in bridges {
            let worker = bridge.worker;
            let handle = accept_loops.spawn(accept_clients(
                endpoint.clone(),
                bridge,
                Arc::clone(&client_slots),
            ));
            workers.insert(handle.id(), worker);
        }
        tokio::select! {
            Some(joined) = accept_loops.join_next() => Err(bridge_stopped(joined, &workers)),
            () = endpoint.closed() => {
                info!("endpoint closed; rpc bridge leader stopping");
                Ok(())
            }
        }
    }
}

impl BoundBridge {
    /// Binds `mapping`'s listener; the address must already have passed [`ensure_loopback`].
    async fn bind(mapping: BridgeMapping) -> anyhow::Result<Self> {
        let BridgeMapping { worker, listen } = mapping;
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("failed to bind rpc bridge listener on {listen}"))?;
        let local = listener
            .local_addr()
            .context("failed to read listener address")?;
        info!(listen = %local, worker = %worker.fmt_short(), "rpc bridge leader listening");
        Ok(Self {
            worker,
            listener,
            local,
        })
    }
}

/// Checks mappings before anything is bound: non-empty, loopback only, each worker once, and
/// each fixed listen address once (any number of mappings may ask for port 0).
fn validate_mappings(mappings: &[BridgeMapping]) -> anyhow::Result<()> {
    if mappings.is_empty() {
        bail!("no rpc bridges to bind: at least one worker mapping is required");
    }
    let mut workers = HashSet::new();
    let mut listens = HashSet::new();
    for BridgeMapping { worker, listen } in mappings {
        ensure_loopback(*listen, "listen")?;
        if !workers.insert(worker) {
            bail!("worker {worker} is mapped to more than one rpc bridge listener");
        }
        if listen.port() != ANY_PORT && !listens.insert(listen) {
            bail!("rpc bridge listen address {listen} is mapped to more than one worker");
        }
    }
    Ok(())
}

/// Leader side of one worker: accepts TCP clients on its listener forever, bridging each one to
/// the worker over a shared Iroh connection. Dropping the future aborts every stream.
async fn accept_clients(
    endpoint: Endpoint,
    bridge: BoundBridge,
    client_slots: Arc<Semaphore>,
) -> Infallible {
    let link = Arc::new(WorkerLink::new(endpoint, bridge.worker));
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            accepted = bridge.listener.accept() => match accepted {
                Ok((tcp, client)) => {
                    let Ok(permit) = client_slots.clone().try_acquire_owned() else {
                        debug!(%client, limit = MAX_BRIDGE_CLIENTS, "rpc bridge client limit reached; dropping client");
                        drop(tcp);
                        continue;
                    };
                    let link = Arc::clone(&link);
                    streams.spawn(async move {
                        let _permit = permit;
                        forward_to_worker(link, tcp, client).await;
                    });
                }
                Err(error) => {
                    warn!(%error, "failed to accept rpc client");
                    tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                }
            },
            Some(joined) = streams.join_next(), if !streams.is_empty() => log_task_panic(joined),
        }
    }
}

/// Describes a worker's accept loop that ended. It never returns, so it panicked.
fn bridge_stopped(
    joined: Result<Infallible, JoinError>,
    workers: &HashMap<task::Id, EndpointId>,
) -> anyhow::Error {
    let Err(join_error) = joined;
    match workers.get(&join_error.id()) {
        Some(worker) => anyhow!("rpc bridge for worker {worker} stopped: {join_error}"),
        None => anyhow!("rpc bridge stopped: {join_error}"),
    }
}

/// Iroh node IDs allowed to use a worker's RPC bridge.
///
/// Never empty: a bridge with no approved peers must not serve anyone, so construction fails
/// instead (fail closed).
#[derive(Clone, Debug)]
pub struct PeerAllowlist(Arc<HashSet<EndpointId>>);

impl PeerAllowlist {
    /// Builds an allowlist from `peers`, refusing an empty one.
    pub fn new(peers: impl IntoIterator<Item = EndpointId>) -> anyhow::Result<Self> {
        let peers: HashSet<EndpointId> = peers.into_iter().collect();
        if peers.is_empty() {
            bail!("no approved peers: the rpc bridge refuses to serve without an allowlist");
        }
        Ok(Self(Arc::new(peers)))
    }

    /// Whether `peer` may use the bridge.
    pub fn contains(&self, peer: &EndpointId) -> bool {
        self.0.contains(peer)
    }
}

/// Worker: accept [`RPC_BRIDGE_ALPN`] connections on `endpoint` from peers on `allowed`; for each
/// incoming bi-stream, dial `target` (loopback only) and copy both ways.
///
/// Connections that negotiated any other ALPN are closed, and so are peers missing from `allowed`
/// (with [`PEER_NOT_APPROVED`]) before any stream reaches `target`. Returns once the endpoint is
/// closed.
pub async fn serve_worker(
    endpoint: Endpoint,
    target: SocketAddr,
    allowed: PeerAllowlist,
) -> anyhow::Result<()> {
    ensure_loopback(target, "target")?;
    info!(id = %endpoint.id(), %target, approved_peers = allowed.0.len(), "rpc bridge worker serving");

    let connection_slots = Arc::new(Semaphore::new(MAX_BRIDGE_CONNECTIONS));
    let stream_slots = Arc::new(Semaphore::new(MAX_BRIDGE_STREAMS_TOTAL));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    info!("endpoint closed; rpc bridge worker stopping");
                    return Ok(());
                };
                let Ok(permit) = connection_slots.clone().try_acquire_owned() else {
                    incoming.refuse();
                    debug!(limit = MAX_BRIDGE_CONNECTIONS, "rpc bridge connection limit reached; refusing connection");
                    continue;
                };
                let allowed = allowed.clone();
                let stream_slots = Arc::clone(&stream_slots);
                connections.spawn(async move {
                    let _permit = permit;
                    serve_connection(incoming, target, allowed, stream_slots).await;
                });
            }
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                log_task_panic(joined);
            }
        }
    }
}

/// Rejects addresses that would expose the unauthenticated ggml RPC port beyond this machine.
fn ensure_loopback(addr: SocketAddr, role: &str) -> anyhow::Result<()> {
    if addr.ip().to_canonical().is_loopback() {
        return Ok(());
    }
    bail!("refusing non-loopback {role} address {addr}: the rpc bridge only uses localhost")
}

/// Leader-side handle to one worker, reusing a single Iroh connection for every stream.
struct WorkerLink {
    endpoint: Endpoint,
    worker: EndpointId,
    connection: Mutex<Option<Connection>>,
}

impl WorkerLink {
    fn new(endpoint: Endpoint, worker: EndpointId) -> Self {
        Self {
            endpoint,
            worker,
            connection: Mutex::new(None),
        }
    }

    /// Opens a new bidirectional stream to the worker.
    async fn open_stream(&self) -> anyhow::Result<(Connection, SendStream, RecvStream)> {
        let connection = self.connection().await?;
        let (send, recv) = connection
            .open_bi()
            .await
            .context("failed to open bridge stream")?;
        Ok((connection, send, recv))
    }

    /// Returns the cached connection, dialling the worker again if there is none or it closed.
    async fn connection(&self) -> anyhow::Result<Connection> {
        let mut cached = self.connection.lock().await;
        if let Some(live) = cached.as_ref().filter(|conn| conn.close_reason().is_none()) {
            return Ok(live.clone());
        }
        let connection = self
            .endpoint
            .connect(self.worker, RPC_BRIDGE_ALPN)
            .await
            .with_context(|| format!("failed to connect to worker {}", self.worker.fmt_short()))?;
        info!(
            worker = %self.worker.fmt_short(),
            path = path_kind(&connection),
            "connected to rpc bridge worker"
        );
        *cached = Some(connection.clone());
        Ok(connection)
    }
}

/// Leader side of one TCP client: open a stream to the worker and bridge the two.
async fn forward_to_worker(link: Arc<WorkerLink>, tcp: TcpStream, client: SocketAddr) {
    match link.open_stream().await {
        Ok((connection, send, recv)) => bridge_stream(&connection, tcp, client, (send, recv)).await,
        Err(error) => warn!(%client, error = %format!("{error:#}"), "dropping rpc client"),
    }
}

/// Worker side of one Iroh connection: bridge each incoming stream to `target`, but only for
/// peers on `allowed`.
async fn serve_connection(
    incoming: Incoming,
    target: SocketAddr,
    allowed: PeerAllowlist,
    total_stream_slots: Arc<Semaphore>,
) {
    let connection = match incoming.await {
        Ok(connection) => connection,
        Err(error) => {
            warn!(%error, "incoming rpc bridge connection failed");
            return;
        }
    };
    let remote_id = connection.remote_id();
    let remote = remote_id.fmt_short();
    if connection.alpn() != RPC_BRIDGE_ALPN {
        warn!(%remote, alpn = %String::from_utf8_lossy(connection.alpn()), "rejecting connection");
        connection.close(WRONG_ALPN, b"unexpected alpn");
        return;
    }
    // Checked before accepting any stream, so an unapproved peer never reaches the RPC server.
    if !allowed.contains(&remote_id) {
        warn!(peer = %remote_id, "rejecting rpc bridge peer that is not on the allowlist");
        connection.close(PEER_NOT_APPROVED, PEER_NOT_APPROVED_REASON);
        return;
    }
    info!(%remote, path = path_kind(&connection), "rpc bridge leader connected");

    let stream_slots = Arc::new(Semaphore::new(MAX_BRIDGE_STREAMS_PER_CONNECTION));
    let mut streams = JoinSet::new();
    let mut stream_limit_logged = false;
    loop {
        tokio::select! {
            accepted = connection.accept_bi() => match accepted {
                Ok(pair) => {
                    let Ok(connection_permit) = stream_slots.clone().try_acquire_owned() else {
                        let (mut send, mut recv) = pair;
                        abort_stream(&mut send, &mut recv);
                        if !stream_limit_logged {
                            warn!(%remote, limit = MAX_BRIDGE_STREAMS_PER_CONNECTION, "rpc bridge stream limit reached; refusing extra streams");
                            stream_limit_logged = true;
                        }
                        continue;
                    };
                    let Ok(total_permit) = total_stream_slots.clone().try_acquire_owned() else {
                        drop(connection_permit);
                        let (mut send, mut recv) = pair;
                        abort_stream(&mut send, &mut recv);
                        if !stream_limit_logged {
                            warn!(%remote, limit = MAX_BRIDGE_STREAMS_TOTAL, "rpc bridge total stream limit reached; refusing extra streams");
                            stream_limit_logged = true;
                        }
                        continue;
                    };
                    let connection = connection.clone();
                    streams.spawn(async move {
                        let _connection_permit = connection_permit;
                        let _total_permit = total_permit;
                        forward_to_target(connection, pair, target).await;
                    });
                }
                Err(error) => {
                    info!(%remote, %error, "rpc bridge connection closed");
                    return;
                }
            },
            Some(joined) = streams.join_next(), if !streams.is_empty() => log_task_panic(joined),
        }
    }
}

/// Worker side of one stream: dial the local RPC server and bridge the two.
async fn forward_to_target(
    connection: Connection,
    (mut send, mut recv): (SendStream, RecvStream),
    target: SocketAddr,
) {
    match TcpStream::connect(target).await {
        Ok(tcp) => bridge_stream(&connection, tcp, target, (send, recv)).await,
        Err(error) => {
            warn!(%target, %error, "could not reach rpc server; aborting bridge stream");
            abort_stream(&mut send, &mut recv);
        }
    }
}

/// Bytes forwarded in each direction of one bridged stream.
#[derive(Debug, Default)]
struct Traffic {
    tcp_to_iroh: u64,
    iroh_to_tcp: u64,
}

/// Copies a TCP connection and an Iroh stream into each other, then logs the outcome.
async fn bridge_stream(
    connection: &Connection,
    tcp: TcpStream,
    tcp_peer: SocketAddr,
    (mut send, mut recv): (SendStream, RecvStream),
) {
    let started = Instant::now();
    let mut traffic = Traffic::default();
    let result = copy_both_ways(tcp, &mut send, &mut recv, &mut traffic).await;
    if result.is_err() {
        abort_stream(&mut send, &mut recv);
    }

    let remote = connection.remote_id().fmt_short();
    let stream = send.id();
    let path = path_kind(connection);
    let duration_ms = started.elapsed().as_millis();
    let Traffic {
        tcp_to_iroh,
        iroh_to_tcp,
    } = traffic;
    match result {
        Ok(()) => info!(
            %remote, %stream, %tcp_peer, path, tcp_to_iroh, iroh_to_tcp, duration_ms,
            close_reason = "both directions finished", "rpc bridge stream closed"
        ),
        Err(error) => warn!(
            %remote, %stream, %tcp_peer, path, tcp_to_iroh, iroh_to_tcp, duration_ms,
            close_reason = %format!("{error:#}"), "rpc bridge stream failed"
        ),
    }
}

/// Runs both copy directions concurrently; the first error cancels the other direction.
async fn copy_both_ways(
    mut tcp: TcpStream,
    send: &mut SendStream,
    recv: &mut RecvStream,
    traffic: &mut Traffic,
) -> anyhow::Result<()> {
    // RPC commands are small and latency-bound, so Nagle's delay would stall every round trip.
    tcp.set_nodelay(true).context("failed to set TCP_NODELAY")?;
    let (mut tcp_read, mut tcp_write) = tcp.split();
    let outbound = async {
        pump(&mut tcp_read, send, &mut traffic.tcp_to_iroh)
            .await
            .context("tcp to iroh")
    };
    let inbound = async {
        pump(recv, &mut tcp_write, &mut traffic.iroh_to_tcp)
            .await
            .context("iroh to tcp")
    };
    tokio::try_join!(outbound, inbound).map(|_| ())
}

/// Copies `reader` into `writer` through a fixed buffer, then shuts `writer` down so the far
/// side sees EOF. Writes wait on the destination's flow control, which provides backpressure.
async fn pump<R, W>(reader: &mut R, writer: &mut W, forwarded: &mut u64) -> std::io::Result<()>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut buffer = vec![0u8; COPY_BUFFER_LEN];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return writer.shutdown().await;
        }
        writer.write_all(&buffer[..read]).await?;
        *forwarded += read as u64;
    }
}

/// Resets both halves of an Iroh stream so the peer sees an abort instead of a clean EOF
/// (dropping a [`SendStream`] would finish it gracefully).
fn abort_stream(send: &mut SendStream, recv: &mut RecvStream) {
    // Both calls only fail when that half is already closed, which is the state we want.
    let _ = send.reset(STREAM_ABORTED);
    let _ = recv.stop(STREAM_ABORTED);
}

/// Describes the connection's selected network path for logs: `direct`, `relay` or `custom`.
pub(crate) fn path_kind(connection: &Connection) -> &'static str {
    let paths = connection.paths();
    let Some(selected) = paths.iter().find(|path| path.is_selected()) else {
        return "unknown";
    };
    if selected.is_ip() {
        "direct"
    } else if selected.is_relay() {
        "relay"
    } else {
        "custom"
    }
}

/// Stream tasks handle their own errors, so a join error can only be a panic worth reporting.
fn log_task_panic(joined: Result<(), JoinError>) {
    if let Err(join_error) = joined {
        error!(%join_error, "rpc bridge task panicked");
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn accepts_ipv4_and_ipv6_loopback() {
        let v4 = SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_RPC_PORT));
        let v6 = SocketAddr::from((Ipv6Addr::LOCALHOST, DEFAULT_RPC_PORT));
        let other_127 = SocketAddr::from((Ipv4Addr::new(127, 4, 5, 6), 0));
        assert!(ensure_loopback(v4, "listen").is_ok());
        assert!(ensure_loopback(v6, "listen").is_ok());
        assert!(ensure_loopback(other_127, "target").is_ok());
    }

    #[test]
    fn accepts_ipv4_mapped_loopback() {
        let mapped = SocketAddr::from((Ipv4Addr::LOCALHOST.to_ipv6_mapped(), DEFAULT_RPC_PORT));
        assert!(ensure_loopback(mapped, "target").is_ok());
    }

    #[test]
    fn refuses_unspecified_and_routable_addresses() {
        let refused = [
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_RPC_PORT)),
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, DEFAULT_RPC_PORT)),
            SocketAddr::from((Ipv4Addr::new(192, 168, 1, 20), DEFAULT_RPC_PORT)),
            SocketAddr::from((
                Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped(),
                DEFAULT_RPC_PORT,
            )),
        ];
        for addr in refused {
            let error = ensure_loopback(addr, "listen").expect_err("non-loopback must be refused");
            let message = error.to_string();
            assert!(message.contains("non-loopback listen address"), "{message}");
            assert!(message.contains(&addr.to_string()), "{message}");
        }
    }

    #[tokio::test]
    async fn entry_points_refuse_non_loopback_before_binding() {
        let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .expect("bind endpoint");
        let exposed = SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_RPC_PORT));
        let allowed = PeerAllowlist::new([endpoint.id()]).expect("allowlist");
        let leader = run_leader(endpoint.clone(), endpoint.id(), exposed).await;
        let worker = serve_worker(endpoint.clone(), exposed, allowed).await;
        assert!(leader.is_err_and(|error| error.to_string().contains("non-loopback")));
        assert!(worker.is_err_and(|error| error.to_string().contains("non-loopback")));
        endpoint.close().await;
    }

    fn mapping(port: u16) -> BridgeMapping {
        BridgeMapping {
            worker: iroh::SecretKey::generate().public(),
            listen: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        }
    }

    #[test]
    fn several_mappings_may_ask_for_any_port() {
        let mappings = [
            mapping(ANY_PORT),
            mapping(ANY_PORT),
            mapping(DEFAULT_RPC_PORT),
        ];
        assert!(validate_mappings(&mappings).is_ok());
    }

    #[test]
    fn mappings_refuse_a_reused_fixed_port_on_another_worker() {
        let error = validate_mappings(&[mapping(DEFAULT_RPC_PORT), mapping(DEFAULT_RPC_PORT)])
            .expect_err("a fixed port mapped twice must be refused");
        assert!(
            error.to_string().contains("more than one worker"),
            "{error}"
        );
    }

    #[test]
    fn empty_allowlist_fails_closed() {
        let error = PeerAllowlist::new([]).expect_err("an empty allowlist must be refused");
        assert!(error.to_string().contains("no approved peers"), "{error}");
    }

    #[test]
    fn allowlist_contains_only_listed_peers() {
        let approved = iroh::SecretKey::generate().public();
        let stranger = iroh::SecretKey::generate().public();
        let allowed = PeerAllowlist::new([approved, approved]).expect("allowlist");
        assert!(allowed.contains(&approved));
        assert!(!allowed.contains(&stranger));
    }
}
