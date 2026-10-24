//! Opaque TCP-over-Iroh bridge for llama.cpp RPC traffic.
//!
//! On the leader, llama.cpp connects to a loopback TCP port served by [`run_leader`], which
//! carries every TCP connection over its own bidirectional QUIC stream (on [`RPC_BRIDGE_ALPN`])
//! to the worker. There [`serve_worker`] dials the local `ggml-rpc-server` for each stream.
//! Bytes are forwarded untouched: the bridge never frames or inspects RPC traffic.
//!
//! `ggml-rpc-server` has no authentication of its own, so the worker only serves peers on a
//! [`PeerAllowlist`]: an authenticated Iroh identity proves who a peer is, not that it may use
//! this machine. Unapproved peers are closed before any byte reaches the RPC server.
//!
//! Both entry points are cancel-safe: dropping their future stops accepting and aborts every
//! in-flight stream, and both return `Ok(())` once their endpoint is closed.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use iroh::endpoint::{Connection, Incoming, RecvStream, SendStream, VarInt};
use iroh::{Endpoint, EndpointId};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::{error, info, warn};

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

/// Leader: accept TCP on `listen` (loopback only — refuse non-loopback addresses); for each TCP
/// connection open one bidirectional Iroh stream to `worker` on [`RPC_BRIDGE_ALPN`] and copy
/// bytes both ways until both directions finish.
///
/// All streams share one Iroh connection, which is re-dialled if it closes. A failing stream is
/// torn down on its own without affecting the listener or other streams.
pub async fn run_leader(
    endpoint: Endpoint,
    worker: EndpointId,
    listen: SocketAddr,
) -> anyhow::Result<()> {
    ensure_loopback(listen, "listen")?;
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind rpc bridge listener on {listen}"))?;
    let local = listener
        .local_addr()
        .context("failed to read listener address")?;
    info!(listen = %local, worker = %worker.fmt_short(), "rpc bridge leader listening");

    let link = Arc::new(WorkerLink::new(endpoint.clone(), worker));
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((tcp, client)) => {
                    streams.spawn(forward_to_worker(Arc::clone(&link), tcp, client));
                }
                Err(error) => {
                    warn!(%error, "failed to accept rpc client");
                    tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                }
            },
            Some(joined) = streams.join_next(), if !streams.is_empty() => log_task_panic(joined),
            () = endpoint.closed() => {
                info!("endpoint closed; rpc bridge leader stopping");
                return Ok(());
            }
        }
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

    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    info!("endpoint closed; rpc bridge worker stopping");
                    return Ok(());
                };
                connections.spawn(serve_connection(incoming, target, allowed.clone()));
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
async fn serve_connection(incoming: Incoming, target: SocketAddr, allowed: PeerAllowlist) {
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

    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            accepted = connection.accept_bi() => match accepted {
                Ok(pair) => {
                    streams.spawn(forward_to_target(connection.clone(), pair, target));
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
fn path_kind(connection: &Connection) -> &'static str {
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
