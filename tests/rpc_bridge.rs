//! End-to-end test of the RPC bridge: TCP client → leader bridge → Iroh QUIC → worker bridge →
//! a TCP echo server standing in for `ggml-rpc-server`. Everything runs on loopback with relays
//! disabled, so the endpoints talk over a direct path.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{ConnectionError, presets};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use llm_cluster::protocol::RPC_BRIDGE_ALPN;
use llm_cluster::rpc_bridge::{
    PEER_NOT_APPROVED, PEER_NOT_APPROVED_REASON, PeerAllowlist, run_leader, serve_worker,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

/// Large enough to overrun every QUIC and TCP window, so backpressure must kick in.
const LARGE_PAYLOAD_LEN: usize = 8 * 1024 * 1024 + 4099;
const SMALL_PAYLOAD: &[u8] = b"ggml rpc hello";
/// Small enough to fit in socket buffers, so the abandoning client never blocks on its send.
const ABORTED_PAYLOAD_LEN: usize = 64 * 1024;
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(20);
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
/// Grace period for a wrongly forwarded stream to reach the RPC server before asserting it never did.
const STRAY_FORWARD_GRACE: Duration = Duration::from_millis(200);

/// The three running pieces of a bridge plus the leader's client-facing address.
struct Bridge {
    leader_listen: SocketAddr,
    leader_endpoint: Endpoint,
    worker_endpoint: Endpoint,
    tasks: Vec<JoinHandle<()>>,
}

impl Bridge {
    async fn start() -> Self {
        init_tracing();
        let echo_addr = spawn_echo_server().await;
        let lookup = MemoryLookup::new();
        let worker_endpoint = bind_endpoint(vec![RPC_BRIDGE_ALPN.to_vec()], None).await;
        let leader_endpoint = bind_endpoint(Vec::new(), Some(lookup.clone())).await;
        lookup.add_endpoint_info(direct_addr(&worker_endpoint));

        let leader_listen = free_loopback_port().await;
        let allowed = PeerAllowlist::new([leader_endpoint.id()]).expect("allowlist");
        let worker = tokio::spawn({
            let endpoint = worker_endpoint.clone();
            async move {
                serve_worker(endpoint, echo_addr, allowed)
                    .await
                    .expect("worker bridge")
            }
        });
        let leader = tokio::spawn({
            let endpoint = leader_endpoint.clone();
            let worker_id = worker_endpoint.id();
            async move {
                run_leader(endpoint, worker_id, leader_listen)
                    .await
                    .expect("leader bridge")
            }
        });
        wait_until_listening(leader_listen).await;
        Self {
            leader_listen,
            leader_endpoint,
            worker_endpoint,
            tasks: vec![worker, leader],
        }
    }

    /// Closing the endpoints must make both bridge loops return `Ok(())`.
    async fn shut_down(self) {
        self.leader_endpoint.close().await;
        self.worker_endpoint.close().await;
        for task in self.tasks {
            timeout(TEST_TIMEOUT, task)
                .await
                .expect("bridge task stops after endpoint close")
                .expect("bridge task did not panic");
        }
    }
}

fn init_tracing() {
    // Another test in this binary may already have installed the subscriber; that is fine.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Binds an endpoint on loopback only, with relays and public address lookup disabled.
async fn bind_endpoint(alpns: Vec<Vec<u8>>, lookup: Option<MemoryLookup>) -> Endpoint {
    let mut builder = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr(LOOPBACK_ANY_PORT)
        .expect("valid bind address")
        .alpns(alpns);
    if let Some(lookup) = lookup {
        builder = builder.address_lookup(lookup);
    }
    builder.bind().await.expect("bind endpoint")
}

/// The worker's id plus its bound loopback sockets, so the leader dials it directly.
fn direct_addr(endpoint: &Endpoint) -> EndpointAddr {
    endpoint
        .bound_sockets()
        .into_iter()
        .fold(EndpointAddr::new(endpoint.id()), EndpointAddr::with_ip_addr)
}

/// Fake `ggml-rpc-server`: echoes every byte and half-closes once the client half-closes.
async fn spawn_echo_server() -> SocketAddr {
    let listener = TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("bind echo");
    let addr = listener.local_addr().expect("echo address");
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut reader, mut writer) = socket.split();
                tokio::io::copy(&mut reader, &mut writer)
                    .await
                    .expect("echo copy");
                writer.shutdown().await.expect("echo shutdown");
            });
        }
    });
    addr
}

/// Fake `ggml-rpc-server` that only counts the connections it receives.
async fn spawn_counting_server() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("bind counting server");
    let addr = listener.local_addr().expect("counting server address");
    let connects = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&connects);
    tokio::spawn(async move {
        while listener.accept().await.is_ok() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
    (addr, connects)
}

/// Reserves an ephemeral loopback port for the leader listener (run_leader binds it itself).
async fn free_loopback_port() -> SocketAddr {
    let probe = TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("probe bind");
    probe.local_addr().expect("probe address")
}

async fn wait_until_listening(addr: SocketAddr) {
    timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(addr).await.is_err() {
            tokio::time::sleep(LISTENER_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("leader listener comes up");
}

fn patterned_payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index % 251) as u8 ^ (index >> 16) as u8)
        .collect()
}

/// Sends `payload` through the bridge while reading concurrently, half-closes, and returns
/// everything received up to EOF.
async fn round_trip(leader: SocketAddr, payload: Vec<u8>) -> Vec<u8> {
    let mut stream = TcpStream::connect(leader).await.expect("connect to leader");
    let (mut reader, mut writer) = stream.split();
    let send = async {
        writer.write_all(&payload).await.expect("send payload");
        writer.shutdown().await.expect("half-close");
    };
    let receive = async {
        let mut echoed = Vec::with_capacity(payload.len());
        reader
            .read_to_end(&mut echoed)
            .await
            .expect("read echo to eof");
        echoed
    };
    let ((), echoed) = tokio::join!(send, receive);
    echoed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_get_byte_exact_echoes() {
    let bridge = Bridge::start().await;
    let large = patterned_payload(LARGE_PAYLOAD_LEN);

    let (large_echo, small_echo) = timeout(TEST_TIMEOUT, async {
        tokio::join!(
            round_trip(bridge.leader_listen, large.clone()),
            round_trip(bridge.leader_listen, SMALL_PAYLOAD.to_vec()),
        )
    })
    .await
    .expect("round trips finish");

    assert_eq!(large_echo.len(), large.len());
    assert!(large_echo == large, "large payload corrupted in transit");
    assert_eq!(small_echo, SMALL_PAYLOAD);
    bridge.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_stream_propagates_eof() {
    let bridge = Bridge::start().await;
    let echoed = timeout(TEST_TIMEOUT, round_trip(bridge.leader_listen, Vec::new()))
        .await
        .expect("eof propagates both ways");
    assert!(echoed.is_empty());
    bridge.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listener_survives_an_aborted_client() {
    let bridge = Bridge::start().await;
    {
        let mut quitter = TcpStream::connect(bridge.leader_listen)
            .await
            .expect("connect");
        quitter
            .write_all(&patterned_payload(ABORTED_PAYLOAD_LEN))
            .await
            .expect("partial send");
    }

    let echoed = timeout(
        TEST_TIMEOUT,
        round_trip(bridge.leader_listen, SMALL_PAYLOAD.to_vec()),
    )
    .await
    .expect("later client is still served");
    assert_eq!(echoed, SMALL_PAYLOAD);
    bridge.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unapproved_peer_is_closed_before_reaching_the_rpc_server() {
    init_tracing();
    let (rpc_addr, connects) = spawn_counting_server().await;
    let worker_endpoint = bind_endpoint(vec![RPC_BRIDGE_ALPN.to_vec()], None).await;
    let someone_else = iroh::SecretKey::generate().public();
    let allowed = PeerAllowlist::new([someone_else]).expect("allowlist");
    let worker = tokio::spawn({
        let endpoint = worker_endpoint.clone();
        async move {
            serve_worker(endpoint, rpc_addr, allowed)
                .await
                .expect("worker bridge")
        }
    });

    let stranger = bind_endpoint(Vec::new(), None).await;
    let connection = stranger
        .connect(direct_addr(&worker_endpoint), RPC_BRIDGE_ALPN)
        .await
        .expect("the handshake itself succeeds; authorization happens after it");
    // Try to use the bridge anyway; the worker must refuse before forwarding anything.
    if let Ok((mut send, _recv)) = connection.open_bi().await {
        // The connection may already be closed, in which case the write fails, as it should.
        let _ = send.write_all(b"ggml rpc command").await;
    }
    let closed = timeout(TEST_TIMEOUT, connection.closed())
        .await
        .expect("worker closes the connection");

    let ConnectionError::ApplicationClosed(close) = closed else {
        panic!("expected an application close, got {closed:?}");
    };
    assert_eq!(close.error_code, PEER_NOT_APPROVED);
    assert_eq!(&close.reason[..], PEER_NOT_APPROVED_REASON);
    tokio::time::sleep(STRAY_FORWARD_GRACE).await;
    assert_eq!(
        connects.load(Ordering::SeqCst),
        0,
        "the rpc server must never see an unapproved peer"
    );

    stranger.close().await;
    worker_endpoint.close().await;
    timeout(TEST_TIMEOUT, worker)
        .await
        .expect("worker stops after endpoint close")
        .expect("worker did not panic");
}
