//! End-to-end tests of the worker process: a supervised fake `ggml-rpc-server`, the RPC bridge
//! from a leader-side endpoint, and heartbeats on the control link. Everything runs on loopback
//! with relays disabled, so the endpoints talk over a direct path without internet access.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::endpoint::{ConnectionError, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use llm_cluster::network::{self, HEARTBEAT_TIMEOUT_CODE, PeerSession, SharedPeerTable};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{self, CONTROL_ALPN, Heartbeat, Message, RPC_BRIDGE_ALPN};
use llm_cluster::rpc_bridge;
use llm_cluster::worker::{WorkerConfig, run_worker_on};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod common;
use common::example_bin;

const CLUSTER_ID: &str = "worker-test";
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
const PAYLOAD: &[u8] = b"ggml rpc through the worker";
/// QUIC application close code the test leader uses to drop a worker's control link.
const KICK_CODE: u32 = 7;
/// How often the stalling test leader heartbeats before it goes silent.
const LEADER_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
/// How long the worker waits for the leader's next heartbeat: six leader intervals.
const LEADER_HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);
/// Heartbeats the stalling test leader sends before going silent.
const LEADER_BEATS_BEFORE_SILENCE: u64 = 3;

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

fn worker_alpns() -> Vec<Vec<u8>> {
    vec![CONTROL_ALPN.to_vec(), RPC_BRIDGE_ALPN.to_vec()]
}

/// The endpoint's id plus its bound loopback sockets, so peers can dial it directly.
fn direct_addr(endpoint: &Endpoint) -> EndpointAddr {
    endpoint
        .bound_sockets()
        .into_iter()
        .fold(EndpointAddr::new(endpoint.id()), EndpointAddr::with_ip_addr)
}

/// Reserves an ephemeral loopback port; whoever uses it binds it again afterwards.
async fn free_loopback_port() -> SocketAddr {
    let probe = TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("probe bind");
    probe.local_addr().expect("probe address")
}

fn worker_config(rpc_server: PathBuf, rpc_port: u16, leader: EndpointId) -> WorkerConfig {
    WorkerConfig {
        key_path: std::env::temp_dir().join("meshthing-worker-test-unused.key"),
        cluster_id: CLUSTER_ID.to_owned(),
        rpc_server,
        rpc_port,
        rpc_args: Vec::new(),
        leader: Some(leader),
        allowed_peers: Vec::new(),
        heartbeat_interval: HEARTBEAT_INTERVAL,
        // On in every test: leaders that never heartbeat must not trip it.
        leader_heartbeat_timeout: Some(LEADER_HEARTBEAT_TIMEOUT),
    }
}

/// Starts a worker on `endpoint` in the background, heartbeating to `leader`.
fn spawn_worker(
    endpoint: Endpoint,
    leader: EndpointAddr,
    config: WorkerConfig,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move { run_worker_on(endpoint, Some(leader), &config).await })
}

/// Polls `peers` until `check` returns a value for the worker's entry.
async fn wait_for_peer<T>(
    peers: &SharedPeerTable,
    worker: EndpointId,
    check: impl Fn(Option<u64>) -> Option<T>,
) -> T {
    timeout(TEST_TIMEOUT, async {
        loop {
            let sequence = peers
                .lock()
                .expect("peer table lock")
                .get(&worker)
                .and_then(|info| info.last_heartbeat_sequence);
            if let Some(found) = check(sequence) {
                return found;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("leader sees the expected heartbeats")
}

/// Sends `PAYLOAD` to `addr` and reads the same number of bytes back.
async fn echo_through(addr: SocketAddr) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(PAYLOAD).await.expect("send payload");
    let mut echoed = vec![0; PAYLOAD.len()];
    stream.read_exact(&mut echoed).await.expect("read echo");
    echoed
}

async fn wait_until(addr: SocketAddr, listening: bool) {
    timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(addr).await.is_ok() != listening {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{addr} never became listening={listening}"));
}

/// A leader-side endpoint running `serve_control`, as the real leader does.
struct ControlLeader {
    endpoint: Endpoint,
    peers: SharedPeerTable,
    control: JoinHandle<anyhow::Result<()>>,
}

impl ControlLeader {
    async fn start(lookup: MemoryLookup) -> Self {
        let endpoint = bind_endpoint(vec![CONTROL_ALPN.to_vec()], Some(lookup)).await;
        let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
        let hello = network::local_hello(&endpoint, CLUSTER_ID, 0);
        let control = tokio::spawn(network::serve_control(
            endpoint.clone(),
            hello,
            Arc::clone(&peers),
        ));
        Self {
            endpoint,
            peers,
            control,
        }
    }

    /// Waits for the worker's hello plus two heartbeats with increasing sequence numbers.
    async fn assert_heartbeats_increase(&self, worker: EndpointId) {
        let first = wait_for_peer(&self.peers, worker, |sequence| sequence).await;
        let later = wait_for_peer(&self.peers, worker, |sequence| {
            sequence.filter(|&later| later > first)
        })
        .await;
        assert!(first >= 1 && later >= 2, "sequences {first} then {later}");
    }

    /// Runs the leader half of the RPC bridge to `worker` and echoes bytes through it.
    async fn assert_bridge_echoes(&self, worker: EndpointId) -> JoinHandle<anyhow::Result<()>> {
        let listen = free_loopback_port().await;
        let bridge = tokio::spawn(rpc_bridge::run_leader(
            self.endpoint.clone(),
            worker,
            listen,
        ));
        wait_until(listen, true).await;
        let echoed = timeout(TEST_TIMEOUT, echo_through(listen))
            .await
            .expect("bridge round trip finishes");
        assert_eq!(echoed, PAYLOAD);
        bridge
    }

    async fn stop(self) {
        self.endpoint.close().await;
        self.control
            .await
            .expect("control task")
            .expect("control stops cleanly");
    }
}

/// Cancels the worker and waits until its RPC server no longer accepts connections.
async fn cancel_and_expect_child_gone(
    worker: JoinHandle<anyhow::Result<()>>,
    rpc_addr: SocketAddr,
) {
    worker.abort();
    let cancelled = worker.await.expect_err("worker was still running");
    assert!(cancelled.is_cancelled(), "worker panicked: {cancelled}");
    wait_until(rpc_addr, false).await;
}

/// Accepts one control connection, reads its first heartbeat, then drops the connection.
async fn kick_after_one_heartbeat(leader: &Endpoint, peers: &SharedPeerTable) -> u64 {
    let hello = network::local_hello(leader, CLUSTER_ID, 0);
    let incoming = timeout(TEST_TIMEOUT, leader.accept())
        .await
        .expect("worker dials in time")
        .expect("leader endpoint open");
    let session = network::accept_peer(incoming, &hello, peers)
        .await
        .expect("hello");
    let connection = session.connection();
    let mut stream = connection.accept_uni().await.expect("heartbeat stream");
    let frame = timeout(TEST_TIMEOUT, protocol::read_frame(&mut stream))
        .await
        .expect("heartbeat in time")
        .expect("valid frame");
    let Some(Message::Heartbeat(Heartbeat { sequence, .. })) = frame else {
        panic!("expected a heartbeat, got {frame:?}");
    };
    connection.close(KICK_CODE.into(), b"kicked by test");
    sequence
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_bridges_rpc_and_heartbeats_until_cancelled() {
    init_tracing();
    let lookup = MemoryLookup::new();
    let leader = ControlLeader::start(lookup.clone()).await;
    let worker = bind_endpoint(worker_alpns(), None).await;
    lookup.add_endpoint_info(direct_addr(&worker));
    let worker_id = worker.id();

    let rpc_addr = free_loopback_port().await;
    let config = worker_config(
        example_bin("fake_rpc_server"),
        rpc_addr.port(),
        leader.endpoint.id(),
    );
    let worker_task = spawn_worker(worker, direct_addr(&leader.endpoint), config);
    leader.assert_heartbeats_increase(worker_id).await;

    // Heartbeats start only after the RPC server is ready, so it is listening on loopback now.
    assert_eq!(rpc_addr.ip(), Ipv4Addr::LOCALHOST);
    assert_eq!(echo_through(rpc_addr).await, PAYLOAD);
    let bridge = leader.assert_bridge_echoes(worker_id).await;

    cancel_and_expect_child_gone(worker_task, rpc_addr).await;
    leader.stop().await;
    bridge
        .await
        .expect("bridge task")
        .expect("bridge stops cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_redials_and_keeps_counting_after_the_leader_drops_it() {
    init_tracing();
    let leader = bind_endpoint(vec![CONTROL_ALPN.to_vec()], None).await;
    let worker = bind_endpoint(worker_alpns(), None).await;
    let rpc_addr = free_loopback_port().await;
    let config = worker_config(example_bin("fake_rpc_server"), rpc_addr.port(), leader.id());
    let worker_task = spawn_worker(worker, direct_addr(&leader), config);

    let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
    let first = kick_after_one_heartbeat(&leader, &peers).await;
    let after_redial = kick_after_one_heartbeat(&leader, &peers).await;
    assert!(
        after_redial > first,
        "sequences {first} then {after_redial}"
    );

    cancel_and_expect_child_gone(worker_task, rpc_addr).await;
    leader.close().await;
}

/// Accepts one control connection, heartbeats to the worker a few times, then goes silent
/// while keeping the connection and its stream open; returns both so the caller holds them.
async fn heartbeat_then_go_silent(
    leader: &Endpoint,
    peers: &SharedPeerTable,
) -> (PeerSession, SendStream) {
    let hello = network::local_hello(leader, CLUSTER_ID, 0);
    let incoming = timeout(TEST_TIMEOUT, leader.accept())
        .await
        .expect("worker dials in time")
        .expect("leader endpoint open");
    let session = network::accept_peer(incoming, &hello, peers)
        .await
        .expect("hello");
    let mut stream = session.connection().open_uni().await.expect("open stream");
    for sequence in 1..=LEADER_BEATS_BEFORE_SILENCE {
        let heartbeat = Message::Heartbeat(Heartbeat {
            node_id: hello.node_id.clone(),
            epoch: network::HEARTBEAT_EPOCH,
            sequence,
        });
        protocol::write_frame(&mut stream, &heartbeat)
            .await
            .expect("write heartbeat");
        tokio::time::sleep(LEADER_HEARTBEAT_INTERVAL).await;
    }
    (session, stream)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_redials_when_the_leader_stops_heartbeating() {
    init_tracing();
    let leader = bind_endpoint(vec![CONTROL_ALPN.to_vec()], None).await;
    let worker = bind_endpoint(worker_alpns(), None).await;
    let rpc_addr = free_loopback_port().await;
    let config = worker_config(example_bin("fake_rpc_server"), rpc_addr.port(), leader.id());
    let worker_task = spawn_worker(worker, direct_addr(&leader), config);

    let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
    let (stalled, _stream) = heartbeat_then_go_silent(&leader, &peers).await;
    // The worker closes the silent link itself, then dials again; the redial reaches the leader.
    let closed = timeout(TEST_TIMEOUT, stalled.connection().closed())
        .await
        .expect("worker drops the silent leader");
    let ConnectionError::ApplicationClosed(close) = closed else {
        panic!("expected an application close, got {closed:?}");
    };
    assert_eq!(close.error_code, HEARTBEAT_TIMEOUT_CODE.into());
    let redialled = kick_after_one_heartbeat(&leader, &peers).await;
    assert!(redialled >= 1, "heartbeat {redialled} after the redial");

    drop(stalled);
    cancel_and_expect_child_gone(worker_task, rpc_addr).await;
    leader.close().await;
}

#[tokio::test]
async fn worker_fails_when_the_rpc_server_cannot_start() {
    init_tracing();
    let leader_id = iroh::SecretKey::generate().public();
    let worker = bind_endpoint(worker_alpns(), None).await;
    let rpc_addr = free_loopback_port().await;
    let missing = std::env::temp_dir().join("meshthing-no-such-rpc-server");
    let mut config = worker_config(missing, rpc_addr.port(), leader_id);
    // Approve a peer explicitly: without one the worker would fail on its allowlist instead.
    config.allowed_peers = vec![leader_id];

    let outcome = timeout(TEST_TIMEOUT, run_worker_on(worker.clone(), None, &config))
        .await
        .expect("worker gives up in time");

    assert!(outcome.is_err());
    assert!(worker.is_closed(), "endpoint closed on return");
}

#[tokio::test]
async fn worker_without_leader_or_allowed_peers_refuses_to_start() {
    init_tracing();
    let leader_id = iroh::SecretKey::generate().public();
    let worker = bind_endpoint(worker_alpns(), None).await;
    let rpc_addr = free_loopback_port().await;
    let config = worker_config(example_bin("fake_rpc_server"), rpc_addr.port(), leader_id);

    // No leader is passed (config.leader is ignored by run_worker_on) and no extra peers are allowed.
    let outcome = timeout(TEST_TIMEOUT, run_worker_on(worker.clone(), None, &config))
        .await
        .expect("worker gives up in time");

    let error = outcome.expect_err("a worker nobody may use must not start");
    assert!(format!("{error:#}").contains("--allow-peer"), "{error:#}");
    assert!(
        TcpStream::connect(rpc_addr).await.is_err(),
        "the rpc server must not have been started"
    );
    assert!(worker.is_closed(), "endpoint closed on return");
}
