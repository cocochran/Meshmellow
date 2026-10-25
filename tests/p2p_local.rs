//! Three nodes on localhost connect pairwise, exchange hellos, and see each other in their peer tables;
//! heartbeats in both directions keep control links open, and their absence closes them.
//!
//! Endpoints are bound without relays or address lookup and dialed by their direct addresses, so
//! the test needs no internet access.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::Endpoint;
use iroh::endpoint::{Connection, ConnectionError, SendStream};
use llm_cluster::network::{
    self, ControlOptions, HEARTBEAT_TIMEOUT_CODE, PeerSession, SharedPeerTable,
};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{self, CONTROL_ALPN, Heartbeat, Hello, Message};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const NODE_COUNT: usize = 3;
const CLUSTER_ID: &str = "p2p-local-test";

/// Longest a test waits for a closed connection to leave a peer table.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a test re-checks a peer table while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Heartbeat timeout for the watchdog tests; short, but six heartbeat intervals long.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);

/// How often a test peer heartbeats when it means to stay connected.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// How long the watchdog tests keep a heartbeating peer alive: several whole timeouts.
const SURVIVAL_PERIOD: Duration = Duration::from_millis(1_200);

/// How long a default listener is left with an idle peer before checking it is still known.
const IDLE_PERIOD: Duration = Duration::from_millis(500);

/// Close reason the acceptor sends with [`HEARTBEAT_TIMEOUT_CODE`], as agreed for M-11.
const HEARTBEAT_TIMEOUT_REASON: &[u8] = b"heartbeat timeout";

/// Sends a heartbeat from `node` every [`HEARTBEAT_INTERVAL`] on one unidirectional stream of
/// `connection` until the task is aborted or the connection fails.
fn spawn_heartbeats(connection: &Connection, node: &TestNode) -> JoinHandle<()> {
    let connection = connection.clone();
    let node_id = node.hello.node_id.clone();
    tokio::spawn(async move {
        let Ok(mut stream) = connection.open_uni().await else {
            return;
        };
        let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
        for sequence in 1.. {
            ticker.tick().await;
            let heartbeat = Message::Heartbeat(Heartbeat {
                node_id: node_id.clone(),
                epoch: 0,
                sequence,
            });
            if protocol::write_frame(&mut stream, &heartbeat)
                .await
                .is_err()
            {
                return;
            }
        }
    })
}

/// Waits for `connection` to close and asserts the other side closed it for a missed heartbeat.
async fn assert_closed_for_heartbeat_timeout(connection: &Connection) {
    let closed = timeout(CLEANUP_TIMEOUT, connection.closed())
        .await
        .expect("acceptor closes the silent connection");
    let ConnectionError::ApplicationClosed(close) = closed else {
        panic!("expected an application close, got {closed:?}");
    };
    assert_eq!(close.error_code, HEARTBEAT_TIMEOUT_CODE.into());
    assert_eq!(&close.reason[..], HEARTBEAT_TIMEOUT_REASON);
}

/// Asserts `connection` is still open after waiting `period`.
async fn assert_stays_open(connection: &Connection, period: Duration) {
    let closed = timeout(period, connection.closed()).await;
    assert!(closed.is_err(), "connection closed early: {closed:?}");
}

struct TestNode {
    endpoint: Endpoint,
    hello: Hello,
    peers: SharedPeerTable,
    key_path: PathBuf,
}

impl TestNode {
    async fn start(name: &str) -> Self {
        Self::start_in_cluster(name, CLUSTER_ID).await
    }

    async fn start_in_cluster(name: &str, cluster_id: &str) -> Self {
        Self::start_with(name, cluster_id, ControlOptions::default()).await
    }

    /// Starts a node whose accept loop drops peers that go `timeout` without a heartbeat.
    async fn start_with_heartbeat_timeout(name: &str, timeout: Duration) -> Self {
        let options = ControlOptions {
            heartbeat_timeout: Some(timeout),
            ..ControlOptions::default()
        };
        Self::start_with(name, CLUSTER_ID, options).await
    }

    /// Starts a node whose accept loop heartbeats to every peer each `interval`.
    async fn start_with_heartbeat_interval(name: &str, interval: Duration) -> Self {
        let options = ControlOptions {
            heartbeat_interval: Some(interval),
            ..ControlOptions::default()
        };
        Self::start_with(name, CLUSTER_ID, options).await
    }

    /// The last heartbeat sequence this node accepted from `other`, if any.
    fn heartbeat_sequence_from(&self, other: &TestNode) -> Option<u64> {
        let table = self.peers.lock().expect("peer table lock");
        table
            .get(&other.endpoint.id())
            .and_then(|info| info.last_heartbeat_sequence)
    }

    /// Waits until this node has accepted a heartbeat from `other` with a sequence above `after`.
    async fn wait_for_heartbeat_after(&self, other: &TestNode, after: u64) -> u64 {
        let newer = async {
            loop {
                match self.heartbeat_sequence_from(other) {
                    Some(sequence) if sequence > after => return sequence,
                    _ => tokio::time::sleep(POLL_INTERVAL).await,
                }
            }
        };
        timeout(CLEANUP_TIMEOUT, newer)
            .await
            .expect("acceptor heartbeats reach the dialer")
    }

    async fn start_with(name: &str, cluster_id: &str, options: ControlOptions) -> Self {
        let key_path = std::env::temp_dir().join(format!(
            "meshthing-p2p-local-{}-{name}.key",
            std::process::id()
        ));
        // A leftover file from an aborted run may or may not exist; either way we start fresh.
        let _ = std::fs::remove_file(&key_path);
        let endpoint = network::bind_local_endpoint(&key_path, vec![CONTROL_ALPN.to_vec()])
            .await
            .expect("bind endpoint");
        let hello = network::local_hello(&endpoint, cluster_id, 0);
        let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
        tokio::spawn(network::serve_control_with(
            endpoint.clone(),
            hello.clone(),
            Arc::clone(&peers),
            options,
        ));
        Self {
            endpoint,
            hello,
            peers,
            key_path,
        }
    }

    async fn dial(&self, other: &TestNode) -> PeerSession {
        let session = network::dial_peer(
            &self.endpoint,
            other.endpoint.addr(),
            &self.hello,
            &self.peers,
        )
        .await
        .expect("dial peer");
        assert_eq!(session.hello(), &other.hello);
        session
    }

    /// The connection token under which this node lists `other`, if it lists it at all.
    fn token_for(&self, other: &TestNode) -> Option<u64> {
        let table = self.peers.lock().expect("peer table lock");
        table
            .get(&other.endpoint.id())
            .map(|info| info.connection_token)
    }

    async fn wait_until_forgets(&self, other: &TestNode) {
        let forgotten = async {
            while self.token_for(other).is_some() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        };
        timeout(CLEANUP_TIMEOUT, forgotten)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} still lists {} after its connection closed",
                    self.hello.node_id, other.hello.node_id
                )
            });
    }

    fn assert_knows(&self, other: &TestNode) {
        let table = self.peers.lock().expect("peer table lock");
        let info = table.get(&other.endpoint.id()).unwrap_or_else(|| {
            panic!(
                "{} does not list {}",
                self.hello.node_id, other.hello.node_id
            )
        });
        assert!(info.connected_at_unix_ms > 0, "connected_at not set");
        assert_eq!(table.peers().count(), NODE_COUNT - 1);
    }

    async fn stop(self) {
        self.endpoint.close().await;
        std::fs::remove_file(&self.key_path).expect("remove key file");
    }
}

#[tokio::test]
async fn three_local_peers_see_each_other() {
    let mut nodes = Vec::new();
    for name in ["a", "b", "c"] {
        nodes.push(TestNode::start(name).await);
    }

    let mut sessions = Vec::new();
    for (i, dialer) in nodes.iter().enumerate() {
        for acceptor in &nodes[i + 1..] {
            sessions.push(dialer.dial(acceptor).await);
        }
    }

    for node in &nodes {
        for other in &nodes {
            if node.hello.node_id != other.hello.node_id {
                node.assert_knows(other);
            }
        }
    }

    drop(sessions);
    for node in nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn peer_from_another_cluster_is_rejected() {
    let member = TestNode::start("member").await;
    let outsider = TestNode::start_in_cluster("outsider", "some-other-cluster").await;

    let result = network::dial_peer(
        &outsider.endpoint,
        member.endpoint.addr(),
        &outsider.hello,
        &outsider.peers,
    )
    .await;

    assert!(result.is_err(), "dial across clusters should fail");
    assert_eq!(
        member
            .peers
            .lock()
            .expect("peer table lock")
            .peers()
            .count(),
        0
    );
    assert_eq!(
        outsider
            .peers
            .lock()
            .expect("peer table lock")
            .peers()
            .count(),
        0
    );
    member.stop().await;
    outsider.stop().await;
}

#[tokio::test]
async fn key_file_keeps_node_id_stable() {
    let first = TestNode::start("stable").await;
    let id = first.endpoint.id();
    first.endpoint.close().await;

    let endpoint = network::bind_local_endpoint(&first.key_path, Vec::new())
        .await
        .expect("rebind endpoint");
    assert_eq!(endpoint.id(), id);
    endpoint.close().await;
    std::fs::remove_file(&first.key_path).expect("remove key file");
}

#[tokio::test]
async fn dropping_a_dialed_session_forgets_the_peer_on_both_sides() {
    let dialer = TestNode::start("drop-dialer").await;
    let acceptor = TestNode::start("drop-acceptor").await;
    let session = dialer.dial(&acceptor).await;
    assert_eq!(
        dialer.token_for(&acceptor),
        Some(session.connection_token())
    );
    assert!(acceptor.token_for(&dialer).is_some());

    drop(session);

    assert_eq!(dialer.token_for(&acceptor), None);
    acceptor.wait_until_forgets(&dialer).await;
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn closing_an_old_connection_keeps_the_reconnected_peer() {
    let dialer = TestNode::start("reconnect-dialer").await;
    let acceptor = TestNode::start("reconnect-acceptor").await;
    let old = dialer.dial(&acceptor).await;
    let new = dialer.dial(&acceptor).await;
    assert_ne!(old.connection_token(), new.connection_token());

    let old_closed = old.connection().weak_handle().closed();
    drop(old);
    old_closed.await;
    // Let the old session's close watcher run before checking that it left the new entry alone.
    tokio::task::yield_now().await;

    assert_eq!(dialer.token_for(&acceptor), Some(new.connection_token()));
    assert!(acceptor.token_for(&dialer).is_some());
    drop(new);
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn peer_closing_the_link_is_forgotten_while_the_session_is_held() {
    let dialer = TestNode::start("remote-close-dialer").await;
    let acceptor = TestNode::start("remote-close-acceptor").await;
    let session = dialer.dial(&acceptor).await;

    acceptor.endpoint.close().await;

    dialer.wait_until_forgets(&acceptor).await;
    drop(session);
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn silent_peer_is_closed_and_forgotten_after_the_heartbeat_timeout() {
    let dialer = TestNode::start("silent-dialer").await;
    let acceptor =
        TestNode::start_with_heartbeat_timeout("silent-acceptor", HEARTBEAT_TIMEOUT).await;
    let session = dialer.dial(&acceptor).await;

    assert_closed_for_heartbeat_timeout(session.connection()).await;

    acceptor.wait_until_forgets(&dialer).await;
    drop(session);
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn heartbeating_peer_outlives_several_heartbeat_timeouts() {
    let dialer = TestNode::start("beating-dialer").await;
    let acceptor =
        TestNode::start_with_heartbeat_timeout("beating-acceptor", HEARTBEAT_TIMEOUT).await;
    let session = dialer.dial(&acceptor).await;
    let token = acceptor.token_for(&dialer);
    let heartbeats = spawn_heartbeats(session.connection(), &dialer);

    assert_stays_open(session.connection(), SURVIVAL_PERIOD).await;

    assert!(token.is_some());
    assert_eq!(acceptor.token_for(&dialer), token);
    heartbeats.abort();
    drop(session);
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn default_listener_never_times_out_an_idle_peer() {
    let dialer = TestNode::start("idle-dialer").await;
    let acceptor = TestNode::start("idle-acceptor").await;
    let session = dialer.dial(&acceptor).await;

    assert_stays_open(session.connection(), IDLE_PERIOD).await;

    assert!(acceptor.token_for(&dialer).is_some());
    drop(session);
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn old_connection_timing_out_keeps_the_reconnected_peer() {
    let dialer = TestNode::start("timeout-reconnect-dialer").await;
    let acceptor =
        TestNode::start_with_heartbeat_timeout("timeout-reconnect-acceptor", HEARTBEAT_TIMEOUT)
            .await;
    let old = dialer.dial(&acceptor).await;
    let new = dialer.dial(&acceptor).await;
    let new_token = acceptor.token_for(&dialer);
    let heartbeats = spawn_heartbeats(new.connection(), &dialer);

    assert_closed_for_heartbeat_timeout(old.connection()).await;
    assert_stays_open(new.connection(), SURVIVAL_PERIOD).await;

    assert!(new_token.is_some());
    assert_eq!(acceptor.token_for(&dialer), new_token);
    heartbeats.abort();
    drop((old, new));
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn acceptor_heartbeats_reach_the_dialer_with_increasing_sequences() {
    let dialer = TestNode::start("leader-beats-dialer").await;
    let acceptor =
        TestNode::start_with_heartbeat_interval("leader-beats-acceptor", HEARTBEAT_INTERVAL).await;
    let session = Arc::new(dialer.dial(&acceptor).await);
    let watch = {
        let (session, peers) = (Arc::clone(&session), Arc::clone(&dialer.peers));
        tokio::spawn(async move {
            network::watch_heartbeats(&session, &peers, Some(HEARTBEAT_TIMEOUT)).await
        })
    };

    let first = dialer.wait_for_heartbeat_after(&acceptor, 0).await;
    let later = dialer.wait_for_heartbeat_after(&acceptor, first).await;

    assert!(
        first >= 1 && later > first,
        "sequences {first} then {later}"
    );
    assert_stays_open(session.connection(), SURVIVAL_PERIOD).await;
    watch.abort();
    drop(session);
    dialer.stop().await;
    acceptor.stop().await;
}

#[tokio::test]
async fn dialer_never_times_out_an_acceptor_that_never_heartbeats() {
    let dialer = TestNode::start("quiet-leader-dialer").await;
    let acceptor = TestNode::start("quiet-leader-acceptor").await;
    let session = dialer.dial(&acceptor).await;

    let watched = timeout(
        SURVIVAL_PERIOD,
        network::watch_heartbeats(&session, &dialer.peers, Some(HEARTBEAT_TIMEOUT)),
    )
    .await;

    assert!(watched.is_err(), "watch ended early: {watched:?}");
    assert!(session.connection().close_reason().is_none());
    assert_eq!(
        dialer.token_for(&acceptor),
        Some(session.connection_token())
    );
    drop(session);
    dialer.stop().await;
    acceptor.stop().await;
}

/// Number of heartbeats [`heartbeat_then_go_silent`] sends before going quiet.
const BEATS_BEFORE_SILENCE: u64 = 3;

/// A hand-written acceptor that heartbeats [`BEATS_BEFORE_SILENCE`] times and then goes silent
/// while keeping the connection (and its stream) open; returns both so the caller holds them.
async fn heartbeat_then_go_silent(endpoint: Endpoint, hello: Hello) -> (PeerSession, SendStream) {
    let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
    let incoming = timeout(CLEANUP_TIMEOUT, endpoint.accept())
        .await
        .expect("dialer connects in time")
        .expect("acceptor endpoint open");
    let session = network::accept_peer(incoming, &hello, &peers)
        .await
        .expect("hello");
    let mut stream = session.connection().open_uni().await.expect("open stream");
    for sequence in 1..=BEATS_BEFORE_SILENCE {
        let heartbeat = Message::Heartbeat(Heartbeat {
            node_id: hello.node_id.clone(),
            epoch: network::HEARTBEAT_EPOCH,
            sequence,
        });
        protocol::write_frame(&mut stream, &heartbeat)
            .await
            .expect("write heartbeat");
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
    }
    (session, stream)
}

#[tokio::test]
async fn dialer_closes_the_link_when_acceptor_heartbeats_stop() {
    let dialer = TestNode::start("stalled-leader-dialer").await;
    let key_path = std::env::temp_dir().join(format!(
        "meshthing-p2p-local-{}-stalled-leader.key",
        std::process::id()
    ));
    // A leftover file from an aborted run may or may not exist; either way we start fresh.
    let _ = std::fs::remove_file(&key_path);
    let endpoint = network::bind_local_endpoint(&key_path, vec![CONTROL_ALPN.to_vec()])
        .await
        .expect("bind endpoint");
    let hello = network::local_hello(&endpoint, CLUSTER_ID, 0);
    let acceptor = tokio::spawn(heartbeat_then_go_silent(endpoint.clone(), hello));
    let session = network::dial_peer(
        &dialer.endpoint,
        endpoint.addr(),
        &dialer.hello,
        &dialer.peers,
    )
    .await
    .expect("dial the stalling acceptor");

    let reason = timeout(
        CLEANUP_TIMEOUT,
        network::watch_heartbeats(&session, &dialer.peers, Some(HEARTBEAT_TIMEOUT)),
    )
    .await
    .expect("dialer gives up on the silent acceptor");

    assert!(
        matches!(reason, ConnectionError::LocallyClosed),
        "expected a local close, got {reason:?}"
    );
    let (accepted, _stream) = acceptor.await.expect("acceptor task");
    assert_closed_for_heartbeat_timeout(accepted.connection()).await;
    drop((session, accepted));
    endpoint.close().await;
    dialer.stop().await;
    std::fs::remove_file(&key_path).expect("remove key file");
}
