//! Three nodes on localhost connect pairwise, exchange hellos, and see each other in their peer tables.
//!
//! Endpoints are bound without relays or address lookup and dialed by their direct addresses, so
//! the test needs no internet access.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::Endpoint;
use llm_cluster::network::{self, PeerSession, SharedPeerTable};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{CONTROL_ALPN, Hello};
use tokio::time::timeout;

const NODE_COUNT: usize = 3;
const CLUSTER_ID: &str = "p2p-local-test";

/// Longest a test waits for a closed connection to leave a peer table.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a test re-checks a peer table while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

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
        tokio::spawn(network::serve_control(
            endpoint.clone(),
            hello.clone(),
            Arc::clone(&peers),
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
