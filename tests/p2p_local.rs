//! Three nodes on localhost connect pairwise, exchange hellos, and see each other in their peer tables.
//!
//! Endpoints are bound without relays or address lookup and dialed by their direct addresses, so
//! the test needs no internet access.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use iroh::Endpoint;
use iroh::endpoint::Connection;
use llm_cluster::network::{self, SharedPeerTable};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{CONTROL_ALPN, Hello};

const NODE_COUNT: usize = 3;
const CLUSTER_ID: &str = "p2p-local-test";

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

    async fn dial(&self, other: &TestNode) -> Connection {
        let (connection, their_hello) = network::dial_peer(
            &self.endpoint,
            other.endpoint.addr(),
            &self.hello,
            &self.peers,
        )
        .await
        .expect("dial peer");
        assert_eq!(their_hello, other.hello);
        connection
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

    let mut connections = Vec::new();
    for (i, dialer) in nodes.iter().enumerate() {
        for acceptor in &nodes[i + 1..] {
            connections.push(dialer.dial(acceptor).await);
        }
    }

    for node in &nodes {
        for other in &nodes {
            if node.hello.node_id != other.hello.node_id {
                node.assert_knows(other);
            }
        }
    }

    drop(connections);
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
