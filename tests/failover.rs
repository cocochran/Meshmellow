//! End-to-end leader failover on loopback.
//!
//! A leader serves the shard backend (one local stage of the `fake_shard_runner` model) and
//! replicates the conversation to two standby workers, which run no RPC server and hold only
//! the control link. After a prompt has been saved on both workers the leader stops; the
//! workers elect one of themselves, which takes over on its own endpoint with its copy of the
//! history, and the other follows it. When the leader and one standby go away together, the
//! survivor has no majority and must stand down without serving. Endpoints dial each other by
//! node ID through in-memory address lookup, so no internet is needed.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{ConnectionError, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use llm_cluster::api;
use llm_cluster::history_sync::history::Entry;
use llm_cluster::history_sync::store::{HistoryStore, JobStatus, job_states};
use llm_cluster::leader::{BackendConfig, LeaderConfig, ShardBackendConfig, run_leader_on};
use llm_cluster::network::{self, HEARTBEAT_TIMEOUT_CODE, PeerSession, SharedPeerTable};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{CONTROL_ALPN, Heartbeat, RPC_BRIDGE_ALPN};
use llm_cluster::replication::ReplicationConfig;
use llm_cluster::worker::{TakeoverConfig, WorkerConfig, run_worker_on};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod common;
use common::example_bin;

const CLUSTER_ID: &str = "failover-test";
const CONVERSATION_ID: &str = "main";
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
/// `source_sha256` carried by `examples/fake-shard-model.json`.
const FIXTURE_SHA: &str = "5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";
const LAYERS: u32 = 4;
const BUDGET_BYTES: u64 = 1 << 20;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(1);
const FAILOVER_AFTER: Duration = Duration::from_secs(1);
const WORKER_WAIT: Duration = Duration::from_secs(30);
/// How long a winner waits for the others to join; several failover windows.
const QUORUM_WAIT: Duration = Duration::from_secs(5);
/// Long enough for the leader to send a roster with every worker's progress after it syncs.
const ROSTER_SETTLE: Duration = Duration::from_millis(2500);
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Bound on one probe dial, so a standby that isn't leading yet doesn't hold the probe up.
const PROBE_DIAL_TIMEOUT: Duration = Duration::from_secs(1);

fn init_tracing() {
    // Another test in this binary may already have installed the subscriber; that is fine.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn fixture_model() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("fake-shard-model.json")
}

fn fixture_reply() -> String {
    let text = std::fs::read_to_string(fixture_model()).expect("read model fixture");
    let model: Value = serde_json::from_str(&text).expect("parse model fixture");
    model["reply"].as_str().expect("fixture reply").to_owned()
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("meshthing-failover-{}-{name}", std::process::id()));
    // A leftover directory from an aborted run may or may not exist; either way we start fresh.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// A manifest that runs the whole fixture model in one local stage, so any node can lead.
fn write_manifest(dir: &Path) -> PathBuf {
    let manifest = json!({
        "runner": example_bin("fake_shard_runner"),
        "model_sha256": FIXTURE_SHA,
        "layers": LAYERS,
        "context": 512,
        "max_tokens": 256,
        "chat_template": "chatml",
        "stages": [{"begin": 0, "end": LAYERS, "local": {
            "model": fixture_model(), "budget_bytes": BUDGET_BYTES,
        }}],
    });
    let path = dir.join("manifest.json");
    std::fs::write(&path, manifest.to_string()).expect("write manifest");
    path
}

/// Binds an endpoint on loopback only that finds its peers through `lookup`.
async fn bind_endpoint(alpns: Vec<Vec<u8>>, lookup: &MemoryLookup) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr(LOOPBACK_ANY_PORT)
        .expect("valid bind address")
        .alpns(alpns)
        .address_lookup(lookup.clone())
        .bind()
        .await
        .expect("bind endpoint")
}

/// The endpoint's id plus its bound loopback sockets.
fn direct_addr(endpoint: &Endpoint) -> EndpointAddr {
    endpoint
        .bound_sockets()
        .into_iter()
        .fold(EndpointAddr::new(endpoint.id()), EndpointAddr::with_ip_addr)
}

/// Reserves an ephemeral loopback port for a component that binds it itself.
async fn free_loopback_addr() -> SocketAddr {
    let probe = TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("probe bind");
    probe.local_addr().expect("probe address")
}

async fn wait_until_listening(addr: SocketAddr) {
    timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(addr).await.is_err() {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{addr} never started listening"));
}

async fn prompt(api_addr: SocketAddr, text: &str) -> String {
    timeout(TEST_TIMEOUT, api::prompt(api_addr, text))
        .await
        .expect("prompt finishes")
        .expect("prompt succeeds")
}

/// The conversation saved in `data_dir`, once `ready` accepts it.
async fn wait_for_history(data_dir: &Path, ready: impl Fn(&HistoryStore) -> bool) -> HistoryStore {
    timeout(TEST_TIMEOUT, async {
        loop {
            if let Ok(store) = HistoryStore::load(data_dir, CONVERSATION_ID)
                && ready(&store)
            {
                return store;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("history in {} never got there", data_dir.display()))
}

fn completed_jobs(store: &HistoryStore) -> usize {
    job_states(store.history())
        .iter()
        .filter(|job| job.status == JobStatus::Completed)
        .count()
}

/// Who became leader in `epoch`, according to the history.
fn leader_of(store: &HistoryStore, epoch: u64) -> Option<EndpointId> {
    store
        .history()
        .entries()
        .iter()
        .find_map(|entry| match entry {
            Entry::EpochAdvanced { epoch: e, leader } if *e == epoch => Some(*leader),
            _ => None,
        })
}

/// One standby worker: its endpoint, data directory, takeover API and task.
struct Standby {
    endpoint: Endpoint,
    data_dir: PathBuf,
    api_addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Standby {
    async fn start(endpoint: Endpoint, leader: EndpointAddr, dir: &Path, manifest: &Path) -> Self {
        let data_dir = dir.join(format!("data-{}", endpoint.id().fmt_short()));
        let api_addr = free_loopback_addr().await;
        let takeover = TakeoverConfig {
            manifest_path: manifest.to_path_buf(),
            api_addr,
            upstream_port: free_loopback_addr().await.port(),
            worker_wait: WORKER_WAIT,
            heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
            heartbeat_interval: Some(HEARTBEAT_INTERVAL),
            quorum_wait: QUORUM_WAIT,
        };
        let config = WorkerConfig {
            key_path: dir.join("unused.key"),
            cluster_id: CLUSTER_ID.to_owned(),
            rpc_server: None,
            rpc_port: 0,
            rpc_args: Vec::new(),
            leader: None,
            allowed_peers: Vec::new(),
            heartbeat_interval: HEARTBEAT_INTERVAL,
            leader_heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
            data_dir: Some(data_dir.clone()),
            takeover: Some(takeover),
            failover_after: FAILOVER_AFTER,
        };
        let running = endpoint.clone();
        let task = tokio::spawn(async move { run_worker_on(running, Some(leader), &config).await });
        Self {
            endpoint,
            data_dir,
            api_addr,
            task,
        }
    }

    fn assert_running(&self) {
        assert!(!self.task.is_finished(), "{} stopped", self.endpoint.id());
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        self.endpoint.close().await;
    }
}

/// A leader serving the shard backend and replicating to two standbys, after one prompt has
/// been saved on both and the roster has caught up with it.
struct Cluster {
    dir: PathBuf,
    lookup: MemoryLookup,
    leader_endpoint: Endpoint,
    stop_leader: oneshot::Sender<()>,
    leader: JoinHandle<anyhow::Result<()>>,
    standbys: [Standby; 2],
}

impl Cluster {
    async fn start(name: &str) -> Self {
        init_tracing();
        let dir = scratch_dir(name);
        let manifest = write_manifest(&dir);
        let lookup = MemoryLookup::new();
        let leader_endpoint = bind_endpoint(vec![CONTROL_ALPN.to_vec()], &lookup).await;
        let worker_alpns = vec![CONTROL_ALPN.to_vec(), RPC_BRIDGE_ALPN.to_vec()];
        let first_endpoint = bind_endpoint(worker_alpns.clone(), &lookup).await;
        let second_endpoint = bind_endpoint(worker_alpns, &lookup).await;
        for endpoint in [&leader_endpoint, &first_endpoint, &second_endpoint] {
            lookup.add_endpoint_info(direct_addr(endpoint));
        }

        let leader_api = free_loopback_addr().await;
        let config = LeaderConfig {
            key_path: dir.join("unused.key"),
            cluster_id: CLUSTER_ID.to_owned(),
            backend: BackendConfig::Shard(ShardBackendConfig {
                manifest_path: manifest.clone(),
            }),
            upstream_port: free_loopback_addr().await.port(),
            api_addr: leader_api,
            worker_wait: WORKER_WAIT,
            heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
            heartbeat_interval: Some(HEARTBEAT_INTERVAL),
            replication: Some(ReplicationConfig {
                data_dir: dir.join("data-leader"),
                conversation_id: CONVERSATION_ID.to_owned(),
                min_epoch: 0,
            }),
            quorum: None,
        };
        let (stop_leader, stopped) = oneshot::channel::<()>();
        let leader = tokio::spawn({
            let endpoint = leader_endpoint.clone();
            async move {
                run_leader_on(endpoint, &config, async {
                    let _ = stopped.await;
                })
                .await
            }
        });
        wait_until_listening(leader_api).await;

        let leader_addr = direct_addr(&leader_endpoint);
        let standbys = [
            Standby::start(first_endpoint, leader_addr.clone(), &dir, &manifest).await,
            Standby::start(second_endpoint, leader_addr, &dir, &manifest).await,
        ];

        // A turn on the original leader reaches both workers' disks.
        assert_eq!(
            prompt(leader_api, "before the failover").await,
            fixture_reply()
        );
        for standby in &standbys {
            let store =
                wait_for_history(&standby.data_dir, |store| completed_jobs(store) == 1).await;
            assert_eq!(store.epoch(), 1);
        }
        tokio::time::sleep(ROSTER_SETTLE).await;
        Self {
            dir,
            lookup,
            leader_endpoint,
            stop_leader,
            leader,
            standbys,
        }
    }

    /// Stops the original leader cleanly; returns the standbys and what to clean up.
    async fn stop_leader(self) -> ([Standby; 2], Leftovers) {
        self.stop_leader.send(()).expect("leader still running");
        timeout(TEST_TIMEOUT, self.leader)
            .await
            .expect("leader stops")
            .expect("leader did not panic")
            .expect("leader shuts down cleanly");
        let leftovers = Leftovers {
            dir: self.dir,
            lookup: self.lookup,
            leader_endpoint: self.leader_endpoint,
        };
        (self.standbys, leftovers)
    }
}

/// What a test cleans up once the leader is gone.
struct Leftovers {
    dir: PathBuf,
    lookup: MemoryLookup,
    leader_endpoint: Endpoint,
}

impl Leftovers {
    async fn finish(self) {
        self.leader_endpoint.close().await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_takes_over_with_the_history_and_the_other_follows_it() {
    let cluster = Cluster::start("takeover").await;

    // The leader goes away; exactly one standby takes over in epoch 2.
    let ([first, second], leftovers) = cluster.stop_leader().await;
    let winner_store = timeout(TEST_TIMEOUT, async {
        loop {
            for standby in [&first, &second] {
                if let Ok(store) = HistoryStore::load(&standby.data_dir, CONVERSATION_ID)
                    && leader_of(&store, 2).is_some()
                {
                    return store;
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("a standby takes over");
    let winner_id = leader_of(&winner_store, 2).expect("epoch 2 leader");
    let (winner, follower) = if winner_id == first.endpoint.id() {
        (&first, &second)
    } else {
        (&second, &first)
    };
    assert_eq!(winner_id, winner.endpoint.id());

    // The new leader serves prompts and keeps the conversation from before the failover.
    wait_until_listening(winner.api_addr).await;
    assert_eq!(
        prompt(winner.api_addr, "after the failover").await,
        fixture_reply()
    );
    let winner_history =
        wait_for_history(&winner.data_dir, |store| completed_jobs(store) == 2).await;
    assert_eq!(winner_history.epoch(), 2);

    // The other worker followed the new leader and holds the same history.
    let follower_history = wait_for_history(&follower.data_dir, |store| {
        completed_jobs(store) == 2 && store.epoch() == 2
    })
    .await;
    assert_eq!(follower_history.history(), winner_history.history());
    assert!(
        TcpStream::connect(follower.api_addr).await.is_err(),
        "only the winner serves a job API"
    );
    winner.assert_running();
    follower.assert_running();

    first.stop().await;
    second.stop().await;
    leftovers.finish().await;
}

/// Dials `standby` as a worker would until it answers as a leader (it only accepts control
/// connections while it is trying to lead), then heartbeats to it so it never drops the
/// connection for silence.
async fn dial_until_leading(
    lookup: &MemoryLookup,
    standby: &Standby,
) -> (PeerSession, JoinHandle<()>) {
    let probe = bind_endpoint(Vec::new(), lookup).await;
    let hello = network::local_hello(&probe, CLUSTER_ID, 0);
    let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
    let session = timeout(TEST_TIMEOUT, async {
        loop {
            let dial = network::dial_peer(&probe, direct_addr(&standby.endpoint), &hello, &peers);
            if let Ok(Ok(session)) = timeout(PROBE_DIAL_TIMEOUT, dial).await {
                return session;
            }
        }
    })
    .await
    .expect("the standby starts trying to lead");
    let connection = session.connection().clone();
    let node_id = probe.id().to_string();
    let heartbeats = tokio::spawn(async move {
        let mut sequence = 0;
        let next = || {
            sequence += 1;
            Heartbeat {
                node_id: node_id.clone(),
                epoch: 0,
                sequence,
            }
        };
        let _ = network::write_heartbeats(&connection, HEARTBEAT_INTERVAL, next).await;
        // The endpoint must outlive the heartbeats.
        drop(probe);
    });
    (session, heartbeats)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standby_left_without_a_majority_stands_down_without_serving() {
    let cluster = Cluster::start("no-majority").await;

    // The leader and one standby go away together: two of the three nodes.
    let ([lone, gone], leftovers) = cluster.stop_leader().await;
    gone.stop().await;

    // The survivor wins once the others are left out and starts gathering followers. The probe
    // gets a hello, but it isn't in the cluster, so it doesn't count toward the majority.
    let (probe, heartbeats) = dial_until_leading(&leftovers.lookup, &lone).await;
    assert!(
        TcpStream::connect(lone.api_addr).await.is_err(),
        "no job API before a majority joins"
    );

    // Nobody it counts joins, so it stands down: it closes the probe's connection and goes back
    // to being a worker, having served nothing and recorded no new epoch.
    let closed = timeout(QUORUM_WAIT * 2, probe.connection().closed())
        .await
        .expect("the standby stands down after its quorum wait");
    assert!(
        !matches!(&closed, ConnectionError::ApplicationClosed(close) if close.error_code == HEARTBEAT_TIMEOUT_CODE.into()),
        "closed for the wrong reason: {closed}"
    );
    heartbeats.abort();
    assert!(
        TcpStream::connect(lone.api_addr).await.is_err(),
        "it never served a job API"
    );
    let store = HistoryStore::load(&lone.data_dir, CONVERSATION_ID).expect("saved history");
    assert_eq!(store.epoch(), 1);
    assert_eq!(leader_of(&store, 2), None);
    assert_eq!(completed_jobs(&store), 1);
    lone.assert_running();

    lone.stop().await;
    leftovers.finish().await;
}
