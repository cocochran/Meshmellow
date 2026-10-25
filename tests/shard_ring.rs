//! End-to-end tests of EXPERIMENTAL ring execution (`meshthing/shard-ring/1`) on loopback.
//!
//! The leader runs `run_leader_on` with the shard backend and a ring manifest: a local stage
//! `[0, 1)` on the `fake_shard_runner` example, then two real `serve_ring_worker`s, the entry
//! `[1, 3)` and the terminal `[3, 4)`. Stage 0's rows go to the entry, the entry forwards its
//! rows to the terminal, and the terminal returns each token straight to the leader. Failure
//! cases drive the shard front end directly, so each runs exactly one ring session. Endpoints run
//! without relays and dial each other through one in-memory address lookup.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, ConnectionError, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use llm_cluster::api;
use llm_cluster::leader::{BackendConfig, LeaderConfig, ShardBackendConfig, run_leader_on};
use llm_cluster::network::{self, SharedPeerTable};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::CONTROL_ALPN;
use llm_cluster::rpc_bridge::PEER_NOT_APPROVED;
use llm_cluster::shard::frontend::{FrontendConfig, serve_frontend};
use llm_cluster::shard::manifest::{ShardManifest, sha256_file};
use llm_cluster::shard::ring::{
    RING_ALPN, RingMessage, RingStageInfo, RingWorkerConfig, new_job_id, next_control,
    serve_ring_worker, write_control,
};
use llm_cluster::shard::ring_origin::{ResultChannels, RingSettings};
use llm_cluster::shard::wire::StageMetadata;
use serde_json::{Map, Value, json};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod common;
use common::example_bin;

const CLUSTER_ID: &str = "shard-ring-test";
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
/// `source_sha256` carried by `examples/fake-shard-model.json`: the "original model" identity
/// every ring stage must report.
const FIXTURE_SHA: &str = "5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";
const LAYERS: u32 = 4;
/// The leader's local stage 0.
const LOCAL: (u32, u32) = (0, 1);
/// The ring's entry worker and its terminal.
const ENTRY: (u32, u32) = (1, 3);
const TERMINAL: (u32, u32) = (3, 4);
/// The fixture model's hidden size and vocabulary, as its runner reports them.
const EMBD: u32 = 8;
const VOCAB: u32 = 300;
const CONTEXT: u32 = 512;
const MAX_TOKENS: u32 = 256;
const BUDGET_BYTES: u64 = 1 << 20;
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const WORKER_WAIT: Duration = Duration::from_secs(30);
/// Bound on a ring becoming ready in the failure tests, kept short on purpose.
const SHORT_RESULT_WAIT: Duration = Duration::from_secs(2);
/// A ring that never becomes ready must fail well within this, far below ring.rs's 240 s
/// control-line timeout.
const BOUNDED_FAILURE_LIMIT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// The leader heartbeats its control peers, so the ALPN dispatch must keep that path working.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
const SSE_DATA_PREFIX: &str = "data:";
const SSE_DONE: &str = "[DONE]";
const PROMPTS: [&str; 2] = ["hello through the ring", "and once more"];
/// The readiness probe plus the three jobs of the end-to-end test.
const EXPECTED_SESSIONS: usize = 4;

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
    assert_eq!(
        model["source_sha256"], FIXTURE_SHA,
        "the ring needs the fixture's 64-hex model identity"
    );
    model["reply"].as_str().expect("fixture reply").to_owned()
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "meshthing-shard-ring-{}-{name}",
        std::process::id()
    ));
    // A leftover directory from an aborted run may or may not exist; either way we start fresh.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Reserves an ephemeral loopback port for a component that binds it itself.
async fn free_loopback_addr() -> SocketAddr {
    let probe = TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("probe bind");
    probe.local_addr().expect("probe address")
}

/// Endpoints of one test, all reachable by node ID through a shared in-memory lookup.
struct Mesh {
    lookup: MemoryLookup,
}

impl Mesh {
    fn new() -> Self {
        init_tracing();
        Self {
            lookup: MemoryLookup::new(),
        }
    }

    /// Binds a loopback-only endpoint accepting `alpn` and publishes its address to the mesh.
    async fn bind(&self, alpn: &[u8]) -> Endpoint {
        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .bind_addr(LOOPBACK_ANY_PORT)
            .expect("valid bind address")
            .alpns(vec![alpn.to_vec()])
            .address_lookup(self.lookup.clone())
            .bind()
            .await
            .expect("bind endpoint");
        let addr = endpoint
            .bound_sockets()
            .into_iter()
            .fold(EndpointAddr::new(endpoint.id()), EndpointAddr::with_ip_addr);
        self.lookup.add_endpoint_info(addr);
        endpoint
    }
}

/// A ring manifest: the local stage 0, then the remote `(range, worker)` stages in order.
fn ring_manifest(remote: &[((u32, u32), EndpointId)]) -> Value {
    let (begin, end) = LOCAL;
    let mut stages = vec![json!({"begin": begin, "end": end, "local": {
        "model": fixture_model(), "budget_bytes": BUDGET_BYTES,
    }})];
    stages.extend(remote.iter().map(|&((begin, end), worker)| {
        json!({"begin": begin, "end": end, "remote": {"worker": worker.to_string()}})
    }));
    json!({
        "runner": example_bin("fake_shard_runner"),
        "model_sha256": FIXTURE_SHA,
        "layers": LAYERS,
        "context": CONTEXT,
        "max_tokens": MAX_TOKENS,
        "chat_template": "chatml",
        "execution": "ring",
        "stages": stages,
    })
}

fn worker_config(
    (begin, end): (u32, u32),
    upstream: EndpointId,
    origin: EndpointId,
    next: Option<EndpointId>,
) -> RingWorkerConfig {
    let model = fixture_model();
    RingWorkerConfig {
        runner: example_bin("fake_shard_runner"),
        expected_sha256: sha256_file(&model).expect("hash model fixture"),
        model,
        begin,
        end,
        context: CONTEXT,
        gpu_layers: 0,
        budget_bytes: BUDGET_BYTES,
        source_sha256: Some(FIXTURE_SHA.to_owned()),
        upstream,
        origin,
        next,
    }
}

/// A running `serve_ring_worker`.
struct Worker {
    endpoint: Endpoint,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Worker {
    fn serve(endpoint: Endpoint, config: RingWorkerConfig) -> Self {
        let task = tokio::spawn(serve_ring_worker(endpoint.clone(), config));
        Self { endpoint, task }
    }

    fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    async fn stop(self) {
        self.endpoint.close().await;
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("serve_ring_worker stops after endpoint close")
            .expect("serve_ring_worker did not panic")
            .expect("serve_ring_worker returns Ok");
    }
}

/// The ring of the end-to-end test: the entry forwards to the terminal, and both call back to
/// `origin`.
async fn serve_ring(mesh: &Mesh, origin: EndpointId) -> (Worker, Worker) {
    let entry_endpoint = mesh.bind(RING_ALPN).await;
    let terminal = Worker::serve(
        mesh.bind(RING_ALPN).await,
        worker_config(TERMINAL, entry_endpoint.id(), origin, None),
    );
    let entry = Worker::serve(
        entry_endpoint,
        worker_config(ENTRY, origin, origin, Some(terminal.id())),
    );
    (entry, terminal)
}

/// A leader on the shard backend with a ring manifest.
struct Leader {
    dir: PathBuf,
    endpoint: Endpoint,
    config: LeaderConfig,
    channels: Arc<ResultChannels>,
}

impl Leader {
    /// Binds the leader's endpoint on the control ALPN only: the leader adds the ring ALPN
    /// itself for a ring manifest.
    async fn bind(name: &str, mesh: &Mesh) -> Self {
        let dir = scratch_dir(name);
        let endpoint = mesh.bind(CONTROL_ALPN).await;
        let channels = Arc::new(ResultChannels::new());
        let config = LeaderConfig {
            key_path: dir.join("leader.key"),
            cluster_id: CLUSTER_ID.to_owned(),
            backend: BackendConfig::Shard(ShardBackendConfig {
                manifest_path: dir.join("manifest.json"),
                ring: RingSettings {
                    channels: Arc::clone(&channels),
                    result_channel_wait: WORKER_WAIT,
                },
            }),
            upstream_port: free_loopback_addr().await.port(),
            api_addr: free_loopback_addr().await,
            worker_wait: WORKER_WAIT,
            heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
            heartbeat_interval: Some(HEARTBEAT_INTERVAL),
        };
        Self {
            dir,
            endpoint,
            config,
            channels,
        }
    }

    fn write_manifest(&self, manifest: &Value) {
        let path = self.dir.join("manifest.json");
        let bytes = serde_json::to_vec_pretty(manifest).expect("encode manifest");
        std::fs::write(path, bytes).expect("write manifest");
    }

    /// Runs the leader in the background until the returned sender fires (or is dropped).
    fn start(&self) -> (oneshot::Sender<()>, JoinHandle<anyhow::Result<()>>) {
        let (stop, stopped) = oneshot::channel::<()>();
        let endpoint = self.endpoint.clone();
        let config = self.config.clone();
        let leader = tokio::spawn(async move {
            run_leader_on(endpoint, &config, async {
                // A dropped sender also means stop.
                let _ = stopped.await;
            })
            .await
        });
        (stop, leader)
    }

    async fn finish(self) {
        self.endpoint.close().await;
        // Best effort: a leftover temp directory must not fail a passing test.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
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

async fn stop_cleanly(stop: oneshot::Sender<()>, leader: JoinHandle<anyhow::Result<()>>) {
    stop.send(()).expect("leader still running");
    timeout(TEST_TIMEOUT, leader)
        .await
        .expect("leader stops")
        .expect("leader did not panic")
        .expect("leader shuts down cleanly");
}

/// Sends `text` the way `llm-cluster prompt` does and returns the reply.
async fn prompt(api_addr: SocketAddr, text: &str) -> String {
    timeout(TEST_TIMEOUT, api::prompt(api_addr, text))
        .await
        .expect("prompt finishes")
        .expect("prompt succeeds")
}

/// Posts a chat request for `text` to `addr` and returns the status and raw body.
async fn chat(addr: SocketAddr, text: &str, stream: bool) -> (u16, String) {
    let request = json!({
        "stream": stream,
        "messages": [{"role": "user", "content": text}],
    });
    let response = timeout(
        TEST_TIMEOUT,
        reqwest::Client::new()
            .post(format!("http://{addr}{}", api::CHAT_COMPLETIONS_PATH))
            .json(&request)
            .send(),
    )
    .await
    .expect("chat request timed out")
    .expect("chat request");
    let status = response.status().as_u16();
    let body = timeout(TEST_TIMEOUT, response.text())
        .await
        .expect("chat body timed out")
        .expect("chat body");
    (status, body)
}

/// The text of a streamed reply, which must end with `[DONE]`.
fn streamed_text(body: &str) -> String {
    let payloads: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix(SSE_DATA_PREFIX))
        .map(str::trim)
        .collect();
    assert_eq!(payloads.last().copied(), Some(SSE_DONE), "{body}");
    payloads[..payloads.len() - 1]
        .iter()
        .map(|data| serde_json::from_str::<Value>(data).expect("chunk json"))
        .filter_map(|chunk| {
            chunk["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}

async fn job_statuses(api_addr: SocketAddr) -> Vec<String> {
    let jobs: Value = timeout(
        TEST_TIMEOUT,
        reqwest::get(format!("http://{api_addr}{}", api::JOBS_PATH)),
    )
    .await
    .expect("jobs request timed out")
    .expect("jobs request")
    .json()
    .await
    .expect("jobs json");
    jobs.as_array()
        .expect("jobs array")
        .iter()
        .map(|job| job["status"].as_str().expect("job status").to_owned())
        .collect()
}

/// Dials the leader on the control ALPN through its dispatching accept loop and waits for the
/// leader's own heartbeats, which only the heartbeat-capable control session sends.
async fn control_session_heartbeats(mesh: &Mesh, leader: EndpointId) {
    let dialer = mesh.bind(CONTROL_ALPN).await;
    let peers: SharedPeerTable = Arc::new(std::sync::Mutex::new(PeerTable::new()));
    let local = network::local_hello(&dialer, CLUSTER_ID, 0);
    let session = timeout(
        TEST_TIMEOUT,
        network::dial_peer(&dialer, leader, &local, &peers),
    )
    .await
    .expect("control hello timed out")
    .expect("control hello succeeds alongside the ring");
    assert_eq!(session.node_id(), leader);
    let heartbeat_seen = async {
        loop {
            let seen = peers
                .lock()
                .expect("peer table lock")
                .get(&leader)
                .and_then(|info| info.last_heartbeat_sequence)
                .is_some();
            if seen {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };
    tokio::select! {
        () = heartbeat_seen => {}
        closed = network::watch_heartbeats(&session, &peers, None) => {
            panic!("control connection closed before a leader heartbeat: {closed:?}");
        }
        () = tokio::time::sleep(TEST_TIMEOUT) => panic!("the leader never heartbeated"),
    }
    drop(session);
    dialer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_api_serves_prompts_through_a_three_stage_ring() {
    let mesh = Mesh::new();
    let leader = Leader::bind("end-to-end", &mesh).await;
    let (entry, terminal) = serve_ring(&mesh, leader.endpoint.id()).await;
    leader.write_manifest(&ring_manifest(&[
        (ENTRY, entry.id()),
        (TERMINAL, terminal.id()),
    ]));

    let (stop, running) = leader.start();
    wait_until_listening(leader.config.api_addr).await;
    let api_addr = leader.config.api_addr;

    assert_eq!(prompt(api_addr, PROMPTS[0]).await, fixture_reply());
    let (status, body) = chat(api_addr, PROMPTS[0], true).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(streamed_text(&body), fixture_reply());
    assert_eq!(prompt(api_addr, PROMPTS[1]).await, fixture_reply());
    assert_eq!(job_statuses(api_addr).await, ["completed"; 3]);
    control_session_heartbeats(&mesh, leader.endpoint.id()).await;

    // Every session (the readiness probe and each job) sent stage 0's rows to the entry and
    // read nothing but JSON control lines back: no activation byte reached the leader.
    let traffic = leader.channels.traffic();
    assert_eq!(traffic.len(), EXPECTED_SESSIONS, "{traffic:#?}");
    for session in &traffic {
        assert!(session.succeeded, "{session:?}");
        assert!(session.steps > 0, "{session:?}");
        assert!(session.activation_bytes_sent > 0, "{session:?}");
        assert!(
            session.control_lines > u64::from(session.steps),
            "{session:?}"
        );
        assert!(session.bytes_read > 0, "{session:?}");
        assert_eq!(
            session.bytes_read, session.control_line_bytes,
            "{session:?}"
        );
        assert_eq!(session.activation_bytes_read(), 0, "{session:?}");
    }
    for session in &traffic {
        assert!(!leader.channels.is_pending(&session.job), "{session:?}");
    }

    stop_cleanly(stop, running).await;
    entry.stop().await;
    terminal.stop().await;
    leader.finish().await;
}

/// The shard front end serving a ring manifest from `endpoint`, with a stand-in for the
/// leader's accept loop feeding the registry.
struct RingFrontend {
    addr: SocketAddr,
    endpoint: Endpoint,
    channels: Arc<ResultChannels>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
    accept: JoinHandle<()>,
}

impl RingFrontend {
    async fn start(endpoint: Endpoint, manifest: &Value, result_channel_wait: Duration) -> Self {
        let manifest: ShardManifest =
            serde_json::from_value(manifest.clone()).expect("parse manifest");
        manifest.validate().expect("valid manifest");
        let channels = Arc::new(ResultChannels::new());
        let accept = tokio::spawn(accept_ring(endpoint.clone(), Arc::clone(&channels)));
        let addr = free_loopback_addr().await;
        let config = FrontendConfig {
            listen: addr,
            manifest,
            endpoint: endpoint.clone(),
            ring: Some(RingSettings {
                channels: Arc::clone(&channels),
                result_channel_wait,
            }),
        };
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(serve_frontend(config, async {
            let _ = stopped.await;
        }));
        wait_until_listening(addr).await;
        Self {
            addr,
            endpoint,
            channels,
            stop,
            task,
            accept,
        }
    }

    /// Sends one prompt, which must fail before any response bytes; returns the error body.
    async fn chat_fails(&self) -> String {
        let (status, body) = chat(self.addr, PROMPTS[0], false).await;
        assert_eq!(status, 502, "{body}");
        body
    }

    async fn stop(self) {
        let _ = self.stop.send(());
        let _ = timeout(TEST_TIMEOUT, self.task).await;
        self.accept.abort();
        self.endpoint.close().await;
    }
}

/// Hands every ring connection on `endpoint` to `channels`, as the leader's accept loop does.
async fn accept_ring(endpoint: Endpoint, channels: Arc<ResultChannels>) {
    while let Some(incoming) = endpoint.accept().await {
        let channels = Arc::clone(&channels);
        tokio::spawn(async move {
            if let Ok(connection) = incoming.await {
                channels.handle_connection(connection).await;
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_chain_that_does_not_match_the_manifest_fails_the_job() {
    let mesh = Mesh::new();
    let origin = mesh.bind(RING_ALPN).await;
    let (entry, terminal) = serve_ring(&mesh, origin.id()).await;
    // The workers serve 1..3 and 3..4; the manifest places the cut at layer 2.
    let manifest = ring_manifest(&[((1, 2), entry.id()), ((2, 4), terminal.id())]);
    let frontend = RingFrontend::start(origin, &manifest, WORKER_WAIT).await;

    let body = frontend.chat_fails().await;
    assert!(body.contains("does not match the manifest"), "{body}");
    assert!(body.contains("serves layers 1..3"), "{body}");
    assert!(
        frontend.channels.traffic().is_empty(),
        "no ring session ran"
    );

    frontend.stop().await;
    entry.stop().await;
    terminal.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_in_the_wrong_order_fail_the_job() {
    let mesh = Mesh::new();
    let origin = mesh.bind(RING_ALPN).await;
    let (entry, terminal) = serve_ring(&mesh, origin.id()).await;
    // The manifest swaps the workers: the terminal does not accept the origin upstream.
    let manifest = ring_manifest(&[(ENTRY, terminal.id()), (TERMINAL, entry.id())]);
    let frontend = RingFrontend::start(origin, &manifest, SHORT_RESULT_WAIT).await;

    let body = frontend.chat_fails().await;
    assert!(body.contains("ring job"), "{body}");

    frontend.stop().await;
    entry.stop().await;
    terminal.stop().await;
}

/// Opens a result channel to `origin` for `job` from `from`. Returns the connection and the
/// stream's send side, which the caller keeps open until the origin closes the connection.
async fn open_result_channel(
    from: &Endpoint,
    origin: EndpointId,
    job: &str,
) -> (Connection, Option<SendStream>) {
    let connection = timeout(TEST_TIMEOUT, from.connect(origin, RING_ALPN))
        .await
        .expect("connect timed out")
        .expect("connect to origin");
    let Ok((mut send, _recv)) = connection.open_bi().await else {
        return (connection, None);
    };
    // The origin may already have closed the connection; then the write fails, as it should.
    let hello = RingMessage::ResultChannel {
        job: job.to_owned(),
    };
    let _ = write_control(&mut send, &hello).await;
    (connection, Some(send))
}

/// Waits for the origin to close `connection` and returns the application close code.
async fn close_code(connection: &Connection) -> u64 {
    let closed = timeout(TEST_TIMEOUT, connection.closed())
        .await
        .expect("origin closes the connection");
    let ConnectionError::ApplicationClosed(close) = closed else {
        panic!("expected an application close, got {closed:?}");
    };
    close.error_code.into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_channels_are_accepted_only_from_the_terminal_and_once() {
    let mesh = Mesh::new();
    let origin = mesh.bind(RING_ALPN).await;
    let terminal = mesh.bind(RING_ALPN).await;
    let stranger = mesh.bind(RING_ALPN).await;
    let channels = Arc::new(ResultChannels::new());
    let accept = tokio::spawn(accept_ring(origin.clone(), Arc::clone(&channels)));
    let job = new_job_id();
    let mut pending = channels
        .register(&job, terminal.id())
        .expect("register job");

    // A peer that is not the job's terminal is refused, and the job stays pending.
    let (refused, _send) = open_result_channel(&stranger, origin.id(), &job).await;
    assert_eq!(close_code(&refused).await, PEER_NOT_APPROVED.into_inner());
    assert!(channels.is_pending(&job));

    // The terminal is accepted and its channel reaches the waiting generation.
    let accepted = timeout(TEST_TIMEOUT, terminal.connect(origin.id(), RING_ALPN))
        .await
        .expect("connect timed out")
        .expect("connect to origin");
    let (mut send, recv) = accepted.open_bi().await.expect("open result channel");
    let hello = RingMessage::ResultChannel { job: job.clone() };
    write_control(&mut send, &hello).await.expect("send hello");
    let answer = next_control(&mut BufReader::new(recv))
        .await
        .expect("origin answers");
    assert_eq!(answer, RingMessage::Accepted { job: job.clone() });
    let channel = timeout(TEST_TIMEOUT, pending.wait())
        .await
        .expect("channel delivered in time")
        .expect("channel delivered");
    assert_eq!(channel.job(), job);
    assert!(!channels.is_pending(&job), "the job is consumed");

    // Replaying the consumed job is refused: the terminal has nothing pending any more.
    let (replay, _send) = open_result_channel(&terminal, origin.id(), &job).await;
    assert_eq!(close_code(&replay).await, PEER_NOT_APPROVED.into_inner());

    // An unknown job from a terminal with another pending job is refused too.
    let other = new_job_id();
    let _other_pending = channels.register(&other, terminal.id()).expect("register");
    let (unknown, _send) = open_result_channel(&terminal, origin.id(), &new_job_id()).await;
    let code = close_code(&unknown).await;
    assert_ne!(code, 0, "an unknown job must not be accepted");
    assert!(channels.is_pending(&other), "the other job is untouched");

    drop(channel);
    accept.abort();
    origin.close().await;
    terminal.close().await;
    stranger.close().await;
}

/// Stage metadata a ring worker for `range` of the fixture model would report.
fn fixture_stage((begin, end): (u32, u32), node: EndpointId) -> RingStageInfo {
    RingStageInfo {
        meta: StageMetadata {
            protocol: 1,
            begin,
            end,
            layers: LAYERS,
            embd: EMBD,
            vocab: VOCAB,
            allocated_bytes: 0,
            source_sha256: FIXTURE_SHA.to_owned(),
            tokens: Vec::new(),
            model_sha256: None,
            artifact_sha256: None,
            runner_sha256: None,
            extra: Map::new(),
        },
        node_id: node.to_string(),
    }
}

/// A terminal that answers every `start` with a valid `ready` but never opens its result
/// channel to the origin.
async fn silent_terminal(endpoint: Endpoint) {
    while let Some(incoming) = endpoint.accept().await {
        let stage = fixture_stage(TERMINAL, endpoint.id());
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                return;
            };
            let Ok((mut send, recv)) = connection.accept_bi().await else {
                return;
            };
            let Ok(RingMessage::Start { job }) = next_control(&mut BufReader::new(recv)).await
            else {
                return;
            };
            let ready = RingMessage::Ready {
                job,
                stages: vec![stage],
            };
            if write_control(&mut send, &ready).await.is_ok() {
                connection.closed().await;
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_that_never_opens_its_result_channel_fails_within_the_wait() {
    let mesh = Mesh::new();
    let origin = mesh.bind(RING_ALPN).await;
    let terminal = mesh.bind(RING_ALPN).await;
    let terminal_id = terminal.id();
    let silent = tokio::spawn(silent_terminal(terminal.clone()));
    let entry = Worker::serve(
        mesh.bind(RING_ALPN).await,
        worker_config(ENTRY, origin.id(), origin.id(), Some(terminal_id)),
    );
    let manifest = ring_manifest(&[(ENTRY, entry.id()), (TERMINAL, terminal_id)]);
    let frontend = RingFrontend::start(origin, &manifest, SHORT_RESULT_WAIT).await;

    let started = Instant::now();
    let body = frontend.chat_fails().await;
    let elapsed = started.elapsed();
    assert!(body.contains("did not become ready within"), "{body}");
    assert!(elapsed >= SHORT_RESULT_WAIT, "failed early: {elapsed:?}");
    assert!(elapsed < BOUNDED_FAILURE_LIMIT, "took {elapsed:?}");

    frontend.stop().await;
    entry.stop().await;
    silent.abort();
    terminal.close().await;
}

#[tokio::test]
async fn ring_manifest_without_ring_settings_is_refused() {
    let mesh = Mesh::new();
    let endpoint = mesh.bind(RING_ALPN).await;
    let peer = mesh.bind(RING_ALPN).await;
    let manifest: ShardManifest =
        serde_json::from_value(ring_manifest(&[((1, 4), peer.id())])).expect("parse manifest");
    let config = FrontendConfig {
        listen: free_loopback_addr().await,
        manifest,
        endpoint: endpoint.clone(),
        ring: None,
    };
    let error = timeout(TEST_TIMEOUT, serve_frontend(config, std::future::pending()))
        .await
        .expect("startup check finishes")
        .expect_err("a ring manifest needs ring settings");
    assert!(format!("{error:#}").contains("ring settings"), "{error:#}");
    endpoint.close().await;
    peer.close().await;
}
