//! End-to-end tests of the leader process on loopback.
//!
//! A real worker (`run_worker_on` supervising the `fake_rpc_server` example) contributes to a
//! plan built by `planning::build_plan`, and the `fake_llama_server` example stands in for
//! `llama-server`. Endpoints run without relays or address lookup, so no internet is needed.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use llm_cluster::api;
use llm_cluster::evidence::EvidenceConfig;
use llm_cluster::leader::{BackendConfig, LeaderConfig, LlamaBackendConfig, run_leader_on};
use llm_cluster::planning::{self, PlanningRequest};
use llm_cluster::protocol::{CONTROL_ALPN, RPC_BRIDGE_ALPN};
use llm_cluster::worker::{WorkerConfig, run_worker_on};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod common;
use common::example_bin;

const CLUSTER_ID: &str = "leader-test";
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
/// Peer ID of the leader's own device in the test plan; it has no RPC bridge.
const LEADER_PEER_ID: &str = "leader";
const MIN_PEERS: usize = 2;
const MODEL_HASH: &str = "synthetic-model";
const RUNTIME_ID: &str = "synthetic-runtime";
const CONTEXT_TOKENS: u64 = 8192;
const PEER_MEMORY_BUDGET_BYTES: u64 = 300_000_000;
/// Peak memory reported for each planned peer in run evidence; within the peer budget.
const EVIDENCE_PEAK_BYTES: u64 = 250_000_000;
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
/// A rejected plan must fail well within this, far below the leader's worker wait.
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the leader may take to read and act on run evidence once it is written.
const EVIDENCE_SETTLE: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const WORKER_WAIT: Duration = Duration::from_secs(30);
const SHORT_WORKER_WAIT: Duration = Duration::from_millis(500);
const LLAMA_READY_TIMEOUT: Duration = Duration::from_secs(30);
const SHORT_LLAMA_READY_TIMEOUT: Duration = Duration::from_secs(2);
/// Heartbeat interval of both the worker and the leader.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
/// Ten missed beats: the leader and the worker run with their heartbeat timeouts on, as in
/// production, without a slow test machine dropping the link.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the leader is left without its worker, to show it waits instead of launching.
const WORKER_JOIN_DELAY: Duration = Duration::from_millis(500);
const BRIDGE_PAYLOAD: &[u8] = b"ggml rpc bytes through the leader bridge";
const PROMPT: &str = "hello from the leader test";

/// One leader plus one worker, wired together by a plan written to a scratch directory.
struct Cluster {
    dir: PathBuf,
    config: LeaderConfig,
    leader_endpoint: Endpoint,
    worker_endpoint: Endpoint,
    /// Leader-local bridge port the plan assigns to the worker.
    rpc_endpoint: SocketAddr,
    /// Loopback port the worker's fake ggml-rpc-server listens on.
    worker_rpc_port: u16,
    /// Where fake_llama_server records its argv and PID.
    argv_log: PathBuf,
    /// Exactly what the leader must pass to llama-server.
    expected_argv: Vec<String>,
}

impl Cluster {
    /// Builds the cluster; `fake_flags` reach fake_llama_server as extra llama-server arguments,
    /// so the plan stays exactly what `planning::build_plan` produced.
    async fn new(name: &str, fake_flags: &[&str]) -> Self {
        init_tracing();
        let dir = scratch_dir(name);
        let lookup = MemoryLookup::new();
        let worker_endpoint =
            bind_endpoint(vec![CONTROL_ALPN.to_vec(), RPC_BRIDGE_ALPN.to_vec()], None).await;
        let leader_endpoint =
            bind_endpoint(vec![CONTROL_ALPN.to_vec()], Some(lookup.clone())).await;
        lookup.add_endpoint_info(direct_addr(&worker_endpoint));
        let rpc_endpoint = free_loopback_addr().await;
        let worker_rpc_port = free_loopback_addr().await.port();
        let llama_port = free_loopback_addr().await.port();
        let api_addr = free_loopback_addr().await;
        let argv_log = dir.join("llama-argv.json");
        let extra_llama_args: Vec<String> = ["--argv-log", &path_arg(&argv_log)]
            .into_iter()
            .chain(fake_flags.iter().copied())
            .map(str::to_owned)
            .collect();
        let launch = write_launch_files(&dir, worker_endpoint.id(), rpc_endpoint);
        let expected_argv = [
            launch.server_args,
            extra_llama_args.clone(),
            vec![
                "--host".to_owned(),
                Ipv4Addr::LOCALHOST.to_string(),
                "--port".to_owned(),
                llama_port.to_string(),
            ],
        ]
        .concat();

        let config = LeaderConfig {
            key_path: dir.join("leader.key"),
            cluster_id: CLUSTER_ID.to_owned(),
            backend: BackendConfig::Llama(LlamaBackendConfig {
                plan_path: launch.plan_path,
                profile_path: launch.profile_path,
                min_peers: MIN_PEERS,
                evidence: None,
                llama_server: example_bin("fake_llama_server"),
                extra_llama_args,
                llama_ready_timeout: LLAMA_READY_TIMEOUT,
            }),
            upstream_port: llama_port,
            api_addr,
            worker_wait: WORKER_WAIT,
            heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
            heartbeat_interval: Some(HEARTBEAT_INTERVAL),
        };
        Self {
            dir,
            config,
            leader_endpoint,
            worker_endpoint,
            rpc_endpoint,
            worker_rpc_port,
            argv_log,
            expected_argv,
        }
    }

    fn llama_addr(&self) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.config.upstream_port))
    }

    /// The llama backend settings of the leader's configuration.
    fn llama(&mut self) -> &mut LlamaBackendConfig {
        let BackendConfig::Llama(llama) = &mut self.config.backend else {
            panic!("the leader tests run the llama backend");
        };
        llama
    }

    /// The plan file the leader loads.
    fn plan_path(&mut self) -> PathBuf {
        self.llama().plan_path.clone()
    }

    /// Turns on post-run verification and returns the file the test must write evidence to.
    fn expect_evidence(&mut self) -> PathBuf {
        let path = self.dir.join("evidence.json");
        self.llama().evidence = Some(EvidenceConfig {
            path: path.clone(),
            wait: TEST_TIMEOUT,
        });
        path
    }

    /// Asserts that neither llama-server nor the job API was ever started.
    async fn assert_nothing_launched(&self) {
        assert!(
            !self.argv_log.exists(),
            "llama-server must not start for a rejected plan"
        );
        assert!(
            TcpStream::connect(self.config.api_addr).await.is_err(),
            "the job API must not serve for a rejected plan"
        );
    }

    /// Runs the leader in the background until the returned sender fires (or is dropped).
    fn start_leader(&self) -> (oneshot::Sender<()>, JoinHandle<anyhow::Result<()>>) {
        let (stop, stopped) = oneshot::channel::<()>();
        let endpoint = self.leader_endpoint.clone();
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

    /// Runs the worker in the background, dialling the leader's direct address.
    fn start_worker(&self) -> JoinHandle<anyhow::Result<()>> {
        let endpoint = self.worker_endpoint.clone();
        let leader = direct_addr(&self.leader_endpoint);
        let config = WorkerConfig {
            key_path: self.dir.join("worker.key"),
            cluster_id: CLUSTER_ID.to_owned(),
            rpc_server: example_bin("fake_rpc_server"),
            rpc_port: self.worker_rpc_port,
            rpc_args: Vec::new(),
            leader: None,
            allowed_peers: Vec::new(),
            heartbeat_interval: HEARTBEAT_INTERVAL,
            leader_heartbeat_timeout: Some(HEARTBEAT_TIMEOUT),
        };
        tokio::spawn(async move { run_worker_on(endpoint, Some(leader), &config).await })
    }

    /// Cancels the worker (which stops its child) and removes the scratch directory.
    async fn finish(self, worker: Option<JoinHandle<anyhow::Result<()>>>) {
        if let Some(worker) = worker {
            worker.abort();
            // Cancellation is the expected outcome; a worker error would already have surfaced.
            let _ = worker.await;
        }
        self.worker_endpoint.close().await;
        self.leader_endpoint.close().await;
        // Best effort: a leftover temp directory must not fail a passing test.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Plan and profile files written for one test, plus the plan's llama-server arguments.
struct LaunchFiles {
    plan_path: PathBuf,
    profile_path: PathBuf,
    server_args: Vec<String>,
}

/// Plans the test profile with `planning::build_plan` and writes both files into `dir`.
fn write_launch_files(dir: &Path, worker: EndpointId, rpc_endpoint: SocketAddr) -> LaunchFiles {
    let profile = profile_json(worker, rpc_endpoint);
    let request: PlanningRequest = serde_json::from_value(profile.clone()).expect("valid profile");
    let plan = planning::build_plan(&request, MIN_PEERS).expect("feasible plan");
    let plan_path = dir.join("plan.json");
    let profile_path = dir.join("profile.json");
    write_json(&plan_path, &serde_json::to_value(&plan).expect("plan JSON"));
    write_json(&profile_path, &profile);
    LaunchFiles {
        plan_path,
        profile_path,
        server_args: plan.server_args,
    }
}

/// The planning profile from `examples/placement-profile.json`, with the worker's real node ID
/// and a free bridge port.
fn profile_json(worker: EndpointId, rpc_endpoint: SocketAddr) -> Value {
    let worker = worker.to_string();
    let peer = |id: &str| {
        json!({
            "id": id,
            "model_hash": MODEL_HASH,
            "runtime_id": RUNTIME_ID,
            "context_tokens": CONTEXT_TOKENS,
            "authorized": true,
            "healthy": true,
            "memory_budget_bytes": PEER_MEMORY_BUDGET_BYTES,
            "reserved_bytes": 50_000_000u64,
            "decode_us_per_layer": [1000, 1000, 1000, 1000],
        })
    };
    let layer = json!({"weight_bytes": 100_000_000u64, "kv_bytes_per_token": 1024});
    json!({
        "model": {
            "model_hash": MODEL_HASH,
            "runtime_id": RUNTIME_ID,
            "context_tokens": CONTEXT_TOKENS,
            "layers": [layer, layer, layer, layer],
        },
        "peers": [peer(&worker), peer(LEADER_PEER_ID)],
        "links": [{"from": worker, "to": LEADER_PEER_ID, "transfer_us": 500}],
        "devices": [
            {"peer_id": LEADER_PEER_ID, "device_name": "MTL0", "rpc_endpoint": null},
            {"peer_id": worker, "device_name": "RPC0", "rpc_endpoint": rpc_endpoint.to_string()},
        ],
        "model_path": "/path/to/verified.gguf",
    })
}

fn init_tracing() {
    // Another test in this binary may already have installed the subscriber; that is fine.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("meshthing-leader-{}-{name}", std::process::id()));
    // A leftover directory from an aborted run may or may not exist; either way we start fresh.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn path_arg(path: &Path) -> String {
    path.to_str().expect("UTF-8 temp path").to_owned()
}

fn write_json(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).expect("encode JSON"))
        .expect("write JSON");
}

/// Rewrites the JSON file at `path` with `edit` applied.
fn edit_json(path: &Path, edit: impl FnOnce(&mut Value)) {
    let text = std::fs::read_to_string(path).expect("read JSON");
    let mut value = serde_json::from_str(&text).expect("parse JSON");
    edit(&mut value);
    write_json(path, &value);
}

/// RunEvidence for the test plan: both planned peers within their memory budget, the leader's
/// device with one compute event and the worker with `worker_compute_events`.
fn run_evidence(worker: EndpointId, worker_compute_events: u64) -> Value {
    let device = |peer_id: String, compute_events: u64| {
        json!({
            "peer_id": peer_id,
            "peak_bytes": EVIDENCE_PEAK_BYTES,
            "compute_events": compute_events,
        })
    };
    json!({
        "model_hash": MODEL_HASH,
        "runtime_id": RUNTIME_ID,
        "context_tokens": CONTEXT_TOKENS,
        "devices": [
            device(worker.to_string(), worker_compute_events),
            device(LEADER_PEER_ID.to_owned(), 1),
        ],
    })
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

/// The endpoint's id plus its bound loopback sockets, so peers dial it directly.
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

async fn assert_stops_listening(addr: SocketAddr) {
    timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{addr} still accepts connections after shutdown"));
}

/// Sends `payload` through the bridge listening on `addr` and reads the echo back.
async fn bridge_round_trip(addr: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect to bridge");
    stream.write_all(payload).await.expect("send payload");
    let mut echoed = vec![0; payload.len()];
    stream.read_exact(&mut echoed).await.expect("read echo");
    echoed
}

/// The `{"pid", "args"}` record fake_llama_server writes at startup.
fn read_argv_log(path: &Path) -> Value {
    let text = std::fs::read_to_string(path).expect("fake_llama_server wrote its argv log");
    serde_json::from_str(&text).expect("argv log is JSON")
}

/// Whether a process with this PID exists, asked of the OS's own process tool.
fn process_is_running(pid: u64) -> bool {
    if cfg!(windows) {
        let output = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output()
            .expect("run tasklist");
        String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
    } else {
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .output()
            .expect("run kill -0")
            .status
            .success()
    }
}

async fn assert_process_exits(pid: u64) {
    timeout(TEST_TIMEOUT, async {
        while process_is_running(pid) {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("llama-server (pid {pid}) outlived the leader"));
}

async fn leader_error(leader: JoinHandle<anyhow::Result<()>>) -> String {
    leader_error_within(TEST_TIMEOUT, leader).await
}

async fn leader_error_within(limit: Duration, leader: JoinHandle<anyhow::Result<()>>) -> String {
    let result = timeout(limit, leader)
        .await
        .unwrap_or_else(|_| panic!("leader did not finish within {limit:?}"))
        .expect("leader did not panic");
    format!("{:#}", result.expect_err("leader must fail"))
}

async fn assert_prompt_succeeds(api_addr: SocketAddr) {
    let reply = timeout(TEST_TIMEOUT, api::prompt(api_addr, PROMPT))
        .await
        .expect("prompt finishes")
        .expect("prompt succeeds");
    assert!(reply.contains(PROMPT), "unexpected reply {reply:?}");
}

async fn stop_cleanly(stop: oneshot::Sender<()>, leader: JoinHandle<anyhow::Result<()>>) {
    stop.send(()).expect("leader still running");
    timeout(TEST_TIMEOUT, leader)
        .await
        .expect("leader stops")
        .expect("leader did not panic")
        .expect("leader shuts down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_plan_is_rejected_before_anything_launches() {
    let mut cluster = Cluster::new("legacy-plan", &[]).await;
    edit_json(&cluster.plan_path(), |plan| {
        plan["runtime_devices"] = json!([]);
    });

    // No worker is started: a leader that got past preflight would wait WORKER_WAIT for it.
    let (_stop, leader) = cluster.start_leader();
    let message = leader_error_within(PREFLIGHT_TIMEOUT, leader).await;

    assert!(message.contains("regenerate the plan"), "{message}");
    cluster.assert_nothing_launched().await;
    cluster.finish(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_failing_validation_is_rejected_before_anything_launches() {
    let mut cluster = Cluster::new("invalid-plan", &[]).await;
    edit_json(&cluster.plan_path(), |plan| {
        plan["placement"]["model_hash"] = json!("some-other-model");
    });

    let (_stop, leader) = cluster.start_leader();
    let message = leader_error_within(PREFLIGHT_TIMEOUT, leader).await;

    assert!(message.contains("IdentityMismatch"), "{message}");
    cluster.assert_nothing_launched().await;
    cluster.finish(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_run_evidence_keeps_the_leader_serving() {
    let mut cluster = Cluster::new("evidence-valid", &[]).await;
    let evidence_path = cluster.expect_evidence();

    let (stop, leader) = cluster.start_leader();
    let worker = cluster.start_worker();
    wait_until_listening(cluster.config.api_addr).await;
    write_json(
        &evidence_path,
        &run_evidence(cluster.worker_endpoint.id(), 1),
    );
    tokio::time::sleep(EVIDENCE_SETTLE).await;

    assert!(
        !leader.is_finished(),
        "leader must keep serving after verification passes"
    );
    assert_prompt_succeeds(cluster.config.api_addr).await;
    stop_cleanly(stop, leader).await;
    cluster.finish(Some(worker)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_verification_shuts_the_leader_down() {
    let mut cluster = Cluster::new("evidence-invalid", &[]).await;
    let evidence_path = cluster.expect_evidence();
    let worker_id = cluster.worker_endpoint.id().to_string();

    let (_stop, leader) = cluster.start_leader();
    let worker = cluster.start_worker();
    wait_until_listening(cluster.config.api_addr).await;
    let launch = read_argv_log(&cluster.argv_log);
    write_json(
        &evidence_path,
        &run_evidence(cluster.worker_endpoint.id(), 0),
    );
    let message = leader_error(leader).await;

    assert!(message.contains("NoCompute"), "{message}");
    assert!(message.contains(&worker_id), "{message}");
    assert_process_exits(launch["pid"].as_u64().expect("pid in argv log")).await;
    assert_stops_listening(cluster.config.api_addr).await;
    assert_stops_listening(cluster.llama_addr()).await;
    assert_stops_listening(cluster.rpc_endpoint).await;
    cluster.finish(Some(worker)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_names_the_planned_worker_that_never_connects() {
    let mut cluster = Cluster::new("missing-worker", &[]).await;
    cluster.config.worker_wait = SHORT_WORKER_WAIT;

    let (_stop, leader) = cluster.start_leader();
    let message = leader_error(leader).await;

    assert!(
        message.contains(&cluster.worker_endpoint.id().to_string()),
        "{message}"
    );
    assert!(
        !cluster.argv_log.exists(),
        "llama-server must not start without its workers"
    );
    cluster.finish(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_serves_prompts_over_planned_bridges_and_stops_cleanly() {
    let cluster = Cluster::new("end-to-end", &[]).await;

    let (stop, leader) = cluster.start_leader();
    tokio::time::sleep(WORKER_JOIN_DELAY).await;
    assert!(
        !leader.is_finished(),
        "leader must keep waiting for its worker"
    );
    assert!(
        !cluster.argv_log.exists(),
        "llama-server must not start before the planned worker connects"
    );
    let worker = cluster.start_worker();
    wait_until_listening(cluster.config.api_addr).await;

    let launch = read_argv_log(&cluster.argv_log);
    assert_eq!(launch["args"], json!(cluster.expected_argv));
    let echoed = timeout(
        TEST_TIMEOUT,
        bridge_round_trip(cluster.rpc_endpoint, BRIDGE_PAYLOAD),
    )
    .await
    .expect("bridge round trip finishes");
    assert_eq!(echoed, BRIDGE_PAYLOAD);
    // No evidence file is configured, so no verification runs and serving is unchanged.
    assert_prompt_succeeds(cluster.config.api_addr).await;

    stop_cleanly(stop, leader).await;
    assert_process_exits(launch["pid"].as_u64().expect("pid in argv log")).await;
    assert_stops_listening(cluster.config.api_addr).await;
    assert_stops_listening(cluster.llama_addr()).await;
    assert_stops_listening(cluster.rpc_endpoint).await;
    cluster.finish(Some(worker)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_readiness_probe_keeps_the_api_down() {
    let cluster = Cluster::new("probe-fails", &["--fail-completions"]).await;

    let (_stop, leader) = cluster.start_leader();
    let worker = cluster.start_worker();
    let message = leader_error(leader).await;

    assert!(message.contains("readiness probe"), "{message}");
    assert!(
        TcpStream::connect(cluster.config.api_addr).await.is_err(),
        "API must not serve when the probe failed"
    );
    let launch = read_argv_log(&cluster.argv_log);
    assert_process_exits(launch["pid"].as_u64().expect("pid in argv log")).await;
    cluster.finish(Some(worker)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_health_check_stops_startup() {
    let mut cluster = Cluster::new("health-fails", &["--fail-health"]).await;
    cluster.llama().llama_ready_timeout = SHORT_LLAMA_READY_TIMEOUT;

    let (_stop, leader) = cluster.start_leader();
    let worker = cluster.start_worker();
    leader_error(leader).await;

    assert!(
        TcpStream::connect(cluster.config.api_addr).await.is_err(),
        "API must not serve when llama-server never became healthy"
    );
    assert_stops_listening(cluster.llama_addr()).await;
    cluster.finish(Some(worker)).await;
}
