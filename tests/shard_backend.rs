//! End-to-end tests of the leader's shard backend on loopback.
//!
//! A real `shard-serve` worker (`serve_shard` driving the `fake_shard_runner` example) serves
//! the prefix `[0, 2)` of the fake model; the leader runs `run_leader_on` with the shard backend
//! and a manifest of that remote prefix plus a local final stage `[2, 4)`. Prompts go through
//! the job API exactly as `llm-cluster prompt` sends them. Endpoints run without relays and
//! dial each other through in-memory address lookup, so no internet is needed.

use std::collections::{BTreeMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey};
use llm_cluster::api;
use llm_cluster::leader::{BackendConfig, LeaderConfig, ShardBackendConfig, run_leader_on};
use llm_cluster::protocol::{CONTROL_ALPN, RPC_BRIDGE_ALPN};
use llm_cluster::rpc_bridge::PeerAllowlist;
use llm_cluster::shard::manifest::sha256_file;
use llm_cluster::shard::serve::{ShardServeConfig, serve_shard};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod common;
use common::example_bin;

const CLUSTER_ID: &str = "shard-backend-test";
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
/// `source_sha256` carried by `examples/fake-shard-model.json`: the "original model" identity.
const FIXTURE_SHA: &str = "5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";
/// Layers in the fixture model, and where the remote prefix hands over to the local final stage.
const LAYERS: u32 = 4;
const CUT: u32 = 2;
const CONTEXT: u32 = 512;
const MAX_TOKENS: u32 = 256;
const BUDGET_BYTES: u64 = 1 << 20;
/// Makes the fake runner log every request header it receives, one line each.
const LOG_ENV: &str = "FAKE_SHARD_LOG";
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const WORKER_WAIT: Duration = Duration::from_secs(30);
/// Short enough for the unapproved-leader test, long enough for a few probe retries.
const SHORT_WORKER_WAIT: Duration = Duration::from_secs(1);
/// A rejected manifest must fail well within this, far below any probe wait.
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const SSE_DATA_PREFIX: &str = "data:";
const SSE_DONE: &str = "[DONE]";
const PROMPTS: [&str; 2] = ["hello through the shard backend", "and once more"];

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
    let dir = std::env::temp_dir().join(format!(
        "meshthing-shard-backend-{}-{name}",
        std::process::id()
    ));
    // A leftover directory from an aborted run may or may not exist; either way we start fresh.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
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

fn remote_stage(begin: u32, end: u32, worker: EndpointId) -> Value {
    json!({"begin": begin, "end": end, "remote": {"worker": worker.to_string()}})
}

fn local_stage(begin: u32, end: u32, env: &BTreeMap<&str, String>) -> Value {
    json!({"begin": begin, "end": end, "local": {
        "model": fixture_model(), "budget_bytes": BUDGET_BYTES, "env": env,
    }})
}

/// Writes a manifest over `stages` into `dir` and returns its path.
fn write_manifest(dir: &Path, stages: Vec<Value>) -> PathBuf {
    let manifest = json!({
        "runner": example_bin("fake_shard_runner"),
        "model_sha256": FIXTURE_SHA,
        "layers": LAYERS,
        "context": CONTEXT,
        "max_tokens": MAX_TOKENS,
        "chat_template": "chatml",
        "stages": stages,
    });
    let path = dir.join("manifest.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&manifest).expect("encode manifest"),
    )
    .expect("write manifest");
    path
}

/// A `shard-serve` worker for the prefix `[0, CUT)` of the fixture model.
struct Worker {
    endpoint: Endpoint,
    task: JoinHandle<anyhow::Result<()>>,
}

/// Binds a worker's endpoint, before its allowlist is known.
async fn bind_worker() -> Endpoint {
    bind_endpoint(vec![RPC_BRIDGE_ALPN.to_vec()], None).await
}

impl Worker {
    /// Serves the prefix on `endpoint` to the peers in `approved` only.
    fn serve(endpoint: Endpoint, approved: &[EndpointId]) -> Self {
        let model = fixture_model();
        let config = ShardServeConfig {
            runner: example_bin("fake_shard_runner"),
            expected_sha256: sha256_file(&model).expect("hash model fixture"),
            model,
            begin: 0,
            end: CUT,
            context: CONTEXT,
            gpu_layers: 0,
            budget_bytes: BUDGET_BYTES,
            source_sha256: Some(FIXTURE_SHA.to_owned()),
        };
        let allowed = PeerAllowlist::new(approved.iter().copied()).expect("allowlist");
        let task = tokio::spawn(serve_shard(endpoint.clone(), config, allowed));
        Self { endpoint, task }
    }

    async fn stop(self) {
        self.endpoint.close().await;
        // Closing the endpoint ends serve_shard; a late error would not change the verdict.
        let _ = timeout(TEST_TIMEOUT, self.task).await;
    }
}

/// A leader on the shard backend, with its scratch directory.
struct Leader {
    dir: PathBuf,
    endpoint: Endpoint,
    config: LeaderConfig,
    /// Where the local final stage's runners log the request headers they receive.
    runner_log: PathBuf,
}

impl Leader {
    /// Binds the leader's endpoint; `workers` become dialable by node ID.
    async fn bind(name: &str, workers: &[&Endpoint]) -> Self {
        init_tracing();
        let dir = scratch_dir(name);
        let lookup = MemoryLookup::new();
        for worker in workers {
            lookup.add_endpoint_info(direct_addr(worker));
        }
        let endpoint = bind_endpoint(vec![CONTROL_ALPN.to_vec()], Some(lookup)).await;
        let config = LeaderConfig {
            key_path: dir.join("leader.key"),
            cluster_id: CLUSTER_ID.to_owned(),
            backend: BackendConfig::Shard(ShardBackendConfig {
                manifest_path: dir.join("manifest.json"),
            }),
            upstream_port: free_loopback_addr().await.port(),
            api_addr: free_loopback_addr().await,
            worker_wait: WORKER_WAIT,
            heartbeat_timeout: None,
            heartbeat_interval: None,
            replication: None,
            quorum: None,
        };
        Self {
            runner_log: dir.join("final-stage.log"),
            dir,
            endpoint,
            config,
        }
    }

    /// Writes the v1 manifest: the remote prefix on `worker`, then the local final stage.
    fn write_prefix_manifest(&self, worker: EndpointId) {
        let env = BTreeMap::from([(LOG_ENV, self.runner_log.to_string_lossy().into_owned())]);
        write_manifest(
            &self.dir,
            vec![remote_stage(0, CUT, worker), local_stage(CUT, LAYERS, &env)],
        );
    }

    fn upstream_addr(&self) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.config.upstream_port))
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

    /// Asserts that neither the shard front end nor the job API listens.
    async fn assert_nothing_serves(&self) {
        assert!(
            TcpStream::connect(self.config.api_addr).await.is_err(),
            "the job API must not serve"
        );
        assert!(
            TcpStream::connect(self.upstream_addr()).await.is_err(),
            "the shard front end must not serve"
        );
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

async fn assert_stops_listening(addr: SocketAddr) {
    timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{addr} still accepts connections after shutdown"));
}

async fn leader_error_within(limit: Duration, leader: JoinHandle<anyhow::Result<()>>) -> String {
    let result = timeout(limit, leader)
        .await
        .unwrap_or_else(|_| panic!("leader did not finish within {limit:?}"))
        .expect("leader did not panic");
    format!("{:#}", result.expect_err("leader must fail"))
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

/// Sends a streamed chat request through the job API and returns the raw SSE body.
async fn stream_prompt(api_addr: SocketAddr, text: &str) -> String {
    let request = json!({
        "stream": true,
        "messages": [{"role": "user", "content": text}],
    });
    let response = timeout(
        TEST_TIMEOUT,
        reqwest::Client::new()
            .post(format!("http://{api_addr}{}", api::CHAT_COMPLETIONS_PATH))
            .json(&request)
            .send(),
    )
    .await
    .expect("streamed request timed out")
    .expect("streamed request");
    assert!(response.status().is_success(), "{}", response.status());
    timeout(TEST_TIMEOUT, response.text())
        .await
        .expect("streamed body timed out")
        .expect("streamed body")
}

/// The `data:` payloads of an SSE body, in order.
fn sse_payloads(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|line| line.strip_prefix(SSE_DATA_PREFIX))
        .map(|data| data.trim().to_owned())
        .collect()
}

/// The text of a streamed reply, which must end with `[DONE]`.
fn streamed_text(body: &str) -> String {
    let payloads = sse_payloads(body);
    assert_eq!(
        payloads.last().map(String::as_str),
        Some(SSE_DONE),
        "{body}"
    );
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

/// Process IDs of the runners that received a first step, one line each in the runner log.
fn first_step_pids(log: &Path) -> Vec<String> {
    let lines = std::fs::read_to_string(log).expect("read runner log");
    lines
        .lines()
        .filter(|line| line.contains(" step=0 "))
        .filter_map(|line| line.split_whitespace().next().map(str::to_owned))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_api_serves_prompts_through_shard_serve_and_stops_cleanly() {
    // The worker allowlists the leader's ID and the leader dials the worker's; bind both first.
    let worker_endpoint = bind_worker().await;
    let leader = Leader::bind("end-to-end", &[&worker_endpoint]).await;
    let worker = Worker::serve(worker_endpoint, &[leader.endpoint.id()]);
    leader.write_prefix_manifest(worker.endpoint.id());

    let (stop, running) = leader.start();
    wait_until_listening(leader.config.api_addr).await;
    let api_addr = leader.config.api_addr;

    assert_eq!(prompt(api_addr, PROMPTS[0]).await, fixture_reply());
    let body = stream_prompt(api_addr, PROMPTS[0]).await;
    assert_eq!(streamed_text(&body), fixture_reply());
    assert_eq!(prompt(api_addr, PROMPTS[1]).await, fixture_reply());

    assert_eq!(job_statuses(api_addr).await, ["completed"; 3]);
    // The readiness probe and every job each ran their own final-stage runner.
    let pids = first_step_pids(&leader.runner_log);
    let distinct: HashSet<&String> = pids.iter().collect();
    assert_eq!(distinct.len(), pids.len(), "a runner was reused: {pids:?}");
    assert!(pids.len() >= 4, "probe plus three jobs, got {pids:?}");

    stop_cleanly(stop, running).await;
    assert_stops_listening(api_addr).await;
    assert_stops_listening(leader.upstream_addr()).await;
    worker.stop().await;
    leader.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_the_worker_does_not_allowlist_fails_startup_with_the_probe_error() {
    let someone_else = SecretKey::generate().public();
    let worker = Worker::serve(bind_worker().await, &[someone_else]);
    let mut leader = Leader::bind("unapproved", &[&worker.endpoint]).await;
    leader.config.worker_wait = SHORT_WORKER_WAIT;
    leader.write_prefix_manifest(worker.endpoint.id());

    let (_stop, running) = leader.start();
    let message = leader_error_within(TEST_TIMEOUT, running).await;

    assert!(message.contains("did not become ready"), "{message}");
    assert!(message.contains("readiness probe"), "{message}");
    let stage = format!("remote stage on {}", worker.endpoint.id().fmt_short());
    assert!(message.contains(&stage), "{message}");
    assert!(message.contains("not approved"), "{message}");
    leader.assert_nothing_serves().await;
    worker.stop().await;
    leader.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_manifest_fails_before_anything_is_bound() {
    let leader = Leader::bind("invalid-manifest", &[]).await;
    let first = SecretKey::generate().public();
    let second = SecretKey::generate().public();
    write_manifest(
        &leader.dir,
        vec![
            remote_stage(0, 1, first),
            remote_stage(1, CUT, second),
            local_stage(CUT, LAYERS, &BTreeMap::new()),
        ],
    );

    let (_stop, running) = leader.start();
    let message = leader_error_within(PREFLIGHT_TIMEOUT, running).await;

    assert!(message.contains("invalid shard manifest"), "{message}");
    assert!(message.contains("at most one remote stage"), "{message}");
    assert!(message.contains("M-27"), "{message}");
    leader.assert_nothing_serves().await;
    leader.finish().await;
}
