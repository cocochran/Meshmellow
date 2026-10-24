//! Leader (main) process: plan, worker bridges, llama-server, and the job API.
//!
//! [`run_leader_process`] turns a saved placement plan into a running distributed `llama-server`:
//!
//! 1. Load the plan (`llm-cluster plan` output) and the planning profile it came from, and work
//!    out which planned peers are remote workers and which leader-local port bridges to each.
//! 2. Accept control connections and wait until every remote worker has said hello.
//! 3. Run one RPC bridge per remote worker, all sharing the leader's endpoint.
//! 4. Launch `llama-server` with the plan's `server_args`, wait for `/health`, then send one real
//!    one-token generation: a stale RPC backend can report healthy and still abort on the first
//!    request, so `/health` alone does not prove the cluster works.
//! 5. Serve the job API until shutdown, then stop the API, `llama-server`, the bridges and the
//!    endpoint, in that order.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use iroh::{Endpoint, EndpointId};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::api::{self, ApiConfig};
use crate::history::LoggingHistorySink;
use crate::jobs::JobQueue;
use crate::network::{self, SharedPeerTable};
use crate::node::PeerTable;
use crate::planning::{PlanningOutput, PlanningRequest};
use crate::process::{self, ChildSpec, Readiness, RestartPolicy, Supervised};
use crate::protocol::CONTROL_ALPN;
use crate::rpc_bridge;

/// Loopback port `llama-server` listens on unless configured otherwise.
pub const DEFAULT_LLAMA_PORT: u16 = 8081;

/// Address the job API listens on unless configured otherwise.
pub const DEFAULT_API_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);

/// How long to wait for every planned worker to connect unless configured otherwise.
pub const DEFAULT_WORKER_WAIT: Duration = Duration::from_secs(60);

/// How long `llama-server` may take to load the model and answer `/health` unless configured otherwise.
pub const DEFAULT_LLAMA_READY_TIMEOUT: Duration = Duration::from_secs(300);

/// Log label for the supervised `llama-server` child.
const LLAMA_SERVER_NAME: &str = "llama-server";

/// `llama-server` endpoint that answers 200 once the model is loaded.
const HEALTH_PATH: &str = "/health";

/// OpenAI-compatible endpoint used by the readiness probe.
const CHAT_COMPLETIONS_PATH: &str = "/v1/chat/completions";

/// Prompt sent by the readiness probe; its content does not matter.
const PROBE_PROMPT: &str = "ping";

/// The probe only needs to prove that one token can be generated across the cluster.
const PROBE_MAX_TOKENS: u32 = 1;

/// Upper bound on the probe's single generation, which may cross slow links on a cold model.
const PROBE_TIMEOUT: Duration = Duration::from_secs(300);

/// Longest error body from `llama-server` quoted in a probe failure.
const PROBE_ERROR_BODY_CHARS: usize = 512;

/// How often the worker wait re-checks the peer table.
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long a freshly spawned RPC bridge may take to start listening.
const BRIDGE_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How often to check whether an RPC bridge listens yet.
const BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How long the job API may take to finish in-flight requests at shutdown before it is aborted.
const API_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Last log index announced in hellos until the replicated log exists.
const EMPTY_LOG_INDEX: u64 = 0;

/// Everything `llm-cluster leader` needs to run.
#[derive(Clone, Debug)]
pub struct LeaderConfig {
    /// File holding this node's secret key; created on first run.
    pub key_path: PathBuf,
    /// Cluster ID announced in hellos; workers from other clusters are rejected.
    pub cluster_id: String,
    /// `planning::PlanningOutput` JSON written by `llm-cluster plan`.
    pub plan_path: PathBuf,
    /// `planning::PlanningRequest` JSON the plan was built from; its `devices[].rpc_endpoint`
    /// gives the leader-local bridge port for each remote peer.
    pub profile_path: PathBuf,
    /// `llama-server` binary.
    pub llama_server: PathBuf,
    /// Internal loopback port for `llama-server`.
    pub llama_port: u16,
    /// Loopback address the job API listens on.
    pub api_addr: SocketAddr,
    /// How long to wait for every planned worker to connect.
    pub worker_wait: Duration,
    /// How long `llama-server` may take to answer `/health`.
    pub llama_ready_timeout: Duration,
}

/// Runs the leader on a freshly bound endpoint until Ctrl-C, then shuts everything down in order.
///
/// Prints `node id: <id>` so workers can be started with `--leader <id>`.
pub async fn run_leader_process(config: LeaderConfig) -> anyhow::Result<()> {
    let endpoint = network::bind_endpoint(&config.key_path, vec![CONTROL_ALPN.to_vec()]).await?;
    println!("node id: {}", endpoint.id());
    run_leader_on(endpoint, &config, ctrl_c()).await
}

/// Runs the leader on `endpoint` until `shutdown` resolves or a component fails, then stops the
/// API, `llama-server`, the bridges and finally closes `endpoint`.
///
/// `endpoint` must accept [`CONTROL_ALPN`] and be able to dial workers by node ID.
/// `config.key_path` is ignored; the endpoint already carries the identity.
pub async fn run_leader_on(
    endpoint: Endpoint,
    config: &LeaderConfig,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let result = lead(&endpoint, config, shutdown).await;
    endpoint.close().await;
    result
}

/// Resolves on Ctrl-C; if the signal handler cannot be installed, logs it and never resolves.
async fn ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "cannot listen for Ctrl-C; stop the leader another way");
        std::future::pending::<()>().await;
    }
}

/// Body of [`run_leader_on`]; the caller closes the endpoint afterwards.
async fn lead(
    endpoint: &Endpoint,
    config: &LeaderConfig,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let launch = LaunchPlan::load(&config.plan_path, &config.profile_path)?;
    let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
    let local = network::local_hello(endpoint, &config.cluster_id, EMPTY_LOG_INDEX);
    // Dropping the set aborts the accept loop, so it never outlives this function.
    let mut control = JoinSet::new();
    control.spawn(network::serve_control(
        endpoint.clone(),
        local,
        Arc::clone(&peers),
    ));

    tokio::pin!(shutdown);
    let running = tokio::select! {
        started = start(endpoint, config, &launch, &peers) => started?,
        () = &mut shutdown => {
            info!("shutdown requested during startup");
            return Ok(());
        }
    };
    running.serve_until(shutdown).await
}

/// What the leader launches, derived from the plan and profile files.
#[derive(Debug)]
struct LaunchPlan {
    server_args: Vec<String>,
    workers: Vec<RemoteWorker>,
}

/// A planned peer that runs on another machine and is reached through an RPC bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RemoteWorker {
    /// The worker's Iroh node ID, which is its `peer_id` in the plan.
    id: EndpointId,
    /// Leader-local loopback port `llama-server` reaches the worker's RPC server on.
    rpc_endpoint: SocketAddr,
}

impl LaunchPlan {
    fn load(plan_path: &Path, profile_path: &Path) -> anyhow::Result<Self> {
        let plan: PlanningOutput = read_json(plan_path, "plan")?;
        let profile: PlanningRequest = read_json(profile_path, "profile")?;
        ensure!(
            !plan.server_args.is_empty(),
            "plan {} has no server_args",
            plan_path.display()
        );
        let workers = bridge_targets(&plan, &profile)?;
        Ok(Self {
            server_args: plan.server_args,
            workers,
        })
    }
}

fn read_json<T: DeserializeOwned>(path: &Path, what: &str) -> anyhow::Result<T> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {what} {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("invalid {what} JSON in {}", path.display()))
}

/// Maps every planned remote peer to the leader-local port its RPC bridge listens on.
///
/// A planned peer is remote when its profile device has an `rpc_endpoint`; peers without one run
/// on the leader itself. The whole mapping lives here so plans that carry their own runtime
/// devices (aarush-codex PR #8) can replace the profile lookup in one place.
fn bridge_targets(
    plan: &PlanningOutput,
    profile: &PlanningRequest,
) -> anyhow::Result<Vec<RemoteWorker>> {
    let mut workers = Vec::new();
    let mut unknown = Vec::new();
    for assignment in &plan.placement.assignments {
        let peer_id = assignment.peer_id.as_str();
        let device = profile
            .devices
            .iter()
            .find(|device| device.peer_id == peer_id);
        match device {
            None => unknown.push(peer_id),
            Some(device) => {
                if let Some(rpc_endpoint) = device.rpc_endpoint {
                    let id = network::parse_node_id(peer_id).with_context(|| {
                        format!("remote peer {peer_id:?} must be the worker's node ID")
                    })?;
                    workers.push(RemoteWorker { id, rpc_endpoint });
                }
            }
        }
    }
    ensure!(
        unknown.is_empty(),
        "plan assigns layers to peers missing from the profile's devices: {}",
        unknown.join(", ")
    );
    Ok(workers)
}

/// The leader once every component is up.
struct Running {
    api_stop: oneshot::Sender<()>,
    api: JoinSet<anyhow::Result<()>>,
    llama: Supervised,
    bridges: JoinSet<anyhow::Result<()>>,
}

/// Brings the components up in dependency order: workers, bridges, `llama-server`, probe, API.
async fn start(
    endpoint: &Endpoint,
    config: &LeaderConfig,
    launch: &LaunchPlan,
    peers: &SharedPeerTable,
) -> anyhow::Result<Running> {
    wait_for_workers(peers, &launch.workers, config.worker_wait).await?;
    let bridges = start_bridges(endpoint, &launch.workers).await?;
    let llama = process::spawn_supervised(llama_server_spec(config, &launch.server_args)).await?;
    if let Err(error) = probe_generation(llama_addr(config.llama_port)).await {
        if let Err(stop_error) = llama.shutdown().await {
            warn!("failed to stop llama-server: {stop_error:#}");
        }
        return Err(error);
    }
    let (api_stop, api) = start_api(config);
    Ok(Running {
        api_stop,
        api,
        llama,
        bridges,
    })
}

impl Running {
    /// Serves until `shutdown` resolves or a component stops, then stops everything in order.
    async fn serve_until(mut self, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
        let outcome = tokio::select! {
            () = shutdown => {
                info!("shutting down leader");
                Ok(())
            }
            exited = self.llama.wait() => Err(match exited {
                Ok(()) => anyhow!("llama-server exited"),
                Err(error) => error.context("llama-server stopped"),
            }),
            Some(joined) = self.api.join_next() => Err(task_ended("job API", joined)),
            Some(joined) = self.bridges.join_next() => Err(task_ended("rpc bridge", joined)),
        };
        let stopped = self.stop().await;
        outcome.and(stopped)
    }

    /// Stops the API (letting in-flight requests finish), then `llama-server`, then the bridges.
    async fn stop(mut self) -> anyhow::Result<()> {
        // An error only means the API already stopped, which is the state we want.
        let _ = self.api_stop.send(());
        let drained = tokio::time::timeout(API_STOP_TIMEOUT, async {
            while let Some(joined) = self.api.join_next().await {
                if let Err(error) = joined
                    .map_err(anyhow::Error::from)
                    .and_then(|served| served)
                {
                    warn!("job API stopped with an error: {error:#}");
                }
            }
        })
        .await;
        if drained.is_err() {
            warn!("job API did not stop in time; aborting in-flight requests");
            self.api.shutdown().await;
        }
        let llama = self.llama.shutdown().await;
        self.bridges.shutdown().await;
        llama
    }
}

/// Describes a background task that ended while the leader was serving.
fn task_ended(what: &str, joined: Result<anyhow::Result<()>, JoinError>) -> anyhow::Error {
    match joined {
        Ok(Ok(())) => anyhow!("{what} stopped unexpectedly"),
        Ok(Err(error)) => error.context(format!("{what} failed")),
        Err(join_error) => anyhow!("{what} task panicked: {join_error}"),
    }
}

/// Waits until every worker in `workers` has completed a hello, or fails after `wait` naming the missing ones.
async fn wait_for_workers(
    peers: &SharedPeerTable,
    workers: &[RemoteWorker],
    wait: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + wait;
    info!(
        workers = workers.len(),
        "waiting for planned workers to connect"
    );
    loop {
        let missing = missing_workers(peers, workers)?;
        if missing.is_empty() {
            info!("all planned workers connected");
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "planned workers did not connect within {wait:?}: {}",
                join_ids(&missing)
            );
        }
        tokio::time::sleep(WORKER_POLL_INTERVAL).await;
    }
}

/// Planned workers that have not completed a hello yet, in plan order.
fn missing_workers(
    peers: &SharedPeerTable,
    workers: &[RemoteWorker],
) -> anyhow::Result<Vec<EndpointId>> {
    let table = peers
        .lock()
        .map_err(|_| anyhow!("peer table lock poisoned"))?;
    let connected = table
        .peers()
        .filter_map(|peer| network::parse_node_id(&peer.node_id).ok())
        .collect::<Vec<_>>();
    Ok(workers
        .iter()
        .map(|worker| worker.id)
        .filter(|id| !connected.contains(id))
        .collect())
}

fn join_ids(ids: &[EndpointId]) -> String {
    ids.iter()
        .map(EndpointId::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Spawns one bridge per worker on the shared endpoint and waits until each one listens, so
/// `llama-server` finds every RPC port open when it starts.
async fn start_bridges(
    endpoint: &Endpoint,
    workers: &[RemoteWorker],
) -> anyhow::Result<JoinSet<anyhow::Result<()>>> {
    let mut bridges = JoinSet::new();
    for worker in workers {
        bridges.spawn(rpc_bridge::run_leader(
            endpoint.clone(),
            worker.id,
            worker.rpc_endpoint,
        ));
    }
    for worker in workers {
        wait_for_bridge(&mut bridges, worker.rpc_endpoint).await?;
    }
    Ok(bridges)
}

/// Polls `listen` until it accepts TCP, failing early if any bridge task has already ended.
async fn wait_for_bridge(
    bridges: &mut JoinSet<anyhow::Result<()>>,
    listen: SocketAddr,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + BRIDGE_READY_TIMEOUT;
    loop {
        if let Some(joined) = bridges.try_join_next() {
            return Err(task_ended("rpc bridge", joined));
        }
        if TcpStream::connect(listen).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("rpc bridge on {listen} did not start listening within {BRIDGE_READY_TIMEOUT:?}");
        }
        tokio::time::sleep(BRIDGE_POLL_INTERVAL).await;
    }
}

fn llama_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// `llama-server` argv: the plan's `server_args` unchanged, then the loopback host and port.
fn llama_server_args(server_args: &[String], port: u16) -> Vec<String> {
    let mut args = server_args.to_vec();
    args.extend([
        "--host".to_owned(),
        Ipv4Addr::LOCALHOST.to_string(),
        "--port".to_owned(),
        port.to_string(),
    ]);
    args
}

fn llama_server_spec(config: &LeaderConfig, server_args: &[String]) -> ChildSpec {
    ChildSpec {
        name: LLAMA_SERVER_NAME.to_owned(),
        program: config.llama_server.clone(),
        args: llama_server_args(server_args, config.llama_port),
        readiness: Readiness::HttpOk {
            addr: llama_addr(config.llama_port),
            path: HEALTH_PATH.to_owned(),
        },
        ready_timeout: config.llama_ready_timeout,
        restart: RestartPolicy::default(),
    }
}

/// Generates one token through `llama-server` at `upstream`; fails unless it answers 2xx with a choice.
async fn probe_generation(upstream: SocketAddr) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .context("readiness probe: failed to build HTTP client")?;
    let response = client
        .post(format!("http://{upstream}{CHAT_COMPLETIONS_PATH}"))
        .json(&probe_request())
        .send()
        .await
        .context("readiness probe: request to llama-server failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let body: String = body.chars().take(PROBE_ERROR_BODY_CHARS).collect();
        bail!("readiness probe: llama-server answered {status}: {body}");
    }
    let reply: Value = response
        .json()
        .await
        .context("readiness probe: llama-server reply is not JSON")?;
    let has_choice = reply
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| !choices.is_empty());
    ensure!(
        has_choice,
        "readiness probe: llama-server reply has no choices"
    );
    info!("readiness probe generated a token");
    Ok(())
}

fn probe_request() -> Value {
    json!({
        "messages": [{"role": "user", "content": PROBE_PROMPT}],
        "max_tokens": PROBE_MAX_TOKENS,
        "stream": false,
    })
}

/// Starts the job API in the background; sending on (or dropping) the returned sender stops it.
fn start_api(config: &LeaderConfig) -> (oneshot::Sender<()>, JoinSet<anyhow::Result<()>>) {
    let (stop, stopped) = oneshot::channel::<()>();
    let api_config = ApiConfig {
        listen: config.api_addr,
        upstream: llama_addr(config.llama_port),
    };
    let mut api = JoinSet::new();
    api.spawn(api::serve_api(
        api_config,
        Arc::new(JobQueue::new()),
        Arc::new(LoggingHistorySink),
        async move {
            // Resolves on an explicit stop or when the sender is dropped; both mean stop.
            let _ = stopped.await;
        },
    ));
    info!(api = %config.api_addr, "leader ready; serving the job API");
    (stop, api)
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;
    use crate::protocol::{Hello, PROTOCOL_VERSION};

    fn profile(devices: Value) -> PlanningRequest {
        serde_json::from_value(json!({
            "model": {"model_hash": "m", "runtime_id": "r", "context_tokens": 8, "layers": []},
            "peers": [],
            "links": [],
            "devices": devices,
            "model_path": "model.gguf",
        }))
        .expect("valid profile")
    }

    fn plan(peer_ids: &[&str]) -> PlanningOutput {
        let assignments = peer_ids
            .iter()
            .enumerate()
            .map(|(index, peer_id)| {
                json!({
                    "peer_id": peer_id,
                    "layers": {"start": index, "end": index + 1},
                    "estimated_bytes": 1,
                })
            })
            .collect::<Vec<_>>();
        serde_json::from_value(json!({
            "advisory": true,
            "placement": {
                "model_hash": "m",
                "runtime_id": "r",
                "context_tokens": 8,
                "assignments": assignments,
                "predicted_decode_us": 1,
            },
            "server_args": ["--model", "model.gguf"],
        }))
        .expect("valid plan")
    }

    fn device(peer_id: &str, rpc_endpoint: Option<&str>) -> Value {
        json!({"peer_id": peer_id, "device_name": peer_id, "rpc_endpoint": rpc_endpoint})
    }

    fn table_with(ids: &[EndpointId]) -> SharedPeerTable {
        let mut table = PeerTable::new();
        for id in ids {
            table.record_connection(&Hello {
                node_id: id.to_string(),
                protocol_version: PROTOCOL_VERSION,
                cluster_id: "test".to_owned(),
                last_log_index: 0,
            });
        }
        Arc::new(Mutex::new(table))
    }

    fn worker(id: EndpointId) -> RemoteWorker {
        RemoteWorker {
            id,
            rpc_endpoint: SocketAddr::from((Ipv4Addr::LOCALHOST, 50053)),
        }
    }

    #[test]
    fn remote_workers_are_planned_peers_with_a_bridge_port() {
        let remote = SecretKey::generate().public();
        let unplanned = SecretKey::generate().public();
        let devices = json!([
            device("leader", None),
            device(&remote.to_string(), Some("127.0.0.1:50053")),
            device(&unplanned.to_string(), Some("127.0.0.1:50054")),
        ]);

        let workers =
            bridge_targets(&plan(&["leader", &remote.to_string()]), &profile(devices)).unwrap();

        assert_eq!(workers, vec![worker(remote)]);
    }

    #[test]
    fn planned_peer_missing_from_profile_is_named() {
        let error = bridge_targets(
            &plan(&["leader", "ghost"]),
            &profile(json!([device("leader", None)])),
        )
        .unwrap_err();

        assert!(error.to_string().contains("ghost"), "{error}");
    }

    #[test]
    fn remote_peer_id_must_be_a_node_id() {
        let devices = json!([device("not-a-node-id", Some("127.0.0.1:50053"))]);

        let error = bridge_targets(&plan(&["not-a-node-id"]), &profile(devices)).unwrap_err();

        assert!(error.to_string().contains("not-a-node-id"), "{error}");
    }

    #[test]
    fn llama_server_args_append_loopback_host_and_port() {
        let plan_args = vec!["--rpc".to_owned(), "127.0.0.1:50053".to_owned()];

        let args = llama_server_args(&plan_args, 8081);

        assert_eq!(
            args,
            [
                "--rpc",
                "127.0.0.1:50053",
                "--host",
                "127.0.0.1",
                "--port",
                "8081"
            ]
        );
    }

    #[tokio::test]
    async fn worker_wait_names_every_missing_worker() {
        let connected = SecretKey::generate().public();
        let missing = SecretKey::generate().public();
        let workers = [worker(connected), worker(missing)];

        let error = wait_for_workers(&table_with(&[connected]), &workers, Duration::ZERO)
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains(&missing.to_string()), "{message}");
        assert!(!message.contains(&connected.to_string()), "{message}");
    }

    #[tokio::test]
    async fn worker_wait_succeeds_once_all_have_said_hello() {
        let id = SecretKey::generate().public();

        wait_for_workers(&table_with(&[id]), &[worker(id)], Duration::ZERO)
            .await
            .unwrap();
    }
}
