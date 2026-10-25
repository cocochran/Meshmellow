//! Leader (main) process: an execution backend, the job API in front of it, and ordered shutdown.
//!
//! [`run_leader_process`] serves the job API in front of one of two execution backends, chosen
//! by [`BackendConfig`]. `llama` turns a saved placement plan into a running distributed
//! `llama-server`. `shard` runs the shard runner pipeline described by a shard manifest
//! ([`crate::shard`], see `docs/shard-backend.md`). Either way the backend serves an
//! OpenAI-shaped HTTP upstream on the internal loopback port, and the job API, its queue,
//! history and shutdown are the same:
//!
//! 1. Load and validate the backend's input before any port is bound or process started.
//!    llama: the plan and the planning profile it came from ([`LaunchPlan::load`]); an invalid or
//!    legacy plan fails here. The plan's `runtime_devices` say which planned peers are remote
//!    workers and which leader-local port bridges to each. shard: the manifest
//!    ([`ShardManifest::load`]), including the v1 topology rules.
//! 2. Accept control connections, heartbeating to each connected worker every
//!    `heartbeat_interval` so it can tell when the leader is gone. llama: wait until every remote worker has said hello. shard:
//!    `shard-serve` workers open no control link, so there is nothing to wait for.
//! 3. Start the upstream. llama: one RPC bridge per remote worker, all sharing the leader's
//!    endpoint, then `llama-server` with the plan's `server_args`, waiting for `/health`. shard:
//!    the shard front end ([`serve_frontend`]) in-process, dialling remote stages by node ID
//!    from the leader's endpoint, waiting until it listens.
//! 4. Send one real one-token generation through the upstream: a stale RPC backend can report
//!    healthy and still abort on the first request, so `/health` alone does not prove the
//!    cluster works. llama probes once. shard retries the probe with a short backoff until it
//!    passes or `worker_wait` elapses, because this is where an unreachable or unapproved worker
//!    shows up; the error then carries the last probe failure, naming the failing stage.
//! 5. Serve the job API. llama only: with an evidence file configured, verify the externally
//!    collected run evidence against the plan while serving; a failed verification stops the
//!    leader. Verification is tied to the planning profile, so the shard backend has none.
//!    llama only: a planned worker that leaves the peer table while serving (heartbeat timeout
//!    or closed connection) is logged, as is its rejoin; jobs and the queue are unaffected.
//! 6. On shutdown or failure, stop the API, then the backend (`llama-server` and the bridges, or
//!    the shard front end), then the endpoint, in that order.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow, bail, ensure};
use iroh::{Endpoint, EndpointId};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::api::{self, ApiConfig};
use crate::evidence::{self, EvidenceConfig};
use crate::history::LoggingHistorySink;
use crate::jobs::JobQueue;
use crate::launch::{LaunchPlan, RemoteWorker};
use crate::network::{self, ControlOptions, SharedPeerTable};
use crate::node::PeerTable;
use crate::process::{self, ChildSpec, Readiness, RestartPolicy, Supervised};
use crate::protocol::CONTROL_ALPN;
use crate::rpc_bridge::{BridgeMapping, LeaderBridges};
use crate::shard::frontend::{FrontendConfig, serve_frontend};
use crate::shard::manifest::ShardManifest;

/// Internal loopback port the backend (`llama-server` or the shard front end) listens on unless
/// configured otherwise.
pub const DEFAULT_LLAMA_PORT: u16 = 8081;

/// Address the job API listens on unless configured otherwise.
pub const DEFAULT_API_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);

/// How long to wait for every planned worker to connect (llama), or for the shard pipeline's
/// readiness probe to pass (shard), unless configured otherwise.
pub const DEFAULT_WORKER_WAIT: Duration = Duration::from_secs(60);

/// How long a worker may go without a heartbeat before the leader closes its control connection,
/// unless configured otherwise: three missed beats at [`crate::worker::DEFAULT_HEARTBEAT_INTERVAL`]
/// plus a second of slack.
pub const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(7);

/// How often the leader heartbeats to each connected worker unless configured otherwise; the
/// same as the workers' own [`crate::worker::DEFAULT_HEARTBEAT_INTERVAL`], so their default
/// [`crate::worker::DEFAULT_LEADER_HEARTBEAT_TIMEOUT`] allows three missed beats plus slack.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = crate::worker::DEFAULT_HEARTBEAT_INTERVAL;

/// How long `llama-server` may take to load the model and answer `/health` unless configured otherwise.
pub const DEFAULT_LLAMA_READY_TIMEOUT: Duration = Duration::from_secs(300);

/// Log label for the supervised `llama-server` child.
const LLAMA_SERVER_NAME: &str = "llama-server";

/// Log label for the in-process shard front end.
const SHARD_FRONTEND_NAME: &str = "shard front end";

/// Log label for an RPC bridge task.
const RPC_BRIDGE_NAME: &str = "rpc bridge";

/// Log label for the job API task.
const JOB_API_NAME: &str = "job API";

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

/// Longest error body from the upstream quoted in a probe failure.
const PROBE_ERROR_BODY_CHARS: usize = 512;

/// Delay before the shard backend's first probe retry; it doubles after every failed attempt.
const PROBE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Cap on the delay between the shard backend's probe retries.
const PROBE_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);

/// How long the shard front end may take to start listening. It hashes every local stage file
/// that has an `artifact_sha256` first, which takes a while for a multi-gigabyte shard.
const SHARD_FRONTEND_READY_TIMEOUT: Duration = Duration::from_secs(300);

/// How often the worker wait re-checks the peer table.
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How often the leader re-checks, while serving, that every planned worker is still connected.
const PRESENCE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How often to check whether the shard front end listens yet.
const LISTEN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How long the job API (or the shard front end) may take to finish in-flight requests at
/// shutdown before it is aborted.
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
    /// The execution backend the job API sits in front of.
    pub backend: BackendConfig,
    /// Internal loopback port the backend serves on: `llama-server`'s, or the shard front end's.
    pub upstream_port: u16,
    /// Loopback address the job API listens on.
    pub api_addr: SocketAddr,
    /// llama: how long to wait for every planned worker to connect. shard: how long the
    /// readiness probe is retried before startup fails.
    pub worker_wait: Duration,
    /// How long a connected worker may go without a heartbeat before the leader closes its
    /// control connection with [`network::HEARTBEAT_TIMEOUT_CODE`], dropping it from the peer
    /// table until it reconnects. Must exceed the workers' heartbeat interval and must not be
    /// zero. `None` disables the timeout: a silent worker is then only dropped when QUIC notices
    /// the connection died.
    pub heartbeat_timeout: Option<Duration>,
    /// How often the leader sends its own heartbeat to each connected worker, usually
    /// [`DEFAULT_HEARTBEAT_INTERVAL`], so workers notice a lost leader within their
    /// `leader_heartbeat_timeout` instead of QUIC's idle timeout. Must be shorter than that
    /// timeout and must not be zero; `None` sends no heartbeats.
    pub heartbeat_interval: Option<Duration>,
}

/// Which execution backend the leader runs, with its backend-specific settings.
#[derive(Clone, Debug)]
pub enum BackendConfig {
    /// A distributed `llama-server` over RPC bridges, launched from a placement plan.
    Llama(LlamaBackendConfig),
    /// The shard runner pipeline described by a shard manifest.
    Shard(ShardBackendConfig),
}

/// Settings for the `llama-server` backend.
#[derive(Clone, Debug)]
pub struct LlamaBackendConfig {
    /// `planning::PlanningOutput` JSON written by `llm-cluster plan`.
    pub plan_path: PathBuf,
    /// `planning::PlanningRequest` JSON the plan was built from; the plan is validated against
    /// it before launch and, with `evidence`, the run evidence after.
    pub profile_path: PathBuf,
    /// Minimum number of contributing peers the plan must use.
    pub min_peers: usize,
    /// Where an external harness writes the run evidence; `None` skips post-run verification.
    pub evidence: Option<EvidenceConfig>,
    /// `llama-server` binary.
    pub llama_server: PathBuf,
    /// Extra `llama-server` arguments, passed after the plan's `server_args` (e.g.
    /// `--cache-ram 0`). The leader's own `--host` and `--port` always come last.
    pub extra_llama_args: Vec<String>,
    /// How long `llama-server` may take to answer `/health`.
    pub llama_ready_timeout: Duration,
}

/// Settings for the shard runner backend. Post-run evidence verification is tied to a planning
/// profile, so this backend has none.
#[derive(Clone, Debug)]
pub struct ShardBackendConfig {
    /// Shard manifest JSON ([`ShardManifest`]): the runner, the model identity and the stages.
    pub manifest_path: PathBuf,
}

/// Runs the leader on a freshly bound endpoint until Ctrl-C, then shuts everything down in order.
///
/// Prints `node id: <id>` so workers can be started with `--leader <id>` (or allowlist it with
/// `--allow-peer <id>`).
pub async fn run_leader_process(config: LeaderConfig) -> anyhow::Result<()> {
    let endpoint = network::bind_endpoint(&config.key_path, vec![CONTROL_ALPN.to_vec()]).await?;
    println!("node id: {}", endpoint.id());
    run_leader_on(endpoint, &config, ctrl_c()).await
}

/// Runs the leader on `endpoint` until `shutdown` resolves or a component fails, then stops the
/// API, the backend and finally closes `endpoint`.
///
/// The backend's input (plan or manifest) is loaded and validated first; if it is invalid,
/// nothing is served or launched. `endpoint` must accept [`CONTROL_ALPN`] and be able to dial
/// workers by node ID. `config.key_path` is ignored; the endpoint already carries the identity.
pub async fn run_leader_on(
    endpoint: Endpoint,
    config: &LeaderConfig,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let result = match prepare(&config.backend) {
        Ok(prepared) => lead(&endpoint, config, &prepared, shutdown).await,
        Err(error) => Err(error),
    };
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

/// A backend whose input passed validation; nothing is bound or started yet.
enum Prepared<'a> {
    Llama(LlamaLaunch<'a>),
    Shard(ShardManifest),
}

/// The llama backend's settings plus the plan they validated.
struct LlamaLaunch<'a> {
    config: &'a LlamaBackendConfig,
    /// Boxed: a validated plan is several times larger than a manifest.
    launch: Box<LaunchPlan>,
}

/// Loads and validates the backend's plan or manifest.
fn prepare(backend: &BackendConfig) -> anyhow::Result<Prepared<'_>> {
    match backend {
        BackendConfig::Llama(config) => Ok(Prepared::Llama(LlamaLaunch {
            launch: Box::new(LaunchPlan::load(
                &config.plan_path,
                &config.profile_path,
                config.min_peers,
            )?),
            config,
        })),
        BackendConfig::Shard(config) => {
            Ok(Prepared::Shard(ShardManifest::load(&config.manifest_path)?))
        }
    }
}

/// Body of [`run_leader_on`] once the backend's input is valid; the caller closes the endpoint
/// afterwards.
async fn lead(
    endpoint: &Endpoint,
    config: &LeaderConfig,
    prepared: &Prepared<'_>,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let peers: SharedPeerTable = Arc::new(Mutex::new(PeerTable::new()));
    let local = network::local_hello(endpoint, &config.cluster_id, EMPTY_LOG_INDEX);
    // Dropping the set aborts the accept loop and the worker watch, so neither outlives this
    // function.
    let mut control = JoinSet::new();
    let options = ControlOptions {
        heartbeat_timeout: config.heartbeat_timeout,
        heartbeat_interval: config.heartbeat_interval,
    };
    control.spawn(network::serve_control_with(
        endpoint.clone(),
        local,
        Arc::clone(&peers),
        options,
    ));

    tokio::pin!(shutdown);
    let running = tokio::select! {
        started = start(endpoint, config, prepared, &peers) => started?,
        () = &mut shutdown => {
            info!("shutdown requested during startup");
            return Ok(());
        }
    };
    if let Prepared::Llama(llama) = prepared {
        control.spawn(watch_workers(
            Arc::clone(&peers),
            llama.launch.workers.clone(),
        ));
    }
    let failed_verification = verify_backend_while_serving(prepared);
    running.serve_until(shutdown, failed_verification).await
}

/// Runs the backend's post-run verification, if it has any, and resolves only if it fails.
async fn verify_backend_while_serving(prepared: &Prepared<'_>) -> anyhow::Error {
    match prepared {
        Prepared::Llama(llama) => {
            verify_while_serving(&llama.launch, llama.config.evidence.as_ref()).await
        }
        Prepared::Shard(_) => {
            info!("post-run verification needs a planning profile; the shard backend has none");
            std::future::pending().await
        }
    }
}

/// Runs post-run verification, if configured, and resolves only if it fails.
///
/// Without an evidence file there is nothing to verify: the leader never fabricates evidence.
async fn verify_while_serving(
    launch: &LaunchPlan,
    evidence: Option<&EvidenceConfig>,
) -> anyhow::Error {
    match evidence {
        None => info!("post-run verification not configured"),
        Some(config) => {
            info!(evidence = %config.path.display(), "waiting for run evidence");
            match evidence::verify_run_evidence(launch, config).await {
                Ok(()) => info!("post-run verification passed"),
                Err(error) => return error,
            }
        }
    }
    std::future::pending().await
}

/// The leader once every component is up.
struct Running {
    api_stop: oneshot::Sender<()>,
    api: JoinSet<anyhow::Result<()>>,
    backend: Backend,
}

/// A running execution backend: whatever serves the job API's upstream.
enum Backend {
    /// The supervised `llama-server` and the RPC bridges it reaches the workers through.
    Llama {
        server: Supervised,
        bridges: JoinSet<anyhow::Result<()>>,
    },
    /// The in-process shard front end; sending on (or dropping) `stop` stops it.
    Shard {
        stop: oneshot::Sender<()>,
        frontend: JoinSet<anyhow::Result<()>>,
    },
}

/// Brings the backend up, proves it with the readiness probe, then starts the API.
async fn start(
    endpoint: &Endpoint,
    config: &LeaderConfig,
    prepared: &Prepared<'_>,
    peers: &SharedPeerTable,
) -> anyhow::Result<Running> {
    let backend = match prepared {
        Prepared::Llama(llama) => start_llama(endpoint, config, llama, peers).await?,
        Prepared::Shard(manifest) => start_shard(endpoint, config, manifest.clone()).await?,
    };
    let (api_stop, api) = start_api(config);
    Ok(Running {
        api_stop,
        api,
        backend,
    })
}

/// llama backend, in dependency order: workers, bridges, `llama-server`, probe.
async fn start_llama(
    endpoint: &Endpoint,
    config: &LeaderConfig,
    llama: &LlamaLaunch<'_>,
    peers: &SharedPeerTable,
) -> anyhow::Result<Backend> {
    let workers = &llama.launch.workers;
    wait_for_workers(peers, workers, config.worker_wait).await?;
    let bridges = start_bridges(endpoint, workers).await?;
    let server_args = llama.launch.server_args();
    let spec = llama_server_spec(llama.config, config.upstream_port, server_args);
    let server = process::spawn_supervised(spec).await?;
    let probe = probe_generation(upstream_addr(config.upstream_port), LLAMA_SERVER_NAME).await;
    if let Err(error) = probe {
        if let Err(stop_error) = server.shutdown().await {
            warn!("failed to stop llama-server: {stop_error:#}");
        }
        return Err(error);
    }
    Ok(Backend::Llama { server, bridges })
}

/// shard backend: the front end, then the probe, retried until it passes or `worker_wait` ends.
async fn start_shard(
    endpoint: &Endpoint,
    config: &LeaderConfig,
    manifest: ShardManifest,
) -> anyhow::Result<Backend> {
    let upstream = upstream_addr(config.upstream_port);
    let (stop, mut frontend) = spawn_frontend(endpoint, upstream, manifest);
    if let Err(error) = shard_ready(&mut frontend, upstream, config.worker_wait).await {
        stop_server(SHARD_FRONTEND_NAME, stop, frontend).await;
        return Err(error);
    }
    Ok(Backend::Shard { stop, frontend })
}

/// Starts the shard front end on `listen` in the background, dialling remote stages from
/// `endpoint`; sending on (or dropping) the returned sender stops it.
fn spawn_frontend(
    endpoint: &Endpoint,
    listen: SocketAddr,
    manifest: ShardManifest,
) -> (oneshot::Sender<()>, JoinSet<anyhow::Result<()>>) {
    let (stop, stopped) = oneshot::channel::<()>();
    let config = FrontendConfig {
        listen,
        manifest,
        endpoint: endpoint.clone(),
    };
    let mut frontend = JoinSet::new();
    frontend.spawn(serve_frontend(config, async move {
        // Resolves on an explicit stop or when the sender is dropped; both mean stop.
        let _ = stopped.await;
    }));
    (stop, frontend)
}

/// Waits for the shard front end to listen, then until one generation passes through it.
async fn shard_ready(
    frontend: &mut JoinSet<anyhow::Result<()>>,
    upstream: SocketAddr,
    wait: Duration,
) -> anyhow::Result<()> {
    wait_until_listening(
        frontend,
        SHARD_FRONTEND_NAME,
        upstream,
        SHARD_FRONTEND_READY_TIMEOUT,
    )
    .await?;
    probe_until_ready(frontend, upstream, wait).await
}

/// Retries the readiness probe with a doubling backoff until it passes or `wait` elapses,
/// failing early if the front end stops. The error carries the last probe failure, which names
/// the stage or worker that failed. An attempt already running at the deadline is allowed to
/// finish (each is bounded by the probe's own timeout), since a cold model loads slowly.
async fn probe_until_ready(
    frontend: &mut JoinSet<anyhow::Result<()>>,
    upstream: SocketAddr,
    wait: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + wait;
    let mut backoff = PROBE_RETRY_INITIAL_BACKOFF;
    loop {
        let error = match probe_generation(upstream, SHARD_FRONTEND_NAME).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if let Some(joined) = frontend.try_join_next() {
            return Err(task_ended(SHARD_FRONTEND_NAME, joined));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(error.context(format!(
                "shard pipeline did not become ready within {wait:?}"
            )));
        }
        warn!("shard pipeline not ready yet, retrying in {backoff:?}: {error:#}");
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = backoff.saturating_mul(2).min(PROBE_RETRY_MAX_BACKOFF);
    }
}

impl Running {
    /// Serves until `shutdown` resolves, `failed_verification` reports an error or a component
    /// stops, then stops everything in order.
    async fn serve_until(
        mut self,
        shutdown: impl Future<Output = ()>,
        failed_verification: impl Future<Output = anyhow::Error>,
    ) -> anyhow::Result<()> {
        let outcome = tokio::select! {
            () = shutdown => {
                info!("shutting down leader");
                Ok(())
            }
            error = failed_verification => {
                warn!("post-run verification failed; shutting down the leader, replan before restarting: {error:#}");
                Err(error)
            }
            error = self.backend.stopped() => Err(error),
            Some(joined) = self.api.join_next() => Err(task_ended(JOB_API_NAME, joined)),
        };
        let stopped = self.stop().await;
        outcome.and(stopped)
    }

    /// Stops the API (letting in-flight requests finish), then the backend.
    async fn stop(self) -> anyhow::Result<()> {
        stop_server(JOB_API_NAME, self.api_stop, self.api).await;
        self.backend.stop().await
    }
}

impl Backend {
    /// Resolves when a backend component stops while serving, describing why.
    async fn stopped(&mut self) -> anyhow::Error {
        match self {
            Self::Llama { server, bridges } => tokio::select! {
                exited = server.wait() => match exited {
                    Ok(()) => anyhow!("llama-server exited"),
                    Err(error) => error.context("llama-server stopped"),
                },
                Some(joined) = bridges.join_next() => task_ended(RPC_BRIDGE_NAME, joined),
            },
            Self::Shard { frontend, .. } => match frontend.join_next().await {
                Some(joined) => task_ended(SHARD_FRONTEND_NAME, joined),
                None => std::future::pending().await,
            },
        }
    }

    /// llama: stops `llama-server`, then the bridges. shard: stops the front end, letting an
    /// in-flight generation finish first.
    async fn stop(self) -> anyhow::Result<()> {
        match self {
            Self::Llama {
                server,
                mut bridges,
            } => {
                let stopped = server.shutdown().await;
                bridges.shutdown().await;
                stopped
            }
            Self::Shard { stop, frontend } => {
                stop_server(SHARD_FRONTEND_NAME, stop, frontend).await;
                Ok(())
            }
        }
    }
}

/// Signals `stop`, lets the server in `tasks` finish its in-flight requests within
/// [`API_STOP_TIMEOUT`], then aborts whatever is left.
async fn stop_server(
    what: &str,
    stop: oneshot::Sender<()>,
    mut tasks: JoinSet<anyhow::Result<()>>,
) {
    // An error only means the server already stopped, which is the state we want.
    let _ = stop.send(());
    let drained = tokio::time::timeout(API_STOP_TIMEOUT, async {
        while let Some(joined) = tasks.join_next().await {
            if let Err(error) = joined
                .map_err(anyhow::Error::from)
                .and_then(|served| served)
            {
                warn!("{what} stopped with an error: {error:#}");
            }
        }
    })
    .await;
    if drained.is_err() {
        warn!("{what} did not stop in time; aborting in-flight requests");
        tasks.shutdown().await;
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
    let connected = table.ids().collect::<Vec<_>>();
    Ok(workers
        .iter()
        .map(|worker| worker.id)
        .filter(|id| !connected.contains(id))
        .collect())
}

/// Logs, until aborted, every planned worker that leaves the peer table (its heartbeats stopped
/// or its connection closed) and every one that rejoins. Only logs; serving is unaffected. Every
/// worker is assumed connected at the start, as [`wait_for_workers`] has returned. Fails only if
/// the peer table lock is poisoned.
async fn watch_workers(peers: SharedPeerTable, workers: Vec<RemoteWorker>) -> anyhow::Result<()> {
    let mut missing = Vec::new();
    let mut ticks = tokio::time::interval(PRESENCE_POLL_INTERVAL);
    loop {
        ticks.tick().await;
        let now_missing = missing_workers(&peers, &workers)?;
        let (left, rejoined) = presence_changes(&missing, &now_missing);
        for id in left {
            warn!(worker = %id, "planned worker left the peer table; waiting for it to reconnect");
        }
        for id in rejoined {
            info!(worker = %id, "planned worker reconnected");
        }
        missing = now_missing;
    }
}

/// Workers that are missing now but were not `before` (left), and the reverse (rejoined).
fn presence_changes(
    before: &[EndpointId],
    now: &[EndpointId],
) -> (Vec<EndpointId>, Vec<EndpointId>) {
    let newly = |from: &[EndpointId], absent_in: &[EndpointId]| {
        from.iter()
            .filter(|id| !absent_in.contains(id))
            .copied()
            .collect::<Vec<_>>()
    };
    (newly(now, before), newly(before, now))
}

fn join_ids(ids: &[EndpointId]) -> String {
    ids.iter()
        .map(EndpointId::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Binds one bridge per worker on the shared endpoint, then serves them all in the background.
/// Every listener is bound before this returns, so `llama-server` finds every RPC port open
/// when it starts. A plan without remote workers needs no bridges.
async fn start_bridges(
    endpoint: &Endpoint,
    workers: &[RemoteWorker],
) -> anyhow::Result<JoinSet<anyhow::Result<()>>> {
    let mut bridges = JoinSet::new();
    if workers.is_empty() {
        return Ok(bridges);
    }
    let mappings: Vec<BridgeMapping> = workers
        .iter()
        .map(|worker| BridgeMapping {
            worker: worker.id,
            listen: worker.rpc_endpoint,
        })
        .collect();
    let bound = LeaderBridges::bind(endpoint.clone(), &mappings)
        .await
        .with_context(|| format!("{RPC_BRIDGE_NAME} failed to start"))?;
    bridges.spawn(bound.serve());
    Ok(bridges)
}

/// Polls `listen` until it accepts TCP, failing after `limit` or as soon as any task in `tasks`
/// (the `what` servers) has ended.
async fn wait_until_listening(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    what: &str,
    listen: SocketAddr,
    limit: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(joined) = tasks.try_join_next() {
            return Err(task_ended(what, joined));
        }
        if TcpStream::connect(listen).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("{what} on {listen} did not start listening within {limit:?}");
        }
        tokio::time::sleep(LISTEN_POLL_INTERVAL).await;
    }
}

fn upstream_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// `llama-server` argv: the plan's `server_args` unchanged, then the operator's extra arguments,
/// then the loopback host and port last so the leader's binding always wins.
fn llama_server_args(server_args: &[String], extra_args: &[String], port: u16) -> Vec<String> {
    let mut args = [server_args, extra_args].concat();
    args.extend([
        "--host".to_owned(),
        Ipv4Addr::LOCALHOST.to_string(),
        "--port".to_owned(),
        port.to_string(),
    ]);
    args
}

fn llama_server_spec(llama: &LlamaBackendConfig, port: u16, server_args: &[String]) -> ChildSpec {
    ChildSpec {
        name: LLAMA_SERVER_NAME.to_owned(),
        program: llama.llama_server.clone(),
        args: llama_server_args(server_args, &llama.extra_llama_args, port),
        readiness: Readiness::HttpOk {
            addr: upstream_addr(port),
            path: HEALTH_PATH.to_owned(),
        },
        ready_timeout: llama.llama_ready_timeout,
        restart: RestartPolicy::default(),
    }
}

/// Generates one token through the `server` (named in errors) at `upstream`; fails unless it
/// answers 2xx with a choice.
async fn probe_generation(upstream: SocketAddr, server: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .context("readiness probe: failed to build HTTP client")?;
    let response = client
        .post(format!("http://{upstream}{CHAT_COMPLETIONS_PATH}"))
        .json(&probe_request())
        .send()
        .await
        .with_context(|| format!("readiness probe: request to {server} failed"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let body: String = body.chars().take(PROBE_ERROR_BODY_CHARS).collect();
        bail!("readiness probe: {server} answered {status}: {body}");
    }
    let reply: Value = response
        .json()
        .await
        .with_context(|| format!("readiness probe: {server} reply is not JSON"))?;
    let has_choice = reply
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| !choices.is_empty());
    ensure!(has_choice, "readiness probe: {server} reply has no choices");
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
        upstream: upstream_addr(config.upstream_port),
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
    use tokio::net::TcpListener;

    use super::*;
    use crate::launch::DEFAULT_MIN_PEERS;
    use crate::protocol::{Hello, PROTOCOL_VERSION};

    /// How long a future that must never resolve is watched.
    const UNRESOLVED_CHECK_WINDOW: Duration = Duration::from_millis(50);

    /// The checked-in launcher fixtures, which pass pre-launch validation.
    fn fixture_launch() -> LaunchPlan {
        let plan = serde_json::from_str(include_str!("../examples/launcher-plan.json"))
            .expect("fixture plan");
        let profile = serde_json::from_str(include_str!("../examples/launcher-profile.json"))
            .expect("fixture profile");
        LaunchPlan::from_parts(plan, profile, DEFAULT_MIN_PEERS).expect("fixture plan is valid")
    }

    fn table_with(ids: &[EndpointId]) -> SharedPeerTable {
        let mut table = PeerTable::new();
        for id in ids {
            table.record_connection(
                *id,
                &Hello {
                    node_id: id.to_string(),
                    protocol_version: PROTOCOL_VERSION,
                    cluster_id: "test".to_owned(),
                    last_log_index: 0,
                },
            );
        }
        Arc::new(Mutex::new(table))
    }

    fn worker(id: EndpointId) -> RemoteWorker {
        RemoteWorker {
            id,
            rpc_endpoint: SocketAddr::from((Ipv4Addr::LOCALHOST, 50053)),
        }
    }

    /// A loopback address nothing listens on (the port was free a moment ago).
    async fn unused_loopback_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
        listener.local_addr().expect("probe address")
    }

    /// A task set holding one task that never ends, standing in for a running server.
    fn running_server() -> JoinSet<anyhow::Result<()>> {
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending());
        tasks
    }

    #[test]
    fn llama_server_args_put_extras_after_the_plan_and_host_and_port_last() {
        let plan_args = vec!["--rpc".to_owned(), "127.0.0.1:50053".to_owned()];
        let extra_args = vec!["--cache-ram".to_owned(), "0".to_owned()];

        let args = llama_server_args(&plan_args, &extra_args, 8081);

        assert_eq!(
            args,
            [
                "--rpc",
                "127.0.0.1:50053",
                "--cache-ram",
                "0",
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

    #[test]
    fn presence_changes_report_departures_and_rejoins() {
        let stays = SecretKey::generate().public();
        let leaves = SecretKey::generate().public();
        let rejoins = SecretKey::generate().public();

        let (left, rejoined) = presence_changes(&[stays, rejoins], &[stays, leaves]);

        assert_eq!(left, [leaves]);
        assert_eq!(rejoined, [rejoins]);
        assert_eq!(presence_changes(&[stays], &[stays]), (vec![], vec![]));
    }

    #[tokio::test]
    async fn verification_without_evidence_never_fails() {
        let launch = fixture_launch();

        let outcome =
            tokio::time::timeout(UNRESOLVED_CHECK_WINDOW, verify_while_serving(&launch, None))
                .await;

        assert!(outcome.is_err(), "unconfigured verification must not fail");
    }

    #[tokio::test]
    async fn missing_evidence_fails_verification() {
        let launch = fixture_launch();
        let config = EvidenceConfig {
            path: std::env::temp_dir()
                .join(format!("meshthing-no-evidence-{}", uuid::Uuid::new_v4())),
            wait: Duration::ZERO,
        };

        let error = verify_while_serving(&launch, Some(&config)).await;

        assert!(error.to_string().contains("no run evidence"), "{error:#}");
    }

    #[tokio::test]
    async fn shard_backend_never_fails_verification() {
        let manifest = serde_json::from_value(json!({
            "runner": "shard_runner",
            "model_sha256": "a".repeat(64),
            "layers": 4,
            "context": 64,
            "chat_template": "chatml",
            "stages": [{"begin": 0, "end": 4, "local": {"model": "m.gguf", "budget_bytes": 1}}],
        }))
        .expect("manifest");

        let outcome = tokio::time::timeout(
            UNRESOLVED_CHECK_WINDOW,
            verify_backend_while_serving(&Prepared::Shard(manifest)),
        )
        .await;

        assert!(outcome.is_err(), "the shard backend has nothing to verify");
    }

    #[tokio::test]
    async fn shard_probe_gives_up_after_the_wait_with_the_last_failure() {
        let upstream = unused_loopback_addr().await;
        let mut frontend = running_server();

        let error = probe_until_ready(&mut frontend, upstream, Duration::ZERO)
            .await
            .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("did not become ready"), "{message}");
        assert!(
            message.contains("readiness probe: request to shard front end failed"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn listener_wait_reports_a_server_that_already_failed() {
        let upstream = unused_loopback_addr().await;
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Err(anyhow!("bind refused")) });

        let error = wait_until_listening(
            &mut tasks,
            SHARD_FRONTEND_NAME,
            upstream,
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("shard front end failed"), "{message}");
        assert!(message.contains("bind refused"), "{message}");
    }

    #[tokio::test]
    async fn listener_wait_times_out_naming_the_address() {
        let upstream = unused_loopback_addr().await;
        let mut tasks = running_server();

        let error = wait_until_listening(&mut tasks, RPC_BRIDGE_NAME, upstream, Duration::ZERO)
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(
            message.contains(&format!("rpc bridge on {upstream}")),
            "{message}"
        );
    }
}
