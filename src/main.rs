//! MeshThing command-line entry point: plan, run a worker or the leader, send prompts, or debug
//! the mesh with the lower-level node and bridge commands.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use iroh::Endpoint;
use llm_cluster::history::sync::store::{self, HistoryStore, JobStatus};
use llm_cluster::net::network::{self, SharedPeerTable};
use llm_cluster::net::node::PeerTable;
use llm_cluster::net::protocol::{CONTROL_ALPN, RPC_BRIDGE_ALPN};
use llm_cluster::net::rpc_bridge;
use llm_cluster::planning::evidence::EvidenceConfig;
use llm_cluster::planning::launch;
use llm_cluster::roles::leader::{
    self, BackendConfig, LeaderConfig, LlamaBackendConfig, ShardBackendConfig,
};
use llm_cluster::roles::worker::{self, WorkerConfig};
use llm_cluster::service::api;
use llm_cluster::shard::ring::{RING_ALPN, RingWorkerConfig, serve_ring_worker};
use llm_cluster::shard::ring_origin::RingSettings;
use llm_cluster::shard::serve::{ShardServeConfig, serve_shard};
use serde::Serialize;
use tracing_subscriber::EnvFilter;

/// Log filter used when `RUST_LOG` is unset.
const DEFAULT_LOG_FILTER: &str = "info";

/// Last log index announced in hellos until the replicated log exists.
const EMPTY_LOG_INDEX: u64 = 0;

/// QUIC application close code for a normal, intentional shutdown.
const CLOSE_CODE_DONE: u32 = 0;

/// QUIC close reason sent alongside [`CLOSE_CODE_DONE`].
const CLOSE_REASON_DONE: &[u8] = b"done";

/// `worker --heartbeat-interval` default; matches [`worker::DEFAULT_HEARTBEAT_INTERVAL`].
const DEFAULT_HEARTBEAT_INTERVAL_ARG: &str = "2s";

/// `leader --heartbeat-interval` default; matches [`leader::DEFAULT_HEARTBEAT_INTERVAL`].
const DEFAULT_LEADER_HEARTBEAT_INTERVAL_ARG: &str = "2s";

/// `leader --heartbeat-timeout` default: three missed beats at [`DEFAULT_HEARTBEAT_INTERVAL_ARG`]
/// plus a second of slack; matches [`leader::DEFAULT_HEARTBEAT_TIMEOUT`].
const DEFAULT_HEARTBEAT_TIMEOUT_ARG: &str = "7s";

/// `worker --leader-heartbeat-timeout` default: three missed beats at
/// [`DEFAULT_LEADER_HEARTBEAT_INTERVAL_ARG`] plus a second of slack; matches
/// [`worker::DEFAULT_LEADER_HEARTBEAT_TIMEOUT`].
const DEFAULT_LEADER_HEARTBEAT_TIMEOUT_ARG: &str = "7s";

/// `--worker-wait` default; matches [`leader::DEFAULT_WORKER_WAIT`].
const DEFAULT_WORKER_WAIT_ARG: &str = "60s";

/// `--llama-ready-timeout` default; matches [`leader::DEFAULT_LLAMA_READY_TIMEOUT`].
const DEFAULT_LLAMA_READY_TIMEOUT_ARG: &str = "300s";

/// `--evidence-wait` default; matches [`llm_cluster::planning::evidence::DEFAULT_EVIDENCE_WAIT`].
const DEFAULT_EVIDENCE_WAIT_ARG: &str = "120s";

const SECONDS_PER_MINUTE: u64 = 60;

#[derive(Debug, Parser)]
#[command(
    name = "meshthing",
    about = "Peer-to-peer mesh for splitting one LLM across devices"
)]
struct Cli {
    /// Enable detailed MeshThing diagnostics on stderr (also works with RUST_LOG).
    #[arg(long, global = true)]
    debug: bool,

    /// File holding this node's secret key; created on first run.
    #[arg(long, global = true, default_value = "meshthing.key")]
    key: PathBuf,

    /// Cluster ID announced in hellos; peers from other clusters are rejected.
    #[arg(long, global = true, default_value = "meshthing")]
    cluster: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Verify, serve, and stage exported model shards.
    Artifacts(llm_cluster::artifact::cli::Options),
    /// Run the mesh core, local OpenAI API, and browser UI in this process.
    Ui(llm_cluster::service::ui::Options),
    /// Run a persistent mesh node. Send create/invite/join/decide/snapshot JSON commands on stdin.
    Mesh {
        /// Private identity and signed membership directory, one per running device.
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Check caller-collected memory and compute evidence for an advisory plan.
    Verify {
        /// JSON produced by the plan command.
        plan: PathBuf,
        /// Original planning profile JSON, containing current peer profiles.
        profile: PathBuf,
        /// JSON RunEvidence collected over model load and fresh inference.
        evidence: PathBuf,
        #[arg(long, default_value_t = 2)]
        min_peers: usize,
    },
    /// Read measured profiles from JSON and print an advisory plan; never launches inference.
    Plan {
        /// JSON file containing model, peers, links, devices, and model_path.
        profile: PathBuf,
        /// Require at least this many contributors.
        #[arg(long, default_value_t = 2)]
        min_peers: usize,
    },
    /// Search exact measured shard ranges and directed links; advisory JSON only.
    MeasuredPlan {
        /// Versioned JSON with fresh range, link and memory observations.
        profile: PathBuf,
    },
    /// Prepare an EXPERIMENTAL ring manifest and worker arguments from measured ranges.
    /// Does not launch, reserve capacity, or qualify diagnostic measurements.
    PrepareRing {
        /// Current measured profile; timestamps are checked against current wall time.
        profile: PathBuf,
        /// Trusted device identities, native builds and exact range artifacts.
        registry: PathBuf,
        /// New output directory; existing paths are never overwritten.
        #[arg(long)]
        output_dir: PathBuf,
    },
    /// Verify a prepared ring bundle against original inputs and current expiration.
    ValidateRing {
        bundle: PathBuf,
        profile: PathBuf,
        registry: PathBuf,
    },
    /// Recheck a saved measured plan against refreshed observations; no runtime verification.
    ValidateMeasured {
        /// JSON produced by `measured-plan`.
        plan: PathBuf,
        /// Versioned measured profile with current capacity and time.
        profile: PathBuf,
    },
    /// Print this node's ID and accept control connections from peers.
    Listen,
    /// Connect to a listening node, exchange hellos, and print the peer's hello.
    Connect {
        /// Node ID printed by the peer's `listen`.
        node_id: String,
    },
    /// Worker side of the RPC bridge: forward incoming bridge streams to a local RPC server.
    BridgeServe {
        /// Local RPC server to forward bridge streams to.
        #[arg(long, default_value = "127.0.0.1:50052")]
        target: SocketAddr,
        /// Node ID allowed to use this bridge (the leader's); repeatable, at least one required.
        #[arg(long = "allow-peer", required = true)]
        allow_peers: Vec<String>,
    },
    /// Leader side of the RPC bridge: expose remote workers' RPC servers on local ports.
    ///
    /// Give either one worker (`NODE_ID [--listen ADDR]`) or several (`--worker NODE_ID=ADDR`,
    /// repeated) to bridge them all from one process over one endpoint. Once every listener is
    /// bound, prints one JSON line with the address each worker is reachable on:
    /// `{"bridges":[{"worker":"<id>","listen":"127.0.0.1:50053"},...]}`. A listen port of 0
    /// picks a free port, reported in that line.
    BridgeConnect {
        /// Node ID printed by the worker's `bridge-serve`.
        #[arg(required_unless_present = "workers", conflicts_with = "workers")]
        node_id: Option<String>,
        /// Local address to accept RPC clients on.
        #[arg(long, default_value = "127.0.0.1:50052", conflicts_with = "workers")]
        listen: SocketAddr,
        /// Worker to bridge, as `NODE_ID=ADDR` with a loopback ADDR; repeatable.
        #[arg(long = "worker", value_name = "NODE_ID=ADDR", value_parser = parse_worker_mapping)]
        workers: Vec<rpc_bridge::BridgeMapping>,
    },
    /// Serve one layer range of a model to leaders through a local shard runner.
    ShardServe {
        /// The shard_runner binary.
        #[arg(long)]
        runner: PathBuf,
        /// The model (or exported shard) file passed to the runner.
        #[arg(long)]
        model: PathBuf,
        /// Half-open layer range served, as `BEGIN:END` (e.g. `0:32`).
        #[arg(long, value_parser = parse_layer_range)]
        range: LayerRange,
        /// Context window passed to the runner.
        #[arg(long)]
        context: u32,
        /// Layers the runner offloads to the GPU.
        #[arg(long, default_value_t = 0)]
        gpu_layers: u32,
        /// Runtime buffer budget passed to the runner, in bytes.
        #[arg(long)]
        budget_bytes: u64,
        /// SHA-256 the model file must have; checked at startup.
        #[arg(long)]
        expected_sha256: String,
        /// Original model identity an exported shard must report; announced as its model hash.
        #[arg(long)]
        source_sha256: Option<String>,
        /// Node ID allowed to use this shard (the leader's); repeatable, at least one required.
        #[arg(long = "allow-peer", required = true)]
        allow_peers: Vec<String>,
        #[command(flatten)]
        profiles: WorkerProfileArgs,
    },
    /// EXPERIMENTAL: serve one layer range as a `meshthing/shard-ring/1` worker that forwards
    /// activations straight to the next worker. Without `--next` this worker is the terminal
    /// and returns tokens directly to the origin.
    ShardRingWorker {
        /// The shard_runner binary.
        #[arg(long)]
        runner: PathBuf,
        /// The exported shard file passed to the runner.
        #[arg(long)]
        model: PathBuf,
        /// Half-open layer range served, as `BEGIN:END` (e.g. `28:32`).
        #[arg(long, value_parser = parse_layer_range)]
        range: LayerRange,
        /// Context window passed to the runner.
        #[arg(long)]
        context: u32,
        /// Layers the runner offloads to the GPU.
        #[arg(long, default_value_t = 0)]
        gpu_layers: u32,
        /// Runtime buffer budget passed to the runner, in bytes.
        #[arg(long)]
        budget_bytes: u64,
        /// SHA-256 the model file must have; checked at startup.
        #[arg(long)]
        expected_sha256: String,
        /// Original model identity the runner must report.
        #[arg(long)]
        source_sha256: Option<String>,
        /// Node ID of the only peer allowed to start sessions: the origin for the entry worker,
        /// else the previous worker.
        #[arg(long, value_parser = parse_node_id_arg)]
        upstream: iroh::EndpointId,
        /// Node ID of the origin (leader) the terminal returns results to.
        #[arg(long, value_parser = parse_node_id_arg)]
        origin: iroh::EndpointId,
        /// Node ID of the next worker; omit on the terminal.
        #[arg(long, value_parser = parse_node_id_arg)]
        next: Option<iroh::EndpointId>,
        #[command(flatten)]
        profiles: WorkerProfileArgs,
    },
    /// Contribute this device to a cluster: supervise ggml-rpc-server and bridge it to the leader.
    Worker {
        /// The ggml-rpc-server binary.
        #[arg(long)]
        rpc_server: PathBuf,
        /// Loopback port for ggml-rpc-server.
        #[arg(long, default_value_t = rpc_bridge::DEFAULT_RPC_PORT)]
        rpc_port: u16,
        /// Extra argument for ggml-rpc-server, after `-H 127.0.0.1 -p <port>`; repeatable.
        #[arg(long = "rpc-arg", allow_hyphen_values = true)]
        rpc_args: Vec<String>,
        /// Node ID printed by the leader; omit to run the bridge without a control link.
        /// The leader is always allowed to use this worker's RPC bridge.
        #[arg(long)]
        leader: Option<String>,
        /// Extra node ID allowed to use this worker's RPC bridge; repeatable. Without `--leader`
        /// at least one is required.
        #[arg(long = "allow-peer")]
        allow_peers: Vec<String>,
        /// Time between heartbeats to the leader, e.g. `2s` or `500ms`.
        #[arg(long, default_value = DEFAULT_HEARTBEAT_INTERVAL_ARG, value_parser = parse_duration)]
        heartbeat_interval: Duration,
        /// Drop the control link and redial after this long without a heartbeat from the
        /// leader, e.g. `7s`. Must exceed the leader's --heartbeat-interval. Applies only once
        /// the leader has sent one, so leaders that never heartbeat are not timed out. `0s`
        /// disables it.
        #[arg(long, default_value = DEFAULT_LEADER_HEARTBEAT_TIMEOUT_ARG, value_parser = parse_duration)]
        leader_heartbeat_timeout: Duration,
    },
    /// Run the main process: start the execution backend (llama-server from a plan, or the shard
    /// runner pipeline from a manifest) and serve the job API in front of it.
    Leader(LeaderArgs),
    /// Send one prompt to a running leader's job API and print the reply.
    Prompt {
        /// Job API address of the leader.
        #[arg(long, default_value_t = leader::DEFAULT_API_ADDR)]
        api: SocketAddr,
        /// The prompt text.
        text: String,
    },
    /// Read a conversation's history stored on this machine.
    History {
        #[command(subcommand)]
        command: HistoryCommand,
    },
}

/// Flags that only the llama backend uses; `--shard-manifest` conflicts with every one of them.
const LLAMA_ONLY_ARGS: [&str; 8] = [
    "plan",
    "profile",
    "min_peers",
    "evidence_file",
    "evidence_wait",
    "llama_server",
    "llama_args",
    "llama_ready_timeout",
];

/// Optional worker-local controls share the worker's process admission owner.
#[derive(Debug, Default, Args)]
struct WorkerProfileArgs {
    /// Trusted local registered-candidate configuration (POSIX helper only).
    #[arg(long, requires = "profile_api")]
    profile_config: Option<PathBuf>,
    /// Loopback address for managed start/status/cancel routes; disabled by default.
    #[arg(long, requires = "profile_config")]
    profile_api: Option<SocketAddr>,
}

impl WorkerProfileArgs {
    async fn bind(
        self,
    ) -> anyhow::Result<Option<llm_cluster::runtime::worker_profile::WorkerProfileControl>> {
        match (self.profile_config, self.profile_api) {
            (None, None) => Ok(None),
            (Some(path), Some(address)) => {
                llm_cluster::runtime::worker_profile::validate_address(address)?;
                let manager =
                    llm_cluster::runtime::profiling::ProfileManager::from_file(&path).await?;
                Ok(Some(
                    llm_cluster::runtime::worker_profile::WorkerProfileControl::bind(
                        manager, address,
                    )
                    .await?,
                ))
            }
            _ => anyhow::bail!("worker profiling requires both --profile-config and --profile-api"),
        }
    }
}

/// `leader` flags.
#[derive(Debug, Args)]
struct LeaderArgs {
    /// Execution backend: `llama` runs llama-server over the plan's RPC bridges; `shard` runs
    /// the shard runner pipeline described by --shard-manifest (see docs/shard-backend.md).
    #[arg(long, value_enum, default_value_t = BackendKind::Llama)]
    backend: BackendKind,
    /// Shard manifest JSON: the runner, model identity and stages. Required with, and only
    /// accepted with, `--backend shard`; the llama-only flags are refused alongside it.
    #[arg(
        long,
        requires = "backend",
        required_if_eq("backend", "shard"),
        conflicts_with_all = LLAMA_ONLY_ARGS
    )]
    shard_manifest: Option<PathBuf>,
    /// JSON produced by the plan command (llama backend).
    #[arg(
        long,
        required_unless_present = "shard_manifest",
        required_if_eq("backend", "llama")
    )]
    plan: Option<PathBuf>,
    /// Planning profile JSON the plan was built from; the plan is validated against it before
    /// anything launches (llama backend).
    #[arg(
        long,
        required_unless_present = "shard_manifest",
        required_if_eq("backend", "llama")
    )]
    profile: Option<PathBuf>,
    /// Require at least this many contributors, as for the plan command (llama backend).
    #[arg(long, default_value_t = launch::DEFAULT_MIN_PEERS)]
    min_peers: usize,
    /// RunEvidence JSON an external harness writes after a fresh inference run; when given,
    /// the leader verifies it against the plan and shuts down if verification fails (llama
    /// backend).
    #[arg(long)]
    evidence_file: Option<PathBuf>,
    /// How long to wait for --evidence-file to appear and parse, e.g. `120s` or `2m`.
    #[arg(
        long,
        default_value = DEFAULT_EVIDENCE_WAIT_ARG,
        value_parser = parse_duration,
        requires = "evidence_file"
    )]
    evidence_wait: Duration,
    /// The llama-server binary (llama backend).
    #[arg(
        long,
        required_unless_present = "shard_manifest",
        required_if_eq("backend", "llama")
    )]
    llama_server: Option<PathBuf>,
    /// Extra argument for llama-server, after the plan's arguments (e.g. `--llama-arg
    /// --cache-ram --llama-arg 0`); repeatable. The leader's own --host/--port always win.
    #[arg(long = "llama-arg", allow_hyphen_values = true)]
    llama_args: Vec<String>,
    /// Internal loopback port for the backend: llama-server's, or the shard front end's with
    /// `--backend shard`. The job API forwards to it.
    #[arg(long, default_value_t = leader::DEFAULT_LLAMA_PORT)]
    llama_port: u16,
    /// Loopback address to serve the job API on.
    #[arg(long, default_value_t = leader::DEFAULT_API_ADDR)]
    api: SocketAddr,
    /// How long to wait for every planned worker to connect (llama), or for the shard
    /// pipeline's readiness probe to pass (shard), e.g. `60s` or `2m`.
    #[arg(long, default_value = DEFAULT_WORKER_WAIT_ARG, value_parser = parse_duration)]
    worker_wait: Duration,
    /// How long llama-server may take to load the model and answer /health (llama backend).
    #[arg(long, default_value = DEFAULT_LLAMA_READY_TIMEOUT_ARG, value_parser = parse_duration)]
    llama_ready_timeout: Duration,
    /// Close a worker's control connection after this long without a heartbeat, e.g. `7s`;
    /// the worker reconnects with backoff. Must exceed the workers' --heartbeat-interval.
    /// `0s` disables the timeout.
    #[arg(long, default_value = DEFAULT_HEARTBEAT_TIMEOUT_ARG, value_parser = parse_duration)]
    heartbeat_timeout: Duration,
    /// Time between the leader's own heartbeats to each connected worker, e.g. `2s`; workers
    /// redial after their --leader-heartbeat-timeout without one. `0s` disables them.
    #[arg(long, default_value = DEFAULT_LEADER_HEARTBEAT_INTERVAL_ARG, value_parser = parse_duration)]
    heartbeat_interval: Duration,
}

/// `leader --backend` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum BackendKind {
    /// llama-server over RPC bridges, launched from a placement plan.
    Llama,
    /// The shard runner pipeline, described by a shard manifest.
    Shard,
}

impl LeaderArgs {
    /// Builds the leader's configuration; `key_path` and `cluster_id` come from the global flags.
    fn into_config(self, key_path: PathBuf, cluster_id: String) -> anyhow::Result<LeaderConfig> {
        let backend = match self.backend {
            BackendKind::Llama => BackendConfig::Llama(LlamaBackendConfig {
                plan_path: required_for_llama(self.plan, "--plan")?,
                profile_path: required_for_llama(self.profile, "--profile")?,
                min_peers: self.min_peers,
                evidence: self.evidence_file.map(|path| EvidenceConfig {
                    path,
                    wait: self.evidence_wait,
                }),
                llama_server: required_for_llama(self.llama_server, "--llama-server")?,
                extra_llama_args: self.llama_args,
                llama_ready_timeout: self.llama_ready_timeout,
            }),
            BackendKind::Shard => BackendConfig::Shard(ShardBackendConfig {
                manifest_path: self
                    .shard_manifest
                    .context("--shard-manifest is required with --backend shard")?,
                ring: RingSettings::default(),
            }),
        };
        Ok(LeaderConfig {
            key_path,
            cluster_id,
            backend,
            upstream_port: self.llama_port,
            api_addr: self.api,
            worker_wait: self.worker_wait,
            heartbeat_timeout: zero_disables(self.heartbeat_timeout),
            heartbeat_interval: zero_disables(self.heartbeat_interval),
        })
    }
}

/// Maps a heartbeat timeout or interval flag to its setting: `0s` disables it.
fn zero_disables(duration: Duration) -> Option<Duration> {
    (!duration.is_zero()).then_some(duration)
}

/// A flag clap already requires with `--backend llama`; checked again so no unwrap is needed.
fn required_for_llama(value: Option<PathBuf>, flag: &str) -> anyhow::Result<PathBuf> {
    value.with_context(|| format!("{flag} is required with --backend llama"))
}

#[derive(Debug, Subcommand)]
enum HistoryCommand {
    /// Print the stored conversation as versioned JSON, for recovery tools.
    Export {
        /// Conversation ID the history is stored under.
        conversation: String,
        /// Directory holding the node's stored history.
        #[arg(long, default_value = store::DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Print a short human-readable summary of the stored conversation.
    Inspect {
        /// Conversation ID the history is stored under.
        conversation: String,
        /// Directory holding the node's stored history.
        #[arg(long, default_value = store::DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging(cli.debug);
    match cli.command {
        Command::Artifacts(options) => llm_cluster::artifact::cli::run(options).await,
        Command::Ui(options) => llm_cluster::service::ui::run(options).await,
        Command::Mesh { state_dir } => llm_cluster::mesh::cli::run(state_dir).await,
        Command::Verify {
            plan,
            profile,
            evidence,
            min_peers,
        } => {
            let output: llm_cluster::planning::PlanningOutput =
                llm_cluster::config::read_json_file(&plan, "plan")?;
            let request: llm_cluster::planning::PlanningRequest =
                llm_cluster::config::read_json_file(&profile, "profile")?;
            let evidence = llm_cluster::config::read_json_file(&evidence, "evidence")?;
            llm_cluster::planning::verify_output(&request, &output, &evidence, min_peers)
                .map_err(|error| anyhow::anyhow!("verification failed: {error:?}"))?;
            println!("{{\"verified\":true}}");
            Ok(())
        }
        Command::Plan { profile, min_peers } => {
            let request: llm_cluster::planning::PlanningRequest =
                llm_cluster::config::read_json_file(&profile, "profile")?;
            tracing::debug!(
                peers = request.peers.len(),
                links = request.links.len(),
                min_peers,
                "building placement plan"
            );
            let output = llm_cluster::planning::build_plan(&request, min_peers)
                .map_err(|error| anyhow::anyhow!("planning failed: {error:?}"))?;
            tracing::debug!(
                assignments = output.placement.assignments.len(),
                predicted_decode_us = output.placement.predicted_decode_us,
                "placement plan ready"
            );
            println!("{}", serde_json::to_string_pretty(&output)?);
            Ok(())
        }
        Command::PrepareRing {
            profile,
            registry,
            output_dir,
        } => {
            let request = read_ring_json(&profile)?;
            let registry = read_ring_json(&registry)?;
            let bundle = llm_cluster::planning::ring_deployment::prepare(
                &request,
                &registry,
                current_unix_ms()?,
            )?;
            llm_cluster::planning::ring_deployment::write_bundle(&bundle, &output_dir)?;
            println!(
                "{}",
                serde_json::json!({
                    "preparation_id": bundle.preparation_id,
                    "bundle": output_dir.join("bundle.json"),
                    "expires_at_unix_ms": bundle.expires_at_unix_ms,
                    "activation_ready": false,
                })
            );
            Ok(())
        }
        Command::ValidateRing {
            bundle,
            profile,
            registry,
        } => {
            llm_cluster::planning::ring_deployment::validate(
                &read_ring_json(&bundle)?,
                &read_ring_json(&profile)?,
                &read_ring_json(&registry)?,
                current_unix_ms()?,
            )?;
            println!("{{\"configuration_valid\":true,\"activation_ready\":false}}");
            Ok(())
        }
        Command::MeasuredPlan { profile } => {
            let request: llm_cluster::planning::measured_placement::MeasuredRequest =
                llm_cluster::config::read_json_file(&profile, "measured profile")?;
            let output = llm_cluster::planning::measured_placement::plan(&request)
                .map_err(|error| anyhow::anyhow!("measured planning failed: {error:?}"))?;
            println!("{}", serde_json::to_string_pretty(&output)?);
            Ok(())
        }
        Command::ValidateMeasured { plan, profile } => {
            let request: llm_cluster::planning::measured_placement::MeasuredRequest =
                llm_cluster::config::read_json_file(&profile, "measured profile")?;
            let output: llm_cluster::planning::measured_placement::MeasuredPlacement =
                llm_cluster::config::read_json_file(&plan, "measured plan")?;
            llm_cluster::planning::measured_placement::validate_output(&request, &output)
                .map_err(|error| anyhow::anyhow!("measured plan validation failed: {error:?}"))?;
            println!("{{\"validated\":true}}");
            Ok(())
        }
        Command::Listen => listen(&cli.key, &cli.cluster).await,
        Command::Connect { ref node_id } => connect(&cli.key, &cli.cluster, node_id).await,
        Command::BridgeServe {
            target,
            ref allow_peers,
        } => bridge_serve(&cli.key, target, &parse_node_ids(allow_peers)?).await,
        Command::BridgeConnect {
            ref node_id,
            listen,
            ref workers,
        } => {
            let mappings = bridge_mappings(node_id.as_deref(), listen, workers)?;
            bridge_connect(&cli.key, &mappings).await
        }
        Command::ShardServe {
            runner,
            model,
            range,
            context,
            gpu_layers,
            budget_bytes,
            expected_sha256,
            source_sha256,
            ref allow_peers,
            profiles,
        } => {
            let config = ShardServeConfig {
                runner,
                model,
                begin: range.begin,
                end: range.end,
                context,
                gpu_layers,
                budget_bytes,
                expected_sha256,
                source_sha256,
            };
            shard_serve(&cli.key, config, &parse_node_ids(allow_peers)?, profiles).await
        }
        Command::ShardRingWorker {
            runner,
            model,
            range,
            context,
            gpu_layers,
            budget_bytes,
            expected_sha256,
            source_sha256,
            upstream,
            origin,
            next,
            profiles,
        } => {
            let config = RingWorkerConfig {
                runner,
                model,
                begin: range.begin,
                end: range.end,
                context,
                gpu_layers,
                budget_bytes,
                expected_sha256,
                source_sha256,
                upstream,
                origin,
                next,
            };
            shard_ring_worker(&cli.key, config, profiles).await
        }
        Command::Worker {
            rpc_server,
            rpc_port,
            rpc_args,
            leader,
            allow_peers,
            heartbeat_interval,
            leader_heartbeat_timeout,
        } => {
            let config = WorkerConfig {
                key_path: cli.key,
                cluster_id: cli.cluster,
                rpc_server,
                rpc_port,
                rpc_args,
                leader: leader.as_deref().map(network::parse_node_id).transpose()?,
                allowed_peers: parse_node_ids(&allow_peers)?,
                heartbeat_interval,
                leader_heartbeat_timeout: zero_disables(leader_heartbeat_timeout),
            };
            until_ctrl_c(worker::run_worker(config)).await
        }
        Command::Leader(args) => {
            let config = args.into_config(cli.key, cli.cluster)?;
            // Not wrapped in until_ctrl_c: the leader handles Ctrl-C itself to stop its parts in order.
            leader::run_leader_process(config).await
        }
        Command::Prompt { api, ref text } => {
            println!("{}", api::prompt(api, text).await?);
            Ok(())
        }
        Command::History { command } => history(command),
    }
}

fn history(command: HistoryCommand) -> anyhow::Result<()> {
    match command {
        HistoryCommand::Export {
            conversation,
            data_dir,
        } => {
            let stored = HistoryStore::load(&data_dir, &conversation)?;
            println!("{}", serde_json::to_string_pretty(&stored.export())?);
        }
        HistoryCommand::Inspect {
            conversation,
            data_dir,
        } => {
            let stored = HistoryStore::load(&data_dir, &conversation)?;
            let export = stored.export();
            println!("conversation: {}", export.conversation_id);
            println!("cluster:      {}", export.cluster_id);
            println!("model:        {}", export.model.model_hash);
            println!("epoch:        {}", export.epoch);
            println!("entries:      {}", export.committed_len);
            for job in &export.jobs {
                let status = match job.status {
                    JobStatus::Completed => "completed",
                    JobStatus::Interrupted => "INTERRUPTED",
                };
                println!(
                    "  {} {status}, {} output bytes committed",
                    job.request_id, job.committed_output_offset
                );
            }
        }
    }
    Ok(())
}

/// Sends logs to stderr so stdout carries only intentional CLI output such as the node ID.
fn init_logging(debug: bool) {
    let mut filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    if debug {
        filter = filter.add_directive("llm_cluster=debug".parse().expect("constant log directive"));
    }
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_thread_ids(debug)
        .with_file(debug)
        .with_line_number(debug)
        .init();
}

async fn listen(key: &Path, cluster: &str) -> anyhow::Result<()> {
    let endpoint = network::bind_endpoint(key, vec![CONTROL_ALPN.to_vec()]).await?;
    print_node_id(&endpoint);
    let local = network::local_hello(&endpoint, cluster, EMPTY_LOG_INDEX);
    let serve = network::serve_control(endpoint.clone(), local, new_peer_table());
    run_until_ctrl_c(&endpoint, serve).await
}

async fn connect(key: &Path, cluster: &str, node_id: &str) -> anyhow::Result<()> {
    let remote = network::parse_node_id(node_id)?;
    let endpoint = network::bind_endpoint(key, vec![CONTROL_ALPN.to_vec()]).await?;
    let local = network::local_hello(&endpoint, cluster, EMPTY_LOG_INDEX);
    let session = network::dial_peer(&endpoint, remote, &local, &new_peer_table()).await?;
    println!("peer hello: {:?}", session.hello());
    session
        .connection()
        .close(CLOSE_CODE_DONE.into(), CLOSE_REASON_DONE);
    endpoint.close().await;
    Ok(())
}

async fn bridge_serve(
    key: &Path,
    target: SocketAddr,
    allow_peers: &[iroh::EndpointId],
) -> anyhow::Result<()> {
    let allowed = rpc_bridge::PeerAllowlist::new(allow_peers.iter().copied())?;
    let endpoint = network::bind_endpoint(key, vec![RPC_BRIDGE_ALPN.to_vec()]).await?;
    print_node_id(&endpoint);
    let serve = rpc_bridge::serve_worker(endpoint.clone(), target, allowed);
    run_until_ctrl_c(&endpoint, serve).await
}

async fn shard_serve(
    key: &Path,
    config: ShardServeConfig,
    allow_peers: &[iroh::EndpointId],
    profiles: WorkerProfileArgs,
) -> anyhow::Result<()> {
    let allowed = rpc_bridge::PeerAllowlist::new(allow_peers.iter().copied())?;
    let control = profiles.bind().await?;
    let endpoint = network::bind_endpoint(key, vec![RPC_BRIDGE_ALPN.to_vec()]).await?;
    print_node_id(&endpoint);
    let serve = serve_shard(endpoint.clone(), config, allowed);
    run_worker_until_ctrl_c(&endpoint, serve, control).await
}

async fn shard_ring_worker(
    key: &Path,
    config: RingWorkerConfig,
    profiles: WorkerProfileArgs,
) -> anyhow::Result<()> {
    let control = profiles.bind().await?;
    let endpoint = network::bind_endpoint(key, vec![RING_ALPN.to_vec()]).await?;
    print_node_id(&endpoint);
    let serve = serve_ring_worker(endpoint.clone(), config);
    run_worker_until_ctrl_c(&endpoint, serve, control).await
}

async fn run_worker_until_ctrl_c(
    endpoint: &Endpoint,
    worker: impl Future<Output = anyhow::Result<()>>,
    control: Option<llm_cluster::runtime::worker_profile::WorkerProfileControl>,
) -> anyhow::Result<()> {
    let result = match control {
        Some(control) => {
            control
                .run(worker, async { Ok(tokio::signal::ctrl_c().await?) })
                .await
        }
        None => until_ctrl_c(worker).await,
    };
    endpoint.close().await;
    result
}

fn current_unix_ms() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

fn read_ring_json<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    use std::io::Read;
    const MAX_BYTES: u64 = 8 * 1024 * 1024;
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= MAX_BYTES, "ring input exceeds 8 MiB");
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

/// A half-open layer range `[begin, end)` given as `BEGIN:END`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LayerRange {
    begin: u32,
    end: u32,
}

/// Parses `--range BEGIN:END`, refusing an empty or reversed range.
fn parse_layer_range(text: &str) -> Result<LayerRange, String> {
    let invalid = || format!("invalid layer range {text:?}: expected BEGIN:END, e.g. 0:32");
    let (begin, end) = text.split_once(':').ok_or_else(invalid)?;
    let begin: u32 = begin.trim().parse().map_err(|_| invalid())?;
    let end: u32 = end.trim().parse().map_err(|_| invalid())?;
    if begin >= end {
        return Err(format!(
            "invalid layer range {text:?}: begin must be below end"
        ));
    }
    Ok(LayerRange { begin, end })
}

/// Parses every `--allow-peer` value, naming the first invalid one.
fn parse_node_ids(ids: &[String]) -> anyhow::Result<Vec<iroh::EndpointId>> {
    ids.iter().map(|id| network::parse_node_id(id)).collect()
}

/// Parses a node ID flag such as `--upstream`, for clap.
fn parse_node_id_arg(text: &str) -> Result<iroh::EndpointId, String> {
    network::parse_node_id(text).map_err(|error| format!("{error:#}"))
}

/// Parses `--worker NODE_ID=ADDR`. The loopback check happens when the listener is bound.
fn parse_worker_mapping(text: &str) -> Result<rpc_bridge::BridgeMapping, String> {
    let (id, addr) = text.split_once('=').ok_or_else(|| {
        format!("invalid worker {text:?}: expected NODE_ID=ADDR, e.g. <node id>=127.0.0.1:50053")
    })?;
    let worker = network::parse_node_id(id.trim())
        .map_err(|error| format!("invalid worker {text:?}: bad node ID: {error:#}"))?;
    let listen = addr.trim().parse().map_err(|error| {
        format!("invalid worker {text:?}: bad listen address {addr:?}: {error}")
    })?;
    Ok(rpc_bridge::BridgeMapping { worker, listen })
}

/// The bridges `bridge-connect` should run: the single positional worker on `listen`, or every
/// `--worker` mapping. Clap guarantees exactly one form was given.
fn bridge_mappings(
    node_id: Option<&str>,
    listen: SocketAddr,
    workers: &[rpc_bridge::BridgeMapping],
) -> anyhow::Result<Vec<rpc_bridge::BridgeMapping>> {
    match node_id {
        Some(node_id) => Ok(vec![rpc_bridge::BridgeMapping {
            worker: network::parse_node_id(node_id)?,
            listen,
        }]),
        None => Ok(workers.to_vec()),
    }
}

async fn bridge_connect(key: &Path, mappings: &[rpc_bridge::BridgeMapping]) -> anyhow::Result<()> {
    let endpoint = network::bind_endpoint(key, vec![RPC_BRIDGE_ALPN.to_vec()]).await?;
    let lead = async {
        let bridges = rpc_bridge::LeaderBridges::bind(endpoint.clone(), mappings).await?;
        println!("{}", bridges_report(&bridges.addrs())?);
        bridges.serve().await
    };
    run_until_ctrl_c(&endpoint, lead).await
}

/// The line `bridge-connect` prints once bound: `{"bridges":[{"worker":..,"listen":..},..]}`.
#[derive(Serialize)]
struct BridgesReport {
    bridges: Vec<BridgeReport>,
}

/// One worker in [`BridgesReport`], fields in the documented order.
#[derive(Serialize)]
struct BridgeReport {
    worker: String,
    listen: String,
}

/// Renders the bound mapping as the single JSON line placement scripts read.
fn bridges_report(bound: &[rpc_bridge::BridgeMapping]) -> anyhow::Result<String> {
    let report = BridgesReport {
        bridges: bound
            .iter()
            .map(|bridge| BridgeReport {
                worker: bridge.worker.to_string(),
                listen: bridge.listen.to_string(),
            })
            .collect(),
    };
    serde_json::to_string(&report).context("failed to render the bridge mapping")
}

/// Runs `task` until it finishes or Ctrl-C arrives, then closes `endpoint` so peers see a clean shutdown.
async fn run_until_ctrl_c(
    endpoint: &Endpoint,
    task: impl Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let result = until_ctrl_c(task).await;
    endpoint.close().await;
    result
}

/// Runs `task` until it finishes or Ctrl-C arrives; a cancelled `task` must clean up when dropped.
async fn until_ctrl_c(task: impl Future<Output = anyhow::Result<()>>) -> anyhow::Result<()> {
    tokio::select! {
        result = task => result,
        signal = tokio::signal::ctrl_c() => signal.map_err(anyhow::Error::from),
    }
}

/// Prints the node ID in the one format peers and bots copy into `connect` / `bridge-connect`.
fn print_node_id(endpoint: &Endpoint) {
    println!("node id: {}", endpoint.id());
}

fn new_peer_table() -> SharedPeerTable {
    Arc::new(Mutex::new(PeerTable::new()))
}

/// Parses durations such as `500ms`, `2s` or `5m`; a bare number means seconds.
fn parse_duration(text: &str) -> Result<Duration, String> {
    let invalid = || format!("invalid duration {text:?}: expected e.g. 500ms, 2s or 5m");
    let trimmed = text.trim();
    let unit_start = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(unit_start);
    let value: u64 = digits.parse().map_err(|_| invalid())?;
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "" | "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(SECONDS_PER_MINUTE)
            .map(Duration::from_secs)
            .ok_or_else(invalid),
        _ => Err(invalid()),
    }
}

