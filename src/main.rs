//! MeshThing command-line entry point: plan, run a worker or the leader, send prompts, or debug
//! the mesh with the lower-level node and bridge commands.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Parser, Subcommand};
use iroh::Endpoint;
use llm_cluster::history_sync::store::{self, HistoryStore, JobStatus};
use llm_cluster::leader::{self, LeaderConfig};
use llm_cluster::network::{self, SharedPeerTable};
use llm_cluster::node::PeerTable;
use llm_cluster::protocol::{CONTROL_ALPN, RPC_BRIDGE_ALPN};
use llm_cluster::worker::{self, WorkerConfig};
use llm_cluster::{api, rpc_bridge};
use tracing_subscriber::EnvFilter;

/// Log filter used when `RUST_LOG` is unset.
const DEFAULT_LOG_FILTER: &str = "info";

/// Last log index announced in hellos until the replicated log exists.
const EMPTY_LOG_INDEX: u64 = 0;

/// QUIC application close code for a normal, intentional shutdown.
const CLOSE_CODE_DONE: u32 = 0;

/// QUIC close reason sent alongside [`CLOSE_CODE_DONE`].
const CLOSE_REASON_DONE: &[u8] = b"done";

/// `--heartbeat-interval` default; matches [`worker::DEFAULT_HEARTBEAT_INTERVAL`].
const DEFAULT_HEARTBEAT_INTERVAL_ARG: &str = "2s";

/// `--worker-wait` default; matches [`leader::DEFAULT_WORKER_WAIT`].
const DEFAULT_WORKER_WAIT_ARG: &str = "60s";

/// `--llama-ready-timeout` default; matches [`leader::DEFAULT_LLAMA_READY_TIMEOUT`].
const DEFAULT_LLAMA_READY_TIMEOUT_ARG: &str = "300s";

/// `--evidence-wait` default; matches [`llm_cluster::evidence::DEFAULT_EVIDENCE_WAIT`].
const DEFAULT_EVIDENCE_WAIT_ARG: &str = "120s";

const SECONDS_PER_MINUTE: u64 = 60;

#[derive(Debug, Parser)]
#[command(
    name = "meshthing",
    about = "Peer-to-peer mesh for splitting one LLM across devices"
)]
struct Cli {
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
    /// Leader side of the RPC bridge: expose a remote worker's RPC server on a local port.
    BridgeConnect {
        /// Node ID printed by the worker's `bridge-serve`.
        node_id: String,
        /// Local address to accept RPC clients on.
        #[arg(long, default_value = "127.0.0.1:50052")]
        listen: SocketAddr,
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
    },
    /// Run the main process: wait for planned workers, start llama-server, and serve the job API.
    Leader {
        /// JSON produced by the plan command.
        #[arg(long)]
        plan: PathBuf,
        /// Planning profile JSON the plan was built from; the plan is validated against it before
        /// anything launches.
        #[arg(long)]
        profile: PathBuf,
        /// Require at least this many contributors, as for the plan command.
        #[arg(long, default_value_t = launch::DEFAULT_MIN_PEERS)]
        min_peers: usize,
        /// RunEvidence JSON an external harness writes after a fresh inference run; when given,
        /// the leader verifies it against the plan and shuts down if verification fails.
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
        /// The llama-server binary.
        #[arg(long)]
        llama_server: PathBuf,
        /// Extra argument for llama-server, after the plan's arguments (e.g. `--llama-arg
        /// --cache-ram --llama-arg 0`); repeatable. The leader's own --host/--port always win.
        #[arg(long = "llama-arg", allow_hyphen_values = true)]
        llama_args: Vec<String>,
        /// Internal loopback port for llama-server.
        #[arg(long, default_value_t = leader::DEFAULT_LLAMA_PORT)]
        llama_port: u16,
        /// Loopback address to serve the job API on.
        #[arg(long, default_value_t = leader::DEFAULT_API_ADDR)]
        api: SocketAddr,
        /// How long to wait for every planned worker to connect, e.g. `60s` or `2m`.
        #[arg(long, default_value = DEFAULT_WORKER_WAIT_ARG, value_parser = parse_duration)]
        worker_wait: Duration,
        /// How long llama-server may take to load the model and answer /health.
        #[arg(long, default_value = DEFAULT_LLAMA_READY_TIMEOUT_ARG, value_parser = parse_duration)]
        llama_ready_timeout: Duration,
    },
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
    init_logging();
    let cli = Cli::parse();
    match cli.command {
        Command::Verify {
            plan,
            profile,
            evidence,
            min_peers,
        } => {
            let output: llm_cluster::planning::PlanningOutput =
                serde_json::from_str(&std::fs::read_to_string(plan)?)?;
            let request: llm_cluster::planning::PlanningRequest =
                serde_json::from_str(&std::fs::read_to_string(profile)?)?;
            let evidence = serde_json::from_str(&std::fs::read_to_string(evidence)?)?;
            llm_cluster::planning::verify_output(&request, &output, &evidence, min_peers)
                .map_err(|error| anyhow::anyhow!("verification failed: {error:?}"))?;
            println!("{{\"verified\":true}}");
            Ok(())
        }
        Command::Plan { profile, min_peers } => {
            let input = std::fs::read_to_string(profile)?;
            let request = serde_json::from_str(&input)?;
            let output = llm_cluster::planning::build_plan(&request, min_peers)
                .map_err(|error| anyhow::anyhow!("planning failed: {error:?}"))?;
            println!("{}", serde_json::to_string_pretty(&output)?);
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
        } => bridge_connect(&cli.key, node_id, listen).await,
        Command::Worker {
            rpc_server,
            rpc_port,
            rpc_args,
            leader,
            allow_peers,
            heartbeat_interval,
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
            };
            until_ctrl_c(worker::run_worker(config)).await
        }
        Command::Leader {
            plan,
            profile,
            min_peers,
            evidence_file,
            evidence_wait,
            llama_server,
            llama_args,
            llama_port,
            api,
            worker_wait,
            llama_ready_timeout,
        } => {
            // Not wrapped in until_ctrl_c: the leader handles Ctrl-C itself to stop its parts in order.
            leader::run_leader_process(LeaderConfig {
                key_path: cli.key,
                cluster_id: cli.cluster,
                plan_path: plan,
                profile_path: profile,
                min_peers,
                evidence: evidence_file.map(|path| EvidenceConfig {
                    path,
                    wait: evidence_wait,
                }),
                llama_server,
                extra_llama_args: llama_args,
                llama_port,
                api_addr: api,
                worker_wait,
                llama_ready_timeout,
            })
            .await
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
fn init_logging() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
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
    let (connection, hello) =
        network::dial_peer(&endpoint, remote, &local, &new_peer_table()).await?;
    println!("peer hello: {hello:?}");
    connection.close(CLOSE_CODE_DONE.into(), CLOSE_REASON_DONE);
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

/// Parses every `--allow-peer` value, naming the first invalid one.
fn parse_node_ids(ids: &[String]) -> anyhow::Result<Vec<iroh::EndpointId>> {
    ids.iter().map(|id| network::parse_node_id(id)).collect()
}

async fn bridge_connect(key: &Path, node_id: &str, listen: SocketAddr) -> anyhow::Result<()> {
    let worker = network::parse_node_id(node_id)?;
    let endpoint = network::bind_endpoint(key, vec![RPC_BRIDGE_ALPN.to_vec()]).await?;
    let lead = rpc_bridge::run_leader(endpoint.clone(), worker, listen);
    run_until_ctrl_c(&endpoint, lead).await
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

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    use llm_cluster::evidence;

    use super::*;

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("2s"), Ok(Duration::from_secs(2)));
        assert_eq!(parse_duration("7"), Ok(Duration::from_secs(7)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
    }

    #[test]
    fn rejects_malformed_durations() {
        for text in ["", "s", "1h", "-1s", "1.5s", "99999999999999999999m"] {
            assert!(parse_duration(text).is_err(), "{text:?} should be rejected");
        }
    }

    #[test]
    fn duration_defaults_match_library_constants() {
        assert_eq!(
            parse_duration(DEFAULT_HEARTBEAT_INTERVAL_ARG),
            Ok(worker::DEFAULT_HEARTBEAT_INTERVAL)
        );
        assert_eq!(
            parse_duration(DEFAULT_WORKER_WAIT_ARG),
            Ok(leader::DEFAULT_WORKER_WAIT)
        );
        assert_eq!(
            parse_duration(DEFAULT_LLAMA_READY_TIMEOUT_ARG),
            Ok(leader::DEFAULT_LLAMA_READY_TIMEOUT)
        );
        assert_eq!(
            parse_duration(DEFAULT_EVIDENCE_WAIT_ARG),
            Ok(evidence::DEFAULT_EVIDENCE_WAIT)
        );
    }

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn worker_collects_repeated_rpc_args() {
        let cli = Cli::try_parse_from([
            "meshthing",
            "worker",
            "--rpc-server",
            "rpc",
            "--rpc-arg",
            "-t",
            "--rpc-arg",
            "4",
        ])
        .expect("worker args");

        let Command::Worker {
            rpc_args, rpc_port, ..
        } = cli.command
        else {
            panic!("expected worker, got {:?}", cli.command);
        };
        assert_eq!(rpc_args, ["-t", "4"]);
        assert_eq!(rpc_port, rpc_bridge::DEFAULT_RPC_PORT);
    }

    #[test]
    fn bridge_serve_requires_an_allowed_peer() {
        let missing = Cli::try_parse_from(["meshthing", "bridge-serve"]);
        assert!(
            missing.is_err(),
            "bridge-serve without --allow-peer must be refused"
        );

        let cli = Cli::try_parse_from([
            "meshthing",
            "bridge-serve",
            "--allow-peer",
            "a",
            "--allow-peer",
            "b",
        ])
        .expect("bridge-serve args");
        let Command::BridgeServe { allow_peers, .. } = cli.command else {
            panic!("expected bridge-serve, got {:?}", cli.command);
        };
        assert_eq!(allow_peers, ["a", "b"]);
    }

    #[test]
    fn allow_peer_values_must_be_node_ids() {
        let valid = iroh::SecretKey::generate().public();
        let parsed = parse_node_ids(&[valid.to_string()]).expect("valid node id");
        assert_eq!(parsed, [valid]);
        assert!(parse_node_ids(&["not-a-node-id".to_owned()]).is_err());
    }

    #[test]
    fn leader_and_prompt_use_loopback_api_defaults() {
        let leader = Cli::try_parse_from([
            "meshthing",
            "leader",
            "--plan",
            "plan.json",
            "--profile",
            "profile.json",
            "--llama-server",
            "llama-server",
            "--worker-wait",
            "5s",
        ])
        .expect("leader args");
        let prompt = Cli::try_parse_from(["meshthing", "prompt", "hello there"]).expect("prompt");

        let Command::Leader {
            api,
            llama_port,
            worker_wait,
            ..
        } = leader.command
        else {
            panic!("expected leader, got {:?}", leader.command);
        };
        assert_eq!(api, leader::DEFAULT_API_ADDR);
        assert_eq!(llama_port, leader::DEFAULT_LLAMA_PORT);
        assert_eq!(worker_wait, Duration::from_secs(5));
        let Command::Prompt { api, text } = prompt.command else {
            panic!("expected prompt, got {:?}", prompt.command);
        };
        assert_eq!(api, leader::DEFAULT_API_ADDR);
        assert_eq!(text, "hello there");
    }

    /// `leader` with its required flags plus `extra`.
    fn parse_leader(extra: &[&str]) -> Result<Command, clap::Error> {
        let required = [
            "meshthing",
            "leader",
            "--plan",
            "plan.json",
            "--profile",
            "profile.json",
            "--llama-server",
            "llama-server",
        ];
        Cli::try_parse_from(required.iter().chain(extra)).map(|cli| cli.command)
    }

    #[test]
    fn leader_verification_defaults_off_with_default_min_peers() {
        let command = parse_leader(&[]).expect("leader args");

        let Command::Leader {
            min_peers,
            evidence_file,
            evidence_wait,
            llama_args,
            ..
        } = command
        else {
            panic!("expected leader, got {command:?}");
        };
        assert_eq!(min_peers, launch::DEFAULT_MIN_PEERS);
        assert_eq!(evidence_file, None);
        assert_eq!(evidence_wait, evidence::DEFAULT_EVIDENCE_WAIT);
        assert!(llama_args.is_empty());
    }

    #[test]
    fn leader_parses_verification_and_llama_args() {
        let command = parse_leader(&[
            "--min-peers",
            "3",
            "--evidence-file",
            "evidence.json",
            "--evidence-wait",
            "2m",
            "--llama-arg",
            "--cache-ram",
            "--llama-arg",
            "0",
        ])
        .expect("leader args");

        let Command::Leader {
            min_peers,
            evidence_file,
            evidence_wait,
            llama_args,
            ..
        } = command
        else {
            panic!("expected leader, got {command:?}");
        };
        assert_eq!(min_peers, 3);
        assert_eq!(evidence_file, Some(PathBuf::from("evidence.json")));
        assert_eq!(evidence_wait, Duration::from_secs(120));
        assert_eq!(llama_args, ["--cache-ram", "0"]);
    }

    #[test]
    fn evidence_wait_requires_an_evidence_file() {
        assert!(
            parse_leader(&["--evidence-wait", "5s"]).is_err(),
            "--evidence-wait without --evidence-file must be refused"
        );
    }
}
