//! Integration tests for `process`: supervise the `fake_rpc_server` example as a real child
//! process and check readiness, echo, restarts, timeouts and clean shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use llm_cluster::process::{ChildSpec, Readiness, RestartPolicy, Supervised, spawn_supervised};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

const FAKE_RPC_SERVER: &str = "fake_rpc_server";
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const ECHO_PAYLOAD: &[u8] = b"ggml rpc hello";

/// Path of an example binary built by `cargo test` next to this test's `deps` directory.
fn example_bin(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("test executable path");
    let deps = exe.parent().expect("test executable directory");
    deps.join("..")
        .join("examples")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn fast_policy(max_restarts: u32) -> RestartPolicy {
    RestartPolicy {
        max_restarts,
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(200),
    }
}

/// A fake RPC server listening on `listen_port` and probed on `probe_port`.
fn spec(listen_port: u16, probe_port: u16, extra_args: &[&str]) -> ChildSpec {
    let mut args = vec![
        "-H".to_string(),
        "127.0.0.1".to_string(),
        "-p".to_string(),
        listen_port.to_string(),
    ];
    args.extend(extra_args.iter().map(|arg| arg.to_string()));
    ChildSpec {
        name: "fake-rpc-server".to_string(),
        program: example_bin(FAKE_RPC_SERVER),
        args,
        readiness: Readiness::TcpPort(loopback(probe_port)),
        ready_timeout: READY_TIMEOUT,
        restart: fast_policy(0),
    }
}

async fn echo_round_trip(port: u16) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(loopback(port)).await?;
    stream.write_all(ECHO_PAYLOAD).await?;
    let mut reply = vec![0u8; ECHO_PAYLOAD.len()];
    stream.read_exact(&mut reply).await?;
    Ok(reply)
}

/// Polls until the supervisor reports a PID other than `old_pid` and the port echoes again.
async fn wait_for_restart(supervised: &Supervised, old_pid: u32, port: u16) -> u32 {
    loop {
        if let Some(pid) = supervised.id().filter(|pid| *pid != old_pid)
            && echo_round_trip(port)
                .await
                .is_ok_and(|reply| reply == ECHO_PAYLOAD)
        {
            return pid;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(windows)]
fn process_exists(pid: u32) -> bool {
    let output = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
        .output()
        .expect("run tasklist");
    String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run kill -0")
        .success()
}

#[tokio::test]
async fn child_becomes_ready_and_echoes() {
    init_tracing();
    let port = free_port();
    let supervised = spawn_supervised(spec(port, port, &[]))
        .await
        .expect("spawn");
    assert!(supervised.id().is_some(), "running child has a pid");

    let reply = echo_round_trip(port)
        .await
        .expect("echo through child port");
    assert_eq!(reply, ECHO_PAYLOAD);

    supervised.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_kills_child() {
    init_tracing();
    let port = free_port();
    let supervised = spawn_supervised(spec(port, port, &[]))
        .await
        .expect("spawn");
    let pid = supervised.id().expect("pid");
    assert!(process_exists(pid), "child {pid} runs before shutdown");

    supervised.shutdown().await.expect("shutdown");

    assert!(!process_exists(pid), "child {pid} is gone after shutdown");
    assert!(TcpStream::connect(loopback(port)).await.is_err());
}

#[tokio::test]
async fn exited_child_is_restarted_and_ready_again() {
    init_tracing();
    let port = free_port();
    let mut child = spec(port, port, &["--exit-after-ms", "800"]);
    child.restart = fast_policy(5);
    let supervised = spawn_supervised(child).await.expect("spawn");
    let first_pid = supervised.id().expect("pid");

    let restarted_pid = timeout(TEST_TIMEOUT, wait_for_restart(&supervised, first_pid, port))
        .await
        .expect("child restarted in time");

    assert_ne!(restarted_pid, first_pid);
    supervised.shutdown().await.expect("shutdown");
    assert!(
        !process_exists(restarted_pid),
        "restarted child is gone after shutdown"
    );
}

#[tokio::test]
async fn wait_errors_once_restarts_are_exhausted() {
    init_tracing();
    let port = free_port();
    let mut child = spec(port, port, &["--exit-after-ms", "500"]);
    child.restart = fast_policy(2);
    let mut supervised = spawn_supervised(child).await.expect("spawn");

    let error = timeout(TEST_TIMEOUT, supervised.wait())
        .await
        .expect("wait resolves in time")
        .expect_err("exhausted restarts are an error");

    assert!(
        error.to_string().contains("exhausted 2 restarts"),
        "{error:#}"
    );
    assert_eq!(supervised.id(), None);
    supervised
        .shutdown()
        .await
        .expect("shutdown after giving up");
}

#[tokio::test]
async fn ready_timeout_fails_when_nothing_listens() {
    init_tracing();
    let mut child = spec(free_port(), free_port(), &[]);
    child.ready_timeout = Duration::from_millis(500);

    let started = Instant::now();
    let error = spawn_supervised(child)
        .await
        .err()
        .expect("never-ready child is an error");

    assert!(error.to_string().contains("not ready within"), "{error:#}");
    assert!(started.elapsed() < READY_TIMEOUT);
}

#[tokio::test]
async fn child_exiting_before_ready_fails() {
    init_tracing();
    let child = spec(free_port(), free_port(), &["--exit-after-ms", "0"]);

    let error = spawn_supervised(child)
        .await
        .err()
        .expect("early exit is an error");

    assert!(
        error.to_string().contains("before becoming ready"),
        "{error:#}"
    );
}

#[tokio::test]
async fn missing_program_errors_cleanly() {
    init_tracing();
    let port = free_port();
    let mut child = spec(port, port, &[]);
    child.program = std::env::temp_dir().join(format!(
        "llm-cluster-missing-program{}",
        std::env::consts::EXE_SUFFIX
    ));

    let error = spawn_supervised(child)
        .await
        .err()
        .expect("missing program is an error");

    assert!(error.to_string().contains("failed to start"), "{error:#}");
}
