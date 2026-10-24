//! Pre-launch plan validation and post-run evidence verification against aarush-codex's
//! launcher fixtures (`examples/launcher-plan.json` + `examples/launcher-profile.json`).

use std::path::{Path, PathBuf};
use std::time::Duration;

use llm_cluster::evidence::{EvidenceConfig, verify_run_evidence};
use llm_cluster::launch::{DEFAULT_MIN_PEERS, LaunchPlan, RemoteWorker};
use llm_cluster::network::parse_node_id;
use serde_json::{Value, json};

const REMOTE_ID: &str = "0c668f7dd608a97bde84ae7758ac63a9775e6cb5b3e663ce9c3bd3371139a4a9";
/// Generous upper bound for evidence that does arrive, so slow CI never flakes.
const EVIDENCE_WAIT: Duration = Duration::from_secs(10);
/// How long the simulated harness takes to write its evidence.
const WRITE_DELAY: Duration = Duration::from_millis(300);
/// Wait used when no evidence ever appears.
const SHORT_WAIT: Duration = Duration::from_millis(400);

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(name)
}

fn fixture_text(name: &str) -> String {
    std::fs::read_to_string(fixture_path(name)).expect("fixture file")
}

fn fixture(name: &str) -> Value {
    serde_json::from_str(&fixture_text(name)).expect("fixture JSON")
}

fn from_values(plan: Value, profile: Value) -> anyhow::Result<LaunchPlan> {
    LaunchPlan::from_parts(
        serde_json::from_value(plan).expect("plan shape"),
        serde_json::from_value(profile).expect("profile shape"),
        DEFAULT_MIN_PEERS,
    )
}

fn rejection(plan: Value, profile: Value) -> String {
    from_values(plan, profile)
        .expect_err("plan should be rejected")
        .to_string()
}

fn fixture_launch() -> LaunchPlan {
    LaunchPlan::load(
        &fixture_path("launcher-plan.json"),
        &fixture_path("launcher-profile.json"),
        DEFAULT_MIN_PEERS,
    )
    .expect("fixture plan loads")
}

#[test]
fn valid_plan_loads_with_workers_from_runtime_devices() {
    let launch = fixture_launch();

    let expected: Vec<_> = launch
        .plan
        .runtime_devices
        .iter()
        .filter_map(|device| {
            device.rpc_endpoint.map(|rpc_endpoint| RemoteWorker {
                id: parse_node_id(&device.peer_id).expect("remote peer is a node ID"),
                rpc_endpoint,
            })
        })
        .collect();
    assert_eq!(launch.workers, expected);
    assert_eq!(launch.workers.len(), 1);
    assert_eq!(launch.workers[0].id, parse_node_id(REMOTE_ID).unwrap());
    assert_eq!(launch.server_args(), launch.plan.server_args.as_slice());
}

#[test]
fn legacy_plan_without_runtime_devices_must_be_regenerated() {
    let mut plan = fixture("launcher-plan.json");
    plan.as_object_mut().unwrap().remove("runtime_devices");

    let message = rejection(plan, fixture("launcher-profile.json"));

    assert!(message.contains("regenerate the plan"), "{message}");
}

#[test]
fn model_hash_mismatch_is_named() {
    let mut profile = fixture("launcher-profile.json");
    profile["model"]["model_hash"] = json!("a-different-model");

    let message = rejection(fixture("launcher-plan.json"), profile);

    assert!(message.contains("IdentityMismatch"), "{message}");
}

#[test]
fn empty_server_args_are_rejected() {
    let mut plan = fixture("launcher-plan.json");
    plan["server_args"] = json!([]);

    let message = rejection(plan, fixture("launcher-profile.json"));

    assert!(message.contains("server_args"), "{message}");
}

#[test]
fn malformed_remote_peer_id_is_named() {
    // Rename the remote peer consistently in both files so the plan still validates and only the
    // node-ID parse can fail.
    let bad_id = "not-a-node-id";
    let rename = |name: &str| -> Value {
        serde_json::from_str(&fixture_text(name).replace(REMOTE_ID, bad_id)).unwrap()
    };

    let message = rejection(
        rename("launcher-plan.json"),
        rename("launcher-profile.json"),
    );

    assert!(message.contains(bad_id), "{message}");
}

fn evidence(devices: Value) -> String {
    json!({
        "model_hash": "synthetic-model",
        "runtime_id": "synthetic-runtime",
        "context_tokens": 8192,
        "devices": devices,
    })
    .to_string()
}

fn device(peer_id: &str, peak_bytes: u64, compute_events: u64) -> Value {
    json!({"peer_id": peer_id, "peak_bytes": peak_bytes, "compute_events": compute_events})
}

fn valid_evidence() -> String {
    evidence(json!([
        device(REMOTE_ID, 250_000_000, 4),
        device("leader", 250_000_000, 4),
    ]))
}

/// A fresh evidence path that no other test uses.
fn evidence_config(wait: Duration) -> EvidenceConfig {
    let name = format!("meshthing-launch-test-{}.json", uuid::Uuid::new_v4());
    EvidenceConfig {
        path: std::env::temp_dir().join(name),
        wait,
    }
}

/// Writes each body to `path` in turn, `WRITE_DELAY` apart, like a harness finishing late.
fn write_later(path: &Path, bodies: Vec<String>) -> tokio::task::JoinHandle<()> {
    let path = path.to_owned();
    tokio::spawn(async move {
        for body in bodies {
            tokio::time::sleep(WRITE_DELAY).await;
            tokio::fs::write(&path, body).await.expect("write evidence");
        }
    })
}

async fn verify_after_delay(evidence: String) -> anyhow::Result<()> {
    let config = evidence_config(EVIDENCE_WAIT);
    let writer = write_later(&config.path, vec![evidence]);

    let result = verify_run_evidence(&fixture_launch(), &config).await;

    writer.await.expect("writer task");
    std::fs::remove_file(&config.path).expect("remove evidence");
    result
}

#[tokio::test]
async fn valid_evidence_written_late_passes() {
    verify_after_delay(valid_evidence())
        .await
        .expect("valid evidence passes");
}

#[tokio::test]
async fn bad_evidence_names_the_verification_error() {
    let cases = [
        (
            "NoCompute",
            json!([
                device(REMOTE_ID, 250_000_000, 0),
                device("leader", 250_000_000, 4)
            ]),
        ),
        ("MissingPeer", json!([device("leader", 250_000_000, 4)])),
        (
            "BudgetExceeded",
            json!([
                device(REMOTE_ID, 400_000_000, 4),
                device("leader", 250_000_000, 4)
            ]),
        ),
    ];
    for (variant, devices) in cases {
        let error = verify_after_delay(evidence(devices))
            .await
            .expect_err(variant);

        let message = error.to_string();
        assert!(message.contains(variant), "{variant}: {message}");
    }
}

#[tokio::test]
async fn missing_evidence_times_out_cleanly() {
    let config = evidence_config(SHORT_WAIT);

    let error = verify_run_evidence(&fixture_launch(), &config)
        .await
        .expect_err("no evidence file");

    let message = error.to_string();
    assert!(message.contains("no run evidence at"), "{message}");
    assert!(
        message.contains(&config.path.display().to_string()),
        "{message}"
    );
}

#[tokio::test]
async fn half_written_evidence_is_retried_until_valid() {
    let config = evidence_config(EVIDENCE_WAIT);
    let full = valid_evidence();
    let half = full[..full.len() / 2].to_owned();
    let writer = write_later(&config.path, vec![half, full]);

    let result = verify_run_evidence(&fixture_launch(), &config).await;

    writer.await.expect("writer task");
    std::fs::remove_file(&config.path).expect("remove evidence");
    result.expect("evidence becomes valid");
}
