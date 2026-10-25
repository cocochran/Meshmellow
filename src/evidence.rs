//! Post-run verification of externally collected RunEvidence against the launch plan.
//!
//! The leader never fabricates evidence: an external harness writes [`RunEvidence`] JSON to a
//! file after a fresh inference run, and [`verify_run_evidence`] waits for it, then checks it with
//! [`planning::verify_output`].

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail};
use tokio::io::AsyncReadExt;
use tokio::time::Instant;

use crate::launch::LaunchPlan;
use crate::planning;
use crate::verification::RunEvidence;

/// How long the leader waits for the evidence file unless configured otherwise.
pub const DEFAULT_EVIDENCE_WAIT: Duration = Duration::from_secs(120);

/// How often the evidence file is re-read while waiting for it.
const EVIDENCE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Evidence reports are small summaries; cap reads even if a configured file keeps growing.
const MAX_EVIDENCE_BYTES: usize = 1024 * 1024;

/// Where to find the run evidence and how long to wait for it.
#[derive(Debug, Clone)]
pub struct EvidenceConfig {
    /// File the external harness writes [`RunEvidence`] JSON to.
    pub path: PathBuf,
    /// Longest time to wait for the file to exist and parse.
    pub wait: Duration,
}

#[derive(Debug)]
enum EvidenceReadError {
    Retry(String),
    TooLarge,
}

/// Waits for the run evidence file, then verifies it against the launch plan.
///
/// Polls until `config.path` exists and parses as [`RunEvidence`] (a missing or half-written file
/// is retried, not fatal) or `config.wait` elapses. The error then names either the timeout (with
/// the last read or parse error, if any) or the failed `VerificationError`. Never fabricates
/// evidence. Cancel-safe: dropping the future leaves nothing behind.
pub async fn verify_run_evidence(
    launch: &LaunchPlan,
    config: &EvidenceConfig,
) -> anyhow::Result<()> {
    let evidence = wait_for_evidence(config).await?;
    planning::verify_output(&launch.profile, &launch.plan, &evidence, launch.min_peers)
        .map_err(|error| anyhow!("post-run verification failed: {error:?}"))
}

async fn wait_for_evidence(config: &EvidenceConfig) -> anyhow::Result<RunEvidence> {
    let deadline = Instant::now() + config.wait;
    let mut last_error = None;
    loop {
        match read_evidence(&config.path).await {
            Ok(Some(evidence)) => return Ok(evidence),
            Ok(None) => {}
            Err(EvidenceReadError::Retry(error)) => last_error = Some(error),
            Err(EvidenceReadError::TooLarge) => {
                bail!(
                    "run evidence at {} exceeds {MAX_EVIDENCE_BYTES} bytes",
                    config.path.display()
                );
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!(timeout_message(config, last_error.as_deref()));
        }
        tokio::time::sleep(remaining.min(EVIDENCE_POLL_INTERVAL)).await;
    }
}

/// Reads the evidence file: `Ok(None)` while it does not exist yet, `Err` with a description
/// while it cannot be read or parsed (for example, because it is still being written).
async fn read_evidence(path: &Path) -> Result<Option<RunEvidence>, EvidenceReadError> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(EvidenceReadError::Retry(format!("failed to read: {error}"))),
    };
    let mut bytes = Vec::new();
    file.take((MAX_EVIDENCE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| EvidenceReadError::Retry(format!("failed to read: {error}")))?;
    if bytes.len() > MAX_EVIDENCE_BYTES {
        return Err(EvidenceReadError::TooLarge);
    }
    let text = String::from_utf8(bytes)
        .map_err(|error| EvidenceReadError::Retry(format!("evidence is not UTF-8: {error}")))?;
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|error| EvidenceReadError::Retry(format!("invalid run evidence JSON: {error}")))
}

fn timeout_message(config: &EvidenceConfig, last_error: Option<&str>) -> String {
    let mut message = format!(
        "no run evidence at {} within {:?}",
        config.path.display(),
        config.wait
    );
    if let Some(error) = last_error {
        message.push_str(&format!(" (last error: {error})"));
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::launch::DEFAULT_MIN_PEERS;

    const PLAN: &str = include_str!("../examples/launcher-plan.json");
    const PROFILE: &str = include_str!("../examples/launcher-profile.json");
    const REMOTE_ID: &str = "0c668f7dd608a97bde84ae7758ac63a9775e6cb5b3e663ce9c3bd3371139a4a9";
    const SHORT_WAIT: Duration = Duration::from_millis(300);

    fn launch() -> LaunchPlan {
        LaunchPlan::from_parts(
            serde_json::from_str(PLAN).unwrap(),
            serde_json::from_str(PROFILE).unwrap(),
            DEFAULT_MIN_PEERS,
        )
        .unwrap()
    }

    fn evidence_json(remote_compute_events: u64) -> String {
        serde_json::json!({
            "model_hash": "synthetic-model",
            "runtime_id": "synthetic-runtime",
            "context_tokens": 8192,
            "devices": [
                {
                    "peer_id": REMOTE_ID,
                    "peak_bytes": 250_000_000,
                    "compute_events": remote_compute_events,
                },
                {"peer_id": "leader", "peak_bytes": 250_000_000, "compute_events": 3},
            ],
        })
        .to_string()
    }

    fn temp_config() -> EvidenceConfig {
        let name = format!("meshthing-evidence-{}.json", uuid::Uuid::new_v4());
        EvidenceConfig {
            path: std::env::temp_dir().join(name),
            wait: SHORT_WAIT,
        }
    }

    #[tokio::test]
    async fn present_valid_evidence_passes() {
        let config = temp_config();
        std::fs::write(&config.path, evidence_json(5)).unwrap();

        let result = verify_run_evidence(&launch(), &config).await;

        std::fs::remove_file(&config.path).unwrap();
        result.expect("valid evidence");
    }

    #[tokio::test]
    async fn verification_error_variant_is_named() {
        let config = temp_config();
        std::fs::write(&config.path, evidence_json(0)).unwrap();

        let error = verify_run_evidence(&launch(), &config).await.unwrap_err();

        std::fs::remove_file(&config.path).unwrap();
        assert!(error.to_string().contains("NoCompute"), "{error}");
    }

    #[tokio::test]
    async fn missing_file_reads_as_not_yet() {
        let config = temp_config();

        assert!(read_evidence(&config.path).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_evidence_is_rejected_at_the_read_limit() {
        let config = temp_config();
        std::fs::write(&config.path, vec![b'x'; MAX_EVIDENCE_BYTES + 1]).unwrap();

        let result = read_evidence(&config.path).await;

        std::fs::remove_file(&config.path).unwrap();
        assert!(matches!(result, Err(EvidenceReadError::TooLarge)));
    }

    #[tokio::test]
    async fn timeout_reports_the_last_parse_error() {
        let config = temp_config();
        std::fs::write(&config.path, "{\"model_hash\": ").unwrap();

        let error = verify_run_evidence(&launch(), &config).await.unwrap_err();

        std::fs::remove_file(&config.path).unwrap();
        let message = error.to_string();
        assert!(message.contains("no run evidence at"), "{message}");
        assert!(message.contains("invalid run evidence JSON"), "{message}");
    }

    #[test]
    fn timeout_without_any_file_has_no_last_error() {
        let config = temp_config();

        let message = timeout_message(&config, None);

        assert!(message.contains(&config.path.display().to_string()));
        assert!(!message.contains("last error"), "{message}");
    }
}
