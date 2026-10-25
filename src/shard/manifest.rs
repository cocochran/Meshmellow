//! Shard manifest: which stages make up a sharded model and where each one runs.
//!
//! The leader's front end reads a JSON manifest such as:
//!
//! ```json
//! {
//!   "runner": "/opt/meshthing/shard_runner",
//!   "model_sha256": "<64 hex digits: the original model's SHA-256>",
//!   "layers": 64,
//!   "context": 4096,
//!   "max_tokens": 512,
//!   "chat_template": "chatml",
//!   "stages": [
//!     {"begin": 0, "end": 32, "remote": {"worker": "<iroh endpoint id>"}},
//!     {"begin": 32, "end": 64, "local": {"model": "/models/qwen-32-64.gguf", "budget_bytes": 1073741824}}
//!   ]
//! }
//! ```
//!
//! Each stage object carries `begin`, `end` and exactly one location key (`remote` or `local`).
//! Unknown fields are refused everywhere, so a typo cannot silently drop a setting.
//!
//! [`ShardManifest::validate`] enforces the structural rules (contiguous coverage of
//! `0..layers`, bounds on `context` and `max_tokens`, well-formed hashes, no duplicate worker)
//! and the v1 topology: a single local whole-model stage, or one remote prefix followed by a
//! local final stage. Chained and remote-final stages need ring forwarding (M-27).

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail, ensure};
use iroh::EndpointId;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::shard::wire::{self, MAX_CONTEXT};

/// Smallest context window the shard runner accepts.
pub const MIN_CONTEXT: u32 = 32;

/// Completion length used when the manifest does not set `max_tokens`.
pub const DEFAULT_MAX_TOKENS: u32 = 512;

/// Length of a SHA-256 digest written as hex.
const SHA256_HEX_LEN: usize = 64;

/// Bytes read per chunk while hashing a model file.
const HASH_CHUNK_BYTES: usize = 1 << 20;

/// Ticket for chained and remote-final stages, named in the v1 topology errors.
const RING_FORWARDING_TICKET: &str = "M-27";

/// A sharded model: the runner binary, the model's identity, and its stages in layer order.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardManifest {
    /// `shard_runner` binary used for local stages.
    pub runner: PathBuf,
    /// SHA-256 of the original (unsharded) model, as 64 hex digits. Every stage must prove it
    /// serves a slice of this model.
    pub model_sha256: String,
    /// Total layers in the model.
    pub layers: u32,
    /// Context window passed to every stage, in `MIN_CONTEXT..=MAX_CONTEXT`.
    pub context: u32,
    /// Upper bound on completion tokens; a request may ask for fewer.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Prompt template the front end renders chat messages with. Required: a wrong template
    /// silently degrades output, so it is never assumed.
    pub chat_template: ChatTemplate,
    /// Stages in layer order, covering `0..layers` without gaps or overlap.
    pub stages: Vec<StageSpec>,
}

/// How chat messages become the prompt string the final stage tokenizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatTemplate {
    /// `<|im_start|>{role}\n{content}<|im_end|>\n` per message, then
    /// `<|im_start|>assistant\n`. The only v1 template; the pinned official Qwen template
    /// is future work (M-27).
    Chatml,
}

/// One contiguous layer range and where it runs.
#[derive(Clone, Debug)]
pub struct StageSpec {
    /// First layer (inclusive).
    pub begin: u32,
    /// End of the range (exclusive).
    pub end: u32,
    /// Where the stage runs.
    pub location: StageLocation,
}

/// Where a stage runs. Open for more roles (for example a ring next-hop in M-27), so match it
/// with a wildcard arm outside this crate.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum StageLocation {
    /// On another peer running `shard-serve` (or `bridge-serve` + `shard_endpoint.py`), dialled
    /// on the RPC bridge ALPN.
    Remote {
        /// The worker's Iroh endpoint ID.
        worker: EndpointId,
    },
    /// In a `shard_runner` child process on this machine.
    Local {
        /// The model or exported shard file the runner loads.
        model: PathBuf,
        /// Layers the runner offloads to the GPU.
        gpu_layers: u32,
        /// Runtime buffer budget passed to the runner; must be positive.
        budget_bytes: u64,
        /// SHA-256 of `model`, checked once when the front end starts. It establishes the
        /// stage's identity when the runner reports no `source_sha256` (a plain GGUF).
        artifact_sha256: Option<String>,
        /// Extra environment variables for the runner process (for example
        /// `CUDA_VISIBLE_DEVICES`). Empty by default.
        env: BTreeMap<String, String>,
    },
}

/// The JSON form of a stage: range plus exactly one location key. Kept separate because
/// `deny_unknown_fields` does not combine with `#[serde(flatten)]`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStageSpec {
    begin: u32,
    end: u32,
    #[serde(default)]
    remote: Option<RawRemote>,
    #[serde(default)]
    local: Option<RawLocal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRemote {
    worker: EndpointId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLocal {
    model: PathBuf,
    #[serde(default)]
    gpu_layers: u32,
    budget_bytes: u64,
    #[serde(default)]
    artifact_sha256: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

impl<'de> Deserialize<'de> for StageSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawStageSpec::deserialize(deserializer)?;
        let location = match (raw.remote, raw.local) {
            (Some(RawRemote { worker }), None) => StageLocation::Remote { worker },
            (None, Some(local)) => StageLocation::Local {
                model: local.model,
                gpu_layers: local.gpu_layers,
                budget_bytes: local.budget_bytes,
                artifact_sha256: local.artifact_sha256,
                env: local.env,
            },
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "stage {}..{} needs exactly one of \"remote\" or \"local\"",
                    raw.begin, raw.end
                )));
            }
        };
        Ok(Self {
            begin: raw.begin,
            end: raw.end,
            location,
        })
    }
}

fn default_max_tokens() -> u32 {
    DEFAULT_MAX_TOKENS
}

impl StageSpec {
    /// Whether the stage runs in a local runner process.
    pub fn is_local(&self) -> bool {
        matches!(self.location, StageLocation::Local { .. })
    }

    /// The remote worker serving this stage, if any.
    pub fn remote_worker(&self) -> Option<EndpointId> {
        match self.location {
            StageLocation::Remote { worker } => Some(worker),
            _ => None,
        }
    }
}

impl ShardManifest {
    /// Reads and parses the manifest at `path` (errors name the file), then validates it.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read shard manifest {}", path.display()))?;
        let manifest: Self = serde_json::from_str(&text)
            .with_context(|| format!("invalid shard manifest {}", path.display()))?;
        manifest
            .validate()
            .with_context(|| format!("invalid shard manifest {}", path.display()))?;
        Ok(manifest)
    }

    /// Checks the structural rules and the v1 topology; see the module docs.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_limits()?;
        self.validate_coverage()?;
        self.validate_stages()?;
        self.validate_v1_topology()
    }

    /// Remote workers in stage order.
    pub fn remote_workers(&self) -> Vec<EndpointId> {
        self.stages
            .iter()
            .filter_map(StageSpec::remote_worker)
            .collect()
    }

    fn validate_limits(&self) -> anyhow::Result<()> {
        ensure!(
            (MIN_CONTEXT..=MAX_CONTEXT).contains(&self.context),
            "context {} is outside {MIN_CONTEXT}..={MAX_CONTEXT}",
            self.context
        );
        ensure!(self.max_tokens >= 1, "max_tokens must be at least 1");
        check_sha256(&self.model_sha256, "model_sha256")
    }

    fn validate_coverage(&self) -> anyhow::Result<()> {
        ensure!(!self.stages.is_empty(), "the manifest has no stages");
        let mut next = 0;
        for stage in &self.stages {
            ensure!(
                stage.begin < stage.end,
                "stage {}..{} is empty or reversed",
                stage.begin,
                stage.end
            );
            ensure!(
                stage.begin == next,
                "stage {}..{} should begin at layer {next}: stages must cover 0..{} contiguously",
                stage.begin,
                stage.end,
                self.layers
            );
            next = stage.end;
        }
        ensure!(
            next == self.layers,
            "stages end at layer {next}, but the model has {} layers",
            self.layers
        );
        Ok(())
    }

    fn validate_stages(&self) -> anyhow::Result<()> {
        let mut workers = HashSet::new();
        for stage in &self.stages {
            match &stage.location {
                StageLocation::Remote { worker } => ensure!(
                    workers.insert(*worker),
                    "worker {} serves more than one stage",
                    worker.fmt_short()
                ),
                StageLocation::Local {
                    budget_bytes,
                    artifact_sha256,
                    ..
                } => {
                    ensure!(
                        *budget_bytes > 0,
                        "local stage {}..{} needs a positive budget_bytes",
                        stage.begin,
                        stage.end
                    );
                    if let Some(hash) = artifact_sha256 {
                        check_sha256(hash, "artifact_sha256")?;
                    }
                }
            }
        }
        Ok(())
    }

    /// v1 accepts a single local stage, or one remote prefix plus a local final stage.
    fn validate_v1_topology(&self) -> anyhow::Result<()> {
        let remote = self.stages.iter().filter(|stage| !stage.is_local()).count();
        let local = self.stages.len() - remote;
        let ends_local = self.stages.last().is_some_and(StageSpec::is_local);
        ensure!(
            ends_local,
            "the final stage must be local in v1 (it tokenizes and samples); \
             remote final stages need ring forwarding ({RING_FORWARDING_TICKET})"
        );
        ensure!(
            remote <= 1,
            "v1 allows at most one remote stage, found {remote}; \
             chained remote stages need ring forwarding ({RING_FORWARDING_TICKET})"
        );
        ensure!(
            local <= 1,
            "v1 runs only the final stage locally, found {local} local stages; \
             other topologies need ring forwarding ({RING_FORWARDING_TICKET})"
        );
        Ok(())
    }
}

/// Whether two hex digests name the same bytes (hex case is not significant).
pub fn same_sha256(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

/// Hashes a file with SHA-256 and returns lowercase hex. Blocking: call it from
/// `spawn_blocking` in async code.
pub fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; HASH_CHUNK_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(wire::encode_hex(&hasher.finalize()))
}

fn check_sha256(value: &str, field: &str) -> anyhow::Result<()> {
    if value.len() != SHA256_HEX_LEN || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{field} must be {SHA256_HEX_LEN} hex digits, got {value:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    const HASH: &str = "5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";

    fn worker_id(seed: u8) -> String {
        iroh::SecretKey::from_bytes(&[seed; 32])
            .public()
            .to_string()
    }

    fn local(begin: u32, end: u32) -> Value {
        json!({"begin": begin, "end": end, "local": {"model": "shard.gguf", "budget_bytes": 1024}})
    }

    fn remote(begin: u32, end: u32, seed: u8) -> Value {
        json!({"begin": begin, "end": end, "remote": {"worker": worker_id(seed)}})
    }

    fn manifest_json(stages: Vec<Value>) -> Value {
        json!({
            "runner": "shard_runner",
            "model_sha256": HASH,
            "layers": 4,
            "context": 128,
            "chat_template": "chatml",
            "stages": stages,
        })
    }

    fn parse(value: Value) -> anyhow::Result<ShardManifest> {
        Ok(serde_json::from_value(value)?)
    }

    fn validate(value: Value) -> anyhow::Result<ShardManifest> {
        let manifest = parse(value)?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn error_of(value: Value) -> String {
        format!(
            "{:#}",
            validate(value).expect_err("manifest must be rejected")
        )
    }

    #[test]
    fn remote_prefix_and_local_final_is_valid() {
        let manifest = validate(manifest_json(vec![remote(0, 2, 1), local(2, 4)])).unwrap();
        assert_eq!(manifest.max_tokens, DEFAULT_MAX_TOKENS);
        assert_eq!(manifest.chat_template, ChatTemplate::Chatml);
        assert_eq!(manifest.remote_workers().len(), 1);
        assert_eq!(manifest.remote_workers()[0].to_string(), worker_id(1));
        let StageLocation::Local {
            gpu_layers,
            artifact_sha256,
            env,
            ..
        } = &manifest.stages[1].location
        else {
            panic!("final stage should be local");
        };
        assert_eq!((*gpu_layers, artifact_sha256, env.len()), (0, &None, 0));
    }

    #[test]
    fn single_local_stage_is_valid() {
        let manifest = validate(manifest_json(vec![local(0, 4)])).unwrap();
        assert!(manifest.remote_workers().is_empty());
    }

    #[test]
    fn chat_template_is_required_and_only_chatml_is_known() {
        let mut value = manifest_json(vec![local(0, 4)]);
        value.as_object_mut().unwrap().remove("chat_template");
        assert!(parse(value.clone()).is_err());
        value["chat_template"] = json!("llama2");
        assert!(parse(value).is_err());
    }

    #[test]
    fn unknown_fields_are_refused_at_every_level() {
        let mut top = manifest_json(vec![local(0, 4)]);
        top["temperature"] = json!(0.7);
        assert!(parse(top).is_err());
        let mut stage = local(0, 4);
        stage["gpu"] = json!(1);
        assert!(parse(manifest_json(vec![stage])).is_err());
        let mut inner = local(0, 4);
        inner["local"]["budget"] = json!(1);
        assert!(parse(manifest_json(vec![inner])).is_err());
    }

    #[test]
    fn stage_needs_exactly_one_location() {
        let mut both = local(0, 4);
        both["remote"] = json!({"worker": worker_id(1)});
        let error = parse(manifest_json(vec![both])).unwrap_err().to_string();
        assert!(error.contains("exactly one"), "{error}");
        let neither = json!({"begin": 0, "end": 4});
        assert!(parse(manifest_json(vec![neither])).is_err());
    }

    #[test]
    fn stages_must_cover_all_layers_contiguously() {
        assert!(error_of(manifest_json(vec![])).contains("no stages"));
        assert!(error_of(manifest_json(vec![local(1, 4)])).contains("begin at layer 0"));
        assert!(error_of(manifest_json(vec![local(0, 3)])).contains("4 layers"));
        let gap = manifest_json(vec![remote(0, 1, 1), local(2, 4)]);
        assert!(error_of(gap).contains("begin at layer 1"));
        let overlap = manifest_json(vec![remote(0, 3, 1), local(2, 4)]);
        assert!(error_of(overlap).contains("begin at layer 3"));
        let empty = manifest_json(vec![remote(0, 0, 1), local(0, 4)]);
        assert!(error_of(empty).contains("empty or reversed"));
    }

    #[test]
    fn context_must_be_in_range() {
        for (context, valid) in [(31, false), (32, true), (MAX_CONTEXT, true), (4097, false)] {
            let mut value = manifest_json(vec![local(0, 4)]);
            value["context"] = json!(context);
            assert_eq!(validate(value).is_ok(), valid, "context {context}");
        }
    }

    #[test]
    fn max_tokens_must_be_positive() {
        let mut value = manifest_json(vec![local(0, 4)]);
        value["max_tokens"] = json!(0);
        assert!(error_of(value.clone()).contains("max_tokens"));
        value["max_tokens"] = json!(1);
        assert_eq!(validate(value).unwrap().max_tokens, 1);
    }

    #[test]
    fn hashes_must_be_64_hex_digits() {
        for bad in ["", "abc", &HASH[1..], &format!("{}g", &HASH[1..])] {
            let mut value = manifest_json(vec![local(0, 4)]);
            value["model_sha256"] = json!(bad);
            assert!(error_of(value).contains("model_sha256"), "{bad:?}");
        }
        let mut upper = manifest_json(vec![local(0, 4)]);
        upper["model_sha256"] = json!(HASH.to_uppercase());
        assert!(validate(upper).is_ok());
        let mut stage = local(0, 4);
        stage["local"]["artifact_sha256"] = json!("1234");
        assert!(error_of(manifest_json(vec![stage])).contains("artifact_sha256"));
    }

    #[test]
    fn local_budget_must_be_positive() {
        let mut stage = local(0, 4);
        stage["local"]["budget_bytes"] = json!(0);
        assert!(error_of(manifest_json(vec![stage])).contains("budget_bytes"));
    }

    #[test]
    fn duplicate_workers_are_refused() {
        // Duplicates are checked before the v1 topology rules, so this is reported first.
        let value = manifest_json(vec![remote(0, 1, 7), remote(1, 2, 7), local(2, 4)]);
        assert!(error_of(value).contains("more than one stage"));
    }

    #[test]
    fn v1_requires_a_local_final_stage() {
        let error = error_of(manifest_json(vec![local(0, 2), remote(2, 4, 1)]));
        assert!(error.contains("final stage must be local"), "{error}");
        assert!(error.contains(RING_FORWARDING_TICKET), "{error}");
    }

    #[test]
    fn v1_allows_one_remote_stage() {
        let value = manifest_json(vec![remote(0, 1, 1), remote(1, 2, 2), local(2, 4)]);
        let error = error_of(value);
        assert!(error.contains("at most one remote"), "{error}");
        assert!(error.contains(RING_FORWARDING_TICKET), "{error}");
    }

    #[test]
    fn v1_runs_only_the_final_stage_locally() {
        let error = error_of(manifest_json(vec![local(0, 2), local(2, 4)]));
        assert!(error.contains("only the final stage locally"), "{error}");
        assert!(error.contains(RING_FORWARDING_TICKET), "{error}");
    }

    #[test]
    fn load_names_the_file_and_validates() {
        let dir = std::env::temp_dir().join(format!("shard-manifest-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("good.json");
        std::fs::write(&good, manifest_json(vec![local(0, 4)]).to_string()).unwrap();
        assert_eq!(ShardManifest::load(&good).unwrap().stages.len(), 1);
        let bad = dir.join("bad.json");
        std::fs::write(&bad, manifest_json(vec![local(0, 3)]).to_string()).unwrap();
        let error = format!("{:#}", ShardManifest::load(&bad).unwrap_err());
        assert!(
            error.contains("bad.json") && error.contains("4 layers"),
            "{error}"
        );
        assert!(ShardManifest::load(&dir.join("missing.json")).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sha256_file_matches_known_digest() {
        let path = std::env::temp_dir().join(format!("shard-hash-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, b"abc").unwrap();
        let digest = sha256_file(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(same_sha256(&digest, &digest.to_uppercase()));
    }
}
