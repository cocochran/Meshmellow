//! Local native compute enrollment and membership-driven ring assignment.
//! Paths and executable policy come only from this device's private configuration.
use super::*;
use crate::shard::{
    manifest::{ShardManifest, StageLocation, StageSpec},
    pipeline::{LocalLaunch, StageLink},
    ring::RingWorkerConfig,
    wire::StageInput,
};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalCompute {
    pub runner: PathBuf,
    pub model: PathBuf,
    pub model_sha256: String,
    pub layers: u32,
    pub context: u32,
    pub gpu_layers: u32,
    pub budget_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ComputeOffer {
    pub model_sha256: String,
    pub layers: u32,
    pub context: u32,
    pub layer_ms: f64,
    #[serde(default)]
    pub total_memory_bytes: Option<u64>,
    #[serde(default)]
    pub available_memory_bytes: Option<u64>,
}

fn host_memory() -> (Option<u64>, Option<u64>) {
    #[cfg(target_os = "macos")]
    {
        let total = std::process::Command::new("/usr/sbin/sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse().ok());
        (total, None)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let info = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let bytes = |name: &str| {
            info.lines().find_map(|line| {
                let mut parts = line.split_whitespace();
                (parts.next()? == name)
                    .then(|| parts.next()?.parse::<u64>().ok()?.checked_mul(1024))
                    .flatten()
            })
        };
        (bytes("MemTotal:"), bytes("MemAvailable:"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Assignment {
    pub model_sha256: String,
    pub begin: u32,
    pub end: u32,
    pub upstream: String,
    pub next: Option<String>,
}

/// What this device's native compute is doing, shared with every member's dashboard.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ComputeStatus {
    /// `verifying`, `benchmarking`, `standby`, `loading`, `ready` or `failed`.
    pub phase: String,
    pub message: String,
    #[serde(default)]
    pub layers: Option<u32>,
    #[serde(default)]
    pub begin: Option<u32>,
    #[serde(default)]
    pub end: Option<u32>,
}

impl ComputeStatus {
    fn new(phase: &str, message: impl Into<String>, layers: Option<u32>) -> Self {
        Self {
            phase: phase.into(),
            message: message.into(),
            layers,
            begin: None,
            end: None,
        }
    }
    fn range(phase: &str, message: impl Into<String>, layers: u32, begin: u32, end: u32) -> Self {
        Self {
            begin: Some(begin),
            end: Some(end),
            ..Self::new(phase, message, Some(layers))
        }
    }
    /// Peer reports are display-only; keep them bounded.
    pub(super) fn bounded(mut self) -> Self {
        self.phase.truncate(16);
        if self.message.len() > 240 {
            let mut cut = 240;
            while !self.message.is_char_boundary(cut) {
                cut -= 1;
            }
            self.message.truncate(cut);
        }
        self
    }
}

pub(super) struct ComputeState {
    config: LocalCompute,
    offer: ComputeOffer,
    assignment: Option<Assignment>,
}

/// Exercise the selected range before it can be assigned. This is a short native
/// admission probe, not a thermal qualification or a numerical equivalence claim.
async fn probe(config: &LocalCompute, begin: u32, end: u32) -> Result<f64> {
    let env = BTreeMap::new();
    let mut link = StageLink::spawn_local(
        &LocalLaunch {
            runner: &config.runner,
            model: &config.model,
            begin,
            end,
            context: config.context,
            gpu_layers: config.gpu_layers,
            budget_bytes: config.budget_bytes,
            prompt: "Hello",
            env: &env,
        },
        Duration::from_secs(90),
    )
    .await?;
    let outcome = async {
        let metadata = link.metadata().clone();
        ensure!(
            metadata.layers == config.layers && metadata.begin == begin && metadata.end == end,
            "Native runner returned a different range"
        );
        let mut elapsed = 0.0;
        for step in 0..4 {
            let tokens = [metadata.tokens.first().copied().unwrap_or(1)];
            let hidden = vec![0.01f32; metadata.embd as usize];
            let input = if begin == 0 {
                StageInput::Tokens(&tokens)
            } else {
                StageInput::Hidden {
                    rows: &hidden,
                    embd: metadata.embd,
                }
            };
            let output = link
                .exchange(
                    step,
                    step,
                    &input,
                    metadata.embd as usize,
                    Duration::from_secs(30),
                )
                .await?;
            ensure!(
                output.reply.compute_ms.is_finite()
                    && output.reply.compute_ms >= 0.0
                    && output.floats.iter().all(|n| n.is_finite()),
                "Invalid native probe output"
            );
            if step > 0 {
                elapsed += output.reply.compute_ms;
            }
        }
        Ok((elapsed / 3.0).max(0.001))
    }
    .await;
    link.close(outcome.is_err()).await;
    outcome
}

impl Node {
    /// True once this device has a native compute configuration, even while it
    /// is still being verified in the background.
    pub async fn compute_enabled(&self) -> bool {
        self.compute_status.lock().await.is_some()
    }

    pub async fn compute_status(&self) -> Option<ComputeStatus> {
        self.compute_status.lock().await.clone()
    }

    async fn set_compute_status(&self, status: ComputeStatus) {
        *self.compute_status.lock().await = Some(status);
    }

    /// Verify and benchmark in the background so dashboards can show progress
    /// instead of waiting for a multi-gigabyte hash before the node opens.
    pub(super) async fn start_compute(&self, config: LocalCompute) {
        let layers = config.layers;
        self.set_compute_status(ComputeStatus::new(
            "verifying",
            "Checking the local model file.",
            Some(layers),
        ))
        .await;
        let node = self.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = node.enable_compute(config).await {
                warn!(error = %format!("{error:#}"), "native compute unavailable");
                node.set_compute_status(ComputeStatus::new(
                    "failed",
                    format!("{error:#}"),
                    Some(layers),
                ))
                .await;
            }
        });
        if let Some(previous) = self
            .control_tasks
            .lock()
            .await
            .insert("native-compute".into(), task)
        {
            previous.abort();
        }
    }

    pub async fn enable_compute(&self, config: LocalCompute) -> Result<()> {
        ensure!(
            config.layers >= 2
                && config.layers <= 4096
                && (32..=4096).contains(&config.context)
                && config.budget_bytes > 0,
            "Invalid compute configuration"
        );
        let path = config.model.clone();
        let actual =
            tokio::task::spawn_blocking(move || crate::shard::manifest::sha256_file(&path))
                .await??;
        ensure!(
            actual == config.model_sha256,
            "Automatic compute requires the pinned whole-model artifact"
        );
        self.set_compute_status(ComputeStatus::new(
            "benchmarking",
            "Model verified. Measuring native layer speed.",
            Some(config.layers),
        ))
        .await;
        let layer_ms = probe(&config, 1, 2).await?;
        let (total_memory_bytes, available_memory_bytes) = host_memory();
        let offer = ComputeOffer {
            model_sha256: config.model_sha256.clone(),
            layers: config.layers,
            context: config.context,
            layer_ms,
            total_memory_bytes,
            available_memory_bytes,
        };
        let layers = config.layers;
        *self.compute.lock().await = Some(ComputeState {
            config,
            offer,
            assignment: None,
        });
        self.set_compute_status(ComputeStatus::new(
            "standby",
            "Model verified. Waiting for a layer assignment.",
            Some(layers),
        ))
        .await;
        Ok(())
    }

    pub(super) async fn compute_offer(&self) -> Option<ComputeOffer> {
        let offer = self.compute.lock().await.as_ref()?.offer.clone();
        #[cfg(not(target_os = "macos"))]
        let offer = ComputeOffer {
            available_memory_bytes: host_memory().1,
            ..offer
        };
        Some(offer)
    }

    pub(super) async fn assign_compute(&self, origin: &str, assignment: Assignment) -> Result<()> {
        let mut epoch = self.membership_epoch.subscribe();
        self.control_hello(origin).await?;
        let root = self
            .record
            .lock()
            .await
            .ledger
            .as_ref()
            .context("No mesh")?
            .genesis
            .root
            .clone();
        ensure!(
            origin == root,
            "Only this mesh's model coordinator may assign compute"
        );
        self.control_hello(&assignment.upstream).await?;
        if let Some(next) = &assignment.next {
            self.control_hello(next).await?;
        }
        let mut state = self.compute.lock().await;
        let state = state
            .as_mut()
            .context("Native compute is not enabled on this device")?;
        ensure!(
            assignment.model_sha256 == state.config.model_sha256
                && assignment.begin > 0
                && assignment.begin < assignment.end
                && assignment.end <= state.config.layers,
            "Invalid compute assignment"
        );
        if state.assignment.as_ref() == Some(&assignment) {
            return Ok(());
        }
        let (layers, begin, end) = (state.config.layers, assignment.begin, assignment.end);
        let label = format!("{}–{}", begin, end - 1);
        self.set_compute_status(ComputeStatus::range(
            "loading",
            "Checking the assigned range, then loading it.",
            layers,
            begin,
            end,
        ))
        .await;
        let loaded = self
            .load_assignment(origin, state, assignment, &mut epoch)
            .await;
        self.set_compute_status(match &loaded {
            Ok(()) => ComputeStatus::range("ready", "Serving the mesh.", layers, begin, end),
            Err(error) => ComputeStatus::range(
                "failed",
                format!("Could not load layers {label}: {error:#}"),
                layers,
                begin,
                end,
            ),
        })
        .await;
        loaded
    }

    async fn load_assignment(
        &self,
        origin: &str,
        state: &mut ComputeState,
        assignment: Assignment,
        epoch: &mut tokio::sync::watch::Receiver<u64>,
    ) -> Result<()> {
        tokio::select! {
            _ = epoch.changed() => bail!("Membership changed during native admission"),
            result = probe(&state.config, assignment.begin, assignment.end) => { result?; }
        }
        let config = RingWorkerConfig {
            runner: state.config.runner.clone(),
            model: state.config.model.clone(),
            begin: assignment.begin,
            end: assignment.end,
            context: state.config.context,
            gpu_layers: state.config.gpu_layers,
            budget_bytes: state.config.budget_bytes,
            expected_sha256: state.config.model_sha256.clone(),
            source_sha256: None,
            upstream: assignment.upstream.parse()?,
            origin: origin.parse()?,
            next: assignment.next.as_ref().map(|s| s.parse()).transpose()?,
        };
        let worker =
            crate::shard::ring::RingWorker::load(self.endpoint.clone(), config, Default::default())
                .await?;
        self.control_hello(origin).await?;
        *self.inference_worker.lock().await = Some(worker);
        state.assignment = Some(assignment);
        Ok(())
    }

    /// Called between generations while the frontend owns its generation lock.
    /// New members are discovered from the authenticated control table, never a
    /// fixed list of device IDs. Existing requests keep their current route.
    pub async fn prepare_compute_ring(&self, base: &ShardManifest) -> Result<ShardManifest> {
        // Workers report their own assignment, and until local verification
        // finishes the coordinator keeps showing that progress instead.
        let coordinator = self
            .record
            .lock()
            .await
            .ledger
            .as_ref()
            .is_some_and(|l| l.genesis.root == self.id());
        ensure!(coordinator, "This device is not the model coordinator");
        let Some(layers) = self.compute.lock().await.as_ref().map(|s| s.config.layers) else {
            bail!("Local automatic compute is not ready yet");
        };
        let result = self.build_compute_ring(base).await;
        let status = match &result {
            Ok(manifest) => {
                let local = &manifest.stages[0];
                ComputeStatus::range(
                    "ready",
                    format!(
                        "Coordinating {} devices. This slice loads for each request.",
                        manifest.stages.len()
                    ),
                    layers,
                    local.begin,
                    local.end,
                )
            }
            Err(error) => ComputeStatus::new("standby", format!("{error:#}"), Some(layers)),
        };
        self.set_compute_status(status).await;
        result
    }

    async fn build_compute_ring(&self, base: &ShardManifest) -> Result<ShardManifest> {
        let root = self
            .record
            .lock()
            .await
            .ledger
            .as_ref()
            .context("No mesh")?
            .genesis
            .root
            .clone();
        ensure!(
            root == self.id(),
            "This device is not the model coordinator"
        );
        let local = self
            .compute
            .lock()
            .await
            .as_ref()
            .map(|s| (s.config.clone(), s.offer.clone()))
            .context("Local automatic compute is not configured")?;
        ensure!(
            local.1.model_sha256 == base.model_sha256
                && local.1.layers == base.layers
                && local.1.context == base.context,
            "Coordinator model differs from local compute"
        );
        let snapshot = self.snapshot().await?;
        let mut offers = vec![(self.id(), local.1)];
        let mut discovery = tokio::task::JoinSet::new();
        for id in snapshot["connected_peers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .take(15)
        {
            if !snapshot["members"]
                .as_array()
                .is_some_and(|members| members.iter().any(|m| m["id"] == id))
            {
                continue;
            }
            let node = self.clone();
            let id = id.to_string();
            discovery.spawn(async move {
                let peer = Peer {
                    id: id.clone(),
                    addresses: vec![],
                    relay: None,
                    invite: String::new(),
                };
                (
                    id,
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        node.rpc(&peer, Wire::ComputeOffer),
                    )
                    .await,
                )
            });
        }
        while let Some(result) = discovery.join_next().await {
            if let Ok((id, Ok(Ok(reply)))) = result
                && let Some(offer) = reply.compute
                && offer.model_sha256 == base.model_sha256
                && offer.layers == base.layers
                && offer.context == base.context
                && offer.layer_ms.is_finite()
                && offer.layer_ms > 0.0
            {
                offers.push((id, offer));
            }
        }
        offers[1..].sort_by(|a, b| a.0.cmp(&b.0));
        *self.compute_hardware.lock().await = offers.clone();
        ensure!(
            offers.len() >= 2,
            "Waiting for another connected native worker for this model"
        );
        offers.truncate(base.layers as usize);
        let signature = offers
            .iter()
            .map(|(id, offer)| format!("{id}:{}", offer.layer_ms))
            .collect::<Vec<_>>()
            .join(",");
        let unchanged = self
            .compute_route
            .lock()
            .await
            .as_ref()
            .is_some_and(|(key, _)| key == &signature);
        // A failed or cancelled preparation must never advertise the previous
        // route as ready after some workers have accepted different ranges.
        if !unchanged {
            *self.compute_route.lock().await = None;
        }
        let speed: Vec<_> = offers.iter().map(|(_, o)| 1.0 / o.layer_ms).collect();
        let sum: f64 = speed.iter().sum();
        let remaining = base.layers as usize - offers.len();
        let mut counts: Vec<u32> = speed
            .iter()
            .map(|s| 1 + (remaining as f64 * s / sum).floor() as u32)
            .collect();
        while counts.iter().sum::<u32>() < base.layers {
            let index = (0..counts.len())
                .min_by(|&a, &b| {
                    (counts[a] as f64 / speed[a]).total_cmp(&(counts[b] as f64 / speed[b]))
                })
                .unwrap();
            counts[index] += 1;
        }
        let mut manifest = base.clone();
        manifest.runner = local.0.runner.clone();
        manifest.stages.clear();
        let mut begin = 0;
        for (index, ((id, _), count)) in offers.iter().zip(counts).enumerate() {
            let end = begin + count;
            let location = if index == 0 {
                if !unchanged {
                    self.set_compute_status(ComputeStatus::range(
                        "loading",
                        "Checking this slice before assigning the mesh.",
                        base.layers,
                        begin,
                        end,
                    ))
                    .await;
                    probe(&local.0, begin, end).await?;
                }
                StageLocation::Local {
                    model: local.0.model.clone(),
                    gpu_layers: local.0.gpu_layers,
                    budget_bytes: local.0.budget_bytes,
                    artifact_sha256: Some(local.0.model_sha256.clone()),
                    env: BTreeMap::new(),
                }
            } else {
                let assignment = Assignment {
                    model_sha256: base.model_sha256.clone(),
                    begin,
                    end,
                    upstream: offers[index - 1].0.clone(),
                    next: offers.get(index + 1).map(|(id, _)| id.clone()),
                };
                self.rpc(
                    &Peer {
                        id: id.clone(),
                        addresses: vec![],
                        relay: None,
                        invite: String::new(),
                    },
                    Wire::AssignCompute { assignment },
                )
                .await?;
                StageLocation::Remote {
                    worker: id.parse()?,
                }
            };
            manifest.stages.push(StageSpec {
                begin,
                end,
                location,
            });
            begin = end;
        }
        manifest.validate()?;
        for (id, _) in offers.iter().skip(1) {
            self.control_hello(id).await?;
        }
        *self.compute_route.lock().await = Some((signature, manifest.clone()));
        info!(
            devices = offers.len(),
            "automatic mesh compute route activated"
        );
        Ok(manifest)
    }

    pub async fn active_compute_manifest(&self) -> Option<ShardManifest> {
        self.compute_route
            .lock()
            .await
            .as_ref()
            .map(|(_, m)| m.clone())
    }

    pub async fn compute_hardware(&self) -> Vec<(String, ComputeOffer)> {
        self.compute_hardware.lock().await.clone()
    }
}
