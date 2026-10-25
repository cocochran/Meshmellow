//! Bounded observations of real native execution, shared with the local dashboard.
use super::{
    manifest::{ShardManifest, StageLocation},
    wire::StageMetadata,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Mutex;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageObservation {
    pub id: String,
    pub begin: u32,
    pub end: u32,
    pub status: String,
    pub executions: u32,
    pub compute_ms: Option<f64>,
    pub buffer_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub updated_at: u64,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Execution {
    pub job: String,
    pub active: bool,
    pub phase: String,
    pub started_at: u64,
    pub elapsed_ms: u64,
    pub output_tokens: u32,
    pub decode_tps: Option<f64>,
    pub ttft_ms: Option<u64>,
    pub return_bytes: u64,
    pub stages: Vec<StageObservation>,
    pub samples: Vec<Value>,
    pub events: Vec<Value>,
    #[serde(skip)]
    first_token_at: Option<u64>,
    #[serde(skip)]
    last_token_at: Option<u64>,
}

#[derive(Default)]
pub struct ExecutionTelemetry(Mutex<Option<Execution>>);

impl ExecutionTelemetry {
    pub fn snapshot(&self) -> Option<Execution> {
        let mut current = self.0.lock().ok()?.clone()?;
        if current.active {
            current.elapsed_ms = now().saturating_sub(current.started_at);
        }
        Some(current)
    }

    pub fn begin(&self, manifest: &ShardManifest, origin: &str) {
        let stages = manifest
            .stages
            .iter()
            .map(|s| StageObservation {
                id: match &s.location {
                    StageLocation::Remote { worker } => worker.to_string(),
                    _ => origin.into(),
                },
                begin: s.begin,
                end: s.end,
                status: "loading".into(),
                ..Default::default()
            })
            .collect();
        if let Ok(mut state) = self.0.lock() {
            *state = Some(Execution {
                active: true,
                phase: "loading".into(),
                started_at: now(),
                stages,
                ..Default::default()
            });
        }
    }

    pub fn ready(&self, job: &str, metadata: impl IntoIterator<Item = StageMetadata>) {
        if let Ok(mut state) = self.0.lock()
            && let Some(s) = state.as_mut()
        {
            s.job = job.into();
            for meta in metadata {
                if let Some(stage) = s.stages.iter_mut().find(|d| d.begin == meta.begin) {
                    stage.buffer_bytes = Some(meta.allocated_bytes);
                    stage.peak_rss_bytes = meta.extra.get("max_rss_bytes").and_then(Value::as_u64);
                    stage.status = "ready".into();
                }
            }
        }
    }

    pub fn progress(&self, job: &str, event: &Value) {
        if let Ok(mut state) = self.0.lock()
            && let Some(s) = state.as_mut()
        {
            if s.job != job || !s.active {
                return;
            }
            let Some(begin) = event["begin"].as_u64() else {
                return;
            };
            let Some(stage) = s.stages.iter_mut().find(|d| u64::from(d.begin) == begin) else {
                return;
            };
            let timestamp = now();
            let step = event["step"].as_u64().unwrap_or(0);
            stage.updated_at = timestamp;
            match event["event"].as_str() {
                Some("stage_start") => {
                    stage.status = "computing".into();
                    stage.input_bytes += event["input_bytes"].as_u64().unwrap_or(0);
                    s.phase = if step == 0 { "prefill" } else { "decode" }.into();
                }
                Some("stage_end") => {
                    stage.status = "returned".into();
                    stage.executions += 1;
                    stage.output_bytes += event["output_bytes"].as_u64().unwrap_or(0);
                    let result = &event["result"];
                    stage.compute_ms = result["compute_ms"]
                        .as_f64()
                        .filter(|n| n.is_finite() && *n >= 0.0);
                    if let Some(bytes) = result["observed_peak_buffer_bytes"].as_u64() {
                        stage.buffer_bytes = Some(stage.buffer_bytes.unwrap_or(0).max(bytes));
                    }
                    if let Some(bytes) = result["max_rss_bytes"].as_u64() {
                        stage.peak_rss_bytes = Some(bytes);
                    }
                }
                _ => return,
            }
            let entry = json!({"at":timestamp,"node":stage.id,"begin":begin,"step":step,"event":event["event"],"bytes":event["output_bytes"].as_u64().or(event["input_bytes"].as_u64())});
            s.events.push(entry);
            if s.events.len() > 64 {
                s.events.remove(0);
            }
        }
    }

    pub fn token(&self, job: &str, eog: bool, return_bytes: u64) {
        if let Ok(mut state) = self.0.lock()
            && let Some(s) = state.as_mut()
        {
            if s.job != job {
                return;
            }
            s.return_bytes += return_bytes;
            if eog {
                return;
            }
            let timestamp = now();
            s.output_tokens += 1;
            let first = *s.first_token_at.get_or_insert(timestamp);
            s.ttft_ms = Some(first.saturating_sub(s.started_at));
            let instant_tps = s
                .last_token_at
                .filter(|last| timestamp > *last)
                .map(|last| 1000.0 / (timestamp - last) as f64);
            if s.output_tokens > 1 && timestamp > first {
                s.decode_tps =
                    Some(f64::from(s.output_tokens - 1) * 1000.0 / (timestamp - first) as f64);
            }
            s.last_token_at = Some(timestamp);
            let buffers: u64 = s.stages.iter().filter_map(|d| d.buffer_bytes).sum();
            let transfer: u64 = s
                .stages
                .iter()
                .filter(|d| d.begin > 0)
                .map(|d| d.input_bytes)
                .sum();
            s.samples.push(json!({"at":timestamp.saturating_sub(s.started_at),"tokens":s.output_tokens,"tps":instant_tps,"buffers":buffers,"activationBytes":transfer}));
            if s.samples.len() > 128 {
                s.samples.remove(0);
            }
        }
    }

    pub fn finish(&self, phase: &str) {
        if let Ok(mut state) = self.0.lock()
            && let Some(s) = state.as_mut()
        {
            s.active = false;
            s.phase = phase.into();
            s.elapsed_ms = now().saturating_sub(s.started_at);
            for stage in &mut s.stages {
                stage.status = phase.into();
            }
        }
    }
}
