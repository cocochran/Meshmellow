//! OpenAI-shaped HTTP frontend that serves completions from a sharded model.
//!
//! [`serve_frontend`] listens on a loopback address and answers like llama-server, so the
//! leader's job API ([`crate::api`]) can sit in front of it unchanged:
//!
//! - `GET /health` answers 200.
//! - `POST /v1/chat/completions` renders the messages with the manifest's chat template, opens
//!   fresh stage links (a runner serves one sequence, so every request gets new processes and
//!   streams), and runs one greedy generation. Sampling fields such as `temperature`, `top_p` or
//!   `seed` are accepted and ignored; `max_tokens` / `max_completion_tokens` are honoured up to
//!   the manifest's `max_tokens`.
//!
//! With an EXPERIMENTAL ring manifest each request instead starts the local stage 0 and a fresh
//! shard-ring session ([`RingLink`]); [`FrontendConfig::ring`] must then carry the registry the
//! leader's endpoint delivers result channels to.
//!
//! One generation runs at a time; later requests wait for the lock. A failure before any
//! response bytes are sent is a JSON error with a 5xx status (400 for a prompt longer than
//! the context). A failure mid-stream sends an SSE `error` event and ends without `[DONE]`,
//! which makes the job API fail the job without committing history. A client that disconnects
//! drops its generation, which kills the local runners and closes the remote streams.
//!
//! Failover: each stage is served by the first of its locations (the manifest's primary, then
//! its `fallbacks`) that opens, starting from the one that last worked. When a stage with
//! another location left stops answering mid-generation, its location is set aside for the
//! rest of the request and the generation restarts from the prompt on fresh links. Greedy
//! decoding is deterministic, so the restart regenerates the same text: a streamed reply
//! resumes where it stopped instead of repeating itself, and a restart whose text differs from
//! what was already sent fails the request rather than splicing two answers together.

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::future::try_join_all;
use futures_util::{StreamExt, stream};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::api::CHAT_COMPLETIONS_PATH;
use crate::shard::manifest::{ShardManifest, StageLocation, StageSpec};
use crate::shard::manifest::{same_sha256, sha256_file};
use crate::shard::pipeline::{
    FinishReason, Generation, GenerationLimits, LocalLaunch, METADATA_TIMEOUT, Pipeline,
    PromptTooLong, StageExpectation, StageFault, StageIdentity, StageLink,
};
use crate::shard::ring_origin::{RingLink, RingPlan, RingSettings};
use crate::shard::template::{ChatMessage, ChatRenderer};

/// Liveness probe, as llama-server serves it.
pub const HEALTH_PATH: &str = "/health";

/// Model name reported in completion objects.
const MODEL_NAME: &str = "meshthing-shard";

/// Largest accepted request body; long chat histories exceed axum's 2 MiB default.
const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Payload that ends an OpenAI-style event stream.
const SSE_DONE: &str = "[DONE]";

/// Content type of a streamed reply.
const EVENT_STREAM: &str = "text/event-stream";

/// `Cache-Control` for a streamed reply: proxies must pass chunks through as they come.
const NO_CACHE: &str = "no-cache";

/// Bounds queued text chunks when a client reads more slowly than inference produces.
const GENERATION_EVENT_BUFFER: usize = 16;

/// Where the frontend listens, what it serves, and the endpoint remote stages are dialled from.
pub struct FrontendConfig {
    /// Address to serve on; must be loopback, anything else is refused.
    pub listen: SocketAddr,
    /// The validated sharded model.
    pub manifest: ShardManifest,
    /// Iroh endpoint used to dial remote stages.
    pub endpoint: iroh::Endpoint,
    /// Required for a ring manifest (`"execution": "ring"`) and ignored otherwise: the registry
    /// the endpoint's accept loop delivers terminals' result channels to, and the readiness wait.
    pub ring: Option<RingSettings>,
}

/// What a ring manifest's generations need besides the manifest.
struct RingRuntime {
    plan: RingPlan,
    settings: RingSettings,
}

struct ReconcileTask(Option<JoinHandle<()>>);
impl Drop for ReconcileTask {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

/// Serves `GET /health` and `POST /v1/chat/completions` on `config.listen` until `shutdown`
/// resolves, then finishes in-flight requests and returns.
///
/// Before binding it validates the manifest and hashes every local stage file that has a
/// configured `artifact_sha256` (once, on a blocking thread), failing on any mismatch. It also
/// loads the chat template once ([`ChatRenderer::load`]), refusing a pinned template that is
/// missing, does not match its `sha256` or does not compile.
pub async fn serve_frontend(
    config: FrontendConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    ensure_loopback(config.listen)?;
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind shard frontend on {}", config.listen))?;
    serve_frontend_on(config, listener, shutdown).await
}

/// Serve on a listener already reserved by the owning desktop client.
pub async fn serve_frontend_on(
    config: FrontendConfig,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    serve_frontend_inner(config, listener, shutdown, None).await
}

pub async fn serve_client_frontend(
    config: FrontendConfig,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    node: crate::mesh::Node,
) -> anyhow::Result<()> {
    let automatic = node.compute_enabled().await.then_some(node);
    serve_frontend_inner(config, listener, shutdown, automatic).await
}

async fn serve_frontend_inner(
    config: FrontendConfig,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    automatic: Option<crate::mesh::Node>,
) -> anyhow::Result<()> {
    ensure_loopback(listener.local_addr()?)?;
    ensure_loopback(config.listen)?;
    config.manifest.validate()?;
    let ring = ring_runtime(&config.manifest, config.ring)?;
    let renderer = ChatRenderer::load(&config.manifest.chat_template)?;
    verify_artifacts(&config.manifest).await?;
    let state = Arc::new(FrontendState {
        preferred: config
            .manifest
            .stages
            .iter()
            .map(|_| AtomicUsize::new(0))
            .collect(),
        manifest: config.manifest,
        endpoint: config.endpoint,
        ring,
        renderer,
        generation_lock: Arc::new(Mutex::new(())),
        automatic,
    });
    let reconcile = ReconcileTask(state.automatic.as_ref().map(|node| {
        let node = node.clone();
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                {
                    let _turn = state.generation_lock.lock().await;
                    if let Err(error) = node.prepare_compute_ring(&state.manifest).await {
                        warn!(%error, "automatic compute route waiting");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        })
    }));
    let router = Router::new()
        .route(HEALTH_PATH, get(health))
        .route("/v1/models", get(|| async {
            Json(serde_json::json!({"object":"list", "data":[{"id":"meshthing-shard", "object":"model", "owned_by":"layercake"}]}))
        }))
        .route(CHAT_COMPLETIONS_PATH, post(chat_completions))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state);
    let local = listener
        .local_addr()
        .context("failed to read shard frontend address")?;
    info!(listen = %local, "shard frontend listening");
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .context("shard frontend failed");
    drop(reconcile);
    result
}

/// State shared by every request handler.
struct FrontendState {
    automatic: Option<crate::mesh::Node>,
    manifest: ShardManifest,
    endpoint: iroh::Endpoint,
    /// Per stage, the location (an index into [`StageSpec::candidates`]) tried first: the last
    /// one that opened, so later requests skip a node that already failed over.
    preferred: Vec<AtomicUsize>,
    /// Held for the whole of a generation, so only one runs at a time.
    generation_lock: Arc<Mutex<()>>,
}

/// One location of one stage: `(stage index, index into StageSpec::candidates)`.
type CandidateId = (usize, usize);

fn ensure_loopback(listen: SocketAddr) -> anyhow::Result<()> {
    if !listen.ip().to_canonical().is_loopback() {
        bail!("shard frontend listen address {listen} is not loopback; refusing to expose it");
    }
    Ok(())
}

/// Hashes each local stage file (fallbacks included) that has an `artifact_sha256` and checks
/// it matches.
async fn verify_artifacts(manifest: &ShardManifest) -> anyhow::Result<()> {
    for stage in &manifest.stages {
        for location in stage.candidates() {
            verify_artifact(stage, location).await?;
        }
    }
    Ok(())
}

async fn verify_artifact(stage: &StageSpec, location: &StageLocation) -> anyhow::Result<()> {
    let StageLocation::Local {
        model,
        artifact_sha256: Some(expected),
        ..
    } = location
    else {
        return Ok(());
    };
    let path = model.clone();
    let actual = tokio::task::spawn_blocking(move || sha256_file(&path))
        .await
        .context("artifact hashing task failed")??;
    if !same_sha256(&actual, expected) {
        bail!(
            "local stage {} file {} has SHA-256 {actual}, but artifact_sha256 is {expected}",
            stage_range(stage),
            model.display()
        );
    }
    info!(stage = %stage_range(stage), "verified artifact_sha256");
    Ok(())
}

/// What a stage's metadata must show when `location` serves it. A configured artifact hash
/// counts as verified because [`verify_artifacts`] has already checked it.
fn stage_expectation(
    manifest: &ShardManifest,
    stage: &StageSpec,
    location: &StageLocation,
) -> StageExpectation {
    StageExpectation {
        begin: stage.begin,
        end: stage.end,
        layers: manifest.layers,
        model_sha256: manifest.model_sha256.clone(),
        identity: match location {
            StageLocation::Remote { .. } => StageIdentity::Remote,
            StageLocation::Local {
                artifact_sha256, ..
            } => StageIdentity::Local {
                artifact_verified: artifact_sha256.is_some(),
            },
        },
    }
}

/// `begin..end`, for logs and errors.
fn stage_range(stage: &StageSpec) -> String {
    format!("{}..{}", stage.begin, stage.end)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

/// A parsed chat request.
#[derive(Debug, PartialEq, Eq)]
struct ChatRequest {
    prompt: String,
    stream: bool,
    max_tokens: u32,
}

#[derive(Deserialize)]
struct RawChatRequest {
    messages: Vec<RawMessage>,
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    max_completion_tokens: Option<u32>,
}

#[derive(Deserialize)]
struct RawMessage {
    role: String,
    content: MessageContent,
}

/// Message content: a string, or an array of text parts.
#[derive(Deserialize)]
#[serde(untagged)]
enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentPart {
    Text { text: String },
}

impl MessageContent {
    fn into_text(self) -> String {
        match self {
            Self::Text(text) => text,
            Self::Parts(parts) => parts
                .into_iter()
                .map(|ContentPart::Text { text }| text)
                .collect(),
        }
    }
}

/// Parses a chat request, renders its prompt, and caps `max_tokens` at `max_tokens_cap`.
fn parse_chat_request(
    body: &[u8],
    renderer: &ChatRenderer,
    max_tokens_cap: u32,
) -> Result<ChatRequest, String> {
    let raw: RawChatRequest = serde_json::from_slice(body).map_err(|error| {
        format!(
            "invalid chat request (messages need a role and string or text-part content): {error}"
        )
    })?;
    if raw.messages.is_empty() {
        return Err("messages must not be empty".to_owned());
    }
    let requested = raw.max_completion_tokens.or(raw.max_tokens);
    if requested == Some(0) {
        return Err("max_tokens must be at least 1".to_owned());
    }
    let messages: Vec<ChatMessage> = raw
        .messages
        .into_iter()
        .map(|message| ChatMessage {
            role: message.role,
            content: message.content.into_text(),
        })
        .collect();
    let prompt = renderer
        .render(&messages)
        .map_err(|error| error.to_string())?;
    Ok(ChatRequest {
        prompt,
        stream: raw.stream.unwrap_or(false),
        max_tokens: requested.map_or(max_tokens_cap, |asked| asked.min(max_tokens_cap)),
    })
}

/// Progress of one generation, sent from its task to the request handler.
enum Event {
    /// Every stage is up and validated; response bytes may now be sent.
    Started,
    /// New completion text.
    Text(String),
    /// The generation finished.
    Finished(Generation),
    /// The generation failed.
    Failed(Failure),
}

/// A failed generation and the HTTP status it maps to before any bytes are sent.
struct Failure {
    status: StatusCode,
    message: String,
}

impl Failure {
    fn from_error(error: &anyhow::Error) -> Self {
        let status = if error.downcast_ref::<PromptTooLong>().is_some() {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::BAD_GATEWAY
        };
        Self {
            status,
            message: format!("{error:#}"),
        }
    }

    fn task_lost() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "the generation task ended unexpectedly".to_owned(),
        }
    }
}

/// Signals cancellation when the client goes away; the task remains alive for pipeline cleanup.
struct GenerationTask {
    cancel: Option<oneshot::Sender<()>>,
    _join: JoinHandle<()>,
}

impl Drop for GenerationTask {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

/// Identifiers shared by every object of one completion.
struct Completion {
    id: String,
    created: u64,
}

impl Completion {
    fn new() -> Self {
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        Self {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
            created,
        }
    }

    fn buffered(&self, generation: &Generation) -> Value {
        json!({
            "id": self.id,
            "object": "chat.completion",
            "created": self.created,
            "model": MODEL_NAME,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": generation.text},
                "finish_reason": generation.finish.as_str(),
            }],
            "usage": {
                "prompt_tokens": generation.prompt_tokens,
                "completion_tokens": generation.completion_tokens,
                "total_tokens": generation.prompt_tokens + generation.completion_tokens,
            },
        })
    }

    fn chunk(&self, delta: Value, finish: Option<FinishReason>) -> Bytes {
        let chunk = json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": MODEL_NAME,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish.map(FinishReason::as_str),
            }],
        });
        sse_data(&chunk.to_string())
    }
}

async fn chat_completions(State(state): State<Arc<FrontendState>>, body: Bytes) -> Response {
    let manifest = &state.manifest;
    let request = match parse_chat_request(&body, &state.renderer, manifest.max_tokens) {
        Ok(request) => request,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
    };
    info!(
        stream = request.stream,
        max_tokens = request.max_tokens,
        "shard generation requested"
    );
    let (events_tx, mut events) = unbounded_channel();
    let task = GenerationTask(tokio::spawn(run_generation(
        Arc::clone(&state),
        request.prompt,
        request.max_tokens,
        Delivered::new(request.stream),
        events_tx,
    )));
    match events.recv().await {
        Some(Event::Started) => {}
        Some(Event::Failed(failure)) => return failure_response(&failure),
        Some(Event::Text(_) | Event::Finished(_)) | None => {
            return failure_response(&Failure::task_lost());
        }
    }
    if request.stream {
        stream_response(events, task)
    } else {
        buffered_response(events, task).await
    }
}

/// Runs one generation under the lock and reports its progress on `events`.
async fn run_generation(
    state: Arc<FrontendState>,
    prompt: String,
    max_tokens: u32,
    delivered: Delivered,
    events: UnboundedSender<Event>,
) {
    let _turn = Arc::clone(&state.generation_lock).lock_owned().await;
    let event = match generate(&state, &prompt, max_tokens, delivered, &events).await {
        Ok(generation) => {
            if let Some(ring) = &state.ring {
                ring.settings.channels.execution.finish("done");
            }
            info!(
                finish = generation.finish.as_str(),
                completion_tokens = generation.completion_tokens,
                "shard generation finished"
            );
            Event::Finished(generation)
        }
        Err(error) if error.downcast_ref::<GenerationCancelled>().is_some() => {
            if let Some(ring) = &state.ring {
                ring.settings.channels.execution.finish("cancelled");
            }
            info!("client disconnected; shard generation canceled after cleanup");
            return;
        }
        Err(error) => {
            if let Some(ring) = &state.ring {
                ring.settings.channels.execution.finish("error");
            }
            warn!(error = %format!("{error:#}"), "shard generation failed");
            Event::Failed(Failure::from_error(&error))
        }
    };
    // The handler may be gone (client disconnected); then nobody needs the outcome.
    let _ = events.send(event).await;
}

/// Runs the generation, restarting it from the prompt whenever a stage with another location
/// left stops answering; the failed location sits out the rest of the request. Any other
/// failure, or a stage with no location left, ends the request.
async fn generate(
    state: &FrontendState,
    prompt: &str,
    max_tokens: u32,
    mut delivered: Delivered,
    events: &UnboundedSender<Event>,
) -> anyhow::Result<Generation> {
    let mut failed = HashSet::new();
    let mut started = false;
    loop {
        let stages = try_join_all(
            state
                .manifest
                .stages
                .iter()
                .enumerate()
                .map(|(index, stage)| open_stage(state, index, stage, prompt, &failed)),
        )
        .await?;
        let mut chosen = Vec::with_capacity(stages.len());
        let mut links = Vec::with_capacity(stages.len());
        let mut expectations = Vec::with_capacity(stages.len());
        for (index, stage) in stages.into_iter().enumerate() {
            failed.extend(
                stage
                    .unreachable
                    .into_iter()
                    .map(|candidate| (index, candidate)),
            );
            chosen.push((index, stage.candidate));
            links.push(stage.link);
            expectations.push(stage.expectation);
        }
        let pipeline = Pipeline::new(links, &expectations)?;
        let context = state.manifest.context;
        // Checked before `Started` so an oversized prompt is still answered with a 400.
        pipeline.ensure_prompt_fits(context)?;
        if !started {
            // A send fails only once the handler is gone, and then the task is being aborted.
            let _ = events.send(Event::Started);
            started = true;
        }
        let limits = GenerationLimits {
            max_tokens,
            context,
        };
        let error = match run_pipeline(pipeline, limits, &mut delivered, events).await {
            Ok(generation) => return Ok(generation),
            Err(error) => error,
        };
        let Some(lost) = failover_target(state, &error, &chosen, &failed) else {
            return Err(error);
        };
        let stage = &state.manifest.stages[lost.0];
        warn!(
            stage = %stage_range(stage),
            error = %format!("{error:#}"),
            "stage failed mid-generation; restarting the prompt on its next location"
        );
        failed.insert(lost);
        delivered.restart();
    }
}

/// The location to set aside when `error` came from a stage that has another location left;
/// `None` when the generation cannot be restarted.
fn failover_target(
    state: &FrontendState,
    error: &anyhow::Error,
    chosen: &[CandidateId],
    failed: &HashSet<CandidateId>,
) -> Option<CandidateId> {
    let fault = error.downcast_ref::<StageFault>()?;
    let lost = *chosen.get(fault.index)?;
    let (stage, _) = lost;
    let remaining = (0..state.manifest.stages[stage].candidates().count())
        .map(|candidate| (stage, candidate))
        .filter(|&candidate| candidate != lost && !failed.contains(&candidate))
        .count();
    (remaining > 0).then_some(lost)
}

/// Runs one attempt, sending the client only text beyond what it already has. Stops early if
/// the text differs from what a previous attempt sent.
async fn run_pipeline(
    pipeline: Pipeline,
    limits: GenerationLimits,
    delivered: &mut Delivered,
    events: &UnboundedSender<Event>,
) -> anyhow::Result<Generation> {
    let diverged = Notify::new();
    let on_text = |text: &str| match delivered.accept(text) {
        Ok(fresh) if !fresh.is_empty() => {
            let _ = events.send(Event::Text(fresh.to_owned()));
        }
        Ok(_) => {}
        Err(Diverged) => diverged.notify_one(),
    };
    // Dropping the pipeline on divergence kills its runners and closes its streams.
    let generation = tokio::select! {
        result = pipeline.generate(limits, on_text) => result?,
        () = diverged.notified() => return Err(Diverged.into()),
    };
    delivered.finish()?;
    Ok(generation)
}

/// A stage's link from the first of its locations that opens.
struct OpenedStage {
    /// Index into [`StageSpec::candidates`] of the location serving the stage.
    candidate: usize,
    link: StageLink,
    expectation: StageExpectation,
    /// Locations tried first that failed to open.
    unreachable: Vec<usize>,
}

/// Opens the stage on its first location that works, starting with the preferred one and
/// skipping any that already `failed` this request.
async fn open_stage(
    state: &FrontendState,
    index: usize,
    stage: &StageSpec,
    prompt: &str,
    failed: &HashSet<CandidateId>,
) -> anyhow::Result<OpenedStage> {
    let locations: Vec<&StageLocation> = stage.candidates().collect();
    let preferred = state.preferred[index].load(Ordering::Relaxed);
    let order = (0..locations.len())
        .map(|offset| (preferred + offset) % locations.len())
        .filter(|&candidate| !failed.contains(&(index, candidate)));
    let mut unreachable = Vec::new();
    let mut last_error = None;
    for candidate in order {
        let location = locations[candidate];
        match open_link(state, stage, location, prompt).await {
            Ok(link) => {
                state.preferred[index].store(candidate, Ordering::Relaxed);
                return Ok(OpenedStage {
                    candidate,
                    link,
                    expectation: stage_expectation(&state.manifest, stage, location),
                    unreachable,
                });
            }
            Err(error) if locations.len() == 1 => return Err(error),
            Err(error) => {
                warn!(
                    stage = %stage_range(stage),
                    error = %format!("{error:#}"),
                    "stage location unavailable; trying the next"
                );
                unreachable.push(candidate);
                last_error = Some(error);
            }
        }
    }
    let range = stage_range(stage);
    Err(match last_error {
        Some(error) => error.context(format!("no location could serve stage {range}")),
        None => anyhow!("every location for stage {range} already failed during this request"),
    })
}

/// Opens every stage of a pipeline manifest at once and validates them.
async fn open_pipeline(state: &FrontendState, prompt: &str) -> anyhow::Result<Pipeline> {
    let links = try_join_all(
        state
            .manifest
            .stages
            .iter()
            .map(|stage| open_link(state, stage, prompt)),
    )
    .await?;
    Pipeline::new(links, &state.expectations)
}

/// Starts the local stage 0 and the ring session at once and validates them together.
async fn open_ring_pipeline(
    state: &FrontendState,
    ring: &RingRuntime,
    prompt: &str,
) -> anyhow::Result<Pipeline> {
    let (stage, expected) = state
        .manifest
        .stages
        .first()
        .zip(state.expectations.first())
        .context("the ring manifest has no local stage")?;
    let (local, link) = tokio::try_join!(
        open_link(state, stage, prompt),
        RingLink::open(&state.endpoint, &ring.plan, &ring.settings),
    )?;
    Pipeline::with_ring(local, link, expected)
}

async fn open_link(
    state: &FrontendState,
    stage: &StageSpec,
    location: &StageLocation,
    prompt: &str,
) -> anyhow::Result<StageLink> {
    match location {
        StageLocation::Remote { worker } => {
            StageLink::connect_remote(&state.endpoint, *worker, METADATA_TIMEOUT).await
        }
        StageLocation::Local {
            model,
            gpu_layers,
            budget_bytes,
            env,
            ..
        } => {
            let launch = LocalLaunch {
                runner: &state.manifest.runner,
                model,
                begin: stage.begin,
                end: stage.end,
                context: state.manifest.context,
                gpu_layers: *gpu_layers,
                budget_bytes: *budget_bytes,
                prompt,
                env,
            };
            StageLink::spawn_local(&launch, METADATA_TIMEOUT).await
        }
    }
}

/// The restarted generation produced different text from what the client already has.
#[derive(Debug, thiserror::Error)]
#[error("the restarted generation produced different text from what was already streamed")]
struct Diverged;

/// What the client has been sent, so a restarted generation repeats none of it.
struct Delivered {
    /// Text sent so far across every attempt.
    sent: String,
    /// How much of `sent` the current attempt has reproduced.
    replayed: usize,
    /// Whether text reaches the client as it is produced; a buffered reply sends only the
    /// final attempt's text, so a restart owes it nothing.
    streaming: bool,
}

impl Delivered {
    fn new(streaming: bool) -> Self {
        Self {
            sent: String::new(),
            replayed: 0,
            streaming,
        }
    }

    /// Begins another attempt, which must first reproduce whatever was sent.
    fn restart(&mut self) {
        self.replayed = 0;
        if !self.streaming {
            self.sent.clear();
        }
    }

    /// Takes the attempt's next piece and returns the part the client has not seen yet.
    fn accept<'a>(&mut self, piece: &'a str) -> Result<&'a str, Diverged> {
        let pending = self.sent.get(self.replayed..).ok_or(Diverged)?;
        let overlap = pending.len().min(piece.len());
        if pending.as_bytes()[..overlap] != piece.as_bytes()[..overlap] {
            return Err(Diverged);
        }
        let fresh = piece.get(overlap..).ok_or(Diverged)?;
        self.replayed += overlap + fresh.len();
        self.sent.push_str(fresh);
        Ok(fresh)
    }

    /// Checks the attempt reproduced everything a previous one sent.
    fn finish(&self) -> Result<(), Diverged> {
        if self.replayed == self.sent.len() {
            Ok(())
        } else {
            Err(Diverged)
        }
    }
}

/// Waits for the whole generation and answers with one completion object.
async fn buffered_response(mut events: Receiver<Event>, _task: GenerationTask) -> Response {
    let completion = Completion::new();
    loop {
        match events.recv().await {
            Some(Event::Started | Event::Text(_)) => {}
            Some(Event::Finished(generation)) => {
                return Json(completion.buffered(&generation)).into_response();
            }
            Some(Event::Failed(failure)) => return failure_response(&failure),
            None => return failure_response(&Failure::task_lost()),
        }
    }
}

/// State of one streamed reply; dropping it (client disconnect) aborts the generation.
struct SseStream {
    events: Receiver<Event>,
    completion: Completion,
    _task: GenerationTask,
    ended: bool,
}

impl SseStream {
    async fn next_event(mut self) -> Option<(Result<Bytes, Infallible>, Self)> {
        if self.ended {
            return None;
        }
        let bytes = loop {
            match self.events.recv().await {
                Some(Event::Started) => {}
                Some(Event::Text(text)) => {
                    break self.completion.chunk(json!({"content": text}), None);
                }
                Some(Event::Finished(generation)) => {
                    self.ended = true;
                    let last = self.completion.chunk(json!({}), Some(generation.finish));
                    break [last, sse_data(SSE_DONE)].concat().into();
                }
                Some(Event::Failed(failure)) => {
                    self.ended = true;
                    break sse_error(&failure.message);
                }
                None => {
                    self.ended = true;
                    break sse_error(&Failure::task_lost().message);
                }
            }
        };
        Some((Ok(bytes), self))
    }
}

/// Streams the reply: a role chunk, content chunks, a final chunk with `finish_reason`, then
/// `[DONE]`; or an `error` event and no `[DONE]` if the generation fails.
fn stream_response(events: Receiver<Event>, task: GenerationTask) -> Response {
    let completion = Completion::new();
    let role = completion.chunk(json!({"role": "assistant", "content": ""}), None);
    let state = SseStream {
        events,
        completion,
        _task: task,
        ended: false,
    };
    let body = stream::once(async move { Ok::<_, Infallible>(role) })
        .chain(stream::unfold(state, SseStream::next_event));
    let mut response = Response::new(Body::from_stream(body));
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(EVENT_STREAM));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(NO_CACHE));
    response
}

fn sse_data(data: &str) -> Bytes {
    format!("data: {data}\n\n").into()
}

fn sse_error(message: &str) -> Bytes {
    sse_data(&json!({"error": {"message": message, "type": "server_error"}}).to_string())
}

fn failure_response(failure: &Failure) -> Response {
    error_response(failure.status, &failure.message)
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let kind = if status.is_client_error() {
        "invalid_request_error"
    } else {
        "server_error"
    };
    (
        status,
        Json(json!({"error": {"message": message, "type": kind}})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::manifest::ChatTemplate;

    const CAP: u32 = 64;

    fn parse(body: Value) -> Result<ChatRequest, String> {
        let renderer = ChatRenderer::load(&ChatTemplate::Chatml).unwrap();
        parse_chat_request(body.to_string().as_bytes(), &renderer, CAP)
    }

    #[test]
    fn chatml_renders_each_message_then_the_assistant_turn() {
        let request = parse(json!({"messages": [
            {"role": "system", "content": "Be brief."},
            {"role": "user", "content": "hi"},
        ]}))
        .unwrap();
        assert_eq!(
            request.prompt,
            "<|im_start|>system\nBe brief.<|im_end|>\n\
             <|im_start|>user\nhi<|im_end|>\n<|im_start|>assistant\n"
        );
        assert!(!request.stream);
        assert_eq!(request.max_tokens, CAP);
    }

    #[test]
    fn text_parts_are_joined() {
        let request = parse(
            json!({"stream": true, "messages": [{"role": "user", "content": [
                {"type": "text", "text": "hel"}, {"type": "text", "text": "lo"},
            ]}]}),
        )
        .unwrap();
        assert!(request.prompt.contains("user\nhello<|im_end|>"));
        assert!(request.stream);
    }

    #[test]
    fn other_content_is_rejected() {
        let image = json!({"type": "image_url", "image_url": {"url": "x"}});
        for content in [
            json!(null),
            json!(7),
            json!([image]),
            json!([{"text": "no type"}]),
        ] {
            let body = json!({"messages": [{"role": "user", "content": content}]});
            assert!(parse(body).is_err(), "{content}");
        }
        assert!(parse(json!({"messages": []})).is_err());
        assert!(parse(json!({"prompt": "hi"})).is_err());
        assert!(parse(json!({"messages": [{"content": "hi"}]})).is_err());
    }

    #[test]
    fn max_tokens_is_honoured_and_capped() {
        let messages = json!([{"role": "user", "content": "hi"}]);
        let asked = |fields: Value| {
            let mut body = json!({"messages": messages});
            body.as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            parse(body).map(|request| request.max_tokens)
        };
        assert_eq!(asked(json!({"max_tokens": 5})), Ok(5));
        assert_eq!(asked(json!({"max_tokens": 5000})), Ok(CAP));
        assert_eq!(
            asked(json!({"max_tokens": 9, "max_completion_tokens": 3})),
            Ok(3)
        );
        assert_eq!(
            asked(json!({"max_tokens": null, "temperature": 0.2})),
            Ok(CAP)
        );
        assert!(asked(json!({"max_tokens": 0})).is_err());
    }

    #[test]
    fn non_loopback_listen_addresses_are_refused() {
        assert!(ensure_loopback("127.0.0.1:8080".parse().unwrap()).is_ok());
        assert!(ensure_loopback("[::1]:8080".parse().unwrap()).is_ok());
        assert!(ensure_loopback("[::ffff:127.0.0.1]:8080".parse().unwrap()).is_ok());
        assert!(ensure_loopback("0.0.0.0:8080".parse().unwrap()).is_err());
        assert!(ensure_loopback("192.168.1.5:8080".parse().unwrap()).is_err());
    }

    /// Feeds `pieces` to `delivered` and returns what each one released to the client.
    fn feed(delivered: &mut Delivered, pieces: &[&str]) -> Result<Vec<String>, Diverged> {
        pieces
            .iter()
            .map(|piece| delivered.accept(piece).map(str::to_owned))
            .collect()
    }

    #[test]
    fn a_streamed_restart_resumes_after_the_text_already_sent() {
        let mut delivered = Delivered::new(true);
        assert_eq!(feed(&mut delivered, &["Hel", "lo"]).unwrap(), ["Hel", "lo"]);
        delivered.restart();
        let released = feed(&mut delivered, &["He", "l", "lo w", "orld"]).unwrap();
        assert_eq!(released, ["", "", " w", "orld"]);
        assert!(delivered.finish().is_ok());
    }

    #[test]
    fn a_streamed_restart_must_reproduce_what_was_sent() {
        let mut delivered = Delivered::new(true);
        feed(&mut delivered, &["Hello"]).unwrap();
        delivered.restart();
        assert!(feed(&mut delivered, &["Help"]).is_err());

        let mut short = Delivered::new(true);
        feed(&mut short, &["Hello"]).unwrap();
        short.restart();
        feed(&mut short, &["He"]).unwrap();
        assert!(
            short.finish().is_err(),
            "stopped before reproducing the text"
        );
    }

    #[test]
    fn replay_respects_multibyte_characters() {
        let mut delivered = Delivered::new(true);
        feed(&mut delivered, &["a€"]).unwrap();
        delivered.restart();
        assert_eq!(feed(&mut delivered, &["a", "€b"]).unwrap(), ["", "b"]);
        assert!(delivered.finish().is_ok());
    }

    #[test]
    fn a_buffered_restart_owes_the_client_nothing() {
        let mut delivered = Delivered::new(false);
        feed(&mut delivered, &["Hello"]).unwrap();
        delivered.restart();
        assert_eq!(feed(&mut delivered, &["Bye"]).unwrap(), ["Bye"]);
        assert!(delivered.finish().is_ok());
    }

    #[test]
    fn prompt_too_long_is_a_client_error() {
        let error: anyhow::Error = PromptTooLong {
            prompt_tokens: 9,
            context: 8,
        }
        .into();
        assert_eq!(Failure::from_error(&error).status, StatusCode::BAD_REQUEST);
        let other = anyhow::anyhow!("stage died");
        assert_eq!(Failure::from_error(&other).status, StatusCode::BAD_GATEWAY);
    }
}
