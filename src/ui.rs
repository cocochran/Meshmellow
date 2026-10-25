//! One process owns membership, the queued API, and the static browser dashboard.
use crate::{
    api,
    history::LoggingHistorySink,
    jobs::JobQueue,
    mesh::{ApiEndpoint, Node},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Args;
use serde_json::{Value, json};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::AsyncReadExt;
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::services::ServeDir;
use tracing::debug;

/// Preferences contain only a display name and refresh interval; reject unexpectedly large files.
const MAX_PREFERENCES_BYTES: usize = 8 * 1024;

#[derive(Debug, Args)]
pub struct Options {
    /// Browser and OpenAI API port on this device. Zero chooses an available port.
    #[arg(long, default_value_t = 3210)]
    pub port: u16,
    /// Private identity, membership and preferences directory.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// Built static dashboard directory.
    #[arg(long)]
    pub assets: Option<PathBuf>,
    /// Existing local inference backend. No model is launched unless configured elsewhere.
    #[arg(long)]
    pub upstream: Option<SocketAddr>,
    /// Run this client's local prefix and remote mesh stages from this ring manifest.
    #[arg(long, conflicts_with_all = ["upstream", "shard_worker"])]
    pub shard_manifest: Option<PathBuf>,
    /// Serve an assigned model slice to approved mesh members using this worker JSON.
    #[arg(long, conflicts_with_all = ["upstream", "shard_manifest"])]
    pub shard_worker: Option<PathBuf>,
    /// Print the URL without opening the browser.
    #[arg(long)]
    pub no_open: bool,
}
struct App {
    node: Node,
    origin: String,
    queue: Arc<JobQueue>,
    upstream: Option<SocketAddr>,
    inference_manifest: Option<crate::shard::manifest::ShardManifest>,
    client: reqwest::Client,
    directory: PathBuf,
    commands: Mutex<()>,
    preferences: Mutex<()>,
}
impl App {
    async fn api_status(&self) -> ApiEndpoint {
        let snapshot = self.node.snapshot().await;
        let worker = snapshot
            .as_ref()
            .ok()
            .is_some_and(|s| s["inference"]["worker"].is_object());
        let active_manifest = self.node.active_compute_manifest().await;
        let connected = snapshot
            .as_ref()
            .ok()
            .and_then(|s| s["connected_peers"].as_array());
        let peers_ready = active_manifest
            .as_ref()
            .or(self.inference_manifest.as_ref())
            .is_none_or(|manifest| {
                manifest.stages.iter().all(|stage| match &stage.location {
                    crate::shard::manifest::StageLocation::Remote { worker } => connected
                        .is_some_and(|ids| {
                            ids.iter()
                                .any(|id| id.as_str() == Some(&worker.to_string()))
                        }),
                    _ => true,
                })
            });
        let member = match mesh_membership(snapshot) {
            Ok(member) => Some(member),
            Err(error) => {
                debug!(%error, "mesh state unavailable while checking inference readiness");
                None
            }
        };
        let route_ready = self.inference_manifest.is_none()
            || !self.node.compute_enabled().await
            || active_manifest.is_some();
        let ready = if member == Some(true)
            && peers_ready
            && route_ready
            && let Some(upstream) = self.upstream
        {
            self.client
                .get(format!("http://{upstream}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        } else {
            false
        };
        ApiEndpoint {
            base_url: format!("{}/v1", self.origin),
            inference_ready: ready,
            scope: "device_local".into(),
            message: if member.is_none() {
                "Mesh status is unavailable; inference readiness cannot be confirmed."
            } else if member == Some(false) {
                "Create or join a mesh to enable inference."
            } else if self.upstream.is_none() {
                if worker {
                    "Contributing a model slice to the mesh. Send prompts through the coordinator."
                } else {
                    "Connected to the mesh; waiting for a native compute assignment."
                }
            } else if !route_ready {
                "Preparing model slices for the connected native workers."
            } else if !peers_ready {
                "Waiting for the model's contributing mesh clients to connect."
            } else if !ready {
                "API listening. Inference backend is unavailable."
            } else {
                "Inference backend is responding."
            }
            .into(),
        }
    }
    async fn publish_status(&self) {
        self.node
            .set_api_endpoint(Some(self.api_status().await))
            .await;
    }
}

/// Preserve snapshot failures separately from a valid snapshot with no mesh membership.
fn mesh_membership(snapshot: Result<Value>) -> Result<bool> {
    snapshot.map(|snapshot| !snapshot["mesh"].is_null())
}
pub async fn run(options: Options) -> Result<()> {
    if let Some(upstream) = options.upstream {
        ensure!(
            upstream.ip().is_loopback(),
            "Inference upstream must be loopback."
        );
    }
    let directory = options
        .state_dir
        .or_else(|| std::env::var_os("LAYERCAKE_MESH_STATE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| {
            PathBuf::from(
                std::env::var_os("HOME")
                    .or_else(|| std::env::var_os("USERPROFILE"))
                    .unwrap_or_else(|| ".".into()),
            )
            .join(".local/state/layercake/mesh")
        });
    let assets = options.assets.unwrap_or_else(|| {
        let bundled = std::env::current_exe()
            .unwrap_or_default()
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("web/build");
        if bundled.join("index.html").is_file() {
            bundled
        } else {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("apps/web/build")
        }
    });
    ensure!(
        assets.join("index.html").is_file(),
        "Build the dashboard first: npm --prefix apps/web ci && npm --prefix apps/web run build"
    );
    let listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        options.port,
    ))
    .await
    .context("Cannot bind dashboard port")?;
    let local = listener.local_addr()?;
    ensure!(
        options.upstream != Some(local),
        "Inference upstream cannot be the dashboard itself."
    );
    let node = Node::open(&directory).await?;
    let mut inference_tasks = tokio::task::JoinSet::new();
    if let Some(path) = options.shard_worker {
        let bytes = crate::config::read_file_limited(&path, 64 * 1024)?;
        let config =
            serde_json::from_slice(&bytes).context("Invalid shard worker configuration")?;
        node.configure_inference_worker(config).await?;
    }
    let mut inference_manifest = None;
    let upstream = if let Some(path) = options.shard_manifest {
        let manifest = crate::shard::manifest::ShardManifest::load(&path)?;
        ensure!(
            manifest.is_ring(),
            "Client shared inference requires a ring manifest with remote stages."
        );
        inference_manifest = Some(manifest.clone());
        let backend_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = backend_listener.local_addr()?;
        let config = crate::shard::frontend::FrontendConfig {
            listen: address,
            manifest,
            endpoint: node.inference_endpoint(),
            ring: Some(node.inference_ring_settings()),
        };
        inference_tasks.spawn(crate::shard::frontend::serve_client_frontend(
            config,
            backend_listener,
            std::future::pending(),
            node.clone(),
        ));
        Some(address)
    } else {
        options.upstream
    };
    let app = Arc::new(App {
        node,
        origin: format!("http://{local}"),
        directory,
        queue: Arc::new(JobQueue::new()),
        upstream,
        inference_manifest,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        commands: Mutex::new(()),
        preferences: Mutex::new(()),
    });
    app.publish_status().await;
    let server = router(app.clone(), assets)?;
    let background = {
        let app = app.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(2));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                timer.tick().await;
                app.publish_status().await;
                let _ = tokio::time::timeout(Duration::from_secs(6), app.node.refresh()).await;
            }
        })
    };
    println!(
        "Layercake: {}\nOpenAI API: {}/v1 (on this device)\nPress Ctrl+C to stop this runtime and its UI.",
        app.origin, app.origin
    );
    if !options.no_open {
        open_browser(&app.origin);
    }
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut serving = tokio::spawn(async move {
        axum::serve(listener, server)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let completed = tokio::select! {
        result = &mut serving => Some(result),
        _ = shutdown() => None,
        result = inference_tasks.join_next(), if !inference_tasks.is_empty() => {
            eprintln!("Inference runtime stopped: {result:?}");
            None
        },
    };
    let _ = stop.send(());
    background.abort();
    inference_tasks.abort_all();
    while inference_tasks.join_next().await.is_some() {}
    app.node.set_api_endpoint(None).await;
    app.node.close().await;
    if let Some(result) = completed {
        result?.context("Dashboard server failed")
    } else {
        // A stalled streaming client must not keep the runtime alive indefinitely.
        match tokio::time::timeout(Duration::from_secs(5), &mut serving).await {
            Ok(result) => result?.context("Dashboard server failed"),
            Err(_) => {
                serving.abort();
                let _ = serving.await;
                Ok(())
            }
        }
    }
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let (program, args) = ("open", vec![url]);
    #[cfg(target_os = "linux")]
    let (program, args) = ("xdg-open", vec![url]);
    #[cfg(windows)]
    let (program, args) = ("rundll32.exe", vec!["url.dll,FileProtocolHandler", url]);
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    match std::process::Command::new(program).args(args).spawn() {
        Ok(mut child) => {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => eprintln!("Open {url} in your browser ({e})."),
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    let _ = url;
}
fn router(app: Arc<App>, assets: PathBuf) -> Result<Router> {
    let inference = if let Some(upstream) = app.upstream {
        api::queued_router(upstream, app.queue.clone(), Arc::new(LoggingHistorySink))?
    } else {
        Router::new()
            .route(
                api::CHAT_COMPLETIONS_PATH,
                post(|| async { unavailable("No inference backend is attached.") }),
            )
            .route(
                "/jobs",
                get({
                    let queue = app.queue.clone();
                    move || {
                        let queue = queue.clone();
                        async move { Json(queue.jobs()) }
                    }
                }),
            )
    };
    let inference =
        inference.route_layer(middleware::from_fn_with_state(app.clone(), inference_gate));
    Ok(Router::new()
        .route("/api/enrollment", get(snapshot).post(command))
        .route("/api/preferences", get(preferences).put(save_preferences))
        .route("/api/mesh", get(telemetry))
        .route("/api/status", get(status))
        .route("/v1/models", get(models))
        .layer(DefaultBodyLimit::max(8192))
        .with_state(app.clone())
        .merge(inference)
        .merge(api::peer_router(app.node.control_peer_table()))
        .fallback_service(ServeDir::new(assets))
        .layer(middleware::from_fn_with_state(app, local_access)))
}
fn error(code: StatusCode, message: impl ToString) -> Response {
    (code, Json(json!({"error": message.to_string()}))).into_response()
}
fn unavailable(message: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(
            json!({"error":{"message":message,"type":"server_error","code":"inference_not_ready"}}),
        ),
    )
        .into_response()
}
async fn local_access(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    if host != app.origin.strip_prefix("http://") || origin.is_some_and(|s| s != app.origin) {
        return error(StatusCode::FORBIDDEN, "Use this runtime's local URL.");
    }
    if request.uri().path().starts_with("/api/")
        && !matches!(
            *request.method(),
            axum::http::Method::GET | axum::http::Method::HEAD
        )
        && origin != Some(&app.origin)
    {
        return error(StatusCode::FORBIDDEN, "Same-origin request required.");
    }
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    response
}
async fn inference_gate(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    if request.uri().path() != "/jobs" {
        let status = app.api_status().await;
        if !status.inference_ready {
            return unavailable(&status.message);
        }
    }
    next.run(request).await
}
async fn snapshot(State(app): State<Arc<App>>) -> Response {
    match app.node.snapshot().await {
        Ok(s) => Json(s).into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
async fn command(State(app): State<Arc<App>>, Json(command): Json<Value>) -> Response {
    if !matches!(
        command["op"].as_str(),
        Some("create" | "join" | "invite" | "decide" | "cancel_join" | "leave" | "kick")
    ) {
        return error(StatusCode::BAD_REQUEST, "Unsupported mesh command.");
    }
    let Ok(_guard) = app.commands.try_lock() else {
        return error(StatusCode::CONFLICT, "Another mesh command is running.");
    };
    match app.node.command(command).await {
        Ok(_) => {
            app.publish_status().await;
            snapshot(State(app.clone())).await
        }
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}
async fn status(State(app): State<Arc<App>>) -> Json<Value> {
    Json(json!({"pid": std::process::id(), "api": app.api_status().await}))
}
async fn models(State(app): State<Arc<App>>) -> Response {
    let status = app.api_status().await;
    if !status.inference_ready {
        return unavailable(&status.message);
    }
    // Report the actual attached backend's model identifiers, never a guessed alias.
    match app
        .client
        .get(format!("http://{}/v1/models", app.upstream.unwrap()))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json::<Value>().await {
            Ok(v) => Json(v).into_response(),
            Err(_) => unavailable("Invalid model metadata from backend."),
        },
        _ => unavailable("Model metadata is unavailable."),
    }
}
async fn telemetry(State(app): State<Arc<App>>) -> Json<Value> {
    let api = app.api_status().await;
    let jobs = app.queue.jobs();
    let observed_peers = api::peer_snapshot(&app.node.control_peer_table());
    let peer_status_available = observed_peers.is_ok();
    let peers = observed_peers
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            json!({
                "id": p.node_id, "connectedAt": p.connected_at_unix_ms,
                "lastHeartbeat": p.last_heartbeat_unix_ms
            })
        })
        .collect::<Vec<_>>();
    let membership = app.node.snapshot().await.unwrap_or_default();
    let active_manifest = app.node.active_compute_manifest().await;
    let manifest = active_manifest.as_ref().or(app.inference_manifest.as_ref());
    let sessions = app.node.inference_ring_settings().channels.traffic();
    let execution = app.node.inference_ring_settings().channels.execution.snapshot();
    let hardware = app.node.compute_hardware().await;
    let hardware: Vec<Value> = hardware.iter().filter(|(id, _)| membership["members"].as_array().is_some_and(|ms| ms.iter().any(|m| m["id"] == *id && m["online"] == true))).map(|(id, offer)| json!({"id":id,"totalBytes":offer.total_memory_bytes,"availableBytes":offer.available_memory_bytes})).collect();
    let executed: u64 = sessions
        .iter()
        .filter(|s| s.succeeded)
        .map(|s| u64::from(s.steps))
        .sum();
    let devices: Vec<Value> = manifest.map(|manifest| manifest.stages.iter().map(|stage| {
        let id = match &stage.location {
            crate::shard::manifest::StageLocation::Remote { worker } => worker.to_string(),
            _ => app.node.id(),
        };
        let member = membership["members"].as_array().and_then(|members| members.iter().find(|m| m["id"] == id));
        json!({"id": id, "name": member.and_then(|m| m["name"].as_str()).unwrap_or("Assigned peer"),
            "hardware": "Native model worker", "begin": stage.begin, "end": stage.end,
            "status": if member.is_some_and(|m| m["online"] == true) {"connected"} else {"offline"},
            "executions": executed, "computeMs": null, "bufferBytes": null})
    }).collect()).unwrap_or_default();
    Json(
        json!({"connected": true, "source": "core", "endpoint": app.origin,
        "checkedAt": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
        "message": api.message, "active": execution.as_ref().is_some_and(|e| e.active) || jobs.iter().any(|j| !j.status.is_finished()), "event": execution.as_ref().map(|e| e.phase.as_str()).unwrap_or(if api.inference_ready {"ready"} else {"waiting"}),
        "model": if app.inference_manifest.is_some() {"Shared model"} else {"No distributed model configured"},
        "devices":devices, "peers":peers, "peerStatusAvailable":peer_status_available,
        "layers":app.inference_manifest.as_ref().map(|m| m.layers), "decodeRate":execution.as_ref().and_then(|e| e.decode_tps),
        "execution":execution, "hardware":hardware, "inferenceReady":api.inference_ready,
        "outputSteps":sessions.last().map(|s| s.steps),
        "jobs": jobs.iter().map(|j| json!({"id":j.request_id,"status":j.status,"createdAt":j.created_at_unix_ms})).collect::<Vec<_>>() }),
    )
}
fn validate_preferences(value: Value) -> Result<Value> {
    let name = value["displayName"]
        .as_str()
        .context("Display name required")?
        .trim();
    ensure!(
        !name.is_empty() && name.chars().count() <= 48 && !name.chars().any(char::is_control),
        "Choose a display name of 1–48 characters."
    );
    let seconds = value["refreshSeconds"]
        .as_u64()
        .context("Refresh interval required")?;
    ensure!(
        [2, 5, 10, 30].contains(&seconds),
        "Unsupported refresh interval."
    );
    Ok(json!({"displayName":name,"refreshSeconds":seconds}))
}
async fn preferences(State(app): State<Arc<App>>) -> Response {
    let _guard = app.preferences.lock().await;
    match read_preferences_file(&app.directory.join("preferences.json")).await {
        Ok(bytes) => match serde_json::from_slice(&bytes)
            .map_err(anyhow::Error::from)
            .and_then(validate_preferences)
        {
            Ok(v) => Json(v).into_response(),
            Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Json(json!({"displayName":"My mesh","refreshSeconds":5})).into_response()
        }
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

/// Reads at most the preferences size limit, including when the file grows after metadata check.
async fn read_preferences_file(path: &Path) -> io::Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    if file.metadata().await?.len() > MAX_PREFERENCES_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("preferences file exceeds the {MAX_PREFERENCES_BYTES}-byte limit"),
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_PREFERENCES_BYTES.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > MAX_PREFERENCES_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("preferences file exceeds the {MAX_PREFERENCES_BYTES}-byte limit"),
        ));
    }
    Ok(bytes)
}
async fn save_preferences(State(app): State<Arc<App>>, Json(value): Json<Value>) -> Response {
    let value = match validate_preferences(value) {
        Ok(v) => v,
        Err(e) => return error(StatusCode::BAD_REQUEST, e),
    };
    let _guard = app.preferences.lock().await;
    let temp = app.directory.join("preferences.tmp");
    let result = async {
        tokio::fs::write(&temp, serde_json::to_vec(&value)?).await?;
        tokio::fs::rename(&temp, app.directory.join("preferences.json")).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match result {
        Ok(()) => Json(value).into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mesh_snapshot_errors_are_not_mistaken_for_non_membership() {
        assert!(mesh_membership(Ok(json!({"mesh": {}}))).unwrap());
        assert!(!mesh_membership(Ok(json!({"mesh": null}))).unwrap());
        assert!(mesh_membership(Err(anyhow::anyhow!("unavailable"))).is_err());
    }

    #[tokio::test]
    async fn preferences_reader_enforces_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("preferences.json");
        tokio::fs::write(&path, vec![b'x'; MAX_PREFERENCES_BYTES])
            .await
            .unwrap();
        assert_eq!(
            read_preferences_file(&path).await.unwrap().len(),
            MAX_PREFERENCES_BYTES
        );

        tokio::fs::write(&path, vec![b'x'; MAX_PREFERENCES_BYTES + 1])
            .await
            .unwrap();
        let error = read_preferences_file(&path).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("8192-byte limit"));
    }
}
