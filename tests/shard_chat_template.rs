//! The shard frontend with a pinned Jinja chat template: it is verified once at startup and
//! rendered per request, and template errors are client errors.
//!
//! Generations run on a single local `fake_shard_runner` stage; the endpoint never dials out.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode};
use llm_cluster::shard::frontend::{FrontendConfig, serve_frontend};
use llm_cluster::shard::manifest::{ShardManifest, sha256_file};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

mod common;
use common::example_bin;

/// `source_sha256` carried by `examples/fake-shard-model.json`.
const FIXTURE_SHA: &str = "5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";
const LAYERS: u32 = 4;
const CONTEXT: u32 = 512;
const BUDGET_BYTES: u64 = 1_048_576;
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
/// Name the template is copied to, next to the manifest that names it by a relative path.
const TEMPLATE_FILE: &str = "chat_template.jinja";
/// A template that refuses conversations without a leading system message.
const SYSTEM_REQUIRED: &str = "{% if messages[0].role != 'system' %}\
    {{ raise_exception('a system message is required') }}{% endif %}\
    {% for message in messages %}{{ message.content }}{% endfor %}";

fn fixture_model() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("fake-shard-model.json")
}

fn qwen3_template() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("qwen3-chat-template.jinja")
}

fn fixture_reply() -> String {
    let text = std::fs::read_to_string(fixture_model()).expect("read model fixture");
    let model: Value = serde_json::from_str(&text).expect("parse model fixture");
    model["reply"].as_str().expect("fixture reply").to_owned()
}

/// A scratch directory holding a manifest and its template.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("shard-chat-template-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Self(dir)
    }

    /// Writes `source` as the template and a manifest that pins it by `sha256` through a
    /// relative path, then loads the manifest from disk.
    fn manifest(&self, source: &str, sha256: Option<&str>) -> ShardManifest {
        let template = self.0.join(TEMPLATE_FILE);
        std::fs::write(&template, source).expect("write template");
        let sha256 = sha256.map_or_else(
            || sha256_file(&template).expect("hash template"),
            str::to_owned,
        );
        let value = json!({
            "runner": example_bin("fake_shard_runner"),
            "model_sha256": FIXTURE_SHA,
            "layers": LAYERS,
            "context": CONTEXT,
            "chat_template": {"jinja": {"path": TEMPLATE_FILE, "sha256": sha256}},
            "stages": [{"begin": 0, "end": LAYERS, "local": {
                "model": fixture_model(), "budget_bytes": BUDGET_BYTES,
            }}],
        });
        let path = self.0.join("manifest.json");
        std::fs::write(&path, value.to_string()).expect("write manifest");
        ShardManifest::load(&path).expect("load manifest")
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Best-effort cleanup of a temp dir; a leftover is harmless and must not mask a failure.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn bind_endpoint() -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr(LOOPBACK_ANY_PORT)
        .expect("valid bind address")
        .alpns(Vec::new())
        .bind()
        .await
        .expect("bind endpoint")
}

fn free_loopback_addr() -> SocketAddr {
    TcpListener::bind(LOOPBACK_ANY_PORT)
        .and_then(|listener| listener.local_addr())
        .expect("free loopback port")
}

fn config(manifest: ShardManifest, endpoint: &Endpoint) -> FrontendConfig {
    FrontendConfig {
        listen: free_loopback_addr(),
        manifest,
        endpoint: endpoint.clone(),
        ring: None,
    }
}

/// A running frontend and how to stop it.
struct Frontend {
    addr: SocketAddr,
    endpoint: Endpoint,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Frontend {
    async fn start(manifest: ShardManifest) -> Self {
        let endpoint = bind_endpoint().await;
        let config = config(manifest, &endpoint);
        let addr = config.listen;
        let (shutdown, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(serve_frontend(config, async {
            // A dropped sender also means shut down.
            let _ = stopped.await;
        }));
        let health = format!("http://{addr}/health");
        timeout(TEST_TIMEOUT, async {
            loop {
                if let Ok(response) = reqwest::get(&health).await
                    && response.status().is_success()
                {
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("frontend becomes healthy");
        Self {
            addr,
            endpoint,
            shutdown,
            task,
        }
    }

    async fn chat(&self, messages: Value) -> (u16, Value) {
        let response = timeout(
            TEST_TIMEOUT,
            reqwest::Client::new()
                .post(format!("http://{}/v1/chat/completions", self.addr))
                .json(&json!({"messages": messages}))
                .send(),
        )
        .await
        .expect("chat request timed out")
        .expect("chat request");
        let status = response.status().as_u16();
        let body = timeout(TEST_TIMEOUT, response.json())
            .await
            .expect("chat body timed out")
            .expect("chat body json");
        (status, body)
    }

    async fn stop(self) {
        // Fails only if the frontend already exited, which the join below reports.
        let _ = self.shutdown.send(());
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("frontend stops")
            .expect("frontend task did not panic")
            .expect("frontend exits cleanly");
        self.endpoint.close().await;
    }
}

fn system_and_user() -> Value {
    json!([
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "hi"},
    ])
}

#[tokio::test]
async fn pinned_qwen3_template_serves_completions() {
    let scratch = Scratch::new();
    let source = std::fs::read_to_string(qwen3_template()).expect("read Qwen3 fixture");
    let frontend = Frontend::start(scratch.manifest(&source, None)).await;
    let (status, body) = frontend.chat(system_and_user()).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["choices"][0]["message"]["content"], fixture_reply());
    frontend.stop().await;
}

#[tokio::test]
async fn raise_exception_is_answered_with_a_client_error() {
    let scratch = Scratch::new();
    let frontend = Frontend::start(scratch.manifest(SYSTEM_REQUIRED, None)).await;
    let (status, body) = frontend
        .chat(json!([{"role": "user", "content": "hi"}]))
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    let message = body["error"]["message"].as_str().expect("error message");
    assert!(
        message.contains("a system message is required"),
        "{message}"
    );
    let (status, body) = frontend.chat(system_and_user()).await;
    assert_eq!(status, 200, "{body}");
    frontend.stop().await;
}

#[tokio::test]
async fn frontend_refuses_to_start_with_a_mismatched_or_missing_template() {
    let endpoint = bind_endpoint().await;
    let scratch = Scratch::new();
    let wrong = "0".repeat(FIXTURE_SHA.len());
    let mismatched = scratch.manifest(SYSTEM_REQUIRED, Some(&wrong));
    let error = serve_frontend(config(mismatched, &endpoint), std::future::pending())
        .await
        .expect_err("a mismatched template hash must be refused");
    assert!(
        format!("{error:#}").contains("chat_template.jinja.sha256"),
        "{error:#}"
    );

    let missing = scratch.manifest(SYSTEM_REQUIRED, None);
    std::fs::remove_file(scratch.path().join(TEMPLATE_FILE)).expect("remove template");
    let error = serve_frontend(config(missing, &endpoint), std::future::pending())
        .await
        .expect_err("a missing template must be refused");
    assert!(
        format!("{error:#}").contains("failed to read chat template"),
        "{error:#}"
    );
    endpoint.close().await;
}
