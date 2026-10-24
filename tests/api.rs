//! The local API proxies chat requests to a fake llama-server, one job at a time.
//!
//! Each test launches the `fake_llama_server` example (built by `cargo test`) on a free loopback
//! port and serves the API in-process in front of it.

use std::net::{SocketAddr, TcpListener};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::routing::post;
use llm_cluster::api::{self, ApiConfig, REQUEST_ID_HEADER};
use llm_cluster::history::{CompletedTurn, HistorySink};
use llm_cluster::jobs::{JobQueue, JobStatus};
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod common;
use common::example_bin;

/// Upper bound for anything a test waits on, generous for slow CI machines.
const WAIT_LIMIT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// How long the fake server sleeps before answering in the FIFO test.
const SLOW_REPLY_MS: u64 = 300;

fn free_loopback_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("free loopback port")
}

async fn wait_for<T, F, Fut>(what: &str, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + WAIT_LIMIT;
    loop {
        if let Some(value) = probe().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn is_http_ok(url: &str) -> bool {
    match reqwest::get(url).await {
        Ok(response) => response.status().is_success(),
        Err(_) => false,
    }
}

struct FakeLlama {
    addr: SocketAddr,
    child: Child,
}

impl FakeLlama {
    /// Launches the fake server without waiting for it to listen.
    fn spawn(extra_args: &[&str]) -> Self {
        let addr = free_loopback_addr();
        let child = Command::new(example_bin("fake_llama_server"))
            .args(["--host", "127.0.0.1", "--port", &addr.port().to_string()])
            .args(["--model", "ignored.gguf", "-ngl", "99"])
            .args(extra_args)
            .stdout(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn fake_llama_server");
        Self { addr, child }
    }

    /// Launches the fake server and waits until `/health` answers 200.
    async fn start(extra_args: &[&str]) -> Self {
        let fake = Self::spawn(extra_args);
        let health = fake.url("/health");
        wait_for("fake llama-server", || async {
            is_http_ok(&health).await.then_some(())
        })
        .await;
        fake
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

#[derive(Default)]
struct RecordingSink {
    turns: Mutex<Vec<CompletedTurn>>,
}

impl RecordingSink {
    fn turns(&self) -> Vec<CompletedTurn> {
        self.turns.lock().unwrap().clone()
    }
}

impl HistorySink for RecordingSink {
    fn record(&self, turn: CompletedTurn) {
        self.turns.lock().unwrap().push(turn);
    }
}

struct TestApi {
    addr: SocketAddr,
    sink: Arc<RecordingSink>,
    client: reqwest::Client,
    shutdown: Option<oneshot::Sender<()>>,
    server: JoinHandle<anyhow::Result<()>>,
}

impl TestApi {
    async fn start(upstream: SocketAddr) -> Self {
        let addr = free_loopback_addr();
        let sink = Arc::new(RecordingSink::default());
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let config = ApiConfig {
            listen: addr,
            upstream,
        };
        let server = tokio::spawn(api::serve_api(
            config,
            Arc::new(JobQueue::new()),
            Arc::clone(&sink) as Arc<dyn HistorySink>,
            async move {
                // A dropped sender also means "shut down".
                let _ = shutdown_rx.await;
            },
        ));
        let jobs = format!("http://{addr}/jobs");
        wait_for("api", || async { is_http_ok(&jobs).await.then_some(()) }).await;
        Self {
            addr,
            sink,
            client: reqwest::Client::new(),
            shutdown: Some(shutdown),
            server,
        }
    }

    fn chat_url(&self) -> String {
        format!("http://{}/v1/chat/completions", self.addr)
    }

    async fn post_chat(&self, body: &Value) -> reqwest::Response {
        self.client
            .post(self.chat_url())
            .json(body)
            .send()
            .await
            .expect("chat request")
    }

    async fn jobs(&self) -> Vec<Value> {
        let url = format!("http://{}/jobs", self.addr);
        let jobs: Value = reqwest::get(url).await.unwrap().json().await.unwrap();
        jobs.as_array().expect("jobs array").clone()
    }

    async fn job_status(&self, request_id: &str) -> String {
        let jobs = self.jobs().await;
        let job = jobs
            .iter()
            .find(|job| job["request_id"] == request_id)
            .expect("job listed");
        job["status"].as_str().unwrap().to_owned()
    }

    async fn stop(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        let result = tokio::time::timeout(WAIT_LIMIT, self.server)
            .await
            .expect("api stops after shutdown")
            .unwrap();
        result.expect("api exits cleanly");
    }
}

fn chat_body(text: &str) -> Value {
    json!({"messages": [{"role": "user", "content": text}]})
}

fn request_id_of(response: &reqwest::Response) -> String {
    response.headers()[REQUEST_ID_HEADER]
        .to_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn non_streaming_requests_are_proxied_and_strip_conversation_id() {
    let fake = FakeLlama::start(&[]).await;
    let api = TestApi::start(fake.addr).await;

    assert_eq!(
        api::prompt(api.addr, "hello mesh").await.unwrap(),
        "hello mesh"
    );

    let mut body = chat_body("which conversation?");
    body["conversation_id"] = json!("conv-1");
    // The fake server rejects requests that still carry conversation_id, so 200 proves stripping.
    let response = api.post_chat(&body).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let request_id = request_id_of(&response);
    let reply: Value = response.json().await.unwrap();
    assert_eq!(
        reply["choices"][0]["message"]["content"],
        "which conversation?"
    );

    let turns = api.sink.turns();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].response_text, "hello mesh");
    assert_eq!(turns[0].conversation_id, None);
    assert_eq!(turns[1].request_id, request_id);
    assert_eq!(turns[1].conversation_id.as_deref(), Some("conv-1"));
    assert_eq!(turns[1].response_text, "which conversation?");
    assert!(turns[1].request.get("conversation_id").is_none());

    let jobs = api.jobs().await;
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job["status"] == "completed"));
    assert_eq!(jobs[1]["request_id"], request_id.as_str());
    assert_eq!(jobs[1]["conversation_id"], "conv-1");
    api.stop().await;
}

#[tokio::test]
async fn streamed_replies_are_relayed_chunk_by_chunk() {
    let fake = FakeLlama::start(&[]).await;
    let api = TestApi::start(fake.addr).await;
    let text = "one two three four five six seven eight";
    let mut body = chat_body(text);
    body["stream"] = json!(true);
    body["conversation_id"] = json!("conv-stream");

    let mut response = api.post_chat(&body).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let request_id = request_id_of(&response);
    let first = response.chunk().await.unwrap().expect("first chunk");
    // The upstream is still streaming, so the proxy is not buffering the whole reply.
    assert_eq!(api.job_status(&request_id).await, "running");

    let mut received = first.to_vec();
    let mut chunk_count = 1;
    while let Some(chunk) = response.chunk().await.unwrap() {
        received.extend_from_slice(&chunk);
        chunk_count += 1;
    }
    let received = String::from_utf8(received).unwrap();
    assert!(
        chunk_count > 1,
        "expected several chunks, got {chunk_count}"
    );
    assert!(received.matches("data: ").count() > text.split(' ').count());
    assert!(received.trim_end().ends_with("data: [DONE]"));

    let turns = api.sink.turns();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].response_text, text);
    assert_eq!(turns[0].conversation_id.as_deref(), Some("conv-stream"));
    assert_eq!(api.job_status(&request_id).await, "completed");
    api.stop().await;
}

#[tokio::test]
async fn client_disconnect_mid_stream_cancels_the_job() {
    let fake = FakeLlama::start(&[]).await;
    let api = TestApi::start(fake.addr).await;
    let mut body = chat_body(&"word ".repeat(100));
    body["stream"] = json!(true);

    let mut response = api.post_chat(&body).await;
    let request_id = request_id_of(&response);
    response.chunk().await.unwrap().expect("first chunk");
    drop(response);

    wait_for("job canceled", || async {
        (api.job_status(&request_id).await == "canceled").then_some(())
    })
    .await;
    assert!(api.sink.turns().is_empty());
    assert_eq!(api::prompt(api.addr, "next").await.unwrap(), "next");
    api.stop().await;
}

#[tokio::test]
async fn concurrent_requests_run_strictly_one_after_another() {
    let fake = FakeLlama::start(&["--delay-ms", &SLOW_REPLY_MS.to_string()]).await;
    let api = TestApi::start(fake.addr).await;
    let first = tokio::spawn(api::prompt(api.addr, "first"));
    let second = tokio::spawn(api::prompt(api.addr, "second"));

    let observed = wait_for("one running and one queued job", || async {
        let statuses: Vec<String> = api
            .jobs()
            .await
            .iter()
            .map(|job| job["status"].as_str().unwrap().to_owned())
            .collect();
        let is_running = statuses.iter().any(|status| status == "running");
        let is_queued = statuses.iter().any(|status| status == "queued");
        (is_running && is_queued).then_some(statuses)
    })
    .await;
    assert_eq!(observed.len(), 2);

    assert_eq!(first.await.unwrap().unwrap(), "first");
    assert_eq!(second.await.unwrap().unwrap(), "second");
    let mut jobs = api.jobs().await;
    assert!(jobs.iter().all(|job| job["status"] == "completed"));
    jobs.sort_by_key(|job| job["started_at_unix_ms"].as_u64().unwrap());
    let earlier_finished = jobs[0]["finished_at_unix_ms"].as_u64().unwrap();
    let later_started = jobs[1]["started_at_unix_ms"].as_u64().unwrap();
    assert!(
        later_started >= earlier_finished,
        "second job started at {later_started} before the first finished at {earlier_finished}"
    );
    api.stop().await;
}

#[tokio::test]
async fn upstream_failures_mark_jobs_failed() {
    let fake = FakeLlama::start(&["--fail-completions"]).await;
    let api = TestApi::start(fake.addr).await;
    let response = api.post_chat(&chat_body("hi")).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let request_id = request_id_of(&response);
    assert!(api::prompt(api.addr, "hi").await.is_err());
    let jobs = api.jobs().await;
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job["status"] == "failed"));
    assert_eq!(jobs[0]["request_id"], request_id.as_str());
    assert!(jobs[0]["error"].as_str().unwrap().contains("500"));
    assert!(api.sink.turns().is_empty());
    api.stop().await;

    let unreachable = TestApi::start(free_loopback_addr()).await;
    let response = unreachable.post_chat(&chat_body("hi")).await;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);
    let jobs = unreachable.jobs().await;
    assert_eq!(jobs[0]["status"], "failed");
    assert!(jobs[0]["error"].as_str().unwrap().contains("upstream"));
    unreachable.stop().await;
}

async fn upstream_with_success_body(body: &'static str) -> (SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake upstream");
    let addr = listener.local_addr().expect("fake upstream address");
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move || async move {
            (
                axum::http::StatusCode::OK,
                [("content-type", "application/json")],
                body,
            )
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve fake upstream")
    });
    (addr, server)
}

#[tokio::test]
async fn invalid_success_replies_fail_job_and_are_not_saved_to_history() {
    for body in ["not json", r#"{"choices":[{"message":{}}]}"#] {
        let (upstream, server) = upstream_with_success_body(body).await;
        let api = TestApi::start(upstream).await;
        let response = api.post_chat(&chat_body("hello")).await;

        assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);
        let request_id = request_id_of(&response);
        let jobs = api.jobs().await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["request_id"], request_id);
        assert_eq!(jobs[0]["status"], "failed");
        assert!(jobs[0]["error"].as_str().is_some());
        assert!(api.sink.turns().is_empty());

        api.stop().await;
        server.abort();
    }
}

#[tokio::test]
async fn malformed_requests_are_rejected_without_a_job() {
    let api = TestApi::start(free_loopback_addr()).await;
    let response = api
        .client
        .post(api.chat_url())
        .body("not json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(api.jobs().await.is_empty());
    api.stop().await;
}

#[tokio::test]
async fn non_loopback_listen_address_is_refused() {
    let config = ApiConfig {
        listen: "0.0.0.0:0".parse().unwrap(),
        upstream: free_loopback_addr(),
    };
    let result = tokio::time::timeout(
        WAIT_LIMIT,
        api::serve_api(
            config,
            Arc::new(JobQueue::new()),
            Arc::new(RecordingSink::default()),
            std::future::pending(),
        ),
    )
    .await
    .expect("refused without serving");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("loopback"), "unexpected error: {err}");
}

#[tokio::test]
async fn fake_server_logs_its_argv_and_ignores_llama_server_flags() {
    let log = std::env::temp_dir().join(format!(
        "meshthing-fake-llama-argv-{}.json",
        std::process::id()
    ));
    // A leftover log from an aborted run may or may not exist; either way we start fresh.
    let _ = std::fs::remove_file(&log);
    let log_arg = log.to_str().unwrap();
    let fake = FakeLlama::start(&["--argv-log", log_arg]).await;

    let logged: Value = serde_json::from_slice(&std::fs::read(&log).unwrap()).unwrap();
    assert_eq!(logged["pid"].as_u64(), fake.child.id().map(u64::from));
    let port = fake.addr.port().to_string();
    let expected = json!([
        "--host",
        "127.0.0.1",
        "--port",
        port,
        "--model",
        "ignored.gguf",
        "-ngl",
        "99",
        "--argv-log",
        log_arg,
    ]);
    assert_eq!(logged["args"], expected);
    std::fs::remove_file(&log).unwrap();
}

#[tokio::test]
async fn fake_server_can_fail_health_forever() {
    let fake = FakeLlama::spawn(&["--fail-health"]);
    let health = fake.url("/health");
    let status = wait_for("fake llama-server to answer", || async {
        reqwest::get(&health)
            .await
            .ok()
            .map(|response| response.status())
    })
    .await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn job_status_serializes_as_lowercase() {
    let json = serde_json::to_value(JobStatus::Queued).unwrap();
    assert_eq!(json, "queued");
}
