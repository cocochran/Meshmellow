//! End-to-end tests of `shard-ring-worker`: ring workers run the `fake_shard_runner` example and
//! the test plays the origin by hand with the `shard::ring` protocol helpers. Everything runs on
//! loopback with relays disabled; peers find each other through one in-memory address lookup.

use std::path::PathBuf;
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, ConnectionError, RecvStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use llm_cluster::rpc_bridge::{PEER_NOT_APPROVED, PEER_NOT_APPROVED_REASON};
use llm_cluster::shard::ring::{
    ExecuteFrame, RING_ALPN, RingMessage, RingResult, RingStream, RingWorkerConfig,
    accept_result_channel, encode_activations, new_job_id, open_ring_session, read_control,
    read_result, read_result_channel_hello, serve_ring_worker, validate_chain, write_control,
    write_execute,
};
use llm_cluster::shard::serve::PLACEHOLDER_PROMPT;
use llm_cluster::shard::wire;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::BufReader;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

mod common;
use common::example_bin;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";
const CONTEXT: u32 = 128;
const BUDGET_BYTES: u64 = 1 << 20;
/// The origin's own stage is `[0, ORIGIN_END)` of the fixture's four layers.
const ORIGIN_END: u32 = 1;
/// The chained workers: a middle stage, then the terminal.
const MIDDLE: (u32, u32) = (1, 3);
const TERMINAL: (u32, u32) = (3, 4);
/// A lone worker that is both the ring's entry and its terminal.
const LONE: (u32, u32) = (1, 4);
/// Prefill tokens sent in the first frame; the rest of the prompt goes in the second.
const FIRST_PREFILL_TOKENS: usize = 6;
/// Progress lines each stage sends per step (stage_start and stage_end).
const PROGRESS_PER_STAGE_STEP: usize = 2;
/// The fake tokenizer's beginning-of-sequence token.
const BOS_TOKEN: u32 = 0;

fn init_tracing() {
    // Another test in this binary may already have installed the subscriber; that is fine.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn model_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("fake-shard-model.json")
}

fn fixture_reply() -> String {
    let text = std::fs::read_to_string(model_fixture()).expect("read model fixture");
    let model: Value = serde_json::from_str(&text).expect("parse model fixture");
    model["reply"].as_str().expect("fixture reply").to_owned()
}

fn sha256_hex(path: &PathBuf) -> String {
    let bytes = std::fs::read(path).expect("read file to hash");
    wire::encode_hex(&Sha256::digest(&bytes))
}

/// The fake runner's tokenization of the workers' prompt: BOS, then each byte plus one. Prefilling
/// exactly these tokens makes the terminal start its reply on the last prefill step.
fn prompt_tokens() -> Vec<u32> {
    std::iter::once(BOS_TOKEN)
        .chain(PLACEHOLDER_PROMPT.bytes().map(|byte| u32::from(byte) + 1))
        .collect()
}

/// Endpoints of one test ring, all reachable through a shared in-memory lookup.
struct Mesh {
    lookup: MemoryLookup,
}

impl Mesh {
    fn new() -> Self {
        init_tracing();
        Self {
            lookup: MemoryLookup::new(),
        }
    }

    /// Binds a loopback-only endpoint on the ring ALPN and publishes its address to the mesh.
    async fn bind(&self) -> Endpoint {
        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .bind_addr(LOOPBACK_ANY_PORT)
            .expect("valid bind address")
            .alpns(vec![RING_ALPN.to_vec()])
            .address_lookup(self.lookup.clone())
            .bind()
            .await
            .expect("bind endpoint");
        let addr = endpoint
            .bound_sockets()
            .into_iter()
            .fold(EndpointAddr::new(endpoint.id()), EndpointAddr::with_ip_addr);
        self.lookup.add_endpoint_info(addr);
        endpoint
    }
}

fn worker_config(
    (begin, end): (u32, u32),
    upstream: EndpointId,
    origin: EndpointId,
    next: Option<EndpointId>,
) -> RingWorkerConfig {
    RingWorkerConfig {
        runner: example_bin("fake_shard_runner"),
        model: model_fixture(),
        begin,
        end,
        context: CONTEXT,
        gpu_layers: 0,
        budget_bytes: BUDGET_BYTES,
        expected_sha256: sha256_hex(&model_fixture()),
        source_sha256: None,
        upstream,
        origin,
        next,
    }
}

/// A running `serve_ring_worker`.
struct Worker {
    endpoint: Endpoint,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Worker {
    fn serve(endpoint: Endpoint, config: RingWorkerConfig) -> Self {
        let task = tokio::spawn(serve_ring_worker(endpoint.clone(), config));
        Self { endpoint, task }
    }

    fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Closing the endpoint must make `serve_ring_worker` return `Ok(())`.
    async fn shut_down(self) {
        self.endpoint.close().await;
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("serve_ring_worker stops after endpoint close")
            .expect("serve_ring_worker did not panic")
            .expect("serve_ring_worker returns Ok");
    }
}

/// The terminal's result channel, as the origin accepted it.
struct ResultChannel {
    _connection: Connection,
    recv: BufReader<RecvStream>,
    job: String,
}

/// Accepts the next result channel on `origin`, checking it comes from `terminal`.
async fn accept_results(origin: &Endpoint, terminal: EndpointId) -> ResultChannel {
    let accept = async {
        let incoming = origin.accept().await.expect("origin endpoint open");
        let connection = incoming.await.expect("accept result connection");
        assert_eq!(
            connection.remote_id(),
            terminal,
            "only the terminal calls back"
        );
        let (mut send, recv) = connection.accept_bi().await.expect("accept result stream");
        let mut recv = BufReader::new(recv);
        let job = read_result_channel_hello(&mut recv)
            .await
            .expect("result channel hello");
        accept_result_channel(&mut send, &job)
            .await
            .expect("accept result channel");
        ResultChannel {
            _connection: connection,
            recv,
            job,
        }
    };
    timeout(TEST_TIMEOUT, accept)
        .await
        .expect("result channel timed out")
}

/// Starts a session at `entry`, expecting the terminal's result channel; the outcome of the
/// entry side is returned as is.
async fn try_start(
    origin: &Endpoint,
    entry: EndpointId,
    terminal: EndpointId,
) -> (String, anyhow::Result<RingStream>, ResultChannel) {
    let job = new_job_id();
    let start = timeout(TEST_TIMEOUT, open_ring_session(origin, entry, &job));
    let (started, results) = tokio::join!(start, accept_results(origin, terminal));
    let started = started.expect("ring readiness timed out");
    (job, started, results)
}

/// A ready ring session seen from the origin.
struct OriginSession {
    job: String,
    entry: RingStream,
    results: ResultChannel,
    embd: u32,
    step: u32,
    pos: u32,
}

impl OriginSession {
    async fn start(origin: &Endpoint, entry: EndpointId, terminal: EndpointId) -> Self {
        let (job, started, results) = try_start(origin, entry, terminal).await;
        let entry = started.expect("ring session starts");
        assert_eq!(results.job, job, "the result channel names our job");
        validate_chain(&entry.stages).expect("valid ready chain");
        let embd = entry.stages[0].meta.embd;
        Self {
            job,
            entry,
            results,
            embd,
            step: 0,
            pos: 0,
        }
    }

    /// Sends `frame` with the rows the origin's own stage `[0, ORIGIN_END)` would output for
    /// `tokens`: `embd` copies of `t + ORIGIN_END` per token, as the fake runner computes them.
    async fn send_frame(&mut self, frame: &ExecuteFrame, tokens: &[u32]) {
        let rows: Vec<f32> = tokens
            .iter()
            .flat_map(|&token| std::iter::repeat_n((token + ORIGIN_END) as f32, self.embd as usize))
            .collect();
        write_execute(&mut self.entry.send, frame, &encode_activations(&rows))
            .await
            .expect("send execute frame");
    }

    /// Sends the next in-order frame for `tokens`.
    async fn send_tokens(&mut self, tokens: &[u32]) {
        let n = u32::try_from(tokens.len()).expect("small batch");
        let frame = ExecuteFrame::new(&self.job, self.step, self.pos, n, self.embd);
        self.send_frame(&frame, tokens).await;
        self.step += 1;
        self.pos += n;
    }

    /// Runs one in-order step and returns the terminal's result for it.
    async fn run_step(&mut self, tokens: &[u32]) -> RingResult {
        self.send_tokens(tokens).await;
        let step = self.step - 1;
        timeout(
            TEST_TIMEOUT,
            read_result(&mut self.results.recv, &self.job, step),
        )
        .await
        .expect("result timed out")
        .expect("read result")
    }

    /// Prefills the workers' prompt in two frames, then feeds every token back until
    /// end-of-generation, checking each result's metrics. Returns the generated text.
    async fn generate(&mut self) -> String {
        let prompt = prompt_tokens();
        let (first, rest) = prompt.split_at(FIRST_PREFILL_TOKENS);
        let prefill = self.run_step(first).await;
        self.check_metrics(&prefill);
        assert!(prefill.reply.piece_bytes().expect("piece").is_empty());
        let mut result = self.run_step(rest).await;
        let mut text = Vec::new();
        loop {
            self.check_metrics(&result);
            if result.reply.eog {
                break;
            }
            text.extend(result.reply.piece_bytes().expect("piece"));
            let token = u32::try_from(result.reply.token).expect("sampled token");
            result = self.run_step(&[token]).await;
        }
        String::from_utf8(text).expect("utf-8 reply")
    }

    /// Every stage adds its metrics, tagged with its worker's node id.
    fn check_metrics(&self, result: &RingResult) {
        let expected: Vec<&str> = self
            .entry
            .stages
            .iter()
            .map(|stage| stage.node_id.as_str())
            .collect();
        let reported: Vec<&str> = result
            .chain_metrics
            .iter()
            .map(|metric| metric["node_id"].as_str().expect("metric node id"))
            .collect();
        assert_eq!(reported, expected);
        assert_eq!(
            result.reply.extra["node_id"],
            Value::from(expected[expected.len() - 1])
        );
    }

    /// Ends the session and checks that only JSON control lines ever came back: progress lines on
    /// the entry stream (from every stage, for every step) and nothing beyond the results on the
    /// result channel. No activation bytes reach the origin.
    async fn finish(mut self) {
        self.entry.send.finish().expect("finish entry stream");
        let mut progress = 0;
        while let Some(message) = timeout(TEST_TIMEOUT, read_control(&mut self.entry.recv))
            .await
            .expect("entry stream timed out")
            .expect("entry stream carries only control lines")
        {
            let RingMessage::Progress(body) = message else {
                panic!("entry stream carried a {} line", message.kind());
            };
            let event = body["event"].as_str().expect("progress event");
            assert!(matches!(event, "stage_start" | "stage_end"), "{event}");
            progress += 1;
        }
        let steps = self.step as usize;
        let expected = steps * self.entry.stages.len() * PROGRESS_PER_STAGE_STEP;
        assert_eq!(progress, expected, "progress lines for {steps} steps");
        let trailing = timeout(TEST_TIMEOUT, read_control(&mut self.results.recv))
            .await
            .expect("result channel timed out")
            .expect("result channel ends cleanly");
        assert_eq!(trailing, None, "nothing follows the last result");
    }

    /// Expects the workers to have failed the session: the entry stream ends in an error after
    /// any progress lines, and the result channel yields no further result.
    async fn expect_failure(mut self) {
        loop {
            let read = timeout(TEST_TIMEOUT, read_control(&mut self.entry.recv))
                .await
                .expect("entry stream timed out");
            match read {
                Ok(Some(RingMessage::Progress(_))) => {}
                Ok(Some(other)) => panic!("entry stream carried a {} line", other.kind()),
                Ok(None) => panic!("a failed session must not end cleanly"),
                Err(_) => break,
            }
        }
        let trailing = timeout(TEST_TIMEOUT, read_control(&mut self.results.recv))
            .await
            .expect("result channel timed out");
        assert!(
            !matches!(trailing, Ok(Some(_))),
            "a failed session sends no result: {trailing:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chained_workers_forward_activations_and_return_results_to_the_origin() {
    let mesh = Mesh::new();
    let origin = mesh.bind().await;
    let middle_endpoint = mesh.bind().await;
    let terminal_endpoint = mesh.bind().await;
    let terminal = Worker::serve(
        terminal_endpoint,
        worker_config(TERMINAL, middle_endpoint.id(), origin.id(), None),
    );
    let middle = Worker::serve(
        middle_endpoint,
        worker_config(MIDDLE, origin.id(), origin.id(), Some(terminal.id())),
    );

    // The second session starts over at step 0, which only fresh runners accept.
    for _ in 0..2 {
        let mut session = OriginSession::start(&origin, middle.id(), terminal.id()).await;
        let stages = &session.entry.stages;
        let ranges: Vec<_> = stages
            .iter()
            .map(|stage| (stage.meta.begin, stage.meta.end))
            .collect();
        assert_eq!(ranges, [MIDDLE, TERMINAL]);
        let nodes: Vec<_> = stages.iter().map(|stage| stage.node_id.clone()).collect();
        assert_eq!(nodes, [middle.id().to_string(), terminal.id().to_string()]);
        assert!(
            stages
                .iter()
                .all(|stage| stage.meta.runner_sha256.is_some())
        );

        assert_eq!(session.generate().await, fixture_reply());
        session.finish().await;
    }

    origin.close().await;
    middle.shut_down().await;
    terminal.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_or_reordered_frames_fail_only_their_session() {
    let mesh = Mesh::new();
    let origin = mesh.bind().await;
    let endpoint = mesh.bind().await;
    let worker = Worker::serve(
        endpoint,
        worker_config(LONE, origin.id(), origin.id(), None),
    );
    let prompt = prompt_tokens();
    let first = &prompt[..FIRST_PREFILL_TOKENS];

    let mut replay = OriginSession::start(&origin, worker.id(), worker.id()).await;
    assert_eq!(replay.entry.stages.len(), 1);
    replay.run_step(first).await;
    let n = u32::try_from(first.len()).expect("small batch");
    let replayed = ExecuteFrame::new(&replay.job, 0, 0, n, replay.embd);
    replay.send_frame(&replayed, first).await;
    replay.expect_failure().await;

    let mut reordered = OriginSession::start(&origin, worker.id(), worker.id()).await;
    let skipped = ExecuteFrame::new(&reordered.job, 1, 0, n, reordered.embd);
    reordered.send_frame(&skipped, first).await;
    reordered.expect_failure().await;

    let mut fresh = OriginSession::start(&origin, worker.id(), worker.id()).await;
    assert_eq!(fresh.generate().await, fixture_reply());
    fresh.finish().await;

    origin.close().await;
    worker.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn noncontiguous_next_range_fails_readiness() {
    let mesh = Mesh::new();
    let origin = mesh.bind().await;
    let middle_endpoint = mesh.bind().await;
    let terminal_endpoint = mesh.bind().await;
    // The terminal starts at layer 2 while the middle stage ends at layer 3.
    let terminal = Worker::serve(
        terminal_endpoint,
        worker_config((2, 4), middle_endpoint.id(), origin.id(), None),
    );
    let middle = Worker::serve(
        middle_endpoint,
        worker_config(MIDDLE, origin.id(), origin.id(), Some(terminal.id())),
    );

    let (_job, started, _results) = try_start(&origin, middle.id(), terminal.id()).await;
    assert!(started.is_err(), "the middle worker must refuse the chain");

    origin.close().await;
    middle.shut_down().await;
    terminal.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unapproved_upstream_is_refused() {
    let mesh = Mesh::new();
    let stranger = mesh.bind().await;
    let endpoint = mesh.bind().await;
    let someone_else = iroh::SecretKey::generate().public();
    let worker = Worker::serve(
        endpoint,
        worker_config(LONE, someone_else, stranger.id(), None),
    );

    // The handshake itself succeeds; authorization happens after it.
    let connection = timeout(TEST_TIMEOUT, stranger.connect(worker.id(), RING_ALPN))
        .await
        .expect("connect timed out")
        .expect("connect to worker");
    if let Ok((mut send, _recv)) = connection.open_bi().await {
        // The connection may already be closed, in which case the write fails, as it should.
        let start = RingMessage::Start { job: new_job_id() };
        let _ = write_control(&mut send, &start).await;
    }
    let closed = timeout(TEST_TIMEOUT, connection.closed())
        .await
        .expect("worker closes the connection");
    let ConnectionError::ApplicationClosed(close) = closed else {
        panic!("expected an application close, got {closed:?}");
    };
    assert_eq!(close.error_code, PEER_NOT_APPROVED);
    assert_eq!(&close.reason[..], PEER_NOT_APPROVED_REASON);

    stranger.close().await;
    worker.shut_down().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_session_is_dropped_while_one_runs() {
    let mesh = Mesh::new();
    let origin = mesh.bind().await;
    let endpoint = mesh.bind().await;
    let worker = Worker::serve(
        endpoint,
        worker_config(LONE, origin.id(), origin.id(), None),
    );

    let mut running = OriginSession::start(&origin, worker.id(), worker.id()).await;
    let job = new_job_id();
    let second = timeout(TEST_TIMEOUT, open_ring_session(&origin, worker.id(), &job))
        .await
        .expect("dropped session ends promptly");
    assert!(second.is_err(), "a concurrent session gets no readiness");

    assert_eq!(running.generate().await, fixture_reply());
    running.finish().await;

    origin.close().await;
    worker.shut_down().await;
}

#[tokio::test]
async fn wrong_expected_sha256_fails_at_startup() {
    let mesh = Mesh::new();
    let endpoint = mesh.bind().await;
    let mut config = worker_config(LONE, endpoint.id(), endpoint.id(), None);
    config.expected_sha256 = "0".repeat(64);
    let result = timeout(TEST_TIMEOUT, serve_ring_worker(endpoint.clone(), config))
        .await
        .expect("startup check finishes");
    let error = result.expect_err("a mismatched model must be refused");
    assert!(error.to_string().contains("checksum mismatch"), "{error:#}");
    endpoint.close().await;
}
