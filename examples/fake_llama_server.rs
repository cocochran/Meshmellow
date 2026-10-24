//! Stand-in for llama.cpp's `llama-server`, used by integration tests.
//!
//! ```text
//! fake_llama_server --host <ip> --port <port> [--delay-ms <n>] [--fail-health]
//!                   [--fail-completions] [--argv-log <path>] [any llama-server args...]
//! ```
//!
//! - `GET /health` → 200 `{"status":"ok"}`, or 503 forever with `--fail-health`.
//! - `POST /v1/chat/completions` → an OpenAI-shaped reply whose content echoes the last user
//!   message, after sleeping `--delay-ms`. With `"stream": true` the reply is SSE chunks (one per
//!   word, a short pause between them) ending in `data: [DONE]`. With `--fail-completions` it
//!   answers 500 while `/health` stays 200, like llama-server over a stale RPC link. A request
//!   that still carries `conversation_id` gets a 400, so tests can prove the proxy strips it.
//! - `--argv-log <path>` writes `{"pid": <u32>, "args": [argv[1..]]}` to `path` at startup,
//!   atomically (temp file, then rename).
//!
//! Unknown arguments (`--model`, `--rpc`, `-ngl`, `--tensor-split`, ...) are ignored, and for a
//! repeated flag the last value wins, so the leader can launch it with real plan arguments.

use std::convert::Infallible;
use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};

/// llama-server's default port.
const DEFAULT_PORT: u16 = 8080;

/// Pause before each streamed chunk, so clients observe chunks arriving one at a time.
const STREAM_CHUNK_GAP: Duration = Duration::from_millis(40);

/// Model name reported in replies.
const MODEL_NAME: &str = "fake-llama";

/// Field the API proxy must strip before forwarding.
const CONVERSATION_ID_FIELD: &str = "conversation_id";

const SSE_CONTENT_TYPE: &str = "text/event-stream";

const HEALTH_PATH: &str = "/health";

const CHAT_COMPLETIONS_PATH: &str = "/v1/chat/completions";

#[derive(Debug)]
struct Options {
    host: IpAddr,
    port: u16,
    delay: Duration,
    is_health_failing: bool,
    are_completions_failing: bool,
    argv_log: Option<PathBuf>,
}

impl Options {
    /// Parses known flags (last occurrence wins) and skips everything else.
    fn parse(args: &[String]) -> anyhow::Result<Self> {
        let mut options = Self {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            delay: Duration::ZERO,
            is_health_failing: false,
            are_completions_failing: false,
            argv_log: None,
        };
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            let mut value = || args.next().with_context(|| format!("{arg} needs a value"));
            match arg.as_str() {
                "--host" => options.host = value()?.parse().context("invalid --host")?,
                "--port" => options.port = value()?.parse().context("invalid --port")?,
                "--delay-ms" => {
                    let millis = value()?.parse().context("invalid --delay-ms")?;
                    options.delay = Duration::from_millis(millis);
                }
                "--argv-log" => options.argv_log = Some(PathBuf::from(value()?)),
                "--fail-health" => options.is_health_failing = true,
                "--fail-completions" => options.are_completions_failing = true,
                _ => {}
            }
        }
        Ok(options)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let options = Options::parse(&args)?;
    if let Some(path) = &options.argv_log {
        write_argv_log(path, &args)?;
    }
    let listen = SocketAddr::new(options.host, options.port);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind {listen}"))?;
    let router = Router::new()
        .route(HEALTH_PATH, get(health))
        .route(CHAT_COMPLETIONS_PATH, post(chat_completions))
        .with_state(Arc::new(options));
    axum::serve(listener, router).await.context("server failed")
}

/// Writes the argv log via a temp file and rename, so readers never see a partial file.
fn write_argv_log(path: &Path, args: &[String]) -> anyhow::Result<()> {
    let log = json!({"pid": std::process::id(), "args": args});
    let mut temp: OsString = path.as_os_str().to_owned();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    std::fs::write(&temp, log.to_string())
        .with_context(|| format!("failed to write {}", temp.display()))?;
    std::fs::rename(&temp, path).with_context(|| format!("failed to write {}", path.display()))
}

async fn health(State(options): State<Arc<Options>>) -> Response {
    if options.is_health_failing {
        let body = json!({"error": {"message": "loading model", "code": 503}});
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    Json(json!({"status": "ok"})).into_response()
}

async fn chat_completions(
    State(options): State<Arc<Options>>,
    Json(request): Json<Value>,
) -> Response {
    tokio::time::sleep(options.delay).await;
    if options.are_completions_failing {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "rpc backend unavailable");
    }
    if request.get(CONVERSATION_ID_FIELD).is_some() {
        return error_response(StatusCode::BAD_REQUEST, "unexpected field conversation_id");
    }
    let reply = last_user_message(&request);
    if request.get("stream").and_then(Value::as_bool) == Some(true) {
        stream_reply(&reply)
    } else {
        Json(completion(&reply)).into_response()
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = json!({"error": {"message": message, "code": status.as_u16()}});
    (status, Json(body)).into_response()
}

fn last_user_message(request: &Value) -> String {
    request
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages
                .iter()
                .rev()
                .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        })
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

fn completion(reply: &str) -> Value {
    json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion",
        "created": unix_secs(),
        "model": MODEL_NAME,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": reply},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    })
}

fn chunk(delta: Value, finish_reason: Option<&str>) -> String {
    let chunk = json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "created": unix_secs(),
        "model": MODEL_NAME,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    });
    format!("data: {chunk}\n\n")
}

fn stream_reply(reply: &str) -> Response {
    let mut events = vec![chunk(json!({"role": "assistant"}), None)];
    events.extend(
        reply
            .split_inclusive(' ')
            .map(|word| chunk(json!({"content": word}), None)),
    );
    events.push(chunk(json!({}), Some("stop")));
    events.push("data: [DONE]\n\n".to_owned());
    let events = stream::iter(events).then(|event| async move {
        tokio::time::sleep(STREAM_CHUNK_GAP).await;
        Ok::<_, Infallible>(event)
    });
    match Response::builder()
        .header(CONTENT_TYPE, SSE_CONTENT_TYPE)
        .body(Body::from_stream(events))
    {
        Ok(response) => response,
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|arg| (*arg).to_owned()).collect()
    }

    #[test]
    fn unknown_args_are_ignored_and_the_last_value_wins() {
        let options = Options::parse(&args(&[
            "--model",
            "m.gguf",
            "--host",
            "0.0.0.0",
            "-ngl",
            "99",
            "--rpc",
            "127.0.0.1:1",
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
            "--delay-ms",
            "5",
            "--fail-completions",
        ]))
        .unwrap();
        assert_eq!(options.host, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(options.port, 9000);
        assert_eq!(options.delay, Duration::from_millis(5));
        assert!(options.are_completions_failing);
        assert!(!options.is_health_failing);
    }

    #[test]
    fn a_known_flag_without_value_is_an_error() {
        let err = Options::parse(&args(&["--port"])).unwrap_err();
        assert!(err.to_string().contains("--port"));
    }

    #[test]
    fn echoes_the_last_user_message() {
        let request = json!({"messages": [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
            {"role": "user", "content": "second"},
        ]});
        assert_eq!(last_user_message(&request), "second");
    }
}
