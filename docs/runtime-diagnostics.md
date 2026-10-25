# Runtime diagnostics

The `llm-cluster` CLI writes logs to stderr and keeps stdout for command output.
Normal runs use `info` logging. Add the global `--debug` flag to enable detailed
MeshThing logs for one process:

```sh
llm-cluster --debug leader --backend shard --shard-manifest manifest.json
llm-cluster --debug shard-serve --runner /path/to/runner --model /path/to/shard.gguf \
  --range 0:32 --context 512 --budget-bytes 1073741824 --allow-peer <leader-id>
```

`--debug` keeps existing `RUST_LOG` settings and enables `llm_cluster=debug`.
For a narrower view, leave off `--debug` and use, for example:

```sh
RUST_LOG=warn,llm_cluster::shard::pipeline=debug llm-cluster leader --backend shard --shard-manifest manifest.json
```

Shard diagnostics include local runner startup and remote connection/metadata
durations, then one `shard stage exchange` per stage and step. Each exchange
records stage, step, position, payload kind and byte counts, write duration,
read duration and total duration in microseconds. The remote connection event
also records its selected path at connection time (`direct`, `relay` or
`custom`). A `shard pipeline step`
records the whole serial step. Timeout errors identify whether the write or
read phase exceeded the shared step deadline. These new events do not include
prompt text, token IDs or generated text.

The HTTP API's `job running` event includes `queue_wait_ms`, measuring how long
the request waited for the single inference slot. Streamed responses reject
individual SSE lines larger than 1 MiB and do not append those lines to the
history sink. Child process stdout/stderr and local runner stderr are split into
8 KiB chunks per tracing event, so a process that emits an unusually long line
cannot force the logger to buffer the entire line at once.
Non-streaming upstream responses are capped at 16 MiB while buffering. Streaming responses
continue to the client, but responses over 16 MiB are omitted from conversation history to keep
history capture bounded; this is logged when the stream completes.
Mesh membership refreshes warn on the first sync failure for a member, use debug logs for
retries, and log when sync recovers. Mesh control connections follow the same pattern for
hello, address, dial and timeout failures; the member ID is included in each event. The
enrollment CLI warns on the first periodic membership refresh failure or timeout, uses
debug logs for continuing failures, and logs when refresh recovers; this avoids hiding an
outage while keeping the normal two-second polling loop from flooding warning logs.
The shared control listener accepts at most 128 concurrent sessions, and each session reads at
most 16 concurrent heartbeat streams; excess connections are refused and excess streams are
stopped. This bounds task growth from peers that open many idle streams.
RPC bridges cap each worker bridge at 128 local clients, each worker endpoint at 128 incoming
connections, and each remote connection at 128 streams. When a cap is reached, new clients are
dropped or streams are reset; limit events are logged at debug level, with one warning per
connection for repeated excess streams.
The shard relay caps incoming connections at 128, including sessions waiting for the single
runner slot. Experimental ring workers cap them at 16. Excess connections are refused and
logged at debug level.
Readiness-probe error excerpts are capped at 512 characters (at most 2 KiB read), and the
external run-evidence file is capped at 1 MiB per poll.
The CLI rejects plan, profile and shard-manifest JSON files larger than 64 MiB before parsing;
the limit is also enforced while reading in case a file grows after its size check.

Durations use a local monotonic clock. A stage's `read_us` includes waiting for
the remote stage to compute and reply; it is not a pure network transfer time.
Do not subtract timestamps recorded on different machines to infer hop latency.
For a stalled run, collect the leader and worker logs, exact model/runtime
identities, stage ranges, context and backend, plus the selected Iroh path and
device thermal state when available. The experimental shard ring has its own
runtime and needs matching instrumentation there; these core logs describe the
current `llm-cluster` shard backend.
