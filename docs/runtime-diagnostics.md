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

Durations use a local monotonic clock. A stage's `read_us` includes waiting for
the remote stage to compute and reply; it is not a pure network transfer time.
Do not subtract timestamps recorded on different machines to infer hop latency.
For a stalled run, collect the leader and worker logs, exact model/runtime
identities, stage ranges, context and backend, plus the selected Iroh path and
device thermal state when available. The experimental shard ring has its own
runtime and needs matching instrumentation there; these core logs describe the
current `llm-cluster` shard backend.
