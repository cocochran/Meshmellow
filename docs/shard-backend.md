# Shard backend

The leader can run jobs on one of two execution backends:

- `llama` (the default): a distributed `llama-server` over ggml RPC bridges, launched from a
  placement plan (`llm-cluster plan`).
- `shard`: david-codex's v1 shard runner (`experiments/model-validation/shard_runner.cpp`,
  unchanged). Each machine loads only its own layer range of the model, so a model larger than
  any one device's memory can run. This is how Qwen2.5-32B Q4_K_M ran across two 16 GiB Macs
  (see [SHARD_RUNNER.md](../experiments/model-validation/SHARD_RUNNER.md)).

The job API, `GET /jobs`, the browser client, `llm-cluster prompt`, history and shutdown are the
same on both backends. With `--backend shard` the leader runs an OpenAI-shaped front end
in-process on its internal loopback port (`--llama-port`, default 8081), and the job API
forwards to it just as it forwards to `llama-server`.

## How a job runs

1. The job API takes a job off its queue and forwards the chat request to the front end.
2. The front end renders the messages with the manifest's chat template and opens fresh stage
   links: it dials the remote prefix worker by node ID over Iroh (RPC bridge ALPN) and spawns
   the local final stage's runner. A runner serves exactly one sequence, so every job (and the
   readiness probe) gets new processes.
3. Token IDs go to the prefix, its hidden states come back to the leader, the local final stage
   samples the next token greedily, and the token is fed back to the prefix. Pieces stream back
   to the job API as OpenAI-style SSE.

## Commands

On the worker that serves the prefix:

```sh
llm-cluster shard-serve \
  --runner /path/to/mesh-shard-runner \
  --model /path/to/prefix-0-32.gguf \
  --range 0:32 --context 512 --gpu-layers 99 --budget-bytes 11811160064 \
  --expected-sha256 <sha256 of the --model file> \
  --source-sha256 <sha256 of the original full model> \
  --allow-peer <leader node id>
```

It prints `node id: <id>`; put that ID in the leader's manifest. `--allow-peer` is required and
repeatable: other peers are refused. `--source-sha256` is for an exported shard, which reports
the original model's identity; leave it out when `--model` is the full model file.

On the leader:

```sh
llm-cluster leader --backend shard --shard-manifest m.json
```

`--plan`, `--profile`, `--llama-server`, `--llama-arg`, `--min-peers`, `--evidence-file`,
`--evidence-wait` and `--llama-ready-timeout` belong to the llama backend and are refused with
`--backend shard`. `--api`, `--llama-port` and `--worker-wait` work on both. Use the global
`--key` flag to keep the leader's identity stable; `llm-cluster --key leader.key listen` prints
the node ID that key gives (stop it with Ctrl-C), which the worker needs for `--allow-peer`.

## Startup

1. The leader loads and validates the manifest before binding anything. An invalid manifest
   (for example two remote stages) fails here with a message naming the rule.
2. It starts the front end and waits until it listens. The front end first hashes every local
   stage file that has an `artifact_sha256` and refuses to start on a mismatch.
3. `shard-serve` workers open no control link to the leader, so there are no worker hellos to
   wait for. Instead the leader sends a one-token readiness probe through the front end and
   retries it with a short backoff (100 ms doubling to 2 s) until it passes or `--worker-wait`
   elapses (default 60 s). The final error carries the last probe failure, which names the
   failing stage, for example `remote stage on <worker>: ... peer not approved`.
4. Only then does the job API start serving.

Post-run evidence verification (`--evidence-file`) checks a run against its planning profile,
so it exists only on the llama backend.

## Manifest

```json
{
  "runner": "/path/to/mesh-shard-runner",
  "model_sha256": "2e5f6daea180dbc59f65a40641e94d3973b5dbaa32b3c0acf54647fa874e519e",
  "layers": 64,
  "context": 512,
  "max_tokens": 64,
  "chat_template": "chatml",
  "stages": [
    {"begin": 0, "end": 32, "remote": {"worker": "<worker node id>"}},
    {"begin": 32, "end": 64, "local": {
      "model": "/path/to/suffix-32-64.gguf",
      "gpu_layers": 99,
      "budget_bytes": 11811160064,
      "artifact_sha256": "91e7c4513c290df2d9546f29250a91db28cba2163c7c8ab812dd65cccef06513"
    }}
  ]
}
```

| Field | Meaning |
| --- | --- |
| `runner` | The `shard_runner` binary the leader runs local stages with. |
| `model_sha256` | SHA-256 of the original, unsharded model (64 hex digits). Every stage must prove it serves a slice of this model: a remote stage through the hash `shard-serve` announces, a local stage through the `source_sha256` its runner reports or a verified `artifact_sha256`. |
| `layers` | Total layers in the model. |
| `context` | Context window passed to every stage, 32 to 4096. A prompt longer than this is refused with HTTP 400. |
| `max_tokens` | Cap on completion tokens (default 512); a request may ask for fewer. |
| `chat_template` | Required. `chatml` is the only v1 value; the template is never guessed. |
| `stages` | Layer ranges in order, half-open, covering `0..layers` with no gaps. Each has `begin`, `end` and exactly one of `remote` or `local`. |
| `remote.worker` | Node ID of the `shard-serve` worker for this range. The leader dials it by ID: through Iroh's n0 discovery in production, or in-memory address lookup in tests. |
| `local.model` | Model or exported shard file the local runner loads. |
| `local.gpu_layers` | Layers offloaded to the GPU (default 0). |
| `local.budget_bytes` | Runtime buffer budget passed to the runner; must be positive. |
| `local.artifact_sha256` | Optional SHA-256 of `local.model`, checked once at startup. It establishes the stage's identity when the runner reports no `source_sha256` (a plain GGUF). |
| `local.env` | Optional extra environment variables for the runner, such as `CUDA_VISIBLE_DEVICES`. |
| `fallbacks` | Optional, on any stage but the final one: other locations that serve the same range, tried in order when the stage's node fails. Each is written like a stage's location, `{"remote": {...}}` or `{"local": {...}}`. See [Failover](#failover). |

Unknown fields are refused everywhere, so a typo cannot silently drop a setting.

## Failover

A stage can list `fallbacks` that serve the same layer range:

```json
{"begin": 0, "end": 32, "remote": {"worker": "<M1 node id>"},
 "fallbacks": [
   {"remote": {"worker": "<standby node id>"}},
   {"local": {"model": "/path/to/prefix-0-32.gguf", "gpu_layers": 99, "budget_bytes": 11811160064}}
 ]}
```

- A remote fallback is another worker running `shard-serve` with the same `--range` and model
  identity (and `--allow-peer` for the leader). Nothing is copied over the network, so the
  standby needs the shard file on its own disk.
- A local fallback runs the range on the leader too, next to its final stage, so the leader
  needs memory for both.

When a job starts, each stage opens on the first of its locations that answers, starting from
the one that last worked. A worker that can't be reached within 20 s is skipped for the next
location. If a stage's node fails partway through a reply, the front end sets that location
aside for the rest of the job and restarts the prompt on the stage's next location. Decoding
is greedy and deterministic, so the restart regenerates the same text: a streamed reply picks
up where it stopped instead of repeating what was already sent. If the restarted text differs
from what was sent, the job fails rather than splicing two answers together.

The job fails as before when the failed stage has no location left, or when the failure isn't
the stage's node going away (the leader's own final stage, a stage refusing the prompt, a
model hash mismatch). The final stage takes no fallbacks: it runs on the leader, which
tokenizes and samples. A restart pays prefill and model load again, and the generation is
silent while it catches up to the text already sent.

## v1 limits

- Topology: one remote prefix plus a local final stage, or a single local stage that runs the
  whole model. The prefix may fail over to fallbacks with the same range; layer ranges are never
  re-split among the remaining nodes. Chained remote stages and a remote final stage need ring forwarding over
  david-codex's `meshthing/shard-ring/1` (M-27). Hidden states never take an extra hop through
  the leader ("hub forwarding"); that design was rejected.
- Greedy decoding only. `temperature`, `top_p`, `seed` and similar fields are accepted and
  ignored.
- `chatml` is the only chat template. The pinned official Qwen template is future work (M-27).
- One sequence per runner process and no reset, so every job starts fresh runner processes
  (and a fresh Iroh connection to the worker). Model load time is paid on every job.
- One generation runs at a time, matching the job queue.

## Tests

`tests/shard_backend.rs` runs the job API end to end on the shard backend: a real
`shard-serve` worker drives the `fake_shard_runner` example as the prefix, and the leader runs
the local final stage. It covers buffered and streamed prompts, `GET /jobs`, fresh runners per
job, a worker that does not allowlist the leader, and an invalid manifest. Build the helpers
first:

```sh
cargo build --examples
cargo test --test shard_backend
```

## Reproducing the two-Mac 32B run through `llm-cluster prompt`

This manual acceptance step is still pending: it has not yet been run on the hardware. The
values below come from david-codex's capacity run
([results](../experiments/model-validation/results/shard-runner-2026-10-24)).

- Model: [bartowski/Qwen2.5-32B-Instruct-GGUF](https://huggingface.co/bartowski/Qwen2.5-32B-Instruct-GGUF)
  Q4_K_M, SHA-256 `2e5f6daea180dbc59f65a40641e94d3973b5dbaa32b3c0acf54647fa874e519e`, 64 layers.
- Shards exported with `export_prefix.py`: the prefix `0..32` (9,828,263,360 bytes, SHA-256
  `8455d69052c1fc055c9a1c1f48f20451c4fc6f1d725f7bf9f43b0a60f66646f6`) on the M1 Air, and the
  suffix `32..64` (10,029,008,480 bytes, SHA-256
  `91e7c4513c290df2d9546f29250a91db28cba2163c7c8ab812dd65cccef06513`) on the M2 Pro.
- Both Macs need `mesh-shard-runner` built as in SHARD_RUNNER.md. Pause the normal demo during
  the run to free memory.

1. On the M2 Pro (leader), print the leader's node ID: `llm-cluster --key leader.key listen`,
   then Ctrl-C.
2. On the M1 Air (worker):

   ```sh
   llm-cluster --key worker.key shard-serve \
     --runner "$PWD/shard-runtime-v3/mesh-shard-runner" \
     --model "$PWD/models/Qwen2.5-32B-prefix-0-32.gguf" \
     --range 0:32 --context 512 --gpu-layers 99 --budget-bytes 11811160064 \
     --expected-sha256 8455d69052c1fc055c9a1c1f48f20451c4fc6f1d725f7bf9f43b0a60f66646f6 \
     --source-sha256 2e5f6daea180dbc59f65a40641e94d3973b5dbaa32b3c0acf54647fa874e519e \
     --allow-peer <leader node id>
   ```

3. On the M2 Pro, write the manifest above as `qwen32.json`, with the M1's node ID as
   `remote.worker`, the M2's runner path as `runner` and its suffix file as `local.model`.
   The 11,811,160,064-byte budget is the one the prefix used; the suffix reported
   10,177,193,984 runtime buffer bytes, so it fits. Then start the leader, allowing time for
   the cold load in the first probe:

   ```sh
   llm-cluster --key leader.key leader --backend shard --shard-manifest qwen32.json --worker-wait 5m
   ```

4. Once it logs `leader ready; serving the job API`:

   ```sh
   llm-cluster prompt "Explain why splitting one language model across two computers allows a larger model to run. Answer in two short sentences."
   ```

Expected: a coherent two-sentence answer, with each Mac's runner loading only its own shard.
David's run reported about 9.96 GB of runtime buffers on the prefix and 10.18 GB on the suffix,
and 2.33 output steps per second after prefill. `llm-cluster prompt` sends a single user
message, while David's run also had the system message `You are a helpful assistant.`, so the
text need not match his byte for byte. To replay his exact prompt, send both messages to the
job API:

```sh
curl -s http://127.0.0.1:8080/v1/chat/completions -H 'content-type: application/json' -d '{
  "messages": [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Explain why splitting one language model across two computers allows a larger model to run. Answer in two short sentences."}
  ]
}'
```
