# Shard runner feasibility

Investigated 2026-10-24 against local llama.cpp commit
`71255fefc092b0344f2acddb891388814ae961be`.

A private partition runner now executes selective weights and layer ranges
using llama.cpp/GGML. CPU, Metal and real two-Mac Iroh tests passed 16-position
Llama-3.2-1B comparisons with bit-identical logits against unsplit execution.
The small model is only a correctness fixture. The capacity target is
Qwen2.5-32B Q4_K_M, whose 19,851,336,576-byte weights exceed either Mac's
16 GiB physical RAM. Its 32-layer prefix and suffix each loaded independently;
a sequential local prefill produced ` Paris`. Two-Mac capacity validation then
passed over Iroh: both partitions remained active for 41 output steps including
end-of-generation, producing a coherent two-sentence response.

The original llama.cpp runtime remains unchanged. The normal demo is paused
during large-model tests to release memory and restored afterward. The prototype uses the existing
core Iroh bridge for inference and SSH only for staging and process setup.

## Two Mac capacity result

Qwen2.5-32B Q4_K_M ran with a 512-token context and a 32/32 layer split on an
M1 Air and M2 Pro, each with 17,179,869,184 physical RAM bytes. The model's
weights are 19,851,336,576 bytes. Each runner loaded its exported shard file;
neither runner loaded the full weight set. This demonstrates combined-memory
execution, not that disk-offloaded single-device execution is impossible.

| Metric | M1 prefix | M2 suffix |
| --- | ---: | ---: |
| Owned layers | 0-31 | 32-63 |
| Reported runtime buffer bytes | 9,955,481,600 | 10,177,193,984 |
| Task physical-footprint peak bytes | 10,125,385,504 | 10,967,097,648 |
| Median synchronized decode compute | 171.10 ms | 64.85 ms |

The request completed at 2.33 output steps/sec after prefill (40 steps including
EOG). The first execution step took 4.02 seconds after initialization. The Iroh
stream closed with `path=relay`; the earlier small-model correctness run closed
with `path=direct`. This single capacity run includes cold decode kernels and
is not a controlled throughput comparison with RPC.

The output was:

> Splitting a language model across two computers distributes the computational load, allowing each machine to handle a portion of the model. This distribution enables the processing of a larger model than either computer could handle individually.

Only 1,153 execution-request bytes crossed toward the prefix, including the
client preface, versus 1,679,360 activation payload bytes returned. JSON metadata,
Iroh framing and encryption add traffic; bridge logs contain actual stream byte
counts. A decode activation is 20,480 bytes; token input requests are 24 bytes.

Task VM counters cover the native inference process, not the bridge, endpoint,
coordinator or all external driver allocations. Compressed pages were nonzero
(maximum sampled 629 MB on M1 and 859 MB on M2). Host swap counters are included
as separate diagnostics; no swap-free or entirely uncompressed-residency claim.
Do not add RSS, task footprint and Metal/shared-buffer totals. Complete
contribution-budget acceptance and a hard peak-memory cap remain pending.

## Prototype and reproduction

`shard_runner.cpp` is a persistent single-sequence process. `shard_trial.py`
coordinates prefix and suffix, optionally comparing logits with an unsplit
reference. `shard_endpoint.py` exposes one worker session on loopback for the
existing opaque Iroh bridge. `export_prefix.py` exports either a prefix or a
suffix without dequantizing or renumbering tensors. Shard files preserve global
model dimensions and embed source identity and assigned range; they require
the private runtime, not a stock loader.

Apply `results/shard-runner-2026-10-24/shard-runtime.patch` to a private clone
of the pinned base, then build:

```sh
cmake -S . -B build-shard -DCMAKE_BUILD_TYPE=Release \
  -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF -DLLAMA_BUILD_TOOLS=OFF
cmake --build build-shard --target llama -j 6
c++ -std=c++17 -O2 -I include -I src -I ggml/include \
  /path/to/shard_runner.cpp -L build-shard/bin \
  -Wl,-rpath,@executable_path -lllama -lggml -lggml-base \
  -o build-shard/bin/mesh-shard-runner
```

The link command above is the tested macOS command. Rebuild backends for each
host architecture; the M1 deployment uses its original pinned M1 CPU library,
with private llama and unchanged-version GGML libraries. Original runtime
files are not overwritten. The binary's stdio protocol is platform-independent
on little-endian IEEE754 hosts; other CPU/GPU/platform combinations need tests.

Runner arguments are `MODEL BEGIN END CONTEXT GPU_LAYERS BUDGET_BYTES PROMPT
RETURN_LOGITS`. Ranges are half-open. Context is bounded to 4096, batches to
128, and a session has one sequence and strictly increasing step/position.
Budget checks inspect buffer totals after initialization and completed steps;
they do not enforce a hard peak-memory cap during allocations.

The endpoint checks its file's SHA256 before listening. An exported shard
also has a verified source-model SHA256, which the coordinator matches to its
full source. Native loading rejects a requested range different from the
exported range. A five-byte client preface makes the QUIC stream visible before
the server sends model metadata; omitting it deadlocks a server-first handshake
through the current Iroh bridge. Tensor data is raw float32, with bounded JSON
metadata and big-endian request integers. No tensors are JSON encoded.

For an Iroh trial, start the endpoint on the M1's loopback port 50171 and the
core `bridge-serve --target 127.0.0.1:50171`, with the tested fail-closed
leader allowlist guard. Start `bridge-connect WORKER_ID --listen
127.0.0.1:50170` on the M2. Then:

```sh
python3 shard_trial.py --runner /path/to/mesh-shard-runner \
  --model /path/to/full-source.gguf --suffix-artifact /path/to/suffix.gguf \
  --layers 64 --cut 32 --context 512 --tokens 64 --iroh-port 50170 \
  --expected-sha256 2e5f6daea180dbc59f65a40641e94d3973b5dbaa32b3c0acf54647fa874e519e \
  --output-dir /path/to/new-result-directory
```

The full source file is used to verify identity; the suffix process loads the
specified suffix artifact. M1 receives only the 9,828,263,360-byte prefix. The
suffix artifact is 10,029,008,480 bytes. The source is
[bartowski/Qwen2.5-32B-Instruct-GGUF](https://huggingface.co/bartowski/Qwen2.5-32B-Instruct-GGUF).
The endpoint's prompt, context and result mode must match the trial. This is
an experimental execution adapter, not integration with the leader/job API.

Evidence and runtime hashes: [results/shard-runner-2026-10-24](results/shard-runner-2026-10-24).
The single Iroh correctness run occurred during weight staging, so its timing
is not a controlled throughput comparison. Duplicate execution steps were
rejected with session closure. Exported prefix-only and prefix-plus-suffix
files also passed the 16-position reference comparison.

## Existing RPC already does part of this

`rpc_dispatcher::graph_compute` caches graphs by UID and sends GRAPH_RECOMPUTE
for subsequent executions. The worker retains the graph and executes it with
`ggml_backend_graph_compute_async`. Model weights and attention/recurrent
buffers already remain on their assigned devices during inference.

The generation-only command summary in `rpc-profile-01/command-times.json`
recorded 2 GRAPH_COMPUTE, 77 GRAPH_RECOMPUTE, 711 SET_TENSOR and 79 GET_TENSOR.
The complete diagnostic log also contains startup and warmup; its totals differ.
GRAPH_RECOMPUTE enqueue times are not GPU execution times.

A representative single-token decode in that log uploads nine tensors:

| Tensor | Payload bytes |
| --- | ---: |
| inp_mixed_embd | 10240 |
| node_1 | 10240 |
| inp_mixed_slots | 8 |
| model.input_embed | 10240 |
| rs_s_copy | 4 |
| inp_pos | 16 |
| attn_inp_k_idxs | 8 |
| attn_inp_v_idxs | 8 |
| attn_inp_kq_mask | 512 |

That is 31,276 input bytes, plus a 10,240-byte activation response, excluding
RPC framing. These tensor names and sizes do not establish identical contents
or prove the uploads can safely be removed. The previously reported 10 KiB
per token described the activation response, not total bidirectional traffic.

GET_TENSOR waits include remote compute, synchronization and communication.
They cannot be counted entirely as avoidable RPC overhead. Earlier packing,
batching and zstd experiments found no repeatable throughput improvement.

## Recommended execution boundary

For the first two-node design, the remote prefix owns embedding lookup and
layers [0, cut). The local suffix owns layers [cut, layer_count), final norm,
output projection and sampling. Each partition owns its layer state. The
coordinator sends token IDs/positions to the prefix, receives boundary hidden
states and runs the suffix. The sampled token becomes the next prefix input.

This arrangement could replace centrally supplied mixed-embedding and cache
index/mask tensors with worker-generated inputs. A remote suffix is also
possible, but sampling must then be placed deliberately to avoid sending full
vocabulary logits across the link. Tied embedding/output weights may need a
copy at both ends and must be included in memory accounting.

Autoregressive decoding still waits for both partitions before choosing the
next token. A single conversation does not gain pipeline concurrency merely
by splitting its layers. Multiple independent jobs can provide pipeline work,
but that is a separate throughput experiment.

## Internal changes required

| Area | Existing hook | Required change |
| --- | --- | --- |
| Weight loading | `llama_model_loader::create_tensor` supports TENSOR_SKIP | Load only owned layers and required endpoint weights; audit tied/duplicated weights and loader accounting |
| Graph construction | Architecture builders loop over all layers | Accept a layer range and hidden-state input/output; skip embedding/head where unowned |
| State allocation | KV and recurrent constructors have layer filters | Intersect architecture filters with partition ownership; preserve global layer indices |
| Context execution | `llama_decode` handles full model batches | Expose partition decode with positions, sequence identity, causal state and bounded prefill chunks |
| Intermediate output | Internal `llama-ext.h` exports layer-input extraction | Use for reference comparisons; extraction alone neither stops execution nor resumes at a layer |

Relevant files: `src/llama-model-loader.cpp`, `src/llama-model.cpp`,
`src/models/llama.cpp`, `src/models/qwen35.cpp`, `src/llama-context.cpp`,
`src/llama-graph.cpp`, `src/llama-kv-cache.cpp`,
`src/llama-memory-recurrent.cpp`, `src/llama-memory-hybrid.cpp`, and
`ggml/src/ggml-rpc/ggml-rpc.cpp`.

`llama_batch.embd` substitutes the model's initial embeddings. It does not
select a starting decoder layer. Output embedding APIs similarly do not
provide a partition execution interface.

## Smallest useful prototype

1. Use the existing Llama-3.2-1B GGUF for correctness. Start with one sequence,
   a fixed split, greedy decoding, no adapters, speculation or context shifting.
   Its small size is a test fixture, not pooled-memory acceptance.
2. In an isolated private fork, implement prefix/suffix execution on one host.
   Compare boundary tensors and full logits against unsplit execution during
   prefill and multiple decode positions, then compare generated tokens.
   Record numerical tolerances; cross-backend results need not be bit exact.
3. Verify only owned weights/state are allocated. Map-only visibility of a full
   GGUF is not proof of resident memory; track runtime allocations and process
   memory separately. Do not renumber RoPE positions or global layer indices.
4. Move the prefix to the other Mac over loopback transport through SSH, then
   the existing Iroh bridge. Use persistent sessions and one bounded execute
   request/response per step. Reuse the Rust supervisor and planner interfaces.
5. Compare against RPC using the same model, split, context, prompts, seed,
   concurrency and link, with warmup excluded and interleaved repeated trials.
   Record TTFT, decode tok/s, per-step bytes, synchronized worker compute,
   host staging and full request time. Avoid subtracting clocks on different
   hosts; worker-side intervals and coordinator intervals are separate metrics.
6. Add Qwen3.5 hybrid-state support only after the conventional decoder passes.
   Then validate a model requiring combined memory within measured budgets.

Each session binds model hash, runtime/patch identity, partition range,
context limit and precision. Each execution carries session/job identity,
sequence, step and expected position. Bound shapes and lengths. Repeated or
out-of-order steps must not apply state twice: reject them initially and restart
the session/re-prefill after ambiguous failure. A response acknowledges a
completed step, not durable conversation storage or recoverable KV state.

## Decision gates

Continue the experiment if partitions reconstruct reference execution, exclude
unowned resident allocations, and either improve repeated matched throughput
or enable pooled-memory placement that existing RPC cannot provide adequately.
Retain RPC if the custom path only changes command names or if remote compute
and unavoidable link latency dominate. No throughput gain is predicted yet.

The first profiling task is worker-side synchronized compute measurement.
Synchronization added for diagnosis may change overlap; keep diagnostic runs
separate from normal throughput trials. Cross-platform kernels remain GGML's
responsibility, but Mac correctness alone does not validate CUDA/Vulkan/CPU.

## Paper connection

[SplitLLM](https://arxiv.org/html/2410.10759v2) motivates measured compute and
transfer costs for placement. Our proposal additionally needs memory capacity,
per-token latency, KV/recurrent state and explicit partition execution. Its
server-throughput objective is not evidence of a speedup on our wireless Macs.
