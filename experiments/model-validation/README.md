# Model validation (M-12, M-13)

See [throughput tuning](TUNING.md) for placement, prediction, Iroh and RPC trace
comparisons, including optimizations that did not reproduce a speedup.

This harness measures an already running llama.cpp server using Python 3.10+
and the standard library. It saves the model SHA-256, runtime commit, chat
template hash, context, full request, streamed response, first-visible-text
latency, server timings and failures as JSON. Each output path must be new.

## Current test matrix

| Item | Baseline |
| --- | --- |
| Coordinator | Apple M2 Pro, 16 GiB unified memory, macOS 26.7.1 |
| Remote worker | Apple M1, 16 GiB unified memory, macOS 27.0.1 |
| llama.cpp on both | `71255fefc092b0344f2acddb891388814ae961be` |
| Build | Release, Metal ON, RPC ON |
| Model | Qwen3.5-4B, Q4_K_M, 3,013,027,808 bytes |
| Model SHA-256 | `13c16f426047e2de38cd075bdade4a7bcbc8c774384876f677740cda65f8a983` |
| Context / slots | 8192 / 1 |
| Offload | `-ngl 99`; RPC uses `--split-mode layer --tensor-split 1,1` |
| Reasoning | Disabled with `--reasoning off` |
| Requests | Same prompt, seed 42, temperature 0, 64-token maximum, no prompt cache reuse |
| Repetitions | Three per configuration, sequentially |
| Baseline transport | Local-only, then RPC over authenticated SSH |

The architecture document proposes **Qwen2.5-1.5B Q4_K_M** first. The existing
working demo instead supplies Qwen3.5-4B, which has recurrent state as well as
KV state. These results validate that existing runtime; they do not establish
the document's dense 28-layer reference model or its per-layer cost assumptions.
The `.part` download in the existing demo is incomplete and is not used.

No explicit per-device model memory cap is enforced in this baseline. Hardware
RAM is not an application budget. Startup allocations are not peak memory
measurements. Record and enforce contribution budgets before a memory-pooling
demonstration; this small model already fits on either test device.

## Observed results (October 24, 2026)

| Configuration | Median first visible text | Median server decode rate | Median elapsed |
| --- | --- | --- | --- |
| Local M2 Pro | 0.126 s | 43.72 tokens/s | 1.569 s |
| M2 Pro + M1, 1:1 split over SSH RPC | 5.774 s | 9.47 tokens/s | 12.428 s |

These are three short runs each, with a 34-token prompt and 64 generated tokens.
The split configuration is slower here; these observations are not a general
performance estimate. Both servers were loaded, with requests issued sequentially;
memory pressure, thermals and network conditions were not independently controlled.
`finish_reason: length` means the response reached the chosen token budget.

Raw evidence: [local runs](results/local.json), [RPC runs](results/ssh-rpc.json),
[hardware and capture boundary](results/test-matrix.json), and
[fresh worker execution log](results/remote-compute.txt). The remote log begins
after startup/warmup and contains 195 graph-execution events during the measured
requests. The RPC result includes local Metal and remote RPC model/cache/compute
allocations from the most recent server startup only.

## Run

Use the exact same pinned runtime and model for every comparison. Build with
RPC and the appropriate backend enabled. These commands assume the paths are
relative to the llama.cpp checkout and that the model is available there.

Local server:

```sh
./build/bin/llama-server -m /path/to/model.gguf -ngl 99 -c 8192 -np 1 \
  --reasoning off --host 127.0.0.1 --port 8081 -lv 4
```

Remote worker (on the other approved computer):

```sh
GGML_RPC_NO_RDMA=1 GGML_RPC_DEBUG=1 ./build/bin/ggml-rpc-server \
  -H 127.0.0.1 -p 50052 -c
```

On the coordinator, forward the worker's loopback port through SSH:

```sh
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 -L 127.0.0.1:50053:127.0.0.1:50052 user@worker
```

Then start the distributed server:

```sh
GGML_RPC_NO_RDMA=1 ./build/bin/llama-server -m /path/to/model.gguf \
  --rpc 127.0.0.1:50053 -ngl 99 --split-mode layer --tensor-split 1,1 \
  -c 8192 -np 1 --reasoning off --host 127.0.0.1 --port 8080 -lv 4
```

Wait for model loading to finish. From the HackOHIO26 checkout:

```sh
python3 experiments/model-validation/benchmark.py \
  --base-url http://127.0.0.1:8081 \
  --model /path/to/model.gguf --llama-cpp /path/to/llama.cpp \
  --transport local --server-log /path/to/local-server.log \
  --runs 3 --output /path/to/results/local.json
```

Repeat with port `8080`, `--transport ssh-rpc`, the distributed server log and
a new output filename. `--timeout` controls socket inactivity and checks the
run budget between events. A preflight or generation failure exits nonzero
and saves the failure; incomplete streams cannot pass. JSON output is evidence,
not a semantic grading of the generated answer.

For Iroh, replace only the SSH tunnel with an authenticated opaque-byte bridge,
retain the worker loopback binding, and use `--transport iroh-rpc`. That label
is operator-supplied; the harness cannot verify which transport is actually used.

## Evidence needed beyond a response

Capture startup allocations for both local and remote devices and fresh remote
`graph_compute`/`graph_recompute` log entries during measured requests. Retain
the worker build/identity, command lines and log boundary. Neither a successful
answer nor a healthy HTTP endpoint alone proves remote computation.

On October 24, the old server returned healthy while its tunnel was absent.
A fresh request aborted with an RPC send failure. Restoring the network alone
did not restore inference: restarting the stale worker and rebuilding the SSH
tunnel was necessary. The test records failures rather than silently reporting
the earlier successful response as current evidence.

llama.cpp RPC is experimental. Keep it on loopback behind the approved tunnel;
the runtime pin here is for reproducibility, not a security audit claim.

## Checks and remaining work

```sh
python3 -m unittest discover -s experiments/model-validation -v
```

The tests cover SSE framing, successful completion, usage/timing capture,
truncated streams, empty output, invalid JSON and connection failures.

M-14 requires the Iroh bridge. M-15 still needs split-ratio exploration, peak
memory telemetry, enforced contribution budgets and repeated experiments under
specified network conditions. M-16 needs the durable history interface and
controlled worker/controller/UI failure tests. No seamless failover, speedup,
or model-too-large-for-one-device claim follows from the current baseline.
