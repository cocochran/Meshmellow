# Qwen3.8 native shards and direct peer forwarding

Experimental validation, 2026-10-24. This is not the production job backend.

## Evidence and limits

| Capability | Evidence | Remaining limits |
|---|---|---|
| Qwen hybrid native layer ranges | Four-stage CPU fixture matches the unsplit fixture for 16 positions; source tensors and reference report under `results/qwen38-three-device-2026-10-24/` | A truncated numerical fixture does not establish full-model numerical equivalence |
| Large-model heterogeneous execution | Qwen3.8-27B UD-Q5_K_M, 19,771,509,664-byte source; M2 Pro Metal 0:28, M4 iPad Metal 28:32, RTX 5060 Ti CUDA 32:64 | Fixed validation ranges, 512-token contexts, one request at a time |
| Direct application peer forwarding | Two fresh 45-step ring runs generate identical coherent output; iPad forwards activations to CUDA; terminal returns tokens directly to Mac | Iroh can use relay transport; peer-to-peer application topology does not guarantee direct UDP connectivity |
| Observed decode performance | Ring observations: 3.9479 and 3.0484 steps/s, including terminal EOG step; earlier hub observation 1.9804 | Network conditions vary; these are not controlled causal speedup measurements |
| Protocol rejection | Rust tests reject wrong job, replay, invalid payload size, oversized metadata | Diagnostic protocol, not a complete security or failure-recovery evaluation |
| Mobile worker | Foreground physical M4 iPad executes Metal layers over Iroh | No background availability; vendored iOS network fallbacks are diagnostic |
| Memory measurements | Native buffer allocation and scoped process memory telemetry recorded | No complete contribution peak accounting, hard cap, swap-free guarantee, or live admission/reservation protocol |
| Automatic placement | Requirements and profile contract agreed with planner owner | Not enabled in this diagnostic; no claim of topology-optimal placement |
| Every-node API and request synchronization | Requirements recorded in `MESH_REQUEST_REQUIREMENTS.md` | Not implemented by this loopback demo; no automatic recovery or KV preservation |

The M1 and unplugged Android phones are not part of these ring measurements. Older `qwen38-three-device-2026-10-24` results use hub forwarding and must not be presented as ring evidence. Ring evidence is in `results/qwen38-p2p-ring-2026-10-24/`.

After the final progress-task cleanup change was rebuilt on all three platforms, the live gateway rejected an out-of-order execution header. A subsequent fresh live-UI request completed 43 steps at 3.6030 steps/s. The completed UI was visually checked using an isolated Helium session; `live-demo.png` captures the result. Nine existing Python harness tests and two Rust protocol tests passed. Rust builds retain warnings in the vendored diagnostic networking code.

## Runtime provenance

Use llama.cpp base `71255fefc092b0344f2acddb891388814ae961be` plus `results/qwen38-three-device-2026-10-24/runtime.patch`. Header, library, and callers must be rebuilt together because the experimental shard parameters change the ABI. Native runner source is `shard_runner.cpp`; socket wrapper is `../mobile-probe/android/socket_main.cpp` and the repeated-session executable wrapper is `../mobile-probe/worker/worker_loop.cpp`.

Qwen GGUF block count includes one MTP block. The exporter excludes that block from the 64 decoder-layer partition and retains the output head on the final artifact. Each assigned artifact carries the original model SHA; manifests include artifact SHA values. `artifact_range_fetch.py` can retrieve only assigned tensor ranges from the pinned source revision and verifies the resulting artifact SHA before rename.

Native execution uses per-session streams rather than redirecting process-wide stdin/stdout, so concurrent app logs cannot corrupt the wire. Hidden-input batches supply Qwen's four-coordinate M-RoPE positions. Every job creates fresh contexts; a failure closes the job rather than silently continuing with partial state.

## Diagnostic transport

`../mobile-probe/iroh-core` pins the project core dependency and preserves third-party vendored licenses. Build the host CLI with:

```sh
cargo build --locked --manifest-path experiments/mobile-probe/iroh-core/Cargo.toml --bin mesh-ring
cargo test --locked --manifest-path experiments/mobile-probe/iroh-core/Cargo.toml --lib
```

CLI arguments:

```text
mesh-ring gateway KEY ENTRY_PEER FINAL_PEER LOOPBACK_PORT
mesh-ring worker KEY APPROVED_UPSTREAM ORIGIN_PEER NEXT_OR_DASH NATIVE_LOOPBACK_PORT
```

The ring uses separate ALPN `meshthing/shard-ring/1`; native MSH1 v1 remains unchanged. Only configured upstream identities are accepted by workers. The gateway accepts a final-result channel only from its configured terminal identity and consumes a pending random job nonce once. Configurations and private keys are local; do not publish key material.

1. Origin opens an authenticated entry stream with a random job ID.
2. Each worker initializes its native range and connects to its configured next peer. Readiness contains contiguous ranges, dimensions, source model identity, and peer identities.
3. Terminal opens a result stream directly to origin and correlates it with the pending job.
4. Each execution sends bounded JSON metadata followed by raw little-endian float32 activations to the next peer. Step/position checks reject replay; batches are limited to 128 tokens and activation payloads to 16 MiB.
5. Small progress messages return on control streams. Final token/piece and actual stage metrics return directly from terminal. Intermediate activations never return to origin.
6. Session end closes native contexts. One active session per worker; no mid-token reroute, retries, or automatic re-prefill.

`ring_trial.py` validates the configured model/range chain and execution provenance. Its configuration has `execution: "p2p_ring"`, `ring_port`, source SHA, token limit, and stages; only the first stage has a local runner command. The other stage ranges describe expected peers. Do not run an older bridge concurrently with the same test identity.

`live_demo.py` and `live_demo.html` expose a loopback-only diagnostic UI. Native compute, local stage time, and total chain overhead have distinct scopes. Reload reconnects to the active run; it does not launch another request. This is not the mesh's production OpenAI-compatible API.

## Research informing the next steps

These are design inputs, not proof of our implementation's performance:

- [Prima.cpp](https://arxiv.org/abs/2504.08791): heterogeneous consumer hardware, RAM/VRAM-aware device selection, overlapping disk, compute, and communication.
- [EdgeShard](https://arxiv.org/abs/2405.14371): joint device selection and layer partitioning, with separate latency and throughput objectives.
- [Petals](https://arxiv.org/abs/2312.08361): load balancing and fault-tolerant inference with peers joining and leaving.
- [Helix](https://arxiv.org/abs/2406.01566): directed heterogeneous network model and joint placement/request scheduling.
- [MDI-LLM](https://arxiv.org/abs/2505.18164): device-to-device activation forwarding and pipelining multiple sequences.
- [STAGE](https://arxiv.org/abs/2511.10480): synthetic execution graphs for comparing compute, memory, and communication strategies before deployment.
- [SplitLLM](https://arxiv.org/abs/2410.10759): resource/latency-constrained layer placement in a server/client setting; general multi-peer ordering still needs our own implementation and validation.

Next acceptance gates: measured directed link profiles; fresh available-memory reports and destination reservations; planner-selected device subset/order/ranges; replicated request ingress with ownership/deduplication; and explicit restart/stream semantics under real peer loss. Request replication alone cannot recover a missing shard unless its weights are available on another eligible device.
