---
title: "feat: Benchmark joined devices and deploy measured mesh placements"
type: feat
status: draft
date: 2026-10-24
author: david-apps-codex
baseline_commit: f28e1193df8625a6ad5ba574b0a631bf04106eec
---

# Benchmark joined devices and deploy measured mesh placements

## Outcome and scope

After a trusted device joins, measure its eligible backends and actual Iroh links,
choose useful contiguous layer assignments for the mesh's one model, reserve
capacity, load verified shards, and demonstrate that the resulting execution
matches the plan. Prioritize fitting larger models within usable memory; among
feasible placements minimize measured single-request decode latency. Report
prefill, startup and tail latency separately.

Retain the Rust/Iroh core and private native shard runtime. This is an integration
plan, not a migration to PRIMA.cpp. It proposes no immediate changes to live
workers. Research techniques become candidates for experiments, not promised
speedups. The first deliverable is a repeatable measurement-to-execution trial;
the apps then expose the same lifecycle.

## Confirmed product requirements

Source: David's apps-planning conversation, recorded in the
[requirements handoff](https://discord.com/channels/1563598920800542840/1563603695688622092/1563751086832095243)
and subsequent [research handoff](https://discord.com/channels/1563598920800542840/1563603695688622092/1563752523179888732).

- Desktop launches a local web server from the terminal, with a SvelteKit and
  Tailwind CSS interface in the browser. This supersedes the earlier Tauri shell
  requirement. iOS and Android apps share the core contracts; mobile UI framework
  selection remains open.
- A joining device pastes a full signed invitation. Any already trusted device can
  approve it. The invitation requests membership; approval authorizes membership.
- Pairing must work across networks through the existing Iroh core, including
  relay fallback. Same-Wi-Fi discovery is optional, never a prerequisite. The
  mobile implementation uses self-contained signed invitations over Iroh with no
  Layercake lookup server to host.
- A device belongs to one mesh at a time.
- Benchmark after trust approval, before assigning compute.
- One model per mesh. Current capacity work uses the existing Qwen 27B artifact.
  Larger-model recommendations should use measured capacity and require user
  selection. In-app download and local GGUF import are supported product goals.
  Model-switching workflows and simultaneous different models are out of scope.
- Every peer exposes an OpenAI-compatible API. Ingress, scheduling ownership and
  layer-0 execution are separate roles. Synchronize accepted request state before
  execution; activations travel directly between successive compute stages.
- Built-in chat, shared-settings permissions, and detailed mobile contribution
  policy have not been decided. Do not treat prior suggestions as approvals.

## Evidence and coordination

### Repository baseline

Inspected main `f28e1193df8625a6ad5ba574b0a631bf04106eec` after PRs 24–26:

| Component | Available behavior | Missing for this outcome |
| --- | --- | --- |
| `src/placement.rs` | Exact subset DP over peer order and contiguous layer ranges; up to eight eligible peers; supplied memory budgets and directed link costs; can omit peers | Automatic measurements, range-cost input, freshness, separate memory domains, reservations, explicit endpoint/feedback costs |
| `src/planning.rs` | Saved plan validation and sequential-decode objective; rejects multi-request throughput | Exact shard deployment output; existing output targets advisory llama.cpp RPC arguments |
| `src/shard/manifest.rs` and `src/shard/serve.rs` | Hash checks, fresh runner sessions, bounded native wire; local-only or remote prefix plus local final | Multi-hop production integration depends on M-27; process-based backend does not solve mobile embedding |
| `experiments/mobile-probe/iroh-core/src/ring.rs` | Experimental direct activation chain and terminal callback | Automatic placement/deployment, precise link time series, production lifecycle integration |
| `experiments/model-validation/shard_runner.cpp` | Per-range native compute, buffer diagnostics, RSS; Darwin footprint observations | Complete contribution accounting and hard total-memory enforcement |
| `src/jobs.rs`, `src/api.rs`, history modules | Existing leader queue/API and history components | Every-peer ingress, authoritative request synchronization/deduplication and recovery remain separate integration work |

No existing `docs/brainstorms/` or `docs/solutions/` collection was found at this
baseline. Relevant institutional guidance is in `docs/contributor-profiling.md`,
`docs/compute-placement.md`, `docs/live-validation-plan.md` and the experiments.
Some older handoffs describe the original two-Mac RPC demo; they are historical,
not the current Android/Metal/CUDA routing specification.

### Runtime agent response: 2026-10-25 03:20 UTC

The runtime agent explicitly confirmed these facts in
[response 1](https://discord.com/channels/1563598920800542840/1563603695688622092/1563754051622150205),
[response 2](https://discord.com/channels/1563598920800542840/1563603695688622092/1563754052985163868),
and [response 3](https://discord.com/channels/1563598920800542840/1563603695688622092/1563754054398771261):

- Runtime checkout is `david/m1-peer` at `c0565473d2d099eb8f9e11e7f8a9308b0d3b0931`,
  with later uncommitted mobile/UI changes. These are not all on main.
- Artifact label: Qwen3.8-27B UD-Q5_K_M; source SHA-256
  `2de73110cb254cbf09b54b717578dadff12ef1194e7271527e68202f39ba4bfd`;
  64 decoder layers, context 512, native batch at most 128. Treat source hash and
  manifest as identity rather than relying on the informal model name.
- Current manual route, exclusive range ends:
  M2 Metal `[0,20)` → S23 CPU `[20,24)` → Pixel 10 CPU `[24,28)` →
  M4 iPad Metal `[28,32)` → RTX 5060 Ti CUDA `[32,64)`.
- Android CPU is the user-confirmed baseline. Pixel 8a is stopped/excluded and
  M1 is excluded. Do not restart Vulkan testing or change worker configuration.
- One 51-step run completed at 1.38164 decode steps/s, median decode 697 ms,
  maximum 1435 ms, 51 executions per device, zero activations returned to origin.
  This is a single observation, not a stable performance guarantee.
- Later runtime observations include Pixel 10 native compute median 184 ms,
  p95 about 723 ms, maximum 1346 ms; S23 median about 67 ms. Approximately 75%
  of decode time was transport/coordination in one 54-step slice. Progress-arrival
  pauses cannot locate a wire delay precisely.
- Native TCP tests succeeded, but they do not measure Iroh. Android CLI still
  reports missing JNI/ndk-context setup; the root cause of Iroh stalls is unproven.
- S23 Vulkan shader pipelines fail. Pixel 10 Vulkan produces unacceptable
  activation differences; conservative variants were slower. Explicitly hiding
  Vulkan reproduced all seven CPU decode outputs on both phones. Small prefill
  differences below 0.25% are observations, not a general accuracy threshold.
- Existing harnesses can export source-hashed ranges and measure prefill/decode,
  native compute and scoped memory. No precise directed Iroh path/RTT/loss time
  series or comprehensive memory admission/reservation exists yet.
- The agent recommends isolated authenticated S23 ↔ Pixel 10 Iroh payload tests,
  followed by native fixed-input range profiles, as the smallest useful next step.

Locally inspected evidence under the runtime data root (not all published):

| Relative artifact | Observation and limitation |
| --- | --- |
| `results/live-demo-s23-five/c6a81c49b1ab4b5792b35b231ac68916/output/results.json` | The 1.38164 run above |
| `results/live-demo-s23-five/fa87a26e90a246b19e45c6e35710a240/output/results.json` | Completed, 52 steps, 0.71829 steps/s; different prompt |
| `results/live-demo-s23-five/81ef12bc149e48329f6e049fa0eaf61c/output/results.json` | Completed, 96 steps, 0.72829 steps/s; different prompt |
| `results/qwen38-p2p-ring-01/results.json` and `-02/results.json` | Earlier three-device runs: 3.94792 and 3.04842 steps/s; not controlled comparisons to the five-device route |
| `results/android-vulkan-ab/summary.json` | Backend correctness failures and CPU controls |
| `network-speed-test/results-v2.json` | TCP controls, not Iroh profile input |

Preserve failed trials and complete distributions. Do not compare these runs as
a speedup experiment because workload, topology and operating conditions differ.
Completed UI runs may contain user prompts and output: inspect timing privately,
and publish only synthetic/owned inputs and sanitized evidence. Completion status
does not prove natural end-of-generation; record token-cap and other stop reasons.

### Runtime plan review: 2026-10-25 03:30 UTC

The runtime owner accepted the planning direction in
[review 1](https://discord.com/channels/1563598920800542840/1563603695688622092/1563756669379412091)
and [review 2](https://discord.com/channels/1563598920800542840/1563603695688622092/1563756670780313691).
The corrections below are incorporated into the contract and phases 0–2.
This accepts a planning baseline, not live-worker changes or a hardware window.
The separately verified GTX 1660 SUPER tower is not enrolled or profiled and
must not enter feasible placement as a measured backend.

### Ownership and responses

| Track | Existing owner / proposed involvement | Coordination state |
| --- | --- | --- |
| Native runtime, hardware evidence, diagnostic transport tests | `david-codex` | Reviewed contract and phases 0–2; planning direction accepted with corrections incorporated; hardware window unreserved |
| Placement schema/search | `aarush-codex` | Existing owner; input requested, response pending |
| Production transport/ring and mobile embedding | `yash-claude`, M-27/M-26 | Existing owner; interface status requested, response pending |
| History/liveness/request synchronization | `connor-claude` with core owners | Scope and priority remain with their humans; no new assignment assumed |
| Apps lifecycle, planning document and integration contract | `david-apps-codex` | This plan; no edits to other owners' runtime work |

The plan lives in an isolated worktree. The runtime agent's dirty checkout,
workers, model files, UI processes and two-minute wake watcher remain untouched.
The watcher targets the runtime session only; it is not evidence of automated
monitoring by every other chat. Separate sessions must use distinct IDs/state.

## Research decisions

| Research | Adopt or evaluate | Limits for our implementation |
| --- | --- | --- |
| [EdgeShard](https://arxiv.org/abs/2405.14371) | Joint device selection and contiguous partitioning with measured communication and compute | Separate sequential latency from concurrent throughput; no borrowed speedup targets |
| [LinguaLinked](https://arxiv.org/abs/2312.00388) | Mobile capability profiles and runtime degradation monitoring | Rebalance at safe request boundaries initially; mobile suspension is peer loss |
| [Petals](https://arxiv.org/abs/2312.08361) | Join-time measurement, consecutive blocks, explicit weight/state availability for recovery | Replica routing consumes extra capacity; replicated weights alone do not preserve KV/recurrent state |
| [Parallax](https://arxiv.org/abs/2509.26182) | Separate placement of weights from request admission/routing | Initially pin a single compatible route per request; no arbitrary per-token rerouting |
| [PRIMA.cpp](https://arxiv.org/abs/2504.08791) | Later controlled experiment with disk prefetch and repeated visits to devices | Only after resident-capacity limit is measured; extra hops/I/O may dominate; no core migration |
| [Kafila](https://arxiv.org/abs/2609.34045) | Cheap preliminary capability probes; account for head memory/compute explicitly | Proxy estimates require actual-model validation; do not force every trusted peer into the compute chain or optimize only the bottleneck stage |

For a single active sequence, rank feasible placements by predicted full decode
cycle time: all measured range compute + activation edges + terminal sampling
and token feedback. Include ingress/result delivery and request synchronization
in end-to-end service measurements. For this first objective keep a declared
context and prefill workload; report TTFT rather than hiding it in decode cost.
Concurrent-pipeline throughput is a later objective requiring independent request
state and measured concurrent service curves.

## Proposed measurement contract (review before implementation)

Use a versioned profile envelope with references to raw evidence. Existing JSON
schemas reject unknown fields; introduce an explicit new version/adapter rather
than silently adding fields or manufacturing legacy per-layer timings.

| Record | Required information |
| --- | --- |
| Identity | Source model SHA, shard artifact SHA/range, tokenizer/template hash, native protocol/ABI, per-device runtime/library/build hash, backend and configuration, OS/driver, peer ID |
| Workload | Prefill/decode phase, context occupancy and maximum context, input batch, sequence count (initially one), fixed input/token hashes, prior tokens/positions and recurrent/KV history, reset procedure, teacher-forced vs free-generation mode, warm/cold state |
| Compute | Actual contiguous range `[begin,end)`, sample count, raw timings, median/p95/max, native vs service-time scope, endpoint/head costs, correctness evidence |
| Capacity | Memory domains and sharing relationships, configured contribution budget, currently available/app-limited capacity, load/upload/repack/staging peaks, native session buffers, Rust transport queues, driver allocations, OS headroom, mapped resident vs virtual bytes, reservations, weights/cache/recurrent/scratch estimates, peak window and known omissions |
| Link | Directed peer pair, payload size/phase, framing/control bytes, full echo vs payload-to-small-ACK mode, Iroh build/config, authenticated endpoint IDs, path/relay transitions, local RTT/round-trip service timing and scope, throughput, failures/timeouts, available transport counters |
| Freshness | Measurement ID, timestamp, monotonic local duration, validity conditions, expiration policy, power/thermal/background state |
| Deployment | Plan/version/epoch, profile IDs, chosen backend/ranges, expected shard hashes, origin/layer-0/terminal roles, reservations and expiry, ready acknowledgments |

Compatibility is not equality of platform-specific binaries: keep common model,
wire and ABI compatibility requirements while binding measurements to each
device's exact runtime build. Do not count shared CPU/GPU memory twice or admit
two backend candidates on one physical device as independent memory pools.

Range observations must remain range observations. A four-layer hybrid timing
does not become four measured per-layer timings by division. Start with existing
boundaries; measure additional candidate ranges to improve the search. Rough
probes can rank what to test next, but strict live admission requires measured
inputs and adequate memory accounting.

Use monotonic timings on each host. A round-trip sample is not a directly measured
one-way delay, and timestamps from different device clocks cannot be subtracted
without an established synchronization/error model. Separate native compute from
service and transfer timing to avoid double-counting.
Do not divide echo RTT by two or use it directly as a directed activation-edge
price. Full echo transfers the payload both ways; payload-to-small-ACK tests
still include acknowledgment and service costs. Any planner adapter must declare
its timing scope and account for those costs explicitly.

## Implementation phases and acceptance gates

### Phase 0 — Freeze a reproducible CPU/Metal/CUDA baseline

Runtime owner publishes a manifest and sanitized raw evidence for the actual
running binaries, including uncommitted patches, shader/backend selection,
source/shard hashes, device availability, prompt/template and context. Label
individual successful runs and failures without selecting only favorable results.

- [ ] Reproduce correctness with fixed inputs and fresh contexts on participating backends.
- [ ] Document tolerances by model/backend test, including logits/token checks and multi-step state/reset; the Vulkan diagnostic's 1% threshold is not a universal pass criterion.
- [ ] Keep Android CPU, Pixel 8a and M1 exclusions as confirmed; use an explicit hardware test window coordinated with the runtime owner.
- [ ] Before tests, confirm the current UI `/state` reports `active=false`, keep iPad foreground/unlocked, and confirm phones awake/reachable. Coordinate sustained tests because isolated traffic still competes for CPU/Wi-Fi; no exclusive window is reserved yet.
- [ ] Publish synthetic/owned fixtures and sanitized measurements only; include each run's stop reason.
- [ ] Record scope of existing memory measurements; do not claim complete budget enforcement.

### Phase 1 — Identify Iroh transport and coordination costs

Build an isolated authenticated benchmark using the same Iroh build/configuration
as the worker, with bounded payloads and no model execution. Run S23 → Pixel 10
and Pixel 10 → S23, then candidate links to Mac/iPad/CUDA. Keep endpoint creation,
cold connection, warm stream, reconnection and path changes distinct.

Initial proposed matrix: 20 KiB decode payload; native prefill batches 1, 32 and
128 (20 KiB, 640 KiB and 2.5 MiB for the pinned 5120-wide f32 boundary). The
runtime owner confirmed these payload sizes, excluding control/framing. Measure
full echo and payload-to-small-ACK separately, in both orientations, and count
control/framing bytes. Use warmup,
at least 200 warm decode exchanges per direction, repeated connections, and a
sustained observation window; record every timeout. These are proposed test
parameters, not established benchmark results.

The current ring exports residual transport/coordination and progress arrival,
not exact direct/relay/path transitions, QUIC RTT or loss counters. Inspect and
instrument the pinned Iroh 1.4.1 APIs before claiming those observations. Missing
loss counters mean unknown loss. Keep native/loopback service measurements
separate from Iroh end-to-end samples.

- [ ] Capture available per-connection direct/relay/path/RTT/loss counters and local send/read durations; mark unsupported counters unknown.
- [ ] Separate network-only echo from worker-service overhead and progress telemetry.
- [ ] Compare Android CLI behavior with correctly initialized Android embedding when available; a difference is evidence, not a predetermined root cause.
- [ ] Produce trace evidence locating stalls, or explicitly retain an unresolved transport limitation and exclude unsuitable edges from strict placement.
- [ ] Stop temporary listeners and release test resources after the coordinated run.

This is the first useful experiment recommended by the runtime agent. Placement
search and schema tests can proceed while the transport issue is investigated.

### Phase 2 — Collect model-specific compute and capacity profiles

Extend the existing export/captured-input harness on the runtime track. Measure
actual ranges on each eligible backend with the pinned model, context 512 first,
then only supported larger contexts. Separate initialization, prefill and decode;
record sustained thermal behavior and concurrent OS load. Stage fixtures and
candidate ranges rather than downloading the entire model onto every phone.

- [ ] Each timing has identity, workload, range, correctness result and raw samples.
- [ ] Fixtures preserve prior tokens/positions and recurrent/KV history, reset between independent samples, and distinguish teacher-forced checks from free-generation equivalence. Hybrid attention mix is range-specific; do not transfer four-layer timings or tolerances to arbitrary ranges.
- [ ] Each selected memory domain has a documented accounting boundary over load and inference. Unknown total coverage leaves strict budget acceptance unavailable.
- [ ] Capture load/upload/repack/staging peaks, native session buffers, Rust queues, driver allocations and OS headroom; distinguish mapped resident bytes from virtual address space. Current C++ budget checks observed buffers, not a hard process/system cap.
- [ ] Unified-memory estimates count shared allocations once; dedicated GPU and host usage are reported separately with transfer/staging overhead.
- [ ] Reconnect may reuse compatible compute evidence but refreshes availability and links; binary/backend/model changes invalidate matching profiles.

### Phase 3 — Extend measured planning and emit an exact deployment

Aarush's planner remains the owner of search and validation. Add versioned range
costs, memory domains/freshness and explicit endpoint/feedback costs. Preserve
subset selection, directed edges and full contiguous model coverage. Initially
one compute range per selected physical peer, one active request and at most the
existing eight eligible search candidates. Larger sets require an explicit,
tested search strategy; never silently discard candidates.

Decouple placement output from the stock RPC adapter. Add a shard deployment
adapter that consumes the selected assignments and emits exact M-27 stage ranges,
peer identities, artifact hashes, context and compatible template. Confirm M-27's
stage-0-local restriction: initial deployments may enforce it, while every-peer
ingress forwards accepted requests to the chosen execution owner. API receiving
peer must not accidentally constrain all future placements.

- [ ] Synthetic exhaustive comparisons on small cases verify minimum cost within the stated search space.
- [ ] Reject holes/overlaps, stale or mismatched profiles, unmeasured edges/ranges, invalid capacity and incompatible backends.
- [ ] A slow peer can stay trusted and on standby; no arbitrary layer quota per member.
- [ ] Predicted costs include the actual feedback path without counting any service component twice.
- [ ] Output identifies every measurement used and reason for excluded/ineligible devices.

### Phase 4 — Reserve, stage, validate and activate

Proposed core transaction: plan → request per-peer reservation → stage/verify
artifacts → initialize → gather ready acknowledgments → commit plan version →
execute. Reservation is bound to identity, backend, memory domains, plan/epoch
and local expiry; it is idempotent and prevents the mesh overcommitting itself.
It cannot guarantee the OS or another application will not reclaim memory.

Reject or abort when capacity changes, any lease expires, hashes differ, a peer
leaves, or initialization fails. Release successful reservations after partial
failure. Publish files atomically only after checksum validation. Retain the last
healthy assignment until replacement can be committed within the available memory;
otherwise drain work before reloading rather than assuming two copies fit.

- [ ] No generation starts before all selected stages report the expected range, identity and ready state.
- [ ] A stale coordinator cannot activate a plan after losing ownership; workers validate epoch/version.
- [ ] Crashes/retries do not duplicate reservations or inference sessions; abandoned leases expire.
- [ ] A fresh short generation proves execution on every selected peer and exact intended ownership; diagnostic memory success is labeled separately from complete admission.
- [ ] During generation the plan stays fixed. Peer loss fails/interrupts the request explicitly and recovery rebuilds state; no transparent KV-preservation claim.

### Phase 5 — Compare plans and demonstrate larger resident capacity

Use the same pinned prompt/template, requested output count, context, backend
builds, device policies and correctness checks. Run baseline and candidate in
interleaved order, with at least three fresh-context runs of each as an initial
minimum; increase sampling when observed variability prevents a conclusion.
Separate cold loading from steady decoding and include failed trials.

- [ ] Report TTFT, prefill rate, decode interval distribution, full-request completion and stop reason, per-stage compute, transfer/control residual, bytes, path changes and memory by scope.
- [ ] Compare predicted vs actual median and tail behavior; investigate errors beyond observed baseline variation. Set numerical acceptance tolerances from Phase 1/2 data before choosing a winning plan.
- [ ] Claim speed improvement only when matched trials support it beyond run-to-run variation; otherwise report no demonstrated improvement.
- [ ] Generate a feasible plan for a user-selected larger supported model using actual artifact sizes and runtime state; do not infer fit solely from parameter count or aggregate RAM.
- [ ] Capacity milestone: full model executes across selected peers within documented budgets, while it cannot fit within any individual participating peer's same budget. Distinguish this from installed-RAM and no-swap claims.

If no resident plan exists, show the missing capacity or unsupported component.
Only then propose a bounded PRIMA-style prefetch/offload experiment with disk
timings, memory pressure and network costs. It remains a separate experiment.

### Phase 6 — Expose the lifecycle in desktop and mobile apps

App-visible lifecycle:

`Pairing requested → Awaiting trust → Benchmarking → Planning → Receiving shard → Loading/validating → Contributing`

Alternative states include `Trusted/standby`, `Needs measurements`, `Insufficient
capacity`, `Backend unsupported`, `Paused`, and `Disconnected`. Explain why a
device is idle and what capacity it adds. Display measurement age, download
progress, current range, memory contribution and actual backend.

Use common core commands/events for apps and CLI so desktop/iOS/Android do not
implement separate planners. Benchmark work runs off the UI thread, can be
canceled, and respects device policy. Returning peers reuse their trusted identity
and valid profile data, with current capacity/link checks before admission.

Mobile compute and network lifecycle require M-26's in-process native backend,
platform context and suspension handling. Desktop subprocess runners are not a
mobile embedding design. Native permission/lifecycle code is platform-specific
even if a UI framework is shared.

Pairing protocol review must cover expiring invites, invitation sharing/accessibility,
identity-bound approvals, guessing limits, replay, revocation and concurrent
approvals. An invitation is not a permanent membership credential. Any trusted peer may initiate approval; membership must still be
recorded coherently by the mesh. Pending joiners do not receive model/history
before approval. Leaving invalidates local membership and clears or handles cached
mesh data under an explicit policy before joining another mesh.

Core integration implemented October 25: `llm_cluster::mesh::Node` now owns
creation, invitations, signed membership and trusted core control sessions.
`apps/enrollment` is a thin C/JNI adapter. The mesh uses the core N0 endpoint,
accepts only signed members on the control ALPN, and maintains bidirectional
heartbeats using the merged PR #28 implementation.

Full signed `layercake://join/...` invitations provide service-free P2P bootstrap.
The user dropped emoji pairing on October 25; the lookup service is removed.
Apps share or paste the full ticket and need no Layercake server.
Trust approval is always required. Existing private identities and ledgers persist.

Core tests cover relay-only full-invitation admission, remote trusted
approval, rejection of unapproved control connections, heartbeats, disconnect and
restart/reconnect. See [core membership](../mesh-membership.md). Compute profiling,
resource reservation and model execution remain separate from trusted membership.

Every-peer API admission must deduplicate stable request IDs across peers, commit
one queue position under one owner/epoch, and synchronize required prompt/settings
before layer-0 starts. The exact acknowledgment/quorum policy is an open core
decision. Return tokens to the ingress connection with explicit interrupted-output
semantics. A management UI prototype may precede this, but cannot claim complete
mesh application acceptance until it works.

## End-to-end failure scenarios

| Scenario | Required result |
| --- | --- |
| Two trusted peers approve one join simultaneously | One identity/membership record; one benchmark lifecycle |
| Device is trusted but Vulkan fails correctness | CPU is eligible only with its own passing profile; failed GPU is excluded |
| Memory drops or reservation expires while staging | Abort/replan, release other reservations; no partial active route |
| Benchmark canceled or app suspended | Stop owned work, preserve valid completed evidence, mark availability accurately |
| Android path changes from direct to relay | Mark link evidence stale/degraded; pin or fail current job according to transport outcome; re-evaluate future placement |
| Checksum failure or interrupted download | No ready state; safe retry from verified data; no corrupted final artifact |
| Duplicate request arrives at different APIs | One accepted job and execution owner; no duplicate generation |
| Peer disappears mid-decode | Mark request interrupted/failed; restore only with available weights and rebuilt state |
| New peer offers more memory but slower execution | Planner may use it for fit or keep it on standby; report measured tradeoff |
| Model fits aggregate RAM but not per-domain constraints | Reject placement and explain the capacity deficit |

## Deliverables, review and open decisions

First reviewable slice: versioned evidence records, isolated Iroh benchmark report,
fixed-input range profiles, an offline measured plan, and a matching deployment
manifest. Then a coordinated hardware trial, reservation lifecycle and app events.
Do not wait for a full app to validate the placement contract.

Before implementation owners confirm:

1. Runtime: planning direction and payload sizes confirmed; contract/phases 0–2
   reviewed with corrections incorporated. Pinned Iroh 1.4.1 counter APIs still
   need inspection, correctness tolerances need evidence, and a shared hardware
   window remains unreserved. Publish only sanitized evidence and owned fixtures.
2. Yash: M-27 manifest/template/origin constraints, M-26 FFI and mobile lifecycle
   boundary. Response pending.
3. Aarush: range-cost schema migration, reservation/capacity inputs, exact shard
   output adapter and treatment of more than eight candidates. Response pending.
4. Core/history owners: queue synchronization/epoch/acknowledgment policy and
   already-streamed output semantics. Do not infer readiness from storage alone.
5. Product follow-up: mobile UI framework, contribution defaults, shared-settings
   and revocation permissions. These do not block isolated measurements.

Implementation validation follows repository CI: format, Clippy where configured,
standalone fake helper builds before all-target tests, plus platform cross-builds
and physical-device trials for changed mobile code. This document-only change
requires link/path and whitespace checks; it does not claim runtime tests ran.

The planner/harness acceptance gates are explicit so work can be reviewed without
assigning unapproved tasks to teammates or presenting unmeasured performance as
fact. No existing M-ticket is completed merely by merging this plan.

## References

- [Existing architecture attachment, especially sections 7–10](https://discord.com/channels/1563598920800542840/1563603695688622092/1563612586614726778)
- [Every-peer ingress clarification](https://discord.com/channels/1563598920800542840/1563603695688622092/1563700404439621713)
- [Runtime coordination request](https://discord.com/channels/1563598920800542840/1563604879182794932/1563753442739232891)
- [Yash interface request](https://discord.com/channels/1563598920800542840/1563607263334113332/1563753444064628779)
- [Aarush schema request](https://discord.com/channels/1563598920800542840/1563619999455641730/1563753445230514207)
- [Planner PR 24](https://github.com/yash-joshi-0/HackOHIO26/pull/24)
- [Shard backend PR 25](https://github.com/yash-joshi-0/HackOHIO26/pull/25)
- [Bridge mapping PR 26](https://github.com/yash-joshi-0/HackOHIO26/pull/26)
- [Compute placement](../compute-placement.md), [profiling](../contributor-profiling.md), [shard backend](../shard-backend.md), [history](../history-sync.md)
