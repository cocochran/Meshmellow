# Compute distribution: placement and RPC planning

Aarush's component exposes a transport-independent Rust library. It selects
contiguous layers for eligible contributors and prepares advisory llama.cpp
server arguments. It does not start workers, run a model, enforce OS memory caps,
or claim that a requested allocation happened.

```sh
cargo test
cargo run --example placement
```

The example uses synthetic numbers and prints a plan; it performs no inference.

## Inputs and integration

1. Supply `ModelProfile`: exact model hash, pinned runtime ID, context tokens,
   actual weight bytes per block and per-layer KV bytes per token. Include all
   batch/slot assumptions when measuring. This version assumes one slot; a
   recurrent/hybrid model needs its additional state included conservatively in
   reservations. Do not reuse a dense-model estimate for Qwen3.5 blindly.
2. Supply `PeerProfile` for each candidate device, ordered by allowed stage
   order. Set explicit authorization, current health, opt-in memory budget,
   measured per-layer decode costs and conservative reserved bytes. Reserve
   embedding/output tensors, scratch, non-KV state and headroom wherever the
   backend may allocate them. Profiles must match model/runtime/context.
3. Supply directed `LinkProfile` costs for allowed transitions. Missing links
   forbid a transition. For RPC use measured controller-mediated overhead, not
   an assumed direct activation path. Costs are estimates for this workload,
   not transport RTT alone. This additive model does not predict prefill,
   concurrency, thermals or every graph transfer.
4. Call `placement::plan(..., min_peers)`. Use 2 to require at least two
   contributors. Use 1 to permit the scheduler to choose a single faster device.
   There is no forced allocation to every connected peer. Every selected peer
   owns at least one block. The planner may skip slow, unhealthy, incompatible or
   unauthorized peers. Failure returns `NoFeasiblePlacement`; it never silently
   increases budgets or falls back to local execution.
5. Yash's bridge supplies authenticated peer ID -> leader-local loopback
   `SocketAddr`. Discover exact runtime device names after registering RPC
   endpoints. Supply one `RuntimeDevice` per selected peer, in placement order.
   The adapter supports devices selectable by llama.cpp `--device`; plain local
   CPU placement is not supported by this adapter. Caller must verify names and
   mappings against runtime discovery; the adapter cannot contact the runtime.
6. `rpc_plan::server_args` returns argv values for `Command::args`, never a shell
   string. It explicitly selects devices, proportions and layer split, requests
   block count + output layer offload, fixes one slot and binds HTTP to loopback.
   Review environment/default runtime options separately. Caller verifies model
   file hash and runtime ID before launching. No process is launched by this API.

The DP minimizes predicted sequential decode time across the supplied order,
subject to per-device estimated memory and minimum contributor count. It tracks
the last device, completed layer prefix and contributor count (saturated at the
minimum). It does not search permutations of devices or benchmark them itself.
Intended for small hackathon clusters; worst-case transition work grows roughly
with peers squared times layers squared times minimum contributors, plus plan
copying. Integer overflow and malformed identities/profiles fail explicitly.

## Runtime verification gate

The RPC adapter is an approximation: `--tensor-split` proportions are block
counts, not a guarantee of exact layer ownership or a hard memory limit. Output
placement, backend rounding and extra allocations can differ from this plan.
After loading, the runtime owner must check actual per-device allocation, peak
memory against contribution budgets, and fresh compute events on each selected
device. Stop/replan if allocation violates budgets or the minimum contributors.
A health endpoint or generated answer alone is insufficient evidence.

David owns runtime measurements and the M-15 sweep; this module consumes those
profiles rather than modifying his harness. Yash owns networking and RPC bridges.
Live capability exchange, profile collection, process supervision, post-load
verification, retry/replanning, and a real two-machine Iroh run remain integration
work. Automatic failover and history persistence belong to separate components.

Flag reference checked against the team's recorded runtime pin:
[llama.cpp argument definitions at 71255fef](https://github.com/ggml-org/llama.cpp/blob/71255fefc092b0344f2acddb891388814ae961be/common/arg.cpp).
The architectural distinction between RPC and a future direct activation pipeline
follows the shared MeshLLM research document. Main's older README describes that
future pipeline; this component targets controller-driven RPC.

### Checking collected runtime evidence

Call `verification::verify_run(&placement, &current_profiles, &evidence, 2)`
after a fresh inference run. `RunEvidence` identifies the loaded model hash,
runtime pin and context; each `DeviceEvidence` supplies a peer ID, nonzero peak
total contribution bytes (load plus inference), and fresh compute-event count.
The check requires every planned contributor, rejects duplicate/unplanned peers,
rechecks current authorization/health and profile identity, and rejects peaks
over the opt-in budget or contributors with zero compute events. Evidence order
is independent of placement order.

The caller is responsible for collecting trustworthy evidence for the exact run,
including all relevant memory allocations. This function does not parse logs,
measure memory, enforce limits, stop processes, or prove future runs will fit.
On failure the launcher must stop/replan before treating the placement as verified.

The confirmed initial integration uses remote M1 `RPC0` then local M2 Pro `MTL0`
with runtime pin `71255fefc092b0344f2acddb891388814ae961be`; discover device
names on each run rather than assuming these IDs are stable. Keep output tensors
local and verify that in allocation logs. Yash's current bridge CLI starts one
`bridge-connect` process per remote worker on distinct supplied loopback ports;
there is no peer-to-endpoint discovery API yet. Use explicitly enrolled peers;
the initial published bridge requires a separate approval guard.
