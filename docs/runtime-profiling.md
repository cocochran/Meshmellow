# Runtime profiling coordinator

## Current state

`measured_placement::plan` consumes range, link, and capacity observations. It
does not collect them, reserve capacity, start workers, or activate a plan. The
current leader job queue serializes chat requests to its upstream; it does not
coordinate profiling across workers or an experimental ring. No runtime
profiler API or candidate-range executor is implemented.

Do not turn a plan fixture or an existing fixed-route run into a candidate
profile. A profile must come from the exact model artifact, runtime, backend,
layer range, and workload it describes. Unknown or incomplete evidence remains
ineligible for placement.

## Ownership

- Runtime owns probe execution, bounded cancellation, profile lifecycle, and
  the admission gate that prevents probes from overlapping active inference.
- Apps own explicit compute opt-in, progress, and cancellation controls. Mesh
  membership alone never opts a device into compute or profiling.
- The planner consumes versioned evidence and returns advisory assignments.
- The runtime integration owns manifest validation and stage readiness.
- Capacity reservation and survivor replanning still need explicit owners.
  They are separate from profiling and from the current planner.

This runtime workstream owns the coordinator contract and local inference
admission gate. It does not claim that worker-side probe execution, remote
reservation, or plan activation already exists.

The initial runtime interface is `start(request) -> run_id`,
`status(run_id)`, and `cancel(run_id)`. Transport and app command names remain
open until the planner and runtime-integration owners confirm their contracts.

## Probe contract

A start request must identify a schema version, model and artifact hashes,
runtime/build identity, exact candidate ranges, context occupancy and limit,
batch and sequence counts, fixture identity, and explicit elapsed-time,
memory-pool, and network-payload limits. It also carries the caller's explicit
compute opt-in. Invalid identities, unsupported ranges, or missing bounds are
rejected before work starts.

Each run has a stable ID and one of `queued`, `running`, `completed`, `failed`,
or `canceled`. Status includes progress and timestamps, but never prompts,
token IDs, generated text, or raw activation values. Cancellation stops child
processes and network sessions, releases the admission gate, and marks partial
observations unusable. Process exit, timeout, thermal/power stop, memory
pressure, and cancellation remain distinct outcomes.

The runtime gate must be shared with every inference entry point on the node.
The leader's current `JobQueue` is not sufficient: it serializes only requests
forwarded through that leader API, not direct worker work or ring execution.
Until a cross-worker reservation/admission protocol exists, the coordinator
must reject a multi-peer profile rather than imply that local serialization
protects the whole mesh. Profiling never begins automatically on mesh join and
does not interrupt an active request.

## Evidence and freshness

Keep reusable model/range compute observations separate from changing
availability, capacity reservations, and directed link observations. Each
result needs a stable profile ID and measurement time. Preserve the first
failure and all exclusions; retries must not replace failed evidence silently.
Per-host durations use local monotonic clocks. Do not subtract timestamps from
different hosts or label remote read/wait time as network-only time.

The current `RangeObservation` contains decode time and peak bytes, but not
prefill timing, fixture/correctness results, backend/build identity, context
occupancy, sequence count, power/thermal conditions, or peak scope. Add those
fields in a versioned schema before treating profiler output as eligible input
to measured placement. Existing version 1 observations remain advisory and
must not be reinterpreted as complete evidence.

Fresh usable capacity and exact directed link costs are still required at plan
time. Reusable compute observations do not imply present availability. A
profiled range does not reserve memory or authorize a worker.

## Integration gates

1. Agree on a versioned evidence schema with the planner owner and runtime
   manifest constraints, including the M-27 leader-local stage-0 rule.
2. Implement and validate a local candidate executor with cancellation,
   cleanup, resource bounds, and the shared inference gate.
3. Add authenticated worker-side execution and reservation only after its
   ownership and wire contract are agreed. A leader-local gate alone cannot
   protect remote stages.
4. Add the app opt-in and lifecycle controls against the status API. Keep
   manual start versus automatic post-opt-in probing an explicit product
   choice; mesh join never starts it.
5. Before placement can use results, verify exact artifacts/ranges, correctness,
   prefill and decode workloads, scoped memory peaks, link path and payload,
   freshness, and cancellation during startup and execution.

No live worker or physical device is part of the implementation checks. Device
profiling requires a separately coordinated test window and owner opt-in.
