# M-5 measured placement path

`llm-cluster measured-plan PROFILE.json` reads version 1 range and link
observations and prints an **advisory** sequential decode placement. It does
not launch a worker, write a shard manifest, reserve memory, or verify a run.
The existing `plan` command and llama RPC launch path remain separate.

Run the synthetic fixture:

```sh
cargo run -- measured-plan examples/measured-placement-profile.json
```

Save the output, refresh the profile's capacity and time observations, then
recheck it before any launch:

```sh
cargo run -- validate-measured saved-plan.json current-profile.json
```

That fixture chooses peer `b` for layers `0..2`, then peer `a` for `2..4`.
Its invented times and capacities are for testing only. Replace every
observation and identity before drawing a hardware conclusion.

## Input contract

The [fixture](../examples/measured-placement-profile.json) shows every field.
`workload` pins the full model source hash, exact runtime/build identity,
context, batch, layer count and payload sizes. Every range and link observation
repeats that identity; a mismatch makes that observation ineligible. Give each
observation a stable `profile_id` and its measurement time. `observed_at_unix_ms`
and `max_profile_age_ms` reject old or future observations. A real request
should use the current time and an age limit appropriate to capacity, thermal
and network changes.

Each peer supplies authorization and health, a timestamped observation of
`available_bytes` after other workloads, an owner-approved `budget_bytes`,
conservative `reserved_bytes` and exact measured `[begin,end)` ranges. A
selected range must fit both the approved limit and currently available
capacity. Stale capacity makes the peer
ineligible. Each range supplies measured
decode time and peak bytes for **that range** over load and inference. A whole
range observation stays whole; the planner never divides it into per-layer
costs. Missing ranges make those assignments impossible.

Memory values have three distinct physical pools: `ram`, `vram` and `shared`.
Put unified-memory allocations in `shared` once. Do not put the same bytes in
both host and device pools. Charge bridge, process, driver, model staging,
KV/recurrent state, scratch, output tensors and OS headroom according to their
actual accounting boundaries. Each selected range's peak plus its peer's
reserve must fit all three budgets. If a scope cannot be measured or bounded,
leave that peer out of a live placement request.

Links are directed observations keyed by `from`, `to` and exact payload size.
The planner requires measurements for origin-to-first control, each adjacent
activation transfer, and final-to-origin control when those peers differ. It
adds those measured transfer times to range decode times. Profile `path`
records direct or relay transport; use observations from the actual expected
path and workload. Do not insert a raw RTT, a whole-ring residual, or a
different wire protocol as a directed transfer measurement. The input allows
one eligible observation per range or directed payload edge so there is no
ambiguous choice between trials.

The search uses each peer at most once and covers every layer with contiguous
ranges. It jointly chooses order and range boundaries for up to eight eligible
peers, respecting `min_peers`. An unmeasured transition or an over-budget
range makes that route impossible. The output includes ordered assignments,
charged memory, contributing profile IDs, planning time and predicted decode
time. It is a prediction for one sequential request, not a throughput or
prefill estimate. `measured_placement::validate_output` recomputes a saved plan
against refreshed inputs and rejects changed or expired profiles.

## Runtime acceptance still required

The v1 shard backend currently accepts one remote prefix followed by a local
final stage. M-27 must supply the production ring manifest and validation for
arbitrary stage order and remote finals before this plan can drive that backend.
The run must then use the same model source, artifacts, runtime, context,
batch, ranges, path and workload as the observations. Reserve capacity on each
destination before starting, and collect trustworthy total contribution peaks
and fresh compute events for the exact run. Compare repeated measured decode
times with predictions and record failures and exclusions. Until those gates
pass, M-5 remains in progress.
