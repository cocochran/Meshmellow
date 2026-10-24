# M-5 live placement validation handoff

Prepared from David's 2026-10-24 final tuning update and Yash's bridge ACK.
Implementation: `placement::plan`, `rpc_plan::server_args`, and
`verification::verify_run`; see `compute-placement.md` for API inputs.

Status update, 2026-10-24: David's [two-Mac shard-runner run](../experiments/model-validation/SHARD_RUNNER.md)
proved a 32B model's exported partitions can execute together over Iroh, with
source weights larger than either Mac's RAM. That prototype is separate from
the leader/job API and its measured process footprints do not cover the full
contribution accounting boundary. M-20's prelaunch plan check and optional
postrun evidence-file verification have landed, but no trustworthy complete
`RunEvidence` source or planner-driven hardware acceptance run is published.
Use the [contributor profiling record](contributor-profiling.md) for the missing
budget, backend and telemetry facts.

## First validation run

1. Pin runtime `71255fefc092b0344f2acddb891388814ae961be` on both Macs.
   Record the exact Qwen3.5-4B GGUF hash, context, prompt, generated token count,
   batch settings and runtime options. Use the same workload for every comparison.
2. Supply explicitly approved worker IDs and start one leader-local bridge per
   worker: `llm-cluster bridge-connect <WORKER_ID> --listen 127.0.0.1:50053`.
   Additional workers need distinct ports and caller-supplied endpoint mappings.
   Discover runtime device names after registering RPC endpoints.
3. Obtain each owner's contribution budget. Installed 16 GiB RAM is not a model
   budget. Reserve output/embedding tensors, hybrid-model state, scratch and
   headroom; measure peak total contribution through load and inference.
4. Reproduce the measured baseline before testing planner output. The initial
   confirmed device order is remote M1 `RPC0`, local M2 Pro `MTL0`. In that order,
   `--tensor-split 1,1` requests 50/50, `1,3` requests 25% remote/75% local,
   and `3,1` requests 75% remote/25% local. These are approximate layer ratios,
   not exact weight-byte or memory allocations. Verify final/output layers local.
5. Collect model/runtime/context-matched per-layer compute and directed link
   costs, call `plan(..., 2)`, and pass matching discovered devices to
   `server_args`. Review the returned argv and runtime allocation logs before
   evaluating throughput. The adapter does not enforce output placement or caps.
6. Collect fresh per-device compute events and peak bytes, then call
   `verify_run(..., 2)`. Reject the run if either contributor is missing, inactive,
   incompatible, unhealthy, unauthorized or over budget. The launcher must stop
   and replan on failure; verification itself does not stop processes.

## Calibration constraints from M-15

David reports no clear decode TPS improvement for local75% over 50/50;
remote75% was slower in the initial sweep. The initial MTP2 18% improvement did
not repeat and must not be used as an expected uplift. Direct Iroh worked with
similar TPS. Cache snapshot changes improved TTFT without proven decode benefit.

The reported decode boundary payload is 10 KiB; a 34-token prefill is 340 KiB.
The trace spent approximately 4.37 seconds waiting on 78 remote activation reads
within approximately 6.37 seconds of generation. Those waits combine remote
compute and communication: do not use their quotient as pure network latency,
or add it to separately measured remote compute and double-count the cost.
Calibrate link overhead for this model and workload; bulk bandwidth alone is
insufficient. The planner estimates sequential decode only, so report measured
TTFT and prefill separately.

## Evidence to return

Record exact commands, runtime/model IDs, bridge endpoint/device mapping,
requested and observed layer placement, budgets/reservations/peak bytes,
per-device compute events, verification result, TTFT and decode TPS for repeated
baseline and candidate runs. Report run count and spread, including failures.
Keep raw logs with David's `experiments/` harness. A successful response alone
does not establish two-device execution or a performance improvement.

This handoff describes the planner-driven acceptance run still needed for M-5;
it does not reinterpret the separate shard-runner capacity result as a budget
or hard-cap pass.
