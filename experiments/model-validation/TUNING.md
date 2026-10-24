# Throughput tuning, October 24

The goal is to improve one distributed chat's generated tokens per second while
keeping both Macs involved. The model hash, quantization, context, seed and
temperature are unchanged from the original validation. No local-only fallback
is counted as a distributed speedup.

## Placement and snapshot sweep

Three 64-token runs per configuration, with a 34-token prompt. Device order is
explicitly `RPC0,MTL0` (remote first, local output layers).

| Configuration | Median decode tokens/s | Median first text, seconds |
| --- | --- | --- |
| 50/50, original caches | 12.20 | 2.862 |
| 50/50, snapshots disabled | 11.28 | 0.553 |
| 25% remote / 75% local, snapshots disabled | 11.23 | 0.404 |
| 75% remote / 25% local, snapshots disabled | 9.69 | 0.583 |
| 50/50, original caches, repeated | 10.73 | 3.514 |

Disabling `--cache-ram` and `--ctx-checkpoints` together substantially reduced
first-text delay on these short uncached requests. Neither that change nor
moving more layers locally demonstrated a repeatable decode speed increase.
The two cache settings were changed together, so their individual effects are
not isolated. Disabling checkpoints trades away fast restoration of earlier
recurrent context; long edited conversations may require more re-evaluation.

Tailscale reported a direct path, with five latency observations of
72, 46, 44, 126 and 9 ms. A direct SSH connection to the worker's LAN address
timed out. Neither machine had low-power mode enabled. No network, firewall
or power policy was changed. These conditions are not controlled enough to
interpret small placement differences as a reliable optimization.

## Multi-token prediction

The existing GGUF includes a trained prediction head. The pinned runtime can
use it with `--spec-type draft-mtp`, verifying proposed tokens with the same
target model. The draft head stays local; the main model still spans both Macs.
Three runs per configuration, up to 128 tokens (the response stopped at 79).

| Comparison | No prediction | Two-token draft |
| --- | --- | --- |
| Initial sweep, median tokens/s | 10.72 | 12.64 |
| Confirmation, median tokens/s | 12.46 | 12.44 |

The initial 18% improvement did **not** reproduce. Do not advertise that as a
confirmed speedup. Four-token drafting achieved 11.61 tokens/s; single-token
drafting achieved 11.14 tokens/s in their respective runs. The baseline and
MTP outputs were identical across the recorded initial and confirmation runs;
this is a sample comparison, not a general quality evaluation.

For the two-token draft, the target accepted 37 of 84 proposed tokens. Four-token
drafting proposed 164 tokens but still accepted only 37, adding wasted work.
MTP uses additional prediction-head and execution-state memory, which matters
when choosing a model close to the combined memory budget.

## Iroh comparison and RPC trace

Built Yash's `74acef0` bridge in release mode with a test-only, fail-closed
`MESHLLM_ALLOWED_PEER` check before opening the worker's loopback target.
The unapproved test identity was rejected with application code 3; the approved
identity successfully ran the distributed model. The guard patch is included
with the results. It is a test patch, not an enrollment implementation.

Iroh initially used a relay and then selected a direct path. Three runs with
the same uncached 50/50 model configuration delivered 79 tokens each at
11.09, 12.53 and 10.74 tokens/s (median 11.09), with median first-text latency
0.483 seconds. This does not demonstrate a speedup over SSH under these changing
network conditions. The model session's bridge log recorded 186,100,086 bytes
sent and 3,484,758 received, including loading and warmup. Both temporary bridge
processes were stopped after the test.

A separate diagnostic SSH run enabled per-command RPC timing. It recorded one
340 KiB activation read for prefill, then 78 reads of 10 KiB for generation.
Those generation reads waited about 4.37 seconds in total, against about 6.37
seconds of generation time. The median activation-read wait was about 43 ms.
This wait includes **remote computation and communication**; it is not a pure
network-latency measurement. The small payload per generated token suggests
latency is worth targeting before bulk link bandwidth. Diagnostic logging can
affect timing, so this run is excluded from the normal comparison table.

## Applied configuration

The existing local demo launcher now pins device order `RPC0,MTL0` and adds
`--cache-ram 0 --ctx-checkpoints 0`, retaining the original 50/50 split and SSH
transport. A copy of the previous launcher is saved beside it. The restored
server passed a health check and a fresh generation check. MTP is not enabled
by default because its throughput gain failed confirmation.

The next useful intervention is a lower and more stable latency link, such as
both machines on the same LAN or a wired link. Neither the placement sweep,
speculation tests, nor swapping tunnels establishes a reliable TPS improvement
on the present connection. They did establish a substantial first-text latency
improvement. Larger-model capacity and memory-budget tests remain separate.

## Reproduce

Stop the existing controller first: the RPC worker serves one controller at a
time. Keep the loopback-only worker and approved SSH tunnel running.

```sh
python3 experiments/model-validation/sweep.py \
  --llama-cpp /path/to/llama.cpp --model /path/to/model.gguf \
  --output-dir /path/to/new-results
```

The default sweep brackets placement tests with a baseline repeat. To compare
prediction against a matching uncached baseline:

```sh
python3 experiments/model-validation/sweep.py \
  --llama-cpp /path/to/llama.cpp --model /path/to/model.gguf \
  --output-dir /path/to/another-new-results --runs 3 --max-tokens 128 \
  --only no-snapshots-50-50 mtp-1 mtp-2 mtp-4
```

The script starts each controller, waits for loading, runs the benchmark, saves
the command and raw measurements, and shuts that controller down. Restore the
normal demo afterward. `--profile` enables RPC debug timing and is diagnostic;
its logging can affect performance and should not be mixed into timing results.

Raw measurements are under [results/tuning-2026-10-24](results/tuning-2026-10-24).
Summary command paths use `${DEMO_ROOT}` as a placeholder for the local demo
directory. No context reduction, smaller main model, different quantization,
or concurrent-request aggregate throughput was substituted for the target metric.
