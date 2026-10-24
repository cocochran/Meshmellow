# RPC experiments on the wireless connection

These tests keep the Qwen3.5-4B Q4_K_M model, 8192 context, 50/50 split,
temperature zero and both Macs. They change RPC writes, not model precision
or the network hardware. Requests generate the same 79-token answer.

## Small-frame packing

The stock client writes a command byte, length and payload separately; the
worker writes response length and payload separately. An opt-in private patch
combines these writes for payloads up to 64 KiB. Stream bytes, ordering and
response validation stay the same. Larger payloads retain the original path.

Three runs per configuration:

| Mode | Median generated tokens/s |
| --- | --- |
| Opening control | 11.27 |
| Client packing only | 11.67 |
| Worker reply packing only | 13.61 |
| Both sides | 11.39 |
| Closing control | 12.31 |

The control changed with time, so the worker-only result needed a better
comparison. A second test kept one model loaded and alternated reply packing
off/on in order `0,1,1,0,1,0,0,1`, with four observations per mode:

| Mode | Median tokens/s | Range |
| --- | --- | --- |
| Off | 13.47 | 12.61-13.55 |
| On | 13.77 | 12.68-14.20 |

That is a 2.2% median difference with overlapping ranges, not a demonstrated
large improvement. All eight outputs were identical. A dedicated SSH tunnel
was used for this paired test. An earlier attempt failed before inference
because its forwarding port was unavailable; it is excluded from timings.

The paired test uses a test-only local control file, checked by the worker,
to switch reply packing between requests without reloading the model. That
instrumentation is not a production configuration API.

## Batched one-way inputs

A second opt-in client patch groups queued one-way tensor uploads and graph
commands in a bounded 64 KiB buffer. It flushes before response reads, on
synchronous operations and backend barriers, and before large transfers.
It is limited to TCP; RDMA behavior is unchanged. The original worker reads
the same sequence of existing protocol frames. No new command or protocol
version is introduced.

Three runs per configuration:

| Mode | Median generated tokens/s |
| --- | --- |
| Opening control, batching off | 12.08 |
| Batched inputs | 12.19 |
| Closing control, batching off | 12.96 |

This did not establish a speedup. It does not remove the next-token dependency
or the required remote activation read. No batching or frame-packing experiment
is enabled in the normal demo.

## Correctness and reproducibility

The native `rpc_frame_smoke.cpp` checks numerical addition results through RPC
for 4, 4096, 65536 and 80000-byte tensors. Synchronous and asynchronous paths
pass with batching enabled against an unchanged worker. The first packing
sweep also passes the synchronous checks for old/new peer combinations.
These cover small frames, the response threshold, and larger fallback frames.

The original llama.cpp checkout, binaries and libraries are unchanged. A
separate `rpc-experiment` clone builds a library loaded through an isolated
`DYLD_LIBRARY_PATH` overlay. Base runtime commit:
`71255fefc092b0344f2acddb891388814ae961be`. The experimental patch is included
as an artifact here; no changes were submitted to upstream llama.cpp.

The first framing patch is saved separately. The final patch is a superset
containing the local test toggle and input batching. `GGML_RPC_COALESCE=1`
enables small-frame packing; `GGML_RPC_BATCH_WRITES=1` enables client input
batching. Both default off. The paired result records its worker library hash;
the input-batching summaries record their overlay hash. The initial framing
sweep did not capture a binary hash and should not be treated as a binary-exact
reproduction record.

Raw data and patches: [results/rpc-experiments](results/rpc-experiments).
The scripts are specific to the existing demo paths and worker setup; inspect
them before reuse. They temporarily stop controllers and restore the original
worker where applicable. The plain `sweep.py` stops its test controller but
requires restoring the normal demo afterward.

Both experiments preserve model output in the sampled runs. They provide no
general quality, larger-model fit, enforced memory budget or production-safety
claim. The next software profiling step is to measure remote compute separately
from network wait, rather than assuming the entire activation-read wait is
network overhead.

The patched variants were measured over SSH. The earlier successful Iroh run
used the original RPC runtime; no patched-Iroh throughput result is claimed.

## Zstd response compression (2026-10-24)

Real synthetic-prompt activations compressed by about 7% at zstd level 1.
Across 78 decode buffers of 10,240 bytes, median compressed size was 9,511
bytes and local compression plus decompression took about 25 microseconds.
All 316 sample/mode combinations reconstructed bit exactly. Raw tensors are
kept private; published capture timings include file I/O and are not throughput evidence.

The two-Mac experiment used four runs per mode in order 0,1,1,0,1,0,0,1,
with one warmup excluded and the same model loaded throughout. Median decode
throughput was **13.83 tok/s uncompressed vs 13.62 tok/s compressed** (-1.5%).
Activation response payload bytes fell from 4,587,520 to 4,257,720 (7.2%).
All eight generations produced identical text; all eight native sync/async
tensor checks passed. Compression showed no throughput benefit in this sample
and remains disabled in the normal demo. These byte totals exclude framing,
SSH and other protocol traffic. Neither latency nor remote compute is isolated.

The private prototype statically links zstd 1.5.7 and negotiates experimental
protocol major 9 when GGML_RPC_ZSTD=1, rejecting stock major-8 peers. It
compresses bounded responses only when saving more than 64 bytes, validates
frame size, and decompresses to the exact expected output size. The test uses
a separate loopback worker on port 50054. GGML_RPC_ZSTD_TEST_FILE toggles
compression while preserving protocol version. Earlier packing/batching features
in the full patch were disabled for this comparison. The patch is against
llama.cpp 71255fefc092b0344f2acddb891388814ae961be; SHA256:
`2ae09562f74320e04971ed6263a74b036eafc9b66a167e247dfea9a71b6fa984`.
This is an experimental private fork, not an upstream submission.

Scripts: `capture_activations.py`, `zstd_samples.py`, and `zstd_live.py`.
Evidence: [results/zstd-2026-10-24](results/zstd-2026-10-24).
