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
