# Mesh request ingress and recovery

User requirement, clarified 2026-10-24. This describes intended behavior, not current prototype capabilities.

Every node exposes an OpenAI-compatible API. A request can arrive at any node. Synchronize the request across participating nodes before starting execution at layer 0. The API ingress node need not host layer 0. Intermediate activations travel directly between assigned execution peers.

Replicated request state should let surviving nodes replan and restart inference at layer 0 after a device fails. This does not imply replication of KV caches, preservation of recurrent state, or transparent continuation of an interrupted token stream.

Protocol decisions still required:

- Stable request IDs and deduplication across ingress nodes.
- The participating membership and acknowledgment policy defining synchronization completion.
- Executor ownership and execution epochs so retries cannot create competing runs.
- Replication of prompt, generation settings, model identity, and sufficient recovery metadata.
- Client stream behavior when output was already delivered before failure; replay may produce different output and must not silently duplicate tokens.
- Separation of the API connection lifetime from the replicated job lifetime. Loss of the ingress node can still break its client connection unless reconnection/resumption is explicitly supported.

The planner should choose eligible devices, layer ranges, and peer order using memory, compute, and measured directed communication costs. Replicating request metadata must not turn activation forwarding back into a hub topology.

## Available memory and admission

Each node reports current usable RAM and GPU VRAM, active-job reservations, measurement age, and whether both belong to a shared unified-memory pool. Count a shared pool once. Placement includes weights, KV/recurrent state, scratch buffers, and operating headroom. The destination must confirm and reserve capacity before launch; stale discovery data is insufficient. Periodic reports and job start/end updates feed replanning between jobs. This capacity protocol is an intended requirement, not implemented by the experimental ring.
