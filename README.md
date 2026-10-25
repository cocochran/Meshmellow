# Layercake
 
A peer-to-peer mesh VPN client that splits a large language model's layers across the devices in a mesh, so a group of ordinary machines can run a model none of them could hold alone. The P2P layer is built on [Iroh](https://iroh.computer), which handles peer discovery, NAT traversal, and encrypted QUIC connections.
 
## Downloads

GitHub Actions builds Windows, Linux, Apple Silicon macOS, and Intel macOS
terminal executables that open the web dashboard in your browser. See [download instructions](docs/downloads.md)
for artifact locations, platform requirements, and what each package contains.

## Applications and core

Application source is separate from the shared Rust core:

- [`apps/android`](apps/android/README.md): native Android client.
- [`apps/ios`](apps/ios/README.md): native iPhone and iPad client.
- [`apps/web`](apps/web/README.md): SvelteKit browser dashboard.
- [`apps/desktop`](apps/desktop/README.md): desktop terminal launcher and packaging.
- [`apps/enrollment`](apps/enrollment/README.md): C/JNI bindings to core membership.
- `src/`: shared networking, membership, planning and execution core.

See [application capabilities and build instructions](apps/README.md). Mobile
clients support enrollment and dashboard views; native inference is still pending.

## How it works
 
### Layer distribution
When peers join the mesh, the model's layers are partitioned among them based on available memory and compute. Each peer loads only its assigned slice. During inference, activations flow peer-to-peer through the mesh in layer order, and the final output returns to the device that issued the prompt.
 
### Processes
The **prompted device** (the one where the user enters a prompt) runs two processes:
 
- **Main process** – accepts the prompt, coordinates the pipeline across peers, tracks task state, and assembles the response.
- **Worker process** – runs the layers assigned to this device, like every other peer.
**Non-prompted devices** run only a worker process. They receive the prompt and the activations they need, compute their layers, and pass results along.
 
### History sync and failover
After every completed task, the main process sends the full conversation history to the workers on all non-prompted devices. Every peer therefore holds an up-to-date copy of the session.
 
If the prompted device drops out (crash, sleep, network loss):
 
1. Workers detect the lost connection to the main process.
2. One non-prompted device is chosen to take over.
3. That device starts a new main process, seeded from its copy of the history.
4. Layers held by the departed device are reassigned to the remaining peers, and the session continues.
Because history is synced after each task, at most the in-progress task is lost and can be re-run.
 
## Architecture
 
```
            Prompted device                     Non-prompted devices
  ┌──────────────────────────────┐        ┌─────────────────────────┐
  │  Main process  ──coordinates──┼──────▶ │  Worker (layers k..m)   │
  │  Worker (layers 0..k)        │◀──────▶│  + history replica      │
  └──────────────────────────────┘        └─────────────────────────┘
                 ▲          Iroh QUIC mesh            │
                 └─────────── activations ────────────┘
```
 
## Open questions
 
- **Leader election** – how the replacement main process is chosen (lowest node ID, most free memory, a consensus round, etc.) and how split-brain is avoided if the original device comes back.
- **Layer rebalancing** – whether peers keep spare copies of neighboring layers to make failover faster.
- **History size** – full history on every peer is simple but grows over long sessions; diffs or snapshots may be needed.
- **Security** – activations and history are visible to every peer, so the mesh should only include trusted devices.
## Status
 
Early development.

The leader can also run jobs on david-codex's shard runner (`leader --backend shard`); see [shard backend](docs/shard-backend.md).

## Browser client

For opt-in step and stage timings while debugging a run, use `llm-cluster
--debug ...`; see [runtime diagnostics](docs/runtime-diagnostics.md).

When the leader is running, open `http://127.0.0.1:8080` for the built-in chat
client. It streams replies from the leader and shows recent queued, running,
completed, failed, and canceled jobs. The client is served by the leader itself;
there is no separate frontend server or dependency install.

Start the leader with the plan and profile you generated, for example:

```sh
cargo run -- leader --plan plan.json --profile profile.json --llama-server /path/to/llama-server
```

Before anything starts, the leader validates the plan against the profile
(`planning::validate_output`) and builds the worker bridges from the plan's
`runtime_devices`. Plans saved before `runtime_devices` existed are rejected;
regenerate them with `llm-cluster plan`. Useful `leader` flags:

- `--min-peers <N>`: contributors the plan must use (default 2).
- `--llama-arg <ARG>`: extra llama-server flag, repeatable, appended after the
  plan's arguments (for example `--llama-arg=--cache-ram --llama-arg=0`).
- `--evidence-file <PATH>` and `--evidence-wait <DURATION>`: after startup, wait
  for externally collected `RunEvidence` JSON and check it with
  `planning::verify_output`. If verification fails, the leader shuts down. The
  leader never generates evidence itself; without this flag, post-run
  verification is skipped.

For the test suite, build the fake runtime helper executables before running
integration tests:

```sh
cargo build --examples
cargo test --all-targets
```

The API, worker and leader tests launch these helper programs. `cargo test`
alone builds their test harnesses but does not provide the standalone helper
executables on a fresh checkout.

The compute placement library selects contiguous model blocks using explicit
contribution budgets and measured compute/link costs. It can require multiple
contributors, produce advisory llama.cpp RPC arguments, and validate collected
runtime evidence against the selected peers and budgets. See
[compute placement](docs/compute-placement.md) and the
[live validation plan](docs/live-validation-plan.md).

Run `cargo run --example placement` for a synthetic plan. Live profile collection,
launch supervision and memory-cap enforcement still require integration.
