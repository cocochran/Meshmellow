# History sync and storage

The `history_sync` module (`src/history_sync/`) keeps one conversation's history identical on every node in the mesh and saved on each node's disk. When the main process (the device that took the prompt) disappears, another node can take over from its own copy.

**Status:** wired into running nodes. `llm-cluster leader` saves every completed turn and replicates it to its workers over their control links, workers save their copy, and a worker started as a standby takes over when the leader is lost (see [Failover in a running cluster](#10-failover-in-a-running-cluster)). Takeover is a ranking, not consensus, and there is no restore-from-export command yet.

## Contents

1. [Module map](#1-module-map)
2. [How it works](#2-how-it-works)
3. [Core ideas: node IDs, epochs, events](#3-core-ideas-node-ids-epochs-events)
4. [Module reference](#4-module-reference)
5. [Storage format and crash safety](#5-storage-format-and-crash-safety)
6. [Using it from a node](#6-using-it-from-a-node)
7. [CLI and demo](#7-cli-and-demo)
8. [Tests](#8-tests)
9. [Limits and next steps](#9-limits-and-next-steps)
10. [Failover in a running cluster](#10-failover-in-a-running-cluster)

---

## 1. Module map

| File | Lines | Holds |
|---|---:|---|
| `src/history_sync.rs` | 16 | Module root; declares the submodules below |
| `history.rs` | 96 | `Entry` (job events), `History`, the `NodeId` / `Epoch` / `Seq` types |
| `message.rs` | 53 | `SyncMessage`, the wire messages between main process and workers |
| `leader.rs` | 223 | `Leader`: the main process's side of sync |
| `replica.rs` | 136 | `Replica`: a worker's side of sync, and promotion to leader |
| `node.rs` | 138 | `Node`: one peer's role (leader or replica), step-down, and disk hooks |
| `election.rs` | 54 | `Candidate`, `Elected`, `elect()`: choosing a new main process |
| `store.rs` | 622 | `HistoryStore`: saving to disk, loading, and `ConversationExport` |
| `sim.rs` | 256 | `Cluster`: an in-memory mesh for tests and the demo |

Outside the module: `tests/history_sync.rs` and `tests/history_store.rs` (tests), `examples/history_sync.rs` (demo), and `llm-cluster history …` in `src/main.rs` (CLI). It also relies on two pieces of the networking layer in `src/node.rs`: `NodeId` and `PeerTable`.

**Design rules used throughout**
- **No I/O inside the sync logic.** `Leader`, `Replica` and `Node` take a message and return the messages to send. The caller owns the network, which keeps the logic testable without Iroh.
- **Messages are safe to repeat.** Any message can be resent or duplicated without harm, so recovering from loss is just calling `sync()` again.
- **History only grows** (within one epoch). That's why "how many entries do you have" fully describes a worker's progress.
- **Epochs, not clocks, decide who's in charge** (architecture §9, §12.5).

## 2. How it works

### Normal operation

```text
Main process (epoch E)                         Worker
  start_job / complete_job ─► persist to disk
  ── Snapshot{E, all entries} ───────────────►  first contact this epoch: replace history,
                                               save to disk, then
  ◄────────────────────────── Ack{E, len} ───  reply
  (next job)
  ── Append{E, start = len, new entries} ────►  save to disk, then
  ◄────────────────────────── Ack{E, len'} ──  reply
```

Workers that are caught up get only new entries (`Append`). New workers, workers with a gap, and workers that just saw a failover get the whole history (`Snapshot`).

### Repairing a gap

```text
  ── Append{E, start = 9, …} ────────────────►  "I only have 6": can't apply
  ◄────────────────────────── Resync{E, 6} ──
  ── Snapshot{E, all entries} ───────────────►  replace, save, Ack
```

### Failover

1. The main process goes away; its peers drop it from their `PeerTable`.
2. Each surviving worker reports a `Candidate` (epoch, history length, connection time).
3. `elect()` ranks them: **newest history (the epoch its history was written under, then its length), then connected longest, then lowest ID**. It returns the winner and an epoch one above the highest any candidate has heard of.
4. The winner calls `Replica::promote`. The new leader records an `EpochAdvanced` event and makes every peer start with a `Snapshot`, so all copies converge on the winner's history.

### A stale main process comes back

```text
Old main (epoch 1) ── Append{1, …} ──► Worker (epoch 2): 1 < 2 → Reject{2}
Old main sees Reject{2} → steps down to a worker
New main adds it back → Snapshot{2} → its unsynced entries are discarded
```

### A node restarts

`Node::from_store` brings it back as a worker with its saved history and epoch. The main process's next `Append` hits `Resync` (a restarted worker always takes a fresh snapshot first), and it catches up.

## 3. Core ideas: node IDs, epochs, events

**`NodeId`** is the Iroh endpoint ID: a 32-byte public key, defined once as `crate::node::NodeId` and shared with the networking layer's `PeerTable`. A peer has the same ID everywhere.

**`Epoch`** is the main process's term number. It starts at 1 and goes up by one at every takeover. Every message carries the sender's epoch, and anything from an older epoch is refused (`Reject`). That's what stops a main process that lost contact, then came back, from overwriting the conversation. A new epoch also tells workers that their history may differ from the new main process's, so they take a full snapshot before accepting partial updates.

**`Seq`** is a history length, which is also the index of the next entry.

**Events.** History is a log of job events, not chat turns:

| Event | Meaning |
|---|---|
| `JobStarted { request_id, prompt }` | A prompt was accepted |
| `OutputCommitted { request_id, offset, text }` | Part of the response, starting at byte `offset` |
| `JobCompleted { request_id }` | The response is complete |
| `EpochAdvanced { epoch, leader }` | `leader` became the main process in `epoch`. Every session starts with one (epoch 1), and every takeover adds one |

`History::last_epoch()` is the newest `EpochAdvanced`: the epoch the history was written under. Elections compare it, not the highest epoch a node has heard of (see `election.rs`).

A job with `JobStarted` but no `JobCompleted` was **interrupted**. This is how recovery tells finished answers from cut-off ones. The model's KV cache is never copied; a new main process rebuilds it by re-reading (re-prefilling) the text.

## 4. Module reference

### `history.rs`

| Item | What it is | Why |
|---|---|---|
| `NodeId` | Re-export of `crate::node::NodeId` (`iroh::EndpointId`) | One ID type across networking and sync |
| `Epoch`, `Seq` | `u64` aliases | Name the meaning at every call site |
| `enum Entry` | The four events above; JSON-tagged by `"type"` | Interrupted jobs are visible; `request_id: String` matches `protocol::Prompt` |
| `Entry::request_id()` | The job an event belongs to, if any | Used by export and tests |
| `struct History` | Append-only `Vec<Entry>` | No remove or insert, so the history can only grow |
| `History::since(start)` | Entries from `start` on; clamps instead of panicking | A peer's reported length is untrusted |

**As the project evolves**
- Commit output in chunks while generating (today `complete_job` writes it in one go).
- Add `JobQueued` and placement events (architecture §9) once the job queue exists.
- Consider storing token IDs for exact replay.

### `message.rs`

| Variant | Direction | Meaning |
|---|---|---|
| `Snapshot { epoch, leader, entries }` | main → worker | Replace your history with this |
| `Append { epoch, leader, start, entries }` | main → worker | Entries from index `start` |
| `Ack { epoch, len }` | worker → main | I hold your first `len` entries (on disk, when using `handle_durable`) |
| `Resync { epoch, len }` | worker → main | I can't apply that; send a snapshot |
| `Reject { epoch }` | anyone → sender | Your epoch is old; the current one is `epoch` |

`to_bytes()` / `from_bytes()` encode JSON. On the network they ride as the opaque payload of Yash's `protocol::Message::History { conversation_id, payload }`. `type Outgoing = (NodeId, SyncMessage)` is a message plus its destination.

**As the project evolves**
- Add `cluster_id` and `prev_hash` (architecture §9, §14).
- Move to postcard to match the rest of the protocol.
- Large snapshots may need chunking under the 16 MiB frame limit.

### `leader.rs`: `Leader`

Holds the authoritative history and how far each peer has confirmed (`NeedsSnapshot` or `Acked(len)`, in a `BTreeMap` for deterministic order).

| Function | Purpose |
|---|---|
| `new(id, epoch, history, peers)` | Fresh session (epoch 1) or a promoted worker |
| `start_job(request_id, prompt)` | Records `JobStarted`, returns sync messages |
| `complete_job(request_id, response)` | Records `OutputCommitted` + `JobCompleted`, returns sync messages |
| `complete_task(request_id, prompt, response)` | Both at once (tests, simulator) |
| `append(entries)` | Adds entries without sending |
| `sync()` | What each lagging peer needs; call it again to retransmit |
| `handle(from, msg)` | Processes `Ack`, `Resync` and `Reject` (see below) |
| `set_peers(ids)` | Replaces the peer list, e.g. from `PeerTable`; known peers keep their progress |
| `add_peer` / `remove_peer` / `peers` / `up_to_date_peers` | Membership and status |
| `deposed_by()` | `Some(epoch)` once a newer main process has replaced this one |

`handle` rules:
- **`Ack`:** only ever raises a peer's count, capped at our own length, because acks can arrive out of order.
- **`Resync`:** gets an immediate snapshot.
- **`Reject` with a newer epoch:** marks the leader deposed, and it goes silent.
- **Unknown peers:** their messages are ignored; membership comes from the peer table.

**As the project evolves**
- Track which acks came from voters to support majority commit.
- Drive `set_peers` from the peer table on a live node. The peer table already drops a worker that misses heartbeats (M-11, section 9); calling `set_peers` when it changes waits on the M-9 live wiring.
- Own the job queue's state changes in this same log.

### `replica.rs`: `Replica`

| Function | Purpose |
|---|---|
| `new(id)` / `with_history(id, epoch, history)` | Empty worker, or one restored from disk or a stepped-down leader |
| `handle(msg)` | Applies a main-process message and returns the reply |
| `candidate(connected_at_unix_ms)` | This worker's election entry |
| `promote(new_epoch, peers)` | Becomes the main process; refuses an epoch that isn't newer. `Leader::new` records the `EpochAdvanced` |

`handle` rules, in order:
1. Older epoch → `Reject`.
2. `Snapshot` → replace the history, adopt the epoch, `Ack`.
3. `Append` that's from a new epoch, arrives before this epoch's snapshot, or leaves a gap → `Resync`. Otherwise skip the entries we already have and `Ack`.

The `in_sync_epoch` flag is what makes rule 3 safe after failover: the new main process's history may differ from ours.

**As the project evolves**
- With `prev_hash`, a worker whose history already matches could accept an append after failover instead of a full snapshot.
- Check that messages come from the expected leader's authenticated peer ID.
- `promote` should also trigger layer reassignment and reloading the job queue (architecture §9).

### `node.rs`: `Node`

`enum Node { Leader(Leader), Replica(Replica) }` is one peer's current role.

| Function | Purpose |
|---|---|
| `handle(from, msg)` | Routes to the role; a leader that sees a newer epoch steps down first and keeps its history |
| `handle_durable(from, msg, &mut store)` | Same, but saves to disk **before** returning replies, so an `Ack` only covers saved entries |
| `persist(&mut store)` | Saves new entries and a newer epoch; the main process calls it before sending |
| `from_store(id, &store)` | Restores a node after restart, as a worker |
| `set_peers(&PeerTable)` | On a leader, sync with exactly the table's peers |
| `id` / `epoch` / `history` / `as_leader_mut` | Accessors |

**As the project evolves**
- This is the natural home for the per-node control loop (§6).
- A `Node::promote` would make promotion symmetric with step-down (it currently lives on `Replica`).

### `election.rs`

`Candidate { id, epoch, history_epoch, len, connected_at_unix_ms }`, built by `Replica::candidate`. `epoch` is the highest epoch the node has heard of; `history_epoch` is `History::last_epoch()`. `elect(&[Candidate]) -> Option<Elected { id, epoch }>` ranks by `(history_epoch, len)`, then the **earliest** `connected_at_unix_ms`, then the lowest `id`. It returns an epoch one above the highest `epoch` or `history_epoch` among the candidates.

**Why the history's epoch, not the heard one:** a node can learn of a newer epoch (through a `Reject`, or an append it refused) without receiving any of that leader's entries. Ranking by what it heard would make its older history look current. This is Raft's "last log term" rule. The new epoch still has to be above everything anyone heard, so two leaders never share one.

**Why this order:** history first, so the winner never has fewer entries than a node that could have won instead. Its snapshot would erase them. Connection time comes second because device clocks can differ, so it only breaks ties. Every node that sees the same candidates picks the same winner.

**Limit:** this is a ranking, not a vote. If the network splits, each side could elect its own winner in the same epoch. Safe automatic failover needs a majority of at least 3 voters (architecture §9). Until then, 2-device demos use **manual takeover** (agreed with david-codex).

**As the project evolves**
- Replace with Raft-style voting once 3+ voters exist.
- Add reliability inputs (always-on desktop over a phone that sleeps; architecture §2).
- `connected_at_unix_ms` resets on reconnect, so a briefly dropped node loses its seniority.

### `store.rs`: `HistoryStore`

| Function | Purpose |
|---|---|
| `open(data_dir, StoreMeta)` | Opens a conversation, creating it if absent. An existing one must match the cluster ID and model hash (`StoreError::Mismatch`) |
| `load(data_dir, conversation_id)` | Opens an existing one only (`NotFound` otherwise) |
| `record(epoch, entries)` | Raises the epoch (if newer) and appends entries in one write, epoch line first; on disk when it returns |
| `append(entries)` / `set_epoch(epoch)` | `record` with only entries, or only an epoch. The epoch never goes down |
| `replace(history, epoch)` | Swaps in a whole history and its epoch in one atomic snapshot write, e.g. from a `Snapshot` |
| `export()` | Returns a `ConversationExport` |
| `history` / `epoch` / `len` / `meta` / `dir` | Accessors |

Supporting types:
- `StoreMeta`: `format_version`, `conversation_id`, `cluster_id`, `model`, `epoch`.
- `ModelManifest`: `model_hash`, plus a `generation` settings map.
- `ConversationExport` and `JobState`: see §5.
- `job_states(&History)`: rebuilds each job's state from the event log.
- `StoreError`: what can go wrong (bad I/O or JSON, corruption, unsafe ID, mismatch, not found, unsupported version).

Constants: `FORMAT_VERSION = 2` (on disk; version 1 kept the epoch in `meta.json` and is refused), `EXPORT_FORMAT_VERSION = 1`, `SNAPSHOT_EVERY = 100`, `DEFAULT_DATA_DIR = "meshthing-data"`.

**As the project evolves**
- Restore: start a main process from an export.
- Majority commit: carry that boundary in `committed_len`.
- Postcard instead of JSON, and chunked snapshots, for long conversations.
- Encryption at rest (architecture §12.3).

### `sim.rs`: `Cluster`

An in-memory mesh used by the tests and the demo. Every message goes through `to_bytes`/`from_bytes`, so the wire format is exercised. Membership lives in a real `PeerTable`, which drives the leader's peer list and election candidates. `node_id(i)` makes repeatable IDs.

Controls:
- Faults: `drop_next_to`, `disconnect`, `reconnect`.
- Running jobs: `complete_task`, `resync`, `run`.
- Recovery: `failover`, `rejoin`.
- Checking: `converged`, `leader_id`, `node`, `id`, `index`.

**As the project evolves**
- Keep it as the test harness after Iroh is wired in; it's faster and deterministic.
- Add seeded reordering and duplication to stress-test repeated messages.
- Give each node its own `PeerTable` and `HistoryStore` to test disk-backed failover end to end.

## 5. Storage format and crash safety

```text
<data-dir>/history/<conversation_id>/
├── meta.json       StoreMeta: identity only, written once at creation
├── snapshot.json   { generation, epoch, entries[] }: the first entries (replaced atomically)
└── log.jsonl       later changes, one per line: { generation, index, entry } or { generation, epoch }
```

- **The epoch lives with the history.** It's in `snapshot.json`, and changes after the snapshot are `{generation, epoch}` log lines. On load, the epoch is the highest of the snapshot's, the log's and the newest `EpochAdvanced` in the history. No crash can leave new history paired with an old epoch.
- **Writes** (`record`) are one write per call, epoch line first, flushed with `sync_data` before returning.
- **Atomic replacement:** write a temporary file, flush it, rename it over the old one, then flush the directory (Unix).
- **Compaction:** every 100 log entries, the log is folded into a new snapshot and truncated.
- **Conversation IDs** become folder names, so only 1–128 letters, digits, `-`, `_`, `.` are allowed (not `.` or `..`).

**Crash safety:** whatever moment a crash happens, loading gives back a prefix of the history:

| Crash during… | What load does |
|---|---|
| A log write | The last line has no newline: it's dropped and cut from the file. Because the epoch line comes first, any entries that survive come with their epoch |
| Compaction, after the snapshot but before the log is cut | Log lines with `index` below the snapshot length are skipped |
| `replace`, before the log is cut | Old lines carry the previous `generation` and are ignored |

A complete line that isn't valid JSON, or an index that skips ahead, is reported as corruption rather than silently dropped.

**`ConversationExport`** (versioned JSON for recovery tools):
- **Identity:** `format_version`, `conversation_id`, `cluster_id`.
- **Model:** `model` (`model_hash` + `generation` settings).
- **State:** `epoch`, and `committed_len`, the log boundary to resume from.
- **Contents:** `jobs[]` and `entries[]`.

Each `JobState` has `request_id`, `prompt`, `status` (`completed` / `interrupted`), `committed_output` and `committed_output_offset` (bytes). Output chunks are joined only while they line up end to end, so a repeated chunk or one after a gap is ignored.

**"Committed" means saved on this node's disk.** It doesn't yet mean a majority of nodes have it.

## 6. Using it from a node

Main process:

```rust
let mut store = HistoryStore::open(&data_dir, StoreMeta::new(conversation, cluster, model))?;
let mut node = Node::Leader(Leader::new(my_id, 1, store.history().clone(), peers.ids()));

let out = node.as_leader_mut().unwrap().start_job(&request_id, &prompt);
node.persist(&mut store)?;        // save first…
send_all(out);                    // …then tell the workers
// after generation:
let out = node.as_leader_mut().unwrap().complete_job(&request_id, &response);
node.persist(&mut store)?;
send_all(out);
```

Worker:

```rust
let mut store = HistoryStore::open(&data_dir, meta)?;
let mut node = Node::from_store(my_id, &store);
// for each History frame received from `from`:
let msg = SyncMessage::from_bytes(&payload)?;
for (to, reply) in node.handle_durable(from, msg, &mut store)? {
    send(to, reply);
}
```

On peer-table changes, the main process calls `node.set_peers(&peer_table)` and then sends `sync()`.

`src/replication.rs` does exactly this on live nodes: `LeaderReplication` on the leader (fed by the job API's `HistorySink`) and `ReplicaState` on each worker. See section 10.

## 7. CLI and demo

```text
llm-cluster history inspect <conversation> [--data-dir meshthing-data]   # summary; interrupted jobs flagged
llm-cluster history export  <conversation> [--data-dir meshthing-data]   # ConversationExport as JSON
cargo run --example history_sync -- 6                                    # simulated 6-node mesh
```

The demo runs three jobs with one dropped message and a retransmit. Then the main process drops out, a failover happens, one job runs under the new main process, and the old one returns, gets refused and resyncs. It prints each node's history length and whether all nodes match.

## 8. Tests

`tests/history_sync.rs` (15): sync, failover and election.
- **Sync:** any number of nodes from 1 to 50; deltas after the first snapshot; repeated appends are harmless; dropped messages recovered; a gap triggers a snapshot.
- **Failover:** continues from a worker's history (`EpochAdvanced` recorded); the most complete history wins; a stale main process is refused and resynced.
- **Peers:** a disconnected peer stops being synced and catches up on rejoin; the leader's peer list follows `PeerTable`.
- **Election and wire format:** the ranking is deterministic; longest-connected wins among equal histories; a newer history beats a longer, older one and the new epoch is above every epoch heard; every session records its epoch; messages survive a round trip.

`tests/history_store.rs` (16): storage and export.
- **Reload:** history and epoch survive a restart.
- **Epoch durability:** the epoch is stored with the history and `meta.json` is never rewritten; the epoch never goes down; a torn write keeps its epoch; the epoch is never below the newest `EpochAdvanced`; version-1 stores are refused.
- **Crashes:** torn last line dropped; corrupt complete line reported; compaction and the crash after it; `replace` and the crash during it.
- **Refusals:** a different model or cluster; unsafe or missing conversation IDs.
- **Export:** completed vs interrupted jobs, including chunk joining.
- **Sync on disk:** a worker acks only what's saved; a restarted worker reloads and catches up; the main process saves before sending.

Run them with `cargo test --test history_sync --test history_store`.

## 9. Limits and next steps

**Connection status** (following david-codex's review of `6114f93`):
- **Connection cleanup now uses a unique token.** `PeerTable::remove_connection` matches the token assigned during the handshake, so an old connection cannot remove a newer one even when both connect in the same millisecond. The wall-clock connection time remains only for diagnostics and election tie-breaking.
- **Connections clean up after themselves (M-21).** `dial_peer` and `accept_peer` return a `PeerSession` that holds the connection token. The peer is removed when the connection closes from either side (via a `WeakConnectionHandle` watch) or when the session is dropped, so callers never remove peers by hand.

In priority order:
1. **Done: control loop.** Workers apply `History` frames with `handle_durable`, the leader persists before sending and follows its `PeerTable` with `set_peers` every second (section 10).
2. **Restore from export:** start a main process from a `ConversationExport` for manual takeover (david-codex's M-16 test needs this).
3. **Reconcile with PR #10 (M-21):** heartbeat fields on the `NodeId`-keyed `PeerTable`; then hook up `HistorySink`.
4. **Failure detection (M-11):** partly done.
   - **Done:** the leader's control acceptor (`network::serve_control_with`) closes a worker's connection with close code `HEARTBEAT_TIMEOUT_CODE` (4, reason `heartbeat timeout`) when no heartbeat arrives within the timeout. `PeerSession`'s token-matched cleanup then removes it from the `PeerTable`, and the worker reconnects with backoff and rejoins with a fresh hello. `llm-cluster leader` enables this by default with `--heartbeat-timeout 7s` (three missed beats at the workers' 2s default, plus 1s slack; `0s` disables it), and the leader logs a warning when a planned worker leaves the peer table while serving, and again when it rejoins.
   - **Done:** leader-to-worker heartbeats. With `ControlOptions::heartbeat_interval` set, the acceptor heartbeats to each worker on its own uni stream (same `Heartbeat` message, epoch 0, sequence from 1 per connection). The worker watches them with `network::watch_heartbeats`, and when they stop it closes the link with code 4 and redials with backoff ("leader stopped heartbeating"), instead of waiting ~30s for QUIC's idle timeout. The worker's watchdog arms only after the first leader heartbeat on a link, so a worker connected to an older leader that never heartbeats is never timed out. Defaults: `leader --heartbeat-interval 2s` and `worker --leader-heartbeat-timeout 7s`; `0s` disables either.
   - **Done:** workers run `elect()` when the main process stays unreachable, and the winner takes over (section 10).
5. **Majority commit:** report a job as committed only once a majority of voters have acked it. Needs 3+ voters, and is also the path to safe automatic failover.
6. **Protocol fields:** `cluster_id`, `prev_hash`, and postcard encoding.
7. **Size and privacy:** output chunks while generating, chunked snapshots, encryption at rest (every node holds the full chat).

## 10. Failover in a running cluster

`src/replication.rs` runs history sync over the same control links the heartbeats use, and `src/worker.rs` runs the election.

**Replication.** `llm-cluster leader` keeps the conversation `--conversation` (default `main`) under `--data-dir` (default `meshthing-data`). Each completed job is recorded with `complete_task`, saved, then sent to the workers. Every second the leader also sends each connected worker a **roster** and any history it lacks. The roster lists the connected workers, which of them are standbys, and how far each has synced. Workers save their copy under their own `--data-dir`; `llm-cluster history inspect main` reads it on any machine.

**Standbys.** A worker started with `--takeover-manifest <manifest>` offers to take over. The manifest is a shard manifest written from that machine's point of view: its own local stages, and remote `shard-serve` stages that allow its node ID. A standby runs no `--rpc-server` (the two are refused together, because the standby leads on the same endpoint), so on a machine that already serves layers with `shard-serve` it runs next to it under its own key. Only the shard backend can take over, because a llama plan is tied to the original leader's devices.

```sh
llm-cluster --key standby.key worker --leader <leader node id> \
  --takeover-manifest standby-manifest.json --takeover-api 127.0.0.1:8080
```

**Election.** When a worker's link to the leader drops (closed, or 7 s without a leader heartbeat), it keeps redialling. Once the leader has been unreachable for `--failover-after` (default 10 s), it runs `elect()` over the last roster's standbys, leaving out the lost leader. Every worker ranks the same roster, so they agree:
- **The winner** stops being a worker and starts the leader on its own endpoint, so its node ID doesn't change. It accepts control connections, but does nothing else until a **majority** has joined it. The cluster is the lost leader plus every worker in the roster, the winner counts itself, and a majority is more than half; with a leader and two workers, one follower must join. Then it records the new epoch, starts the shard backend from its takeover manifest and its copy of the conversation, and serves the job API on `--takeover-api`.
- **Without a majority** within 30 s, the winner stands down. It hasn't written anything, and it goes back to being a worker.
- **Tiebreak.** Once an election has failed (its winner never came up, or stood down), every later one picks the reachable standby with the **lowest node ID** instead of ranking by sync progress, until a leader takes over and sends its own roster. Split votes come from rosters disagreeing on sync progress; node IDs are the same in every copy, so the next round converges. Only replacements a worker couldn't reach are left out. A winner that stood down is alive and stays eligible, so two of them don't end up each following the other.
- **Everyone else** redials the winner. Its first message is a snapshot in the new epoch, so every copy converges on the winner's history.
- **If the winner never comes up** within another `--failover-after`, workers leave it out and elect again. With no standby left, they go back to redialling the original leader.

**Why a majority.** Workers can disagree about the winner when their last rosters differ, and a standby cut off from a leader that is still running sees it as lost. The majority rule turns both into a safe outcome: two nodes can't each collect more than half of the same cluster, and a cut-off standby can't collect any, so at most one leader ever serves.

**Limits.**
- **A split vote costs time.** The tiebreak settles it in one more round, about a failover window plus the 30 s quorum wait. It can still stall if workers disagree on who is in the roster at all (not just on sync progress), or if a cut-off standby keeps retrying; neither ever lets two leaders serve. A real voting round (or Raft) would close this.
- **The tiebreak may pick a shorter history.** Lowest node ID ignores sync progress, so after a split vote the new leader may lack the last turn some workers had.
- **A leader and one worker can't fail over.** The worker alone is half of a two-node cluster, not a majority. Automatic failover needs a leader and at least two workers.
- **Histories can still differ.** A majority must *join* the new leader, but the history it serves is the winner's own copy. Turns only some workers had acknowledged can be lost; majority commit (item 5 above) would fix that.
- **Interrupted jobs are lost.** Only completed jobs are recorded, so a job in flight when the leader died isn't in anyone's history, and its client must resend it.
- **The old leader must rejoin as a worker.** Restart it as `worker --leader <new leader id>`, not as `leader`: it would start one epoch above its own stored one, which may tie the new leader's.
- **The election is untested on real hardware.** `tests/failover.rs` covers it end to end on loopback.
