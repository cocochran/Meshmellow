# History sync — Design Documentation

Prototype of conversation-history synchronization for MeshThing. This file documents every structure and function in the `history_sync` module of `llm-cluster` (`src/history_sync/`): what it does, **why it's built that way**, and — in each **"As the project evolves"** subsection — what will likely need to change.

> **Status:** prototype on the `history-sync` branch, merged into the main `llm-cluster` crate as the `history_sync` module (`src/history_sync/`). History lives in memory only; nothing is persisted to disk yet.

## Contents

1. [Overview](#1-overview)
2. [Protocol walkthrough](#2-protocol-walkthrough)
3. [`history.rs` — core types](#3-historyrs--core-types)
4. [`message.rs` — wire messages](#4-messagers--wire-messages)
5. [`leader.rs` — main-process side](#5-leaderrs--main-process-side)
6. [`replica.rs` — worker side](#6-replicars--worker-side)
7. [`election.rs` — failover choice](#7-electionrs--failover-choice)
8. [`node.rs` — role switching](#8-noders--role-switching)
9. [`sim.rs` — in-memory cluster](#9-simrs--in-memory-cluster)
10. [`examples/history_sync.rs` — demo binary](#10-exampleshistory_syncrs--demo-binary)
11. [Tests](#11-tests)
12. [Cross-cutting future work](#12-cross-cutting-future-work)

---

## 1. Overview

The README requires that after every completed task the main process sends the conversation history to every worker, so any peer can take over if the prompted device drops out. These modules implement that for **any number of nodes**:

- One **`Leader`** (the main process) owns the authoritative history.
- Every other peer runs a **`Replica`** holding a copy.
- Peers that are caught up get a **delta** (`Append`); new, lagging, or confused peers get a full **`Snapshot`**.
- Every leader has an **epoch** (term number). A newer epoch always wins, so an old main process that comes back can't overwrite the session.

### Design principles

| Principle | Why |
|---|---|
| **Transport-agnostic** | The P2P layer (Iroh, owned by Yash) wasn't built yet. Functions take messages in and return messages out; the caller decides how to send them. This also makes everything testable without a network. |
| **Pure state machines, no I/O, no threads, no clocks** | Deterministic and easy to test. Timing (retries, heartbeats) belongs to the runtime that drives these objects. |
| **Idempotent messages** | Networks drop and duplicate packets. Resending any message must be harmless, so recovery is just "call `sync()` again." |
| **Append-only history** | Conversation turns are never edited, so "how many entries do you have?" fully describes a replica's progress *within one epoch*. |
| **Epoch fencing** | Required by the architecture doc (§2, §9, §12.5): reject commands from stale leaders, and never rely on wall-clock time for ordering. |

### Module map

```text
history.rs   Entry, Role, History, NodeId/Epoch/Seq aliases
message.rs   SyncMessage (wire format), Outgoing
leader.rs    Leader — main-process sync logic
replica.rs   Replica — worker sync logic + promote()
election.rs  Candidate, Elected, elect()
node.rs      Node — Leader|Replica with automatic step-down
sim.rs       Cluster — in-memory network for tests and the demo
examples/history_sync.rs  Demo binary
tests/history_sync.rs     Integration tests
```

---

## 2. Protocol walkthrough

```text
Leader (epoch E)                         Replica
     |                                       |
     |-- Snapshot{E, entries[0..n]} -------->|  first contact in epoch E
     |<------------------ Ack{E, len=n} -----|
     |                                       |
     |   (task completes: 2 new entries)     |
     |-- Append{E, start=n, entries} ------->|  delta only
     |<------------------ Ack{E, len=n+2} ---|
     |                                       |
     |-- Append{E, start=n+4, ...} --------->|  gap! replica only has n+2
     |<------------------ Resync{E, len} ----|
     |-- Snapshot{E, entries[0..]} --------->|
     |<------------------ Ack{E, len} -------|

Stale leader (epoch E-1) --- Append ---> Replica (epoch E)
                         <-- Reject{E} --   stale leader steps down
```

**Failover:** survivors build `Candidate`s, `elect()` picks one, and the winner calls `Replica::promote(new_epoch, peers)`. The new leader starts with every peer marked as needing a snapshot, so all replicas converge on *its* history even if theirs had diverged.

---

## 3. `history.rs` — core types

### Type aliases: `NodeId`, `Epoch`, `Seq`

```rust
pub use crate::node::NodeId; // = iroh::EndpointId
pub type Epoch  = u64;
pub type Seq    = u64;
```

- **`NodeId`** identifies a peer.
- **`Epoch`** is the leadership term, bumped on every failover.
- **`Seq`** is a history length, which is also the index of the next entry.

**Why:** `NodeId` is the Iroh endpoint ID (a 32-byte public key), defined once in `src/node.rs` and shared with the `PeerTable`, so a peer has the same ID in networking and in history sync. It's `Copy`, ordered (needed for deterministic tie-breaking) and serializable. `Epoch` and `Seq` stay `u64` aliases that document intent at every call site. `Seq` doubles as "length" and "next index" because the history is append-only, so one number fully describes a replica's progress.

#### As the project evolves
- The wire `Hello.node_id` is still a `String`; peers are recorded under the connection's authenticated ID, not the string.
- Consider **newtypes** (`struct Epoch(u64)`) once the crate stabilises, so an epoch can't be passed where a length is expected.
- The architecture doc's event log uses `(term, index)` — that's `(Epoch, Seq)` under different names. Keep them aligned if the shared protocol crate adopts that naming.

### `enum Role { System, User, Assistant }`

Who authored an entry. **Why:** These are the standard chat-template roles the LLM runtime (llama.cpp) expects when rebuilding the prompt, so a new leader can re-prefill straight from history.

#### As the project evolves
- May need `Tool` or other roles if the chat template grows.
- If entries become general events (see `Entry`), `Role` moves inside a `ChatTurn` event variant rather than being on every entry.

### `struct Entry { task_id, role, content }`

One message in the conversation.

| Function | Purpose |
|---|---|
| `Entry::new(task_id, role, content)` | Convenience constructor; `content: impl Into<String>` accepts `&str` or `String`. |

**Why:** `task_id` groups the prompt and response of one job, so recovery can tell which task an entry belongs to and re-run an interrupted one. `content` is plain text because that's what's needed to rebuild the prompt; the architecture doc says the KV cache is rebuilt by re-prefilling, not replicated.

#### As the project evolves
- **Generalize to the event log in architecture §9:** `JobQueued`, `JobStarted`, `ResponseChunkCommitted{request_id, offset, token_ids}`, `JobCompleted`, `PlacementCommitted`, `LeaderEpochAdvanced`, … Likely `enum Event` replaces `Entry`; the replication logic doesn't care what an entry contains.
- Add **`conversation_id`** — Yash's P2P `History` message is keyed by it, and one mesh may host several conversations.
- **`task_id: u64` → request UUID** to match the doc's job schema (`request_id`).
- Storing **token IDs** as well as text would allow exact replay and deduplication of streamed chunks.

### `struct History`

Append-only list of entries; the LLM context.

| Function | Purpose / why |
|---|---|
| `new()` | Empty history. |
| `from_entries(Vec<Entry>)` | Used when applying a snapshot. Takes ownership, so no copying. |
| `len() -> Seq` | Progress marker for acks and appends. |
| `is_empty()` | Clippy convention alongside `len`. |
| `entries() -> &[Entry]` | Read-only view; callers can't break append-only by editing. |
| `since(start) -> &[Entry]` | The delta for an `Append`. **Clamps** `start` to the end instead of panicking, because a peer's reported length is untrusted input. |
| `extend(entries)` | The only mutation, so the history can only grow. |

**Why a wrapper instead of a bare `Vec<Entry>`:** It enforces append-only at the type level (no `remove`, no `insert`), and gives one place to later add hashing, persistence, or compaction without touching the protocol code.

#### As the project evolves
- **Persistence (the "history storage" task):** back `History` with an on-disk append-only log (e.g. one length-prefixed record per entry) plus periodic snapshots, reloaded at startup. The architecture checklist requires conversation state to survive a leader restart.
- **Hash chain:** store a running hash so `Append` can carry `prev_hash` (architecture §14 `STATE_APPEND`) and replicas can detect divergence directly, rather than relying only on epochs.
- **Size:** full history in memory grows forever (README open question). Options: snapshot + truncate, keep only the last *N* turns in memory, or page from disk.
- Encryption at rest (architecture §12.3): every node holds the full chat.

---

## 4. `message.rs` — wire messages

### `enum SyncMessage`

| Variant | Direction | Meaning |
|---|---|---|
| `Snapshot { epoch, leader, entries }` | Leader → replica | Replace your history with this. |
| `Append { epoch, leader, start, entries }` | Leader → replica | Entries starting at index `start`. |
| `Ack { epoch, len }` | Replica → leader | I hold the first `len` entries of your history. |
| `Resync { epoch, len }` | Replica → leader | I can't apply your append; send a snapshot. |
| `Reject { epoch }` | Anyone → sender | Your epoch is stale; the current one is `epoch`. |

**Why these five:**
- **Snapshot + Append** is the minimum for being both correct and efficient: deltas in the common case, full state whenever there's doubt. Snapshot is the universal repair path, so no complex reconciliation logic is needed.
- **`Append.start`** makes appends idempotent and lets a replica detect gaps.
- **`Ack` carries a length, not a message ID**, so later acks supersede earlier ones and the order they arrive in doesn't matter.
- **`Resync` instead of the leader guessing** — only the replica knows whether it can apply an append.
- **`Reject`** is the fencing mechanism: it's how a stale leader learns it has been replaced.
- **Every variant carries `epoch`**, so any message can be checked for staleness before anything else.
- **`leader` is included** so a replica knows who its current leader is, which is useful for routing and diagnostics.

| Function | Purpose / why |
|---|---|
| `epoch()` | Reads the epoch from any variant; used for the stale check that runs before any other handling. |
| `to_bytes()` | JSON-encodes the message. `expect` is safe because these types always serialize. |
| `from_bytes(&[u8])` | Decodes; returns `Result` because network bytes are untrusted. |

**Why JSON:** Human-readable while debugging the prototype, and `serde` makes switching formats a one-line change.

#### As the project evolves
- **Switch to postcard.** Yash's P2P control stream uses a u32 big-endian length prefix plus postcard-encoded messages, with a `History { conversation_id, payload: Vec<u8> }` variant whose payload is opaque. For now `to_bytes()` output can go straight into `payload`; later use postcard everywhere for consistency and size.
- **Add `cluster_id`** to every variant (architecture §9: "include `cluster_id`, `epoch` and `placement_version` in all control messages").
- **Add `prev_hash` to `Append`** (architecture §14 `STATE_APPEND {term, index, prev_hash, event}`).
- **Protocol version and size limits:** a huge snapshot shouldn't exceed frame limits. May need chunked snapshots, or a dedicated state stream/ALPN (architecture §6 suggests a separate `state` stream; Yash's bot is open to proposing one).
- **Authentication:** the message itself doesn't prove who sent it. That must come from the Iroh connection's authenticated peer ID plus the allowlist, not from the `leader` field.

### `type Outgoing = (NodeId, SyncMessage)`

A message plus the peer it should go to.

**Why:** Lets every state-machine function return "what to send" without owning a network handle (see the "transport-agnostic" principle in §1).

#### As the project evolves
May become a struct with a priority or an "expects reply" flag if the runtime needs to schedule retries per message.

---

## 5. `leader.rs` — main-process side

### `enum PeerState` (private)

- `NeedsSnapshot` — the peer hasn't confirmed a snapshot in this epoch.
- `Acked(Seq)` — the peer has confirmed this many entries.

**Why only two states:** That's all the leader needs to pick the next message: snapshot, append from *n*, or nothing. New peers, peers after failover, and peers that asked to resync all collapse into `NeedsSnapshot`.

#### As the project evolves
Add per-peer `last_sent_at` for retry timers, in-flight tracking so one large snapshot isn't resent every round, and voter eligibility for quorum commit.

### `struct Leader`

Fields: `id`, `epoch`, `history`, `peers: BTreeMap<NodeId, PeerState>`, `deposed_by: Option<Epoch>`.

**Why:**
- **`BTreeMap`** gives deterministic iteration order, so tests and the demo are reproducible and output is stable. It's also a cheap way to handle any number of peers.
- **`deposed_by` instead of self-destructing:** A `Leader` can't turn itself into a `Replica` from inside `&mut self`, so it records that it was replaced and goes silent (`sync`/`handle` return nothing). `Node` performs the actual conversion.

| Function | Purpose | Why it's implemented this way |
|---|---|---|
| `new(id, epoch, history, peers)` | Creates the main process. | Takes an existing `History` so the same constructor serves a fresh session (epoch 1) and a promotion after failover. Peers go through `add_peer`, which filters out self. |
| `id()`, `epoch()`, `history()` | Accessors. | Fields stay private so invariants can't be broken from outside. |
| `into_history(self)` | Gives up ownership of the history. | Used by `Node::step_down` to move the history into a `Replica` without cloning. |
| `deposed_by()` | `Some(epoch)` if replaced. | Lets callers and `Node` detect step-down. |
| `peers()` | Iterator over peer IDs. | Read-only membership view. |
| `add_peer(id)` | Starts tracking a peer as `NeedsSnapshot`. | Re-adding resets the peer to a snapshot, which is the safe choice for a rejoining node with unknown state. Ignores its own ID. |
| `remove_peer(id)` | Stops syncing to a peer. | Called by membership/heartbeat when a node leaves. |
| `up_to_date_peers()` | Peers holding the full current history. | The basis for a future commit/quorum check and status display. |
| `append(entries)` | Adds entries without sending. | Separated from sending so several entries can be batched, and so it's testable. |
| `complete_task(task_id, prompt, response)` | Appends user + assistant entries and returns sync messages. | Implements the README rule "after every completed task, send history to all workers" in one call. |
| `sync()` | Messages that bring every lagging peer up to date. | **The single retransmit mechanism:** it's stateless with respect to what was previously sent, so calling it again after packet loss resends exactly what's missing. Returns nothing once deposed. |
| `message_for(state)` (private) | Picks snapshot, append from *n*, or nothing. | Keeps the decision in one place. |
| `snapshot()` (private) | Builds a full-history `Snapshot`. | Shared by `sync` and `Resync` handling. |
| `handle(from, msg)` | Processes replica replies. | See the rules below. |

**`handle` rules and why:**
- `Reject` with a higher epoch → mark deposed. *We've been replaced; stop issuing commands (fencing).*
- `Ack` for this epoch → raise that peer's acked length, **never lower it**, and cap it at our own length. *Acks can arrive out of order, and a peer claiming more than we have must not cause out-of-range appends.*
- `Resync` for this epoch → mark the peer `NeedsSnapshot` and reply with a snapshot immediately. *Fast repair; no extra round trip.*
- Any other message from a lower epoch → reply `Reject`. *Tells lagging nodes the current epoch.*
- Messages for other epochs, or from unknown peers → ignore. *Unknown peers must be added through membership, not by sending messages.*

#### As the project evolves
- **Quorum commit (open decision B):** use the ack table to compute a commit index (the highest `Seq` acked by a majority of voters), and only report a task as durably saved once it's committed. Needed for the "strict no-loss" policy in architecture §9.
- **Snapshot cost:** after failover, every peer gets a full snapshot. Once `prev_hash` exists, a replica whose prefix matches could get an append instead.
- **Streaming chunks:** `complete_task` only syncs at task end. The architecture doc suggests committing response chunks during generation so less is lost on failure.
- **Membership:** `Leader::set_peers` (or `Node::set_peers(&PeerTable)`) replaces the peer list from the `PeerTable`, keeping existing peers' progress. Incoming connections leave the table when they close; dialed connections must call `network::forget_peer` themselves. Nothing calls `set_peers` on a live node yet, because there's no control loop.
- **Job queue:** the leader is also meant to own the FIFO job queue (§10); its state changes would ride this same log.

---

## 6. `replica.rs` — worker side

### `struct Replica`

Fields: `id`, `epoch`, `leader: Option<NodeId>`, `history`, `in_sync_epoch: bool`.

**Why `in_sync_epoch`:** After a failover, the new leader's history may differ from ours (we may hold an entry it never received). Appending its delta onto our copy could silently mix two histories. So a replica refuses appends until it has applied a **snapshot** from the current epoch's leader. This single flag is what makes failover safe without hash chains.

| Function | Purpose | Why it's implemented this way |
|---|---|---|
| `new(id)` | Empty replica, epoch 0. | Epoch 0 is below any real leader (leaders start at 1), so the first leader is always accepted. |
| `with_history(id, epoch, history)` | Replica that keeps existing history. | Used when a deposed leader steps down; its history is kept until a snapshot replaces it, and will also be needed for loading from disk. |
| `id()`, `epoch()`, `leader()`, `history()`, `len()`, `is_empty()` | Accessors. | `epoch` and `len` feed `Candidate` for elections. |
| `handle(msg) -> Option<SyncMessage>` | Applies a leader message and returns the reply. | See the rules below. |
| `ack()` (private) | Builds `Ack{epoch, len}`. | Shared reply after any successful apply. |
| `promote(self, new_epoch, peers) -> Leader` | Becomes the main process after failover. | **Consumes** the replica so the old role can't still be in use. Asserts that `new_epoch` is newer, because reusing an epoch would break fencing. The history moves into the leader without copying. |

**`handle` rules and why:**
1. **Message from an older epoch → `Reject{our epoch}`.** Checked first, before anything else (fencing). Rejects aren't answered, to avoid reject ping-pong.
2. **`Snapshot` (same or newer epoch) → replace history, adopt epoch and leader, set `in_sync_epoch`, `Ack`.** The universal repair path.
3. **`Append`:**
   - newer epoch, not yet snapshotted this epoch, or `start > len` (a gap) → `Resync`, and adopt the epoch. *Can't apply it safely; ask for the full state.*
   - otherwise → skip the entries we already have (`len - start`), append the rest, `Ack`. *Makes duplicates and overlapping retransmits harmless.*
4. **`Reject` with a newer epoch → adopt it and clear `in_sync_epoch`.** We learned the world moved on.
5. **`Ack` / `Resync` → ignore.** Those are replica-to-leader messages.

#### As the project evolves
- **Persistence:** apply each change to disk before sending `Ack`, so an ack means durably stored. Without that, a quorum commit means nothing.
- **Hash check:** with `prev_hash`, a replica can verify an append matches its prefix and skip unnecessary snapshots after failover.
- **Leader identity check:** verify that messages actually come from the `leader` field's peer, using the authenticated Iroh peer ID.
- `promote` currently copies the candidate list as peers; later it should take membership from the shared cluster state, and should also trigger layer reassignment and the job queue reload (architecture §9 failure scenario).

---

## 7. `election.rs` — failover choice

### `struct Candidate { id, epoch, len, connected_at_unix_ms }`

What a surviving peer reports when the main process is lost: its epoch, history length, and when it last connected (from `PeerInfo.connected_at_unix_ms` in `src/node.rs`).

**Why these fields:** The replacement should lose as little history as possible, so the highest epoch plus the longest history identifies the most up-to-date copy. Among equally up-to-date peers, the one connected longest is preferred as the likeliest to stay online.

### `struct Elected { id, epoch }`

The winner and the **new** epoch it must lead with.

**Why it returns an epoch:** The election decides the next term (`highest epoch seen + 1`). That's what makes the new leader's messages beat the old leader's.

### `fn elect(candidates) -> Option<Elected>`

Picks the candidate with the highest `(epoch, len)`, then the earliest `connected_at_unix_ms` (connected longest), then the lowest `NodeId`, and returns `epoch = max_epoch + 1`. Returns `None` if there are no candidates.

**Why:**
- **Deterministic:** every survivor that sees the same candidate list picks the same winner, without another round of messages.
- **Most-complete history first:** minimizes lost conversation.
- **Connection time second, not first:** a long-connected peer with a shorter history would overwrite newer entries on every other node. And because it's a wall-clock reading that can differ between devices, it only breaks ties.
- **Lowest ID last:** an arbitrary but stable rule.

**Known limitation (documented in code):** this is a *preference*, not consensus. If survivors see different candidate lists (a network partition), two winners are possible. The architecture doc (§9) says automatic failover is only safe with a majority vote of at least 3 voting nodes, and that 2-node demos should use **manual takeover**. David's bot confirmed its tests will use manual takeover.

#### As the project evolves
- **Real connection times:** `Candidate.connected_at_unix_ms` is filled from the simulator's `PeerTable` (a logical clock). A live node uses `Replica::candidate(connected_at)` with the value from its `PeerTable` entry (and its own start time) when building candidates. Note that a reconnect resets it, so a briefly dropped node loses its seniority.
- **Reliability ranking (architecture §2):** prefer always-on desktops over phones that may sleep. Add device class, power source, or uptime to `Candidate`.
- **Real consensus:** replace with Raft-style voting (terms, majority votes, explicit voter set) once 3+ voters exist. The `Candidate` fields map onto Raft's "last log term / index" rule for vote eligibility.
- **Manual takeover mode** for 2 nodes: a function that promotes a user-chosen node, still bumping the epoch.

---

## 8. `node.rs` — role switching

### `enum Node { Leader(Leader), Replica(Replica) }`

A peer's current role.

| Function | Purpose | Why it's implemented this way |
|---|---|---|
| `id()`, `history()` | Same accessors for either role. | Callers (the simulator, a future runtime) don't need to match on the role. |
| `as_leader_mut()` | `Some(&mut Leader)` if this node is the leader. | Used to run tasks on the leader. |
| `handle(from, msg)` | Routes the message to the right role, **switching role first if needed**. | A leader that receives `Reject` with a newer epoch, or a `Snapshot`/`Append` from a newer-epoch leader, steps down and (for snapshots and appends) processes the message as a replica in the same call, so it's in sync straight away. |
| `from_newer_leader(leader, msg)` (private) | Is this a command from a higher-epoch leader? | Keeps the step-down condition readable. |
| `step_down()` (private) | Converts a `Leader` into a `Replica`, keeping its history and the epoch that replaced it. | Uses `std::mem::replace` with a temporary placeholder, the standard Rust way to change an enum variant in place through `&mut self`. History is moved, not cloned. |

**Why a separate type:** `Leader` and `Replica` stay simple single-role state machines. The cross-role rule ("a newer leader exists, so step down") lives in one place.

#### As the project evolves
- This is the natural home for the **node agent** in architecture §2: heartbeats, membership, and job-queue handling would hang off `Node`.
- A node that's both leader and worker (the prompted device) only needs the leader's history; worker compute is separate from history sync.
- Promotion (replica → leader) currently happens outside `Node` (in `Cluster::failover`); a `Node::promote` would make the API symmetric.

---

## 9. `sim.rs` — in-memory cluster

### `struct Envelope` (private)

A message in flight: `from`, `to`, and **encoded bytes**.

**Why bytes and not `SyncMessage`:** Every simulated message goes through `to_bytes`/`from_bytes`, so the wire format is exercised on every test, as it would be over Iroh.

### `struct Cluster`

Fields: `nodes`, a FIFO `queue`, an `offline` set, `drop_next` counts per node, and public `delivered`/`dropped` counters.

**Why:** It stands in for the P2P layer so the protocol can be tested before Iroh integration. It supports exactly the failures the protocol must survive: dropped messages, offline nodes, failover, and a returning stale leader.

| Function | Purpose | Why it's implemented this way |
|---|---|---|
| `new(n)` | `n` nodes; node 0 is the leader in epoch 1. | Matches the README: one prompted device, the rest workers. |
| `node(id)`, `node_ids()` | Inspection. | For tests and reporting. |
| `leader_id()` | The online, non-deposed leader. | A deposed or offline leader doesn't count. |
| `leader_mut(id)` | Mutable access to a leader. | Panics if the node isn't a leader, since that would be a test bug. |
| `set_offline(id, bool)` | Cuts a node off from all traffic. | Simulates a crash, sleep, or network loss. |
| `drop_next_to(id, count)` | Drops the next *count* messages to a node. | Deterministic packet loss; random loss would make tests flaky. |
| `complete_task(...)` | Runs a task on a leader and queues its messages. | Mirrors real usage. |
| `resync(leader)` | Queues the leader's `sync()` retransmit. | Stands in for a periodic retry timer. |
| `send(from, out)` | Encodes and queues messages. | The single path into the network. |
| `run()` | Delivers messages, including replies, until the queue is empty. | FIFO order is deterministic. Messages to or from offline nodes count as dropped. |
| `take_drop(to)` (private) | Applies the drop counter. | |
| `failover()` | Online replicas elect a winner, which is promoted and snapshots everyone. | Shows the full README failover flow. Peers are the online candidates. |
| `rejoin(id)` | A node comes back; the current leader adds it and resyncs. | Models reconnection. A returning stale leader gets a snapshot and steps down. |
| `converged()` | True if every online node holds the leader's exact history. | The main correctness check in tests. |

#### As the project evolves
- Keep it as the **test harness** even after Iroh exists; it's much faster and more deterministic than real networking.
- Add random reordering and duplication (with a fixed seed) to stress-test idempotence.
- The real runtime replaces `Cluster` with an async loop: read frames from Iroh streams, call `Node::handle`, write the returned `Outgoing` messages, and drive `sync()` from a timer.

---

## 10. `examples/history_sync.rs` — demo binary

`cargo run --example history_sync -- <nodes>` (default 5, minimum 2) runs this scenario on a simulated mesh:

1. Three tasks, with one message to the last node dropped.
2. A retransmit.
3. The leader goes offline and failover happens.
4. One task under the new leader.
5. The old leader returns, tries to run a stale task, gets rejected, and is resynced.

After each step it prints every node's history length and whether the mesh has converged.

| Function | Purpose |
|---|---|
| `main()` | Runs the scenario above. |
| `report(cluster, label)` | Prints entry counts per node plus a converged/not-converged status. |

**Why:** A quick, human-readable demonstration of every protocol path, useful for the presentation and for teammates checking behavior without reading tests.

#### As the project evolves
Replace or supplement it with a real CLI once Iroh integration lands, e.g. a `mesh-node` subcommand that runs a live node.

---

## 11. Tests

`tests/history_sync.rs` — integration tests against the public API:

| Test | What it proves |
|---|---|
| `syncs_any_number_of_nodes` | 1, 2, 3, 8 and 50 nodes all converge. |
| `sends_deltas_after_first_snapshot` | A snapshot is sent first, then only deltas; up-to-date peers get nothing. |
| `duplicate_appends_are_idempotent` | Applying the same append twice is harmless. |
| `dropped_messages_are_recovered_by_resync` | Lost messages are recovered by `sync()`. |
| `gap_triggers_snapshot` | A gap leads to `Resync`, then a snapshot, then convergence. |
| `failover_continues_session_from_replica_history` | After the leader dies, a replica takes over with the full history and the session continues. |
| `election_prefers_most_complete_history` | The node holding the latest task wins and the lagging nodes catch up. |
| `returning_stale_leader_is_rejected_and_resynced` | Fencing: the old leader steps down and its divergent entry is discarded. |
| `election_is_deterministic` | Tie-breaks and epoch increments behave as specified. |
| `messages_round_trip_through_wire_format` | Encoding then decoding gives back the original message. |

#### As the project evolves
- Update `election_is_deterministic` and `election_prefers_most_complete_history` if the election rule changes (e.g. connection time).
- Add persistence tests: write, "crash", reload, compare.
- Add property-based tests (random drops, reordering, failovers) checking that the mesh always converges and that no committed entry is ever lost.

---

## 12. Cross-cutting future work

Prioritized list of changes not tied to a single item:

1. **Connection-time ranking in `elect`** — add `connected_at_unix_ms` as a tie-breaker (see §7).
2. **Persistence — the "history storage" task:** an on-disk log plus snapshots, loaded at startup, and an export interface for David's recovery tests (M-16).
3. **Iroh integration:** carry `to_bytes()` inside Yash's `History { conversation_id, payload }` message; later move to postcard and possibly a dedicated state stream.
4. **Protocol fields:** `conversation_id`, `cluster_id`, `prev_hash`, protocol version.
5. **Event-log entries** in place of chat turns (architecture §9).
6. **Commit policy (open decision B):** best-effort replication (current) or majority commit.
7. **Election:** manual takeover for 2 nodes (agreed with David's bot); real consensus once there are 3+ voters; connection time and reliability as a preference.
8. **Membership:** call `Node::set_peers(&PeerTable)` whenever the peer table changes, once a control loop exists; add heartbeat timeouts so a silent peer is removed too.
9. **Security:** encryption at rest, authenticated sender checks, size limits on snapshots.
