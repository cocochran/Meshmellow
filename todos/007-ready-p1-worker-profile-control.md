---
status: ready
priority: p1
issue_id: "007"
tags: [runtime, profiling, workers]
dependencies: []
---

# Expose managed profiling inside native worker owners

## Problem Statement

The leader API can host the managed profiler, but standalone shard/ring workers
have no local profile listener. Running an external profiler would bypass the
worker's in-process admission owner.

## Findings

PR42 integrates native child admission in both worker paths. ProfileManager
already provides bounded registered-candidate routes and shutdown. A worker-local
loopback listener can reuse it without changing Iroh protocols or peer routing.

## Proposed Solutions

1. Optional paired --profile-config/--profile-api flags on shard-serve and
   shard-ring-worker, with supervised worker/API/profile shutdown. Selected.
2. A separate profiler process. Rejected: it would not share admission.

## Recommended Action

Bind only loopback; configure candidates from trusted local files. Keep per-run
compute opt-in. Stop worker admission and request profiler cleanup when either
service ends or Ctrl-C arrives; close the endpoint after cleanup.

## Acceptance Criteria

- [x] Optional paired CLI flags preserve default worker behavior.
- [x] Local control binds only loopback and uses the process admission owner.
- [x] Worker/API exit and Ctrl-C cancel/await profiler cleanup implementation.
- [ ] Existing worker native lease makes POST /profiles return busy.
- [x] Integration tests cover busy/cancel/shutdown and configuration rejection.
- [ ] Docs, formatting, Clippy and relevant checks pass; publication notes socket limits.

## Work Log

### 2026-10-25 — Started

User requested continued implementation with six hours remaining. Independent
worker-profile-control checkout based on publication e31bd8c. Downloader is being
assigned by the user to another agent; do not duplicate that work.

### 2026-10-25 — Implementation and local validation

Added WorkerProfileControl under the same native worker runtime, paired optional
CLI flags, and synchronous ProfileManager::request_shutdown for host task drop
guards. Shutdown closes profile admission and cancels profiles even when external
manager handles remain alive. The feature leaves worker defaults and Iroh wire
protocols unchanged.

Local check results: all-target compile, formatting, all-target Clippy and fake
example builds pass; 35 CLI
tests, one listener-address unit test and 13 managed lifecycle tests pass.
The managed suite's four real-loopback cases (one existing, three new) all stop
at bind with sandbox EPERM. They remain enabled, with no skip or substitute
counted as listener coverage. Independent publication validation must run all
17 managed tests, including native-lease HTTP busy response, normal stop,
host task abort and worker failure. No hardware performance claim.
