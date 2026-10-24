---
status: ready
priority: p1
issue_id: "001"
tags: [inference, p2p, validation]
dependencies: []
---

# Validate and publish peer-to-peer execution

## Problem Statement
The measured Qwen three-device run used hub forwarding. Direct intermediate-to-next-peer execution has not been validated on hardware.

## Findings
The experimental ring compiles on host, Linux, and iOS. Two protocol tests pass. Automatic placement belongs to Aarush; request replication/recovery is not implemented in this diagnostic.

## Proposed Solutions
- Validate the existing three-device split first to isolate transport behavior, then extend device eligibility and measured planning.
- Wait for the complete automatic planner: broader scope, delays transport evidence.

## Recommended Action
Run the existing shards through the direct ring, fix integration failures, record raw evidence and limits, and publish a reviewable experimental release.

## Acceptance Criteria
- [x] Hardware run generates coherent output using Mac → iPad → CUDA PC.
- [x] Logs identify peer hops and show no intermediate activation returned to origin.
- [x] Protocol checks and applicable fixture tests pass.
- [x] Documentation separates observed results, test-only guarantees, and unimplemented requirements.
- [ ] Commit and push the experimental slice with evidence to the existing PR.

## Work Log
### 2026-10-24 — Release validation
- Reviewed pending changes and team ownership.
- Confirmed old hub bridge and CUDA worker are still available.
- Approved measured profile contract with planner owner; dynamic memory must include reservation and unified-memory accounting.

- Two fresh 45-step runs passed (3.9479 and 3.0484 steps/s); final rebuilt live run passed 43 steps at 3.6030 after rejecting a bad execution header.
- Python tests: 9 passed; Rust protocol tests: 2 passed; host/Linux/iOS builds passed.
- Helium screenshot and raw counters saved. Previous PR #1 was already merged; publishing this slice in a new PR.
