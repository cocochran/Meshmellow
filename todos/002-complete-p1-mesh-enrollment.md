---
status: complete
priority: p1
issue_id: "002"
tags: [mobile, membership, pairing]
dependencies: []
---

# Mesh creation and emoji enrollment

## Problem Statement
The mobile apps have a disabled join form and no way to create a mesh.

## Findings
The current core exposes fixed-peer diagnostic workers, not managed enrollment.
The apps need persistent identity, authenticated transport, invitations and approvals.

## Proposed Solutions
- Shared Rust enrollment over Iroh, with platform Bonjour/NSD discovery: reusable trust rules and real peers.
- Independent native membership implementations: quicker initially, but duplicate security rules.

## Recommended Action
Implement a shared Rust enrollment module and native clients. Same-Wi-Fi discovery,
foreground participation, single mesh, eight-emoji invitations expiring in ten minutes,
explicit approval by any trusted member. Invitation issuer serializes decisions.
Signed membership grants persist; diagnostic compute workers remain separate.

## Acceptance Criteria
- [x] Create persists a mesh and first trusted device.
- [x] Another device finds an invitation by its emoji code and requests admission.
- [x] Any trusted device can approve or decline a pending request.
- [x] Expired/consumed invitations, strangers, tampered grants and wrong meshes are rejected.
- [x] Restart restores membership; app background closes owned network endpoints.
- [x] iOS and Android expose creation, invitation, pending and trusted member states.
- [x] Integration tests exercise real Iroh connections between three peers.
- [x] Build both apps, verify screens, reinstall the iPad app.

## Work Log
### 2026-10-25
- User explicitly requested implementation. Checked native UI and core; notified core owner via Discord.
- Continuing existing isolated app checkout. Compute/benchmark lifecycle remains separately tracked.

- Implemented `apps/enrollment` with Rust C/JNI bridges, SwiftUI and Compose flows, and platform Bonjour/NSD discovery.
- Rust: five passing tests, including real three-peer Iroh admission and approval recovery after invitation expiry. Clippy passes for owned code; vendored network layer emits existing deprecation warnings.
- Swift: five passing tests, including a regression for valueless Bonjour TXT records that crashed Foundation dictionary bridging.
- iPad XCTest + companion CLI: full create → emoji invite → Bonjour discovery → real Iroh join → UI trust approval → relaunch persistence passed. Light invitation and dark membership screens inspected.
- Android: debug build and four JVM tests pass; lint has zero errors (14 existing/advisory warnings). Native creation and emoji display verified in emulator. Final APK rebuilt with enrollment core.
- No inference workers, model weights, core allowlists or desktop packaging modified. Enrollment does not claim benchmark/compute readiness.

- Enrollment build installed successfully on PearPad (devicectl success receipt `/tmp/mesh-enrollment-ipad-install.json`). Subsequent install attempts for final status-label/read-only-write refinements timed out; launch verification is pending device connection. Signed latest artifact remains in `apps/ios/build/ipad-signed/Debug-iphoneos/MeshThing.app`.

## Superseding decision — October 25

Emoji pairing and its lookup service were removed at the user’s request. Core and
native apps now share/paste full signed invitations over Iroh, with explicit trusted
member approval. Earlier emoji and same-network notes above are historical.
