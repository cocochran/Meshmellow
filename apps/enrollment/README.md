# Mesh enrollment

Thin Rust C/JNI adapter used by the SwiftUI and Compose apps. Shared membership,
invitation, persistence and transport logic lives in `src/mesh/` in the core,
not in this application folder.
Native apps use the existing core's Internet-capable Iroh endpoint builder with
persistent identities. This module does not start inference, assign layers or
modify diagnostic worker allowlists.

## Use

1. Open Layercake and choose **Create a mesh**. Name the mesh and this device.
2. The first device becomes trusted and publishes an eight-emoji invitation.
3. On another device, choose **Join a mesh** and enter the emojis in order.
4. Any trusted device can choose **Trust device** or **Decline**.

Devices can use different networks. Iroh discovers addresses by endpoint ID and
uses direct connections or relay fallback. Native pairing does not use Bonjour
or Android NSD. Keep the apps open while joining or approving; mobile networking
pauses in the background. Invitations expire after ten minutes and admit one
device. Trusted identities and membership survive restarts and this update.

## Invitation lookup deployment

Eight emojis cannot contain the full Iroh endpoint ID. A separately configured
lookup node maps the code to an expiring, issuer-signed locator. It runs over Iroh,
not an HTTP port or LAN multicast. It never authorizes membership or receives
names, membership ledgers, prompts, model data or private keys.

```sh
cargo build --locked --features cli --bins
target/debug/layercake-rendezvous /path/to/private-lookup-state
```

The process prints its public `LAYERCAKE_RENDEZVOUS_ID`. Keep its private identity
stable and the process online. Set that public ID in the environment when building
the mobile Rust libraries; the CLI also accepts it at runtime. All clients must
use the same lookup node. No public service is assumed or silently provisioned.
Unconfigured builds report an explicit error instead of silently doing LAN pairing.

The core also supports self-contained signed `layercake://join/` invitation
tickets, which do not require this lookup service. The core JSON snapshot exposes
the ticket in `invite.ticket`; pass it as the `code` in a `join` command.
The native emoji flow still needs the configured lookup node. A ticket locates
the invitation and does not bypass approval by a trusted member.

The registry holds only active locators in memory. After a restart, open inviting
apps republish within 60 seconds. Existing members connect by their persisted IDs
independently of the registry. New emoji lookups require the configured node online.
The service caps message size, connections, stored entries, and requests per caller;
these limits bound resource use but are not Internet-scale abuse protection.

## Protocol and persistence

- Iroh authenticates each connection using a persistent Ed25519 identity.
  Enrollment has its own `meshthing/enrollment/1` ALPN and bounded JSON messages.
- The core's `network::endpoint_builder_with_key` shares the same N0 relay and
  address-lookup defaults as `network::bind_endpoint`. Existing mobile binary
  identity files remain unchanged; the core builder accepts the saved key.
- The code is the first 48 bits of SHA-256 of the issuer-ID/invitation-UUID tuple,
  rendered with 64 single-scalar emojis. It is a locator, not a secret credential.
  Registry publication requires the authenticated issuer, and the recipient verifies
  the signed locator, expiry and code binding before connecting to that issuer.
- The lookup ALPN is `layercake/invitations/1`. The registry permits one active
  locator per issuer. A replaced or consumed code cannot bypass the issuer's
  invitation checks. Knowing a code never replaces explicit human approval.
- A founder-signed genesis identifies the mesh. Member grants are signed by a
  trusted inviter and verified through the chain to that genesis. Any existing
  trusted member can approve by authenticating to the invitation issuer, which
  serializes and persists the decision. Conflicting decisions fail; identical
  retries are idempotent. Signed ledgers synchronize between discovered members.
- Requests are bound to the authenticated applicant. A different device cannot
  poll, cancel or reuse that request. An untrusted device cannot approve one.
- Membership writes use a private temporary file, fsync and rename before success.
  Identities use owner-only files. iOS excludes the directory from backup and uses
  file protection; Android uses `noBackupFilesDir`. Never share a private state
  directory between running processes or copy it to another device.
- Limits: 64 trusted members, 128 discovered services, 16 concurrent incoming
  exchanges, 128 KiB messages, bounded network timeouts. One mesh per installation.

Revocation/removal, recovery after app deletion and automatic benchmarking/inference
admission are not implemented. Trusted
membership does not imply compute eligibility. A future compute adapter must
consume verified membership and separately enforce model/backend readiness.

## Build and validate

Install Rust targets `aarch64-apple-ios`, `aarch64-apple-ios-sim` and
`aarch64-linux-android`, then:

```sh
cargo test --locked --manifest-path ../../Cargo.toml --lib mesh::
cargo check --locked --all-targets --features cli
bash build-mobile.sh ios
bash build-mobile.sh ios-sim
ANDROID_NDK_HOME=/path/to/ndk bash build-mobile.sh android
```

The native projects link these artifacts. Current mobile builds target arm64.
The vendored mobile network compatibility fixes are shared with the existing
Iroh probe; no separate fork of those files is introduced here.

The local JSON-lines CLI exposes the same commands for automation and native
integration validation, with Internet invitation lookup:

```sh
cargo build --locked --features cli --bin meshthing-enrollment
target/debug/meshthing-enrollment /path/to/private-state
```

Send one JSON object per line: `{"op":"snapshot"}`, `{"op":"create","name":
"My mesh","device_name":"Mac"}`, `{"op":"invite"}`, `{"op":"join","code":
"eight emojis","device_name":"Mac"}`, or `{"op":"decide","issuer":"endpoint
ID","request":"request UUID","approve":true}`. Every command returns an
`ok` flag with `state` or `error`. `refresh` and `cancel_join` are also available. EOF stops the endpoint. Keep stdout private: it contains
device names, membership and active invitation locators, but never private keys.

Rust tests use real loopback Iroh connections between three peers and verify
approval by another member, persistence, expiry, cancellation, rejection of
strangers, tampered grants and foreign meshes. Native UI integration uses the
iOS XCTest in `../ios/UITests` and the companion `tests/ios-peer.py` driver.

Internet integration validation (requires public Iroh relay/DNS access):

```sh
cargo test --locked --manifest-path ../../Cargo.toml --lib internet_emoji_pairing -- --ignored
```

That test disables all IP transports on three peers and the lookup node. It checks
emoji lookup, two admissions, approval by a remote trusted member using only saved
member IDs, and identity/membership after restart. This forces relay use instead of
accidentally passing through loopback or LAN. Physical cross-network testing remains
a separate deployment check.
