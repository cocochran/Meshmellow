# Native mesh adapter

This crate exports the C/JNI bridge used by Meshmellow's SwiftUI and Compose apps.
All identity, membership, invitation and connection logic lives in the existing
core at `src/mesh`, exposed as `llm_cluster::mesh::Node`.

See [core mesh creation and joining](../../docs/mesh-membership.md) for commands,
protocol details, persistence and validation.

## Build

```sh
bash build-mobile.sh ios
bash build-mobile.sh ios-sim
ANDROID_NDK_HOME=/path/to/ndk bash build-mobile.sh android
```

The apps link the matching arm64 artifacts. Android initializes its platform
context before opening the core. Native network processing pauses in the background.
The vendored network compatibility fixes are shared with the diagnostic mobile probe.

The core preserves installed apps' existing private identity and membership files.
It does not adopt diagnostic worker keys, change inference allowlists, or start model
execution. Full invitations work through Iroh without a separate lookup service.

For compatibility, `cargo build --locked --features cli --bins` also builds the
`meshmellow-enrollment` JSON-lines wrapper.
The primary core CLI is now `llm-cluster mesh --state-dir PRIVATE_DIRECTORY`.
Run protocol tests from the repository root: `cargo test --lib mesh::`, or add
`-- --include-ignored` to exercise public Iroh relay paths.
