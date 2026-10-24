# Foreground iPad Iroh worker diagnostic

This diagnostic embeds the pinned Rust/Iroh core and patched llama.cpp/GGML in one iOS process. It serves Qwen3.8-27B layers 28:32 on Metal through a loopback socket restricted to an approved Iroh leader. No Python, subprocesses, SSH tunnel, or USB port forwarding carries iPad inference.

The app keeps the display awake while running. Keep it open and powered. Background suspension, network changes, automatic recovery, and production worker enrollment remain separate integration work. Restart the diagnostic after changing networks.

Build the private Qwen runtime's static `llama` target for arm64 iOS in `build-ios-probe`, then build `../iroh-core` with `cargo build --release --locked --target aarch64-apple-ios`. Configure this CMake project with Xcode/iOS, passing `MESH_LLAMA_ROOT`, `MESH_SHARD`, `MESH_PROMPT`, `MESH_IROH_LIB`, and your own `MESH_APP_TEAM`. Sign and install with your team's provisioning. See `WorkerApp.swift` for the fixed diagnostic leader identity and layer range; replace these for your own setup.

The core is pinned at `bc50aec0a379ede8e44d5226d0e45a5098741df2`. Two dependency patches are local to this diagnostic:

- netdev 0.46.3: iOS libc does not expose `in6_ifreq`; IPv6 address flags use the crate's existing unknown-flags fallback on iOS.
- netwatch 0.19.3: iOS cannot compile the crate's BSD route definitions against its libc target. The diagnostic retains netdev interface enumeration and uses the existing minimal route-monitor fallback. It does not receive automatic route-change notifications or report a default route/home router.

Both dependency licenses are retained in `../iroh-core/vendor`. These fallbacks need a proper mobile network-monitor implementation before production use. Other platforms retain their existing behavior.

`mesh_shard_run_stream` receives dedicated `FILE*` streams. The socket adapter owns and closes them for each session, leaving the app's stdin/stdout untouched. This prevents app or network logging from corrupting protocol responses and lets the app reopen its listener after a session ends. Each new session loads a fresh model/context; it does not resume live KV from an earlier connection.
