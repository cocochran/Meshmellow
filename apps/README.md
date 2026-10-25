# Layercake applications

- [Web](web/README.md): terminal-launched SvelteKit dashboard; desktop uses a
  browser, with no Tauri or Electron shell.
- [iOS](ios/README.md): native SwiftUI application for iPhone and iPad.
- [Android](android/README.md): native Kotlin / Jetpack Compose application.
- [Desktop executables](desktop/README.md): Windows, Linux and macOS terminal
  launcher and archive packaging for the browser dashboard.

## Source layout

All application clients live under `apps/`. The shared Rust core remains in
`src/`, with membership and invitation logic in `src/mesh/`. The native C/JNI
adapter is in `apps/enrollment/` and depends on the core; the core does not depend
on any app. Shared artwork lives in `apps/branding/`.

The desktop launcher source and packaging scripts live in `apps/desktop/`.
Its binary target is declared in the root Cargo manifest so existing
`cargo build --bins` and `cargo run --bin meshthing` commands continue to work.
The default `cargo run` command still starts the `llm-cluster` core CLI.

The mobile applications are native clients, not WebViews. They currently provide
dashboard telemetry, device inventory, local preferences and a persistent single
GGUF file selection, plus real mesh creation and emoji enrollment through the
shared [native enrollment adapter](enrollment/README.md). They do not execute model layers.

## Implemented mobile behavior

Pairing is intended to work across networks through the existing Iroh core.
The mobile adapter now uses the core endpoint builder and an explicitly configured
Iroh invitation lookup node. See [setup and limitations](enrollment/README.md).

Both apps use the web dashboard's `GET /api/mesh` projection, including configured
stages, model label, layer ranges, latest compute and observed buffers. Network
requests are bounded, reject redirects, have timeouts, and stop polling when the
app leaves the foreground. Dashboard connectivity is not trusted mesh membership.
No prompts or generated responses are collected by the apps.

Local settings save the device name, desired memory contribution and plugged-in
preference. These are preferences only; the apps do not reserve memory or enforce
them through a worker. Device inventory reads OS memory/processor/power state;
it is not a model-specific placement benchmark.

The system file picker selects one GGUF file. iOS persists a file bookmark and
Android persists its document read permission. Both recognize the 24-byte v2/v3
header, recheck access on demand, and can forget the selection without deleting
the original. Header recognition does not validate tensors, checksum, model
compatibility or available execution capacity. No model weights are uploaded,
copied or loaded.

Create a mesh on the first device, share its eight-emoji invitation, and choose
Join a mesh on the second. Any trusted member can approve or decline the request.
Invitations expire after ten minutes and admit one device. Signed invitation
locators resolve through Iroh; connections authenticate endpoint identities and
signed membership persists. Keep the inviting app open until admission completes.
The devices do not need to share a Wi-Fi network. Enrollment does not enable diagnostic compute
or automatically benchmark devices. One mesh and one model remain the scope.

## Next native-core boundary

The current diagnostic `meshthing_iroh_run` blocks on fixed peers, with no owned
stop handle; its worker also embeds a fixed layer range. Production apps must not
hide those limitations behind a Start button. Coordinate M-26 with the core owner:

1. App-owned core handle initialized with private storage and Android/JNI context.
2. Consume the implemented enrollment identity and verified membership snapshots
   from `apps/enrollment`; do not treat dashboard stages as admission evidence.
3. Async device/model-specific profiling with cancellation, evidence identity,
   memory accounting and no automatic eligibility from OS inventory alone.
4. Staged model file access, full verification, reserved capacity and ready state.
5. Start/stop/join lifecycle that terminates owned tasks, reports errors and
   withdraws availability on background/suspension; foreground-only initially.
6. Versioned events consumed by all UI clients. No arbitrary command execution
   or fixed diagnostic peer keys in app code.

This is a proposed adapter boundary, not an implemented FFI or approval of changes
to running workers. Native UI data and settings are centralized in `MeshStore`
on each platform so the transport can be replaced with a core adapter without
duplicating planners or trust rules in the views.
