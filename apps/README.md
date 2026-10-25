# Layercake applications

- [Web](web/README.md): terminal-launched SvelteKit dashboard; desktop uses a
  browser, with no Tauri or Electron shell.
- [iOS](ios/README.md): native SwiftUI application for iPhone and iPad.
- [Android](android/README.md): native Kotlin / Jetpack Compose application.

The mobile applications are native clients, not WebViews. They currently provide
dashboard telemetry, device inventory, local preferences and the single tested
Qwen3.8-27B model, plus real mesh creation and signed invitations through the
shared [Rust enrollment core](enrollment/README.md). They do not execute model layers.

## Implemented mobile behavior

Membership is owned by `llm_cluster::mesh` in the core. The mobile Rust library is
a thin C/JNI adapter. Full signed invitations work across networks through Iroh
without a lookup service or a Layercake server to host.
See [core mesh creation and joining](../docs/mesh-membership.md).

Both apps use the web dashboard's `GET /api/mesh` projection, including configured
stages, model label, layer ranges, latest compute and observed buffers. Network
requests are bounded, reject redirects, have timeouts, and stop polling when the
app leaves the foreground. Dashboard connectivity is not trusted mesh membership.
No prompts or generated responses are collected by the apps.

Local settings save the device name, desired memory contribution and plugged-in
preference. These are preferences only; the apps do not reserve memory or enforce
them through a worker. Device inventory reads OS memory/processor/power state;
it is not a model-specific placement benchmark.

The model page identifies the tested Qwen3.8-27B UD-Q5_K_M artifact. There is no
model upload, file picker, or model switching. Automatic retrieval of assigned
artifacts and native execution on newly enrolled devices remain core integration
work. The pinned source SHA-256 is
`2de73110cb254cbf09b54b717578dadff12ef1194e7271527e68202f39ba4bfd`;
see [validation evidence](../experiments/model-validation/QWEN38_RING.md).

Create a mesh on the first device, share its full signed invitation, and choose
Join a mesh on the second. Any trusted member can approve or decline the request.
Invitations expire after ten minutes and admit one device. Full invitations carry the endpoint identity and relay address; connections authenticate endpoint identities and
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
   withdraws availability on suspension. Android now supports opt-in background
   peer connectivity via a foreground service; iOS membership remains foreground-only.
6. Versioned events consumed by all UI clients. No arbitrary command execution
   or fixed diagnostic peer keys in app code.

This is a proposed adapter boundary, not an implemented FFI or approval of changes
to running workers. Native UI data and settings are centralized in `MeshStore`
on each platform so the transport can be replaced with a core adapter without
duplicating planners or trust rules in the views.

### Leave a mesh

Leave mesh disconnects this device, cancels its invitations and clears its local
membership. The private device identity is retained. Other devices keep their mesh
and signed trust history and see the departed device offline; this is not global
trust revocation. A returning device can use a fresh invitation and must be approved
again. The same core command (`{"op":"leave"}`) is used by web, iOS and Android.

Android’s **Keep mesh active** service keeps the core connected when its Activity
is closed. See [background usage and battery limits](android/README.md#keep-the-mesh-active-in-the-background).
This does not make mobile model execution ready. [iOS background limits](ios/README.md#background-limits)
allow future finite jobs, not an indefinite mesh listener.
