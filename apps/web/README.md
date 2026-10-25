# Layercake web UI

A terminal-launched SvelteKit + Tailwind dashboard. The Node server binds to
`127.0.0.1`; closing it leaves the Rust core and native workers running. Fonts and
assets are served locally. Requires Node 22.17 or later and npm.

From the repository root:

```sh
npm --prefix apps/web ci
npm --prefix apps/web run build
./mesh-ui --runtime http://127.0.0.1:8095 --source live-demo --open
```

Open `http://127.0.0.1:3210`. Use `--port` to change the UI port and `--model`
to supply the runtime's model display label. The label is not model identity or
verification. `./mesh-ui --help` lists all options. Without `--runtime`, the UI
starts disconnected and explains how to connect. It never starts a worker or a
generation. The launcher works from other directories using its absolute path.

To inspect the production core's queue instead:

```sh
./mesh-ui --runtime http://127.0.0.1:8080 --source core
```

## Implemented

- Overview, device details, configured layer distribution, model information,
  and activity. Responsive layout for desktop and mobile browsers.
- `live-demo` reads `/state`: projects topology, reported stage buffers/compute,
  run state and decode rate into a small response. Prompts, generated output,
  raw traces, commands and filesystem paths are not sent to this UI.
- `core` reads `/jobs`: shows queue status without inventing unavailable device
  or model telemetry. These source APIs are different; select one explicitly.
- Device observations are labeled as last reported when idle; configured stages
  do not prove membership or liveness. Buffers do not claim total-memory coverage.
- Saved local display name and polling interval. Refreshes do not overlap;
  background-tab polling pauses. Connection errors remain visible.
- Core-owned mesh creation, signed invitations, cake QR scanning and trust approval.
  New profiles start without membership; runtime stages do not populate the mesh.

## Core integration still required

The web server controls a local core mesh process for membership. Benchmarking,
automatic setup of the tested Qwen3.8-27B model and contribution settings still
require managed compute integration. Native mobile execution remains separate
from trusted membership.

The selected runtime origin is operator-controlled at launch and limited to
loopback HTTP. Redirects are refused, fetches time out after three seconds and
state responses are capped at 1 MiB. The launcher checks Host and same-origin
writes; do not expose it through a public reverse proxy. The only write is UI
preferences, not runtime configuration.

Preferences live at `~/.local/state/meshthing-web/preferences.json`, or under
`MESHTHING_UI_STATE_DIR` when set. They are validated and atomically replaced with
owner-only file permissions. No mesh secret keys or prompt histories are stored.

## Development and checks

```sh
cd apps/web
MESHTHING_RUNTIME_URL=http://127.0.0.1:8095 npm run dev
npm run check
npm test
npm run build
npm run test:integration
```

The same read model is available to tools through `GET /api/mesh`; local
preferences use `GET /api/preferences` and same-origin `PUT /api/preferences`.
Tests use synthetic fixtures and temporary state directories; no hardware tests
or inference requests are needed to validate this application.

Framework setup follows [SvelteKit's Node adapter](https://svelte.dev/docs/kit/adapter-node)
and [Tailwind's SvelteKit integration](https://tailwindcss.com/docs/installation/framework-guides/sveltekit).

## Visual identity

The cake logo uses the [Cake JE4 render from Minecraft Wiki](https://minecraft.wiki/images/Cake_JE4.png?7c59e), supplied for this project’s Layercake branding.

## Mesh membership and cake QR

Build the core with `cargo build --bin llm-cluster` before launching the web UI.
The server owns one local `llm-cluster mesh` process and saves its private identity
and membership under `~/.local/state/layercake/mesh`. Override the directory with
`LAYERCAKE_MESH_STATE_DIR` or the executable with `LAYERCAKE_CORE_BIN` at launch.
Browser requests cannot choose either. Use one server per state directory.

A fresh identity shows Create a mesh / Join a mesh. Runtime route observations and
workspace preferences never establish membership. The local `/api/enrollment`
endpoint permits only create, invite, join, cancel and trust decisions, with
same-origin checks. Existing runtime telemetry remains read-only.

The cake QR encodes the full signed invitation locally. Frosting, strawberries and
cake layers surround the QR's untouched quiet zone. Scan cake QR requests camera
access on demand; images stay in the browser and the camera stops when closed,
scanned, hidden or unmounted. Use localhost or HTTPS for browser camera access.
Pasting remains available. A scan fills the invitation; Request to join and trusted
approval are still required. No Layercake lookup service is involved.

`npm run test:mesh` explicitly tests real Internet/Iroh enrollment between the web
and another core process, including approval and persistence. It uses temporary
state and may retry a cold relay connection. `npm run test:integration` remains
offline. Do not use test-created membership for the normal app profile.
