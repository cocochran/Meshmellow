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
- `core` reads `/jobs` and `/peers`: shows queue status and the authenticated
  peers currently connected to the leader. Peer IDs do not identify hardware,
  assigned layers, compute, memory, or trust membership. These source APIs are
  different; select one explicitly.
- The core peer endpoint returns a JSON array with `node_id`,
  `connected_at_unix_ms`, and nullable `last_heartbeat_unix_ms` fields. The
  dashboard reads both endpoints from the configured loopback origin; if an
  older core has no `/peers` route, the queue remains available and peer status
  is shown as unavailable.
- Device observations are labeled as last reported when idle; configured stages
  do not prove membership or liveness. Buffers do not claim total-memory coverage.
- Saved local display name and polling interval. Refreshes do not overlap;
  background-tab polling pauses. Connection errors remain visible.
- Emoji pairing preview explains entry and approval by any trusted member.
  The illustrative emojis are not an invite, and join/create buttons stay disabled.

## Core integration still required

The core exposes `GET /peers` for currently connected, authenticated control
peers. It does not expose management APIs for emoji invitations, trusted
membership, benchmarking, model import/download, layer assignments, hardware
details, or shared mesh settings. This UI does not simulate successful
operations or write local records that pretend to enroll devices. Those
capabilities need authenticated, identity-bound core commands/events before
enabling the controls. iOS/Android native embedding is separate work; a
responsive browser UI does not supply mobile compute lifecycle.

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
