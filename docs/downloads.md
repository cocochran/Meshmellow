# Executable downloads

The **Build executables** GitHub Actions workflow builds native terminal tools for
Windows x64, Linux x64, Apple Silicon macOS, and Intel macOS. Open a completed
run's **Artifacts** section and download the matching platform archive. Extract
it completely, keeping `web/` beside the executables.

Run in a terminal:

```sh
./meshthing
```

On Windows run `meshthing.exe`. The Rust process owns the mesh runtime,
OpenAI-compatible API and static dashboard. It opens your browser at
`http://127.0.0.1:3210`; Ctrl+C stops this runtime and its UI. No Node runtime
is bundled or required to run the extracted app.

Use `--no-open` to skip opening the browser, `--port` to change the port, and
`--state-dir PATH` for a separate identity. The printed OpenAI base URL is local
to this device. Attach a running inference backend with
`--upstream 127.0.0.1:PORT`; without it, the API clearly reports inference as
unavailable. This does not automatically launch workers or download weights.

Each archive includes `build.json` with revision and executable SHA-256 hashes;
a separate `.sha256` file verifies the archive. These are unsigned development
builds, not notarized/signed releases. Actions retains artifacts for 30 days.
The workflow runs on relevant pull requests and main-branch updates, and supports
**Run workflow** on a branch. Feature-branch pushes do not duplicate PR builds.

For a source checkout, the original terminal-launched web server remains available:
see [web UI instructions](../apps/web/README.md).

Desktop launcher and packaging source lives in [apps/desktop](../apps/desktop/README.md),
separate from the core. Native [Android](../apps/android/README.md) and
[iOS](../apps/ios/README.md) source and local build instructions are also available
under `apps/`; the desktop workflow does not produce mobile release builds.
