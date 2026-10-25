# Executable downloads

The **Build executables** GitHub Actions workflow builds native terminal tools for
Windows x64, Linux x64, Apple Silicon macOS, and Intel macOS. Open a completed
run's **Artifacts** section and download the matching platform archive. Extract
it completely, keeping `runtime/` and `web/` beside the executables.

Run in a terminal:

```sh
./meshthing
```

On Windows run `meshthing.exe`. This starts a loopback web server, opens your
normal browser, and keeps the terminal running. Ctrl+C stops the dashboard and
leaves the core and inference workers running. It is not a desktop/webview app.
Node is bundled; no separate Node/npm installation is needed.

By default, the dashboard reads a separately running local core at
`http://127.0.0.1:8080`. If it is unavailable, the dashboard shows disconnected.
To view the diagnostic runtime:

```sh
./meshthing --source live-demo --runtime http://127.0.0.1:8095
```

Use `--no-open` to print the browser URL without opening it, or `--port 3210` to
choose a port. `llm-cluster --help` lists the separate core executable's commands.
Neither launcher starts inference workers or downloads model weights.
Compatible models and native inference runners must be supplied separately.

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
