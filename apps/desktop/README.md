# Layercake desktop executables

The Windows, Linux and macOS executable starts the bundled web dashboard and
opens it in the system browser. Source lives in `src/main.rs` here; the core
remains in the repository's root `src/` directory. The launcher does not start
inference workers. Ctrl+C stops its owned dashboard server.

Run build commands from the repository root:

```sh
npm --prefix apps/web ci
npm --prefix apps/web run build
cargo build --release --locked --bins --target aarch64-apple-darwin
python3 apps/desktop/scripts/package-executable.py \
  --target aarch64-apple-darwin --revision "$(git rev-parse HEAD)"
python3 apps/desktop/scripts/smoke-packaged-ui.py --target aarch64-apple-darwin
```

Use the target matching your host. Packaging includes the `meshthing` launcher,
`llm-cluster` core CLI, web output, Node runtime, licenses and checksums. Extract
the full archive before running `meshthing`; its `runtime/` and `web/` folders
must remain beside the executable. Use `--no-open` to print the URL without
opening a browser.

The root Cargo manifest explicitly maps the `meshthing` target to this folder,
preserving existing build commands. The four-platform workflow remains in
`.github/workflows/build-executables.yml`; packaging and extracted-archive smoke
tests are in `scripts/` here. See [downloads](../../docs/downloads.md) for artifacts
and platform requirements. Archives are development builds, not signed installers.
