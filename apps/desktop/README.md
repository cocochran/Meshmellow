# Meshmellow desktop executables

The Windows, Linux and macOS executable runs the Rust mesh runtime and serves
its browser dashboard in the same process. Ctrl+C stops both. The core library
remains in the repository's root `src/` directory. Native inference runners
are attached explicitly with `--upstream 127.0.0.1:PORT`.

Run build commands from the repository root:

```sh
npm --prefix apps/web ci
npm --prefix apps/web run build
cargo build --release --locked --bins --target aarch64-apple-darwin
python3 apps/desktop/scripts/package-executable.py \
  --target aarch64-apple-darwin --revision "$(git rev-parse HEAD)"
python3 apps/desktop/scripts/smoke-packaged-ui.py --target aarch64-apple-darwin
```

Use the target matching your host. Packaging includes the `meshmellow` launcher,
`llm-cluster` core CLI, static web output, licenses and checksums. Extract
the full archive before running `meshmellow`; its `web/` folder
must remain beside the executable. Use `--no-open` to print the URL without
opening a browser.

The root Cargo manifest explicitly maps the `meshmellow` target to this folder,
preserving existing build commands. The four-platform workflow remains in
`.github/workflows/build-executables.yml`; packaging and extracted-archive smoke
tests are in `scripts/` here. See [downloads](../../docs/downloads.md) for artifacts
and platform requirements. Archives are development builds, not signed installers.
