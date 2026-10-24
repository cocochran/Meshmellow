//! Helpers shared by the integration tests.

use std::path::PathBuf;

/// Path of a test helper binary from `examples/`, built next to the test binary's `deps` directory.
///
/// Plain `cargo test` builds examples as binaries, but `cargo test --all-targets` builds them as
/// test harnesses instead, so that invocation needs `cargo build --examples` first (CI does this).
pub fn example_bin(name: &str) -> PathBuf {
    let test_exe = std::env::current_exe().expect("test executable path");
    let profile_dir = test_exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("target profile directory");
    let path = profile_dir
        .join("examples")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.exists(),
        "test helper {} not found: run `cargo build --examples` first \
         (plain `cargo test` builds it, `cargo test --all-targets` does not)",
        path.display()
    );
    path
}
