---
name: clean-code-formatter
description: Format and clean-code gate for changed files - runs the project's formatters and linters, then enforces strict clean-code rules (small functions, no dead code or debug output, no unwrap in library code, named constants, documented public API, ticketed TODOs). Adapted from CuriousLearner/devkit's code-formatter with extra restrictions. Use after implementing a change and before committing it; pass --check-only to report without editing.
argument-hint: "[paths...] [--check-only]"
---

# Clean code formatter

Adapted from [devkit's code-formatter](https://github.com/CuriousLearner/devkit/blob/main/skills/code-formatter/SKILL.md), with stricter clean-code rules on top of formatting.

Arguments: `$ARGUMENTS` — paths to check (default: files changed on this branch, `git diff --name-only <base>...HEAD` plus uncommitted changes), and `--check-only` to report without editing.

## Scope rules

- **Only touch files this task changed.** Never reformat untouched files: parallel workers' branches must merge cleanly. If a tool reformats more than that, revert the extra files (`git checkout -- <file>`).
- Skip generated code, vendored code, lock files (`Cargo.lock`, `package-lock.json`, …), binaries, and build output (`target/`, `dist/`, `node_modules/`).
- Existing project config always wins (`rustfmt.toml`, `clippy.toml`, `.editorconfig`, `pyproject.toml`, `.prettierrc`, …). **Never create or edit config files** — if you think one should change, say so in the report.
- Every edit is behaviour-preserving. If a rule can only be satisfied by changing behaviour, report it and leave the code alone.

## 1. Format

| Language | Format | Lint (warnings are errors) |
|---|---|---|
| Rust | `cargo fmt -- <files>` (or `rustfmt --edition 2024 <file>`) | `cargo clippy --all-targets -- -D warnings` |
| Python | `ruff format` (else `black`) | `ruff check` |
| JS/TS | `npx prettier --write` | `npx eslint` if configured |
| Go | `gofmt -w` | `go vet` |
| TOML/JSON/YAML/Markdown | project formatter if configured; otherwise leave as is | — |

Fallback defaults when no config exists: 4 spaces for Rust and Python, 2 for JS/TS/CSS, tabs for Go; 100-character lines; trailing commas in multi-line lists.

With `--check-only`, use the check modes (`cargo fmt --check`, `ruff format --check`, `prettier --check`) and edit nothing.

## 2. Clean-code rules

Check each changed hunk against these. Fix what you can mechanically and safely; report the rest.

**Structure**
- Functions do one thing: aim for ≤ 40 lines, ≤ 4 parameters (group the rest into a struct), nesting ≤ 3 levels (use early returns, `?`, `let … else`).
- No duplicated logic: three or more near-identical lines in two places becomes a helper.
- Modules have one clear responsibility. New public items go in the module that owns the concept, not in `main.rs`.

**Naming**
- Descriptive names. No single letters except loop indices and closures of 1–2 lines, and no abbreviations beyond the conventional (`id`, `msg`, `cfg`, `tx`/`rx`).
- Booleans read as predicates (`is_connected`, `has_peer`). Functions are verbs, types are nouns.
- Follow language conventions (Rust: `snake_case` items, `CamelCase` types, `SCREAMING_SNAKE_CASE` constants).

**Hygiene**
- No dead code, no commented-out code, no unused imports, dependencies, or `#[allow(dead_code)]` added to hide warnings.
- No debug output left behind (`dbg!`, stray `println!`/`eprintln!`, `console.log`, `print(`). Use the project's logging (`tracing`/`log`) or intentional CLI output only.
- No magic numbers or strings: name them as `const` (ports, timeouts, buffer sizes, ALPN strings).
- `TODO`/`FIXME` must name a ticket: `TODO(M-11): …`. Untracked TODOs are errors.
- No wildcard imports except preludes and `use super::*` in tests. Imports grouped std / external / crate.

**Errors and safety (Rust)**
- No `unwrap()`/`expect()`/`panic!` in library or networking code. Propagate with `?` and a typed error (`thiserror`) or `anyhow` at the binary boundary. Allowed in tests, and in `main` for start-up failures with a clear message.
- No `unsafe` without a `// SAFETY:` comment explaining the invariant.
- Don't swallow errors silently (`let _ = fallible()` needs a comment saying why).
- Error messages are lowercase without trailing punctuation.

**Documentation and tests**
- Every new `pub` item gets a `///` doc comment saying what it is for. Comments explain *why*, not *what*.
- New logic has at least one test. Networking code that can't be unit-tested gets a test for its pure parts (serialization, state transitions).

**Dependencies**
- Adding a dependency is not a formatting decision: list any new crate/package in the report so the orchestrator can approve it.

## 3. Verify

Re-run the build and tests after any edit. If formatting or a cleanup broke something, revert that change and report it.

## Report

```text
FORMATTER: clean | fixed | issues-left
FORMATTED: <files reformatted, or none>
FIXED: <rule → file:line, one per line>
LEFT: <rule → file:line → why it wasn't auto-fixed>
LINT: <tool → pass/fail>
CONFIG SUGGESTIONS: <none | proposal>
NEW DEPENDENCIES: <none | list>
```
