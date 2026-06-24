---
title: Contributing
description: Set up a development environment and submit changes to Atomic.
---

## Prerequisites

- **Rust** (stable, 1.82+) via [rustup](https://rustup.rs/)
- **Python 3.11+** and [maturin](https://maturin.rs/) for `atomic-py`
- **Node.js 18+** and the [napi-rs CLI](https://napi.rs/) for `atomic-js`

## Build

```bash
# Build all workspace crates (excludes atomic-py and atomic-worker)
cargo build

# Release build
cargo build --release
```

## Test

```bash
# Unit and integration tests
cargo test --workspace --exclude atomic-py --exclude atomic-worker -- --test-threads=4

# A single test by name
cargo test -p atomic-engine -- test_reduce_by_key_sum

# A crate-specific test
cargo test -p atomic-sql -- test_joins

# Distributed integration tests (spawns a real worker process)
cargo build --release -p atomic-engine
cargo test -p atomic-engine -- --test-threads=1 --ignored

# Python bindings (requires maturin)
cd crates/atomic-py
pip install maturin
maturin develop --release
pytest tests/
```

`atomic-py` is a `cdylib` — build it with maturin, not `cargo build` alone.
`atomic-worker` is excluded from `cargo test --workspace` because it enables
PyO3 auto-initialization, which breaks the link step in non-Python test
binaries.

## Lint

```bash
cargo fmt --all -- --check
cargo clippy --workspace --exclude atomic-py --exclude atomic-worker -- -D warnings
```

## Dependency audit

```bash
cargo install cargo-deny --locked
cargo deny check licenses bans advisories
```

## Rules

- **Stable Rust only** — no unstable features.
- **Serialization** — use `rkyv` for distributed wire payloads; do not add
  generic closure serialization.
- **One backend** — `NativeBackend` only; no Docker or WASM backends.
- **`#[task]` is the unit of distributed work** — every function dispatched to
  workers must be registered with `#[task]` or `task_fn!`.
- **Binding parity** — when adding a `TypedRdd` method, add the equivalent to
  `atomic-py` and `atomic-js`.

## Pull request process

1. Branch from `main`.
2. Write tests for new behavior in `crates/atomic-tests/` or the relevant
   crate's `tests/` directory.
3. Ensure tests, `cargo fmt --all -- --check`, and clippy with `-D warnings`
   pass.
4. Update `CHANGELOG.md` under `[Unreleased]`.
5. Open a pull request against `main` describing what changed and why.

## Commit style

Conventional commits:

```text
feat: add right_outer_join() to TypedRdd pair API
fix: correct shuffle map output key encoding for RangePartitioner
docs: add SQL guide
chore: bump datafusion to 54
```

Contributions are licensed under Apache-2.0.
