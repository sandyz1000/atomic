# Contributing to Atomic

Thank you for your interest in contributing! This document describes how to set up your development environment and submit changes.

## Development Setup

### Prerequisites

- **Rust** (stable, 1.82+): install via [rustup](https://rustup.rs/)
- **Python 3.11+** and [maturin](https://maturin.rs/) for `atomic-py`
- **Node.js 18+** and [napi-rs CLI](https://napi.rs/) for `atomic-js`

### Build

```bash
# Build all workspace crates (excludes atomic-py and atomic-worker)
cargo build

# Build in release mode
cargo build --release
```

### Run Tests

```bash
# Unit and integration tests (excludes atomic-py and atomic-worker)
cargo test --workspace --exclude atomic-py --exclude atomic-worker -- --test-threads=4

# Run a single test by name (most tests live in the root atomic-engine package)
cargo test -p atomic-engine -- test_reduce_by_key_sum

# Run a crate-specific test
cargo test -p atomic-sql -- test_joins

# Distributed integration tests (spawns a real worker process — Linux/macOS only)
cargo build --release -p atomic-engine
cargo test -p atomic-engine -- --test-threads=1 --ignored

# Python bindings (requires maturin)
cd crates/atomic-py
pip install maturin
maturin develop --release
pytest tests/
```

### Lint

```bash
cargo fmt --all -- --check
cargo clippy --workspace --exclude atomic-py --exclude atomic-worker -- -D warnings
```

### Dependency audit

```bash
cargo install cargo-deny --locked
cargo deny check licenses bans advisories
```

## Repository Structure

```
crates/
  atomic-compute/    — execution runtime, RDD DAG, NativeBackend
  atomic-data/       — shared types, task envelopes, shuffle primitives
  atomic-scheduler/  — local and distributed schedulers
  atomic-sql/        — DataFusion SQL layer
  atomic-streaming/  — micro-batch streaming
  atomic-graph/      — GraphX-style graph algorithms
  atomic-nlq/        — natural language query (scaffolded)
  atomic-py/         — Python bindings (PyO3/maturin)
  atomic-js/         — Node.js bindings (napi-rs)
  atomic-worker/     — standalone worker binary
  atomic-cli/        — cross-compilation and binary distribution
  atomic-tests/      — integration test suite
examples/            — runnable examples (word count, π estimation, …)
integration/         — multi-binary integration tests
notes/               — architecture notes and design documents
```

## Key Rules

- **Do not use unstable Rust features** — the project targets stable Rust.
- **Serialization**: use `rkyv` for distributed wire payloads; do not reintroduce generic closure serialization.
- **No Docker or WASM backends** — the only backend is `NativeBackend`.
- **`#[task]` is the unit of distributed work** — every function dispatched to workers must be registered via `#[task]` or `task_fn!`.
- **Python/JS API parity** — when adding a new `TypedRdd` method, add the equivalent to `atomic-py` and `atomic-js`.

See [CLAUDE.md](./CLAUDE.md) for the complete architecture guide and guardrails.

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Write tests for any new behaviour in `crates/atomic-tests/` or the relevant crate's `tests/` directory.
3. Ensure `cargo test --workspace --exclude atomic-py --exclude atomic-worker` passes.
4. Ensure `cargo fmt --all -- --check` and `cargo clippy … -- -D warnings` pass.
5. Update `CHANGELOG.md` under the `[Unreleased]` section.
6. Open a pull request against `main` with a clear description of what changed and why.

## Commit Style

Use conventional commits where possible:

```
feat: add right_outer_join() to TypedRdd pair API
fix: correct shuffle map output key encoding for RangePartitioner
docs: add Configuration Reference to docs/
chore: bump datafusion to 54
```

## License

By contributing you agree that your contributions will be licensed under the Apache-2.0 license.
