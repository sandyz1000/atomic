# WASM Task Definition Example

This directory contains the low-level WAT reference task for Atomic's current WASM map/reduce runtime.

The preferred authoring flow is now the Rust example in `examples/rust_wasm_task`, but this WAT module remains useful as a minimal ABI reference because it matches the worker contract exactly.

## Files

- `task.wat`: exports `memory`, `alloc`, `run_map`, and `run_reduce`

## Build

```sh
cargo run -p atomic-cli -- wasm build --source examples/wasm_task_definition/task.wat --out examples/wasm_task_definition/build
```

## Run

```sh
ATOMIC_RUN_WASM_EXAMPLE=1 \
ATOMIC_WASM_MANIFEST=examples/wasm_task_definition/build/manifest.toml \
cargo run --example wasm_map_reduce
```

This still requires a distributed Atomic setup with at least one WASM worker.
