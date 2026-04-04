# Rust WASM Task Example

This example shows the preferred current authoring flow for Atomic WASM tasks: write the task in Rust, compile it to a plain WebAssembly module, emit an artifact manifest, and run the existing driver-side map/reduce example against that manifest.

The worker ABI is explicit and byte-oriented. This crate exports:

- `alloc(len)` for host-to-guest input buffers
- `run_map(ptr, len)` to transform a partition payload
- `run_reduce(ptr, len)` to aggregate bytes into a little-endian `u32`

Build the artifact bundle:

```sh
cargo run -p atomic-cli -- wasm build --source examples/rust_wasm_task --out examples/rust_wasm_task/build
```

Run the driver example with that manifest:

```sh
ATOMIC_RUN_WASM_EXAMPLE=1 \
ATOMIC_WASM_MANIFEST=examples/rust_wasm_task/build/manifest.toml \
cargo run --example wasm_map_reduce
```

This crate is intentionally minimal. It is not using WASI or the component model because the current worker runtime expects a plain `wasm32-unknown-unknown` module with exported memory and entrypoints.
