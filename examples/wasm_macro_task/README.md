# wasm-macro-task

A WASM task that uses the `#[task]` attribute macro — the high-level alternative
to writing raw C ABI exports by hand (as in `examples/rust_wasm_task`).

## How `#[task]` works on `wasm32`

```
#[task]
fn run_map(input: Vec<u8>) -> Vec<u8> { ... }
```

Expands to:

```rust
fn run_map_impl(input: Vec<u8>) -> Vec<u8> { ... }  // renamed

static __ATOMIC_HEAP: AtomicUsize = AtomicUsize::new(65536);

#[no_mangle] pub unsafe extern "C" fn alloc(len: i32) -> i32 { ... }
#[no_mangle] pub unsafe extern "C" fn dealloc(_ptr: i32, _len: i32) {}

#[no_mangle]
pub unsafe extern "C" fn run_map(ptr: i32, len: i32) -> i64 {
    atomic_runtime::__internal::call_wasm::<Vec<u8>, Vec<u8>, _>(ptr, len, run_map_impl)
}
```

Input and output are rkyv-encoded inside linear memory.

## Build

```bash
# Install the wasm32 target once:
rustup target add wasm32-unknown-unknown

# Build the WASM artifact and generate manifest.toml:
cargo run -p atomic-cli -- wasm build \
  --source examples/wasm_macro_task \
  --out    examples/wasm_macro_task/build \
  --map_op_id      demo.incr.v1 \
  --map_entrypoint run_map
```

## Run

```bash
ATOMIC_RUN_WASM_EXAMPLE=1 \
ATOMIC_WASM_MANIFEST=examples/wasm_macro_task/build/manifest.toml \
cargo run --example wasm_macro_example

# output:
# input  = [1, 10, 100, 200]
# output = [2, 11, 101, 201]
```

## Comparison with `rust_wasm_task`

| | `rust_wasm_task` | `wasm_macro_task` |
|---|---|---|
| ABI | Manual C exports | `#[task]` macro |
| Encoding | Raw bytes | rkyv |
| `alloc` | `std::alloc` | Bump allocator |
| Boilerplate | ~40 lines | ~1 line |
