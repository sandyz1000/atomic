/// End-to-end WASM example using the `#[task]` macro task.
///
/// Shows `Context::wasm_from_manifest` + `WasmStub` + `TypedRdd::collect_via`.
/// The WASM artifact must be built first with `atomic-cli`.
///
/// # Build the WASM artifact
///
///   rustup target add wasm32-unknown-unknown
///
///   cargo run -p atomic-cli -- wasm build \
///     --source examples/wasm_macro_task \
///     --out    examples/wasm_macro_task/build \
///     --map_op_id      demo.incr.v1 \
///     --map_entrypoint run_map
///
/// # Run this example
///
///   ATOMIC_RUN_WASM_EXAMPLE=1 \
///   ATOMIC_WASM_MANIFEST=examples/wasm_macro_task/build/manifest.toml \
///   cargo run --example wasm_macro_example
use std::env;

use atomic_compute::context::Context;
use atomic_data::stub::WasmStub;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("ATOMIC_RUN_WASM_EXAMPLE").is_none() {
        eprintln!(
            "Build the WASM task first, then run:\n\
             \n\
             \x20 rustup target add wasm32-unknown-unknown\n\
             \n\
             \x20 cargo run -p atomic-cli -- wasm build \\\n\
             \x20   --source examples/wasm_macro_task \\\n\
             \x20   --out    examples/wasm_macro_task/build \\\n\
             \x20   --map_op_id      demo.incr.v1 \\\n\
             \x20   --map_entrypoint run_map\n\
             \n\
             \x20 ATOMIC_RUN_WASM_EXAMPLE=1 \\\n\
             \x20 ATOMIC_WASM_MANIFEST=examples/wasm_macro_task/build/manifest.toml \\\n\
             \x20 cargo run --example wasm_macro_example"
        );
        return Ok(());
    }

    let manifest = env::var("ATOMIC_WASM_MANIFEST")
        .expect("ATOMIC_WASM_MANIFEST must point to the built manifest.toml");

    // Load a WASM context and register the manifest's artifacts in one step.
    let ctx = Context::wasm_from_manifest(&manifest)?;

    // Typed stub — compiler enforces Input=u8, Output=u8 matches the RDD type.
    let stub: WasmStub<u8, u8> = WasmStub::from_manifest(&manifest, "demo.incr.v1")?;

    println!("runtime = {:?}", ctx.runtime());

    // Parallelize data into 2 partitions.
    let input = ctx.parallelize_typed(vec![1u8, 10, 100, 200], 2);

    // collect_via dispatches each partition to the WASM module and flattens results.
    let result: Vec<u8> = input.collect_via(&stub)?;

    println!("input  = [1, 10, 100, 200]");
    println!("output = {result:?}"); // [2, 11, 101, 201]

    Ok(())
}
