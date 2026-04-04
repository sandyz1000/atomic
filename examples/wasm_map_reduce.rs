use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use atomic_compute::context::Context;
use atomic_data::stub::WasmStub;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("ATOMIC_RUN_WASM_EXAMPLE").is_none() {
        print_usage();
        return Ok(());
    }

    if let Some(manifest_path) = env::var_os("ATOMIC_WASM_MANIFEST") {
        return run_with_manifest(Path::new(&manifest_path));
    }

    let map_module = env::var("ATOMIC_WASM_MAP_MODULE")
        .map(PathBuf::from)
        .map_err(|_| "ATOMIC_WASM_MAP_MODULE must point to a built wasm artifact")?;
    let reduce_module = env::var("ATOMIC_WASM_REDUCE_MODULE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| map_module.clone());

    ensure_module_exists(&map_module)?;
    ensure_module_exists(&reduce_module)?;

    let manifest_path = write_manifest(&map_module, &reduce_module)?;
    run_with_manifest(&manifest_path)
}

fn run_with_manifest(manifest_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    ensure_file_exists(manifest_path, "manifest")?;

    // Load context and manifest in one step.
    let ctx = Context::wasm_from_manifest(manifest_path)?;

    // Typed stubs — the compiler verifies Input/Output types against the RDD.
    let map_stub: WasmStub<u8, u8> = WasmStub::from_manifest(manifest_path, "demo.map.v1")?;
    let reduce_stub: WasmStub<u8, u32> =
        WasmStub::from_manifest(manifest_path, "demo.reduce.v1")?;

    let input = ctx.parallelize_typed(vec![1_u8, 2, 3, 4], 2);

    // map_via checks at compile time that input is TypedRdd<u8> and returns Vec<u8>.
    let mapped: Vec<u8> = input.collect_via(&map_stub)?;

    let mapped_rdd = ctx.parallelize_typed(mapped.clone(), 2);

    // map_via checks that mapped_rdd is TypedRdd<u8> and reduce_stub is WasmStub<u8, u32>.
    let partials: Vec<u32> = mapped_rdd.map_via(&reduce_stub)?;
    let total: u32 = partials.iter().copied().sum();

    println!("runtime={:?}", ctx.runtime());
    println!("map output = {:?}", mapped);
    println!("reduce partials = {:?}", partials);
    println!("total = {}", total);
    Ok(())
}

fn print_usage() {
    eprintln!(
        "End-to-end WASM map/reduce example.\n\
         See notes/wasm-map-reduce-runbook.md for the full flow.\n\
         Build the WASM task first, then set ATOMIC_WASM_MANIFEST:\n\
         \n\
           cargo run -p atomic-cli -- wasm build \\\n\
             --source examples/rust_wasm_task \\\n\
             --out examples/rust_wasm_task/build\n\
         \n\
           ATOMIC_RUN_WASM_EXAMPLE=1 \\\n\
           ATOMIC_WASM_MANIFEST=examples/rust_wasm_task/build/manifest.toml \\\n\
           cargo run --example wasm_map_reduce"
    );
}

fn ensure_module_exists(module_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    ensure_file_exists(module_path, "WASM module")
}

fn ensure_file_exists(path: &Path, kind: &str) -> Result<(), Box<dyn std::error::Error>> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("{} not found: {}", kind, path.display()).into())
    }
}

fn write_manifest(
    map_module: &Path,
    reduce_module: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest = format!(
        r#"schema_version = 1

[[wasm_artifacts]]
abi_version = 1
module_path = "{map}"

[wasm_artifacts.descriptor]
operation_id = "demo.map.v1"
execution_backend = "wasm"
artifact_kind = "wasm"
artifact_ref = "{map}"
entrypoint = "run_map"
runtime = "rust"
build_target = "wasm32-unknown-unknown"

[wasm_artifacts.descriptor.profile]
cpu_millis = 250
memory_mb = 128
timeout_ms = 1000

[[wasm_artifacts]]
abi_version = 1
module_path = "{reduce}"

[wasm_artifacts.descriptor]
operation_id = "demo.reduce.v1"
execution_backend = "wasm"
artifact_kind = "wasm"
artifact_ref = "{reduce}"
entrypoint = "run_reduce"
runtime = "rust"
build_target = "wasm32-unknown-unknown"

[wasm_artifacts.descriptor.profile]
cpu_millis = 250
memory_mb = 128
timeout_ms = 1000
"#,
        map = map_module.display(),
        reduce = reduce_module.display(),
    );

    let manifest_path = env::temp_dir().join("atomic-wasm-map-reduce-example.toml");
    fs::write(&manifest_path, manifest)?;
    Ok(manifest_path)
}
