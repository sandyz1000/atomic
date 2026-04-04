use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use atomic_compute::context::Context;
use atomic_compute::rdd::WasmBytesRddExt;

const OP_ID: &str = "demo.bytes.v1";
const ENTRYPOINT: &str = "run_map";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("ATOMIC_RUN_WASM_EXAMPLE").is_none() {
        print_usage();
        return Ok(());
    }

    let module_path = env::var("ATOMIC_WASM_MODULE")
        .map(PathBuf::from)
        .map_err(|_| "ATOMIC_WASM_MODULE must point to a built wasm artifact")?;
    ensure_module_exists(&module_path)?;

    let manifest_path = write_manifest(&module_path)?;

    let ctx = Context::wasm()?;
    ctx.load_artifact_manifest_toml(&manifest_path)?;

    let bytes = ctx.parallelize_typed(vec![1_u8, 2, 3, 4], 2);
    let output = bytes.collect_wasm(OP_ID)?;

    println!(
        "runtime={:?} op={} output={:?}",
        ctx.runtime(),
        OP_ID,
        output
    );
    Ok(())
}

fn print_usage() {
    eprintln!(
        "This example shows explicit WASM runtime setup.\n\
         It expects a distributed Atomic setup with a worker configured for the WASM backend.\n\
         The WASM module must already be built ahead of time; the scheduler does not compile RDDs into WASM at runtime.\n\
         Example:\n\
           ATOMIC_RUN_WASM_EXAMPLE=1 \\\n         ATOMIC_WASM_MODULE=/abs/path/to/demo_bytes.wasm \\\n         cargo run --example wasm_runtime"
    );
}

fn ensure_module_exists(module_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if module_path.is_file() {
        Ok(())
    } else {
        Err(format!("WASM module not found: {}", module_path.display()).into())
    }
}

fn write_manifest(module_path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest = format!(
        "schema_version = 1\n\n[[wasm_artifacts]]\nabi_version = 1\nmodule_path = \"{}\"\n\n[wasm_artifacts.descriptor]\noperation_id = \"{}\"\nexecution_backend = \"wasm\"\nartifact_kind = \"wasm\"\nartifact_ref = \"{}\"\nentrypoint = \"{}\"\nruntime = \"rust\"\nbuild_target = \"wasm32-wasip2\"\n\n[wasm_artifacts.descriptor.profile]\ncpu_millis = 250\nmemory_mb = 128\ntimeout_ms = 1000\n",
        module_path.display(),
        OP_ID,
        module_path.display(),
        ENTRYPOINT,
    );

    let manifest_path = env::temp_dir().join("atomic-wasm-runtime-example.toml");
    fs::write(&manifest_path, manifest)?;
    Ok(manifest_path)
}
