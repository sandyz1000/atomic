use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use sha2::{Digest, Sha256};

const DEFAULT_MAP_OP_ID: &str = "demo.map.v1";
const DEFAULT_REDUCE_OP_ID: &str = "demo.reduce.v1";
const DEFAULT_MAP_ENTRYPOINT: &str = "run_map";
const DEFAULT_REDUCE_ENTRYPOINT: &str = "run_reduce";
const DEFAULT_WAT_BUILD_TARGET: &str = "wasm32-wasip2";
const DEFAULT_RUST_BUILD_TARGET: &str = "wasm32-unknown-unknown";
const WAT_TEMPLATE_FILE: &str = "task.wat";
const CARGO_MANIFEST_FILE: &str = "Cargo.toml";

#[derive(Parser)]
#[command(name = "atomic")]
#[command(about = "Atomic CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Wasm(WasmCommands),
}

#[derive(Args)]
struct WasmCommands {
    #[command(subcommand)]
    command: WasmSubcommand,
}

#[derive(Subcommand)]
enum WasmSubcommand {
    Init(InitArgs),
    Build(BuildArgs),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum WasmTemplate {
    Rust,
    Wat,
}

#[derive(Args)]
struct InitArgs {
    #[arg(long)]
    dir: PathBuf,

    #[arg(long, default_value = "map_reduce_task")]
    name: String,

    #[arg(long, value_enum, default_value_t = WasmTemplate::Rust)]
    template: WasmTemplate,

    #[arg(long, default_value_t = false)]
    force: bool,
}

#[derive(Args)]
struct BuildArgs {
    #[arg(long)]
    source: PathBuf,

    #[arg(long)]
    out: PathBuf,

    #[arg(long)]
    build_target: Option<String>,

    #[arg(long, default_value = DEFAULT_MAP_OP_ID)]
    map_op_id: String,

    #[arg(long, default_value = DEFAULT_REDUCE_OP_ID)]
    reduce_op_id: String,

    #[arg(long, default_value = DEFAULT_MAP_ENTRYPOINT)]
    map_entrypoint: String,

    #[arg(long, default_value = DEFAULT_REDUCE_ENTRYPOINT)]
    reduce_entrypoint: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Wasm(wasm) => match wasm.command {
            WasmSubcommand::Init(args) => init_task(args),
            WasmSubcommand::Build(args) => build_task(args),
        },
    }
}

fn init_task(args: InitArgs) -> Result<()> {
    let root = args.dir.join(&args.name);
    if root.exists() {
        if !args.force {
            bail!(
                "destination already exists: {} (use --force to overwrite files in place)",
                root.display()
            );
        }
    } else {
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create {}", root.display()))?;
    }

    match args.template {
        WasmTemplate::Rust => init_rust_task(&root, &args.name)?,
        WasmTemplate::Wat => init_wat_task(&root, &args.name)?,
    }

    println!(
        "Created {} WASM task scaffold at {}",
        match args.template {
            WasmTemplate::Rust => "Rust",
            WasmTemplate::Wat => "WAT",
        },
        root.display()
    );
    println!("Next step:");
    println!(
        "  cargo run -p atomic-cli -- wasm build --source {} --out {}",
        root.display(),
        root.join("build").display()
    );
    Ok(())
}

fn build_task(args: BuildArgs) -> Result<()> {
    if !args.source.exists() {
        bail!("source path not found: {}", args.source.display());
    }

    if let Some(manifest_path) = resolve_rust_manifest(&args.source) {
        build_rust_task(args, manifest_path)
    } else {
        build_wat_task(args)
    }
}

fn init_rust_task(root: &Path, name: &str) -> Result<()> {
    let src_dir = root.join("src");
    fs::create_dir_all(&src_dir)
        .with_context(|| format!("failed to create {}", src_dir.display()))?;

    write_text_file(&root.join(CARGO_MANIFEST_FILE), &rust_cargo_template(name))?;
    write_text_file(&src_dir.join("lib.rs"), rust_lib_template())?;
    write_text_file(&root.join("README.md"), &rust_readme_template(name))?;
    Ok(())
}

fn init_wat_task(root: &Path, name: &str) -> Result<()> {
    write_text_file(&root.join(WAT_TEMPLATE_FILE), wat_task_template())?;
    write_text_file(&root.join("README.md"), &wat_readme_template(name))?;
    Ok(())
}

fn build_rust_task(args: BuildArgs, manifest_path: PathBuf) -> Result<()> {
    let crate_dir = manifest_path
        .parent()
        .context("rust wasm source manifest must have a parent directory")?;
    let package_name = read_package_name(&manifest_path)?;
    let artifact_name = sanitize_artifact_name(&package_name);
    let build_target = args
        .build_target
        .clone()
        .unwrap_or_else(|| DEFAULT_RUST_BUILD_TARGET.to_string());

    let output = Command::new("cargo")
        .arg("build")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("--release")
        .arg("--lib")
        .arg("--target")
        .arg(&build_target)
        .current_dir(crate_dir)
        .output()
        .with_context(|| format!("failed to run cargo for {}", manifest_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let details = if stderr.trim().is_empty() { stdout } else { stderr };
        bail!(
            "cargo build failed for {}\n{}",
            manifest_path.display(),
            details.trim()
        );
    }

    let built_wasm = crate_dir
        .join("target")
        .join(&build_target)
        .join("release")
        .join(format!("{}.wasm", artifact_name));
    let wasm = fs::read(&built_wasm)
        .with_context(|| format!("failed to read built wasm artifact {}", built_wasm.display()))?;

    emit_build_outputs(&args, &artifact_name, &wasm, &build_target)
}

fn build_wat_task(args: BuildArgs) -> Result<()> {
    if !args.source.is_file() {
        bail!(
            "WAT sources must be files; for Rust tasks pass a crate directory or Cargo.toml"
        );
    }

    let wat = fs::read_to_string(&args.source)
        .with_context(|| format!("failed to read {}", args.source.display()))?;
    let wasm = wat::parse_str(&wat)
        .with_context(|| format!("failed to compile WAT source {}", args.source.display()))?;
    let module_name = args
        .source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("task")
        .to_string();
    let build_target = args
        .build_target
        .clone()
        .unwrap_or_else(|| DEFAULT_WAT_BUILD_TARGET.to_string());

    emit_build_outputs(&args, &module_name, &wasm, &build_target)
}

fn emit_build_outputs(
    args: &BuildArgs,
    module_name: &str,
    wasm: &[u8],
    build_target: &str,
) -> Result<()> {
    fs::create_dir_all(&args.out)
        .with_context(|| format!("failed to create output dir {}", args.out.display()))?;

    let wasm_path = args.out.join(format!("{}.wasm", module_name));
    fs::write(&wasm_path, wasm)
        .with_context(|| format!("failed to write {}", wasm_path.display()))?;

    let digest = format!(
        "sha256:{}",
        Sha256::digest(wasm)
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    );
    let manifest = manifest_template(
        &wasm_path,
        &digest,
        &args.map_op_id,
        &args.reduce_op_id,
        &args.map_entrypoint,
        &args.reduce_entrypoint,
        build_target,
    );
    let manifest_path = args.out.join("manifest.toml");
    fs::write(&manifest_path, manifest)
        .with_context(|| format!("failed to write {}", manifest_path.display()))?;

    let build_info_path = args.out.join("build.env");
    fs::write(
        &build_info_path,
        format!(
            "ATOMIC_WASM_MANIFEST={}\nATOMIC_WASM_MAP_MODULE={}\nATOMIC_WASM_REDUCE_MODULE={}\n",
            manifest_path.display(),
            wasm_path.display(),
            wasm_path.display(),
        ),
    )
    .with_context(|| format!("failed to write {}", build_info_path.display()))?;

    println!("Built WASM artifact: {}", wasm_path.display());
    println!("Manifest: {}", manifest_path.display());
    println!("Digest: {}", digest);
    println!("Build target: {}", build_target);
    println!("Run with:");
    println!(
        "  ATOMIC_RUN_WASM_EXAMPLE=1 ATOMIC_WASM_MANIFEST={} cargo run --example wasm_map_reduce",
        manifest_path.display()
    );
    Ok(())
}

fn resolve_rust_manifest(source: &Path) -> Option<PathBuf> {
    if source.is_dir() {
        let manifest = source.join(CARGO_MANIFEST_FILE);
        return manifest.is_file().then_some(manifest);
    }

    source
        .is_file()
        .then_some(source)
        .and_then(|path| path.file_name().and_then(|name| name.to_str()).map(|name| (path, name)))
        .and_then(|(path, name)| (name == CARGO_MANIFEST_FILE).then(|| path.to_path_buf()))
}

fn read_package_name(manifest_path: &Path) -> Result<String> {
    let manifest = fs::read_to_string(manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let value: toml::Value = toml::from_str(&manifest)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
    value
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .map(ToOwned::to_owned)
        .context("Cargo.toml is missing package.name")
}

fn sanitize_artifact_name(name: &str) -> String {
    name.replace('-', "_")
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
}

fn manifest_template(
    wasm_path: &Path,
    digest: &str,
    map_op_id: &str,
    reduce_op_id: &str,
    map_entrypoint: &str,
    reduce_entrypoint: &str,
    build_target: &str,
) -> String {
    format!(
        "schema_version = 1\n\n[[wasm_artifacts]]\nabi_version = 1\nmodule_path = \"{}\"\n\n[wasm_artifacts.descriptor]\noperation_id = \"{}\"\nexecution_backend = \"wasm\"\nartifact_kind = \"wasm\"\nartifact_ref = \"{}\"\nentrypoint = \"{}\"\nruntime = \"rust\"\nartifact_digest = \"{}\"\nbuild_target = \"{}\"\n\n[wasm_artifacts.descriptor.profile]\ncpu_millis = 250\nmemory_mb = 128\ntimeout_ms = 1000\n\n[[wasm_artifacts]]\nabi_version = 1\nmodule_path = \"{}\"\n\n[wasm_artifacts.descriptor]\noperation_id = \"{}\"\nexecution_backend = \"wasm\"\nartifact_kind = \"wasm\"\nartifact_ref = \"{}\"\nentrypoint = \"{}\"\nruntime = \"rust\"\nartifact_digest = \"{}\"\nbuild_target = \"{}\"\n\n[wasm_artifacts.descriptor.profile]\ncpu_millis = 250\nmemory_mb = 128\ntimeout_ms = 1000\n",
        wasm_path.display(),
        map_op_id,
        wasm_path.display(),
        map_entrypoint,
        digest,
        build_target,
        wasm_path.display(),
        reduce_op_id,
        wasm_path.display(),
        reduce_entrypoint,
        digest,
        build_target,
    )
}

fn rust_cargo_template(name: &str) -> String {
    format!(
        "[package]\nname = \"{}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[lib]\ncrate-type = [\"cdylib\"]\n\n[profile.release]\nlto = true\nopt-level = \"s\"\npanic = \"abort\"\ncodegen-units = 1\nstrip = true\n\n[workspace]\n",
        name
    )
}

fn rust_readme_template(name: &str) -> String {
    format!(
        "# {}\n\nThis scaffold defines a Rust-authored WASM task for Atomic's current map/reduce runtime.\n\nFiles:\n- `Cargo.toml`: standalone crate manifest for a `cdylib` wasm build\n- `src/lib.rs`: exports `alloc`, `run_map`, and `run_reduce` for the worker ABI\n\nBuild it with:\n\n```sh\ncargo run -p atomic-cli -- wasm build --source . --out ./build\n```\n\nThen run the driver example with the generated manifest:\n\n```sh\nATOMIC_RUN_WASM_EXAMPLE=1 ATOMIC_WASM_MANIFEST=./build/manifest.toml cargo run --example wasm_map_reduce\n```\n\nThe default Rust target for this flow is `wasm32-unknown-unknown` because the worker expects a plain wasm module with exported memory and allocator symbols.\n",
        name
    )
}

fn rust_lib_template() -> &'static str {
    r#"use std::alloc::{Layout, alloc, dealloc};
use std::slice;

fn pack_result(ptr: *mut u8, len: usize) -> i64 {
    ((ptr as u64) << 32 | len as u64) as i64
}

#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: i32) -> i32 {
    let layout = Layout::from_size_align(len.max(1) as usize, 1).unwrap();
    unsafe { alloc(layout) as i32 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dealloc(ptr: i32, len: i32) {
    if ptr == 0 || len <= 0 {
        return;
    }

    let layout = Layout::from_size_align(len as usize, 1).unwrap();
    unsafe { dealloc(ptr as *mut u8, layout) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_map(ptr: i32, len: i32) -> i64 {
    let input = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    let mut output = input
        .iter()
        .map(|byte| byte.wrapping_add(1))
        .collect::<Vec<u8>>();
    let packed = pack_result(output.as_mut_ptr(), output.len());
    std::mem::forget(output);
    packed
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_reduce(ptr: i32, len: i32) -> i64 {
    let input = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    let sum = input.iter().fold(0_u32, |acc, byte| acc + u32::from(*byte));
    let mut output = sum.to_le_bytes().to_vec();
    let packed = pack_result(output.as_mut_ptr(), output.len());
    std::mem::forget(output);
    packed
}
"#
}

fn wat_readme_template(name: &str) -> String {
    format!(
        "# {}\n\nThis scaffold defines a byte-oriented WAT task for Atomic's current map/reduce runtime.\n\nFiles:\n- `task.wat`: exports `memory`, `alloc`, `run_map`, and `run_reduce`.\n\nBuild it with:\n\n```sh\ncargo run -p atomic-cli -- wasm build --source ./task.wat --out ./build\n```\n\nThen run the driver example with the generated manifest:\n\n```sh\nATOMIC_RUN_WASM_EXAMPLE=1 ATOMIC_WASM_MANIFEST=./build/manifest.toml cargo run --example wasm_map_reduce\n```\n",
        name
    )
}

fn wat_task_template() -> &'static str {
    r#"(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))

  (func $alloc (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    global.get $heap
    local.set $ptr
    global.get $heap
    local.get $len
    i32.add
    global.set $heap
    local.get $ptr)

  (func (export "run_map") (param $ptr i32) (param $len i32) (result i64)
    (local $out i32)
    (local $i i32)
    local.get $len
    call $alloc
    local.set $out
    block $done
      loop $copy
        local.get $i
        local.get $len
        i32.ge_u
        br_if $done
        local.get $out
        local.get $i
        i32.add
        local.get $ptr
        local.get $i
        i32.add
        i32.load8_u
        i32.const 1
        i32.add
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $copy
      end
    end
    local.get $out
    i64.extend_i32_u
    i64.const 32
    i64.shl
    local.get $len
    i64.extend_i32_u
    i64.or)

  (func (export "run_reduce") (param $ptr i32) (param $len i32) (result i64)
    (local $out i32)
    (local $i i32)
    (local $sum i32)
    i32.const 4
    call $alloc
    local.set $out
    block $done
      loop $sum_loop
        local.get $i
        local.get $len
        i32.ge_u
        br_if $done
        local.get $sum
        local.get $ptr
        local.get $i
        i32.add
        i32.load8_u
        i32.add
        local.set $sum
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $sum_loop
      end
    end
    local.get $out
    local.get $sum
    i32.store
    local.get $out
    i64.extend_i32_u
    i64.const 32
    i64.shl
    i64.const 4
    i64.or))"#
}
