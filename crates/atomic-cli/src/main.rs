use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};

const DEFAULT_BUILD_TARGET: &str = "x86_64-unknown-linux-musl";
const CLUSTER_CONFIG_PATH: &str = ".atomic/cluster.toml";

#[derive(Parser)]
#[command(name = "atomic")]
#[command(about = "Atomic — distributed compute CLI (build, deploy, submit, stop)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Cross-compile the project binary for worker deployment
    Build(BuildArgs),
    /// Ship a compiled binary to remote workers and start them
    Deploy(DeployArgs),
    /// Build, deploy, and run the driver in one command
    Submit(SubmitArgs),
    /// Send a graceful shutdown to all workers in .atomic/cluster.toml
    Stop(StopArgs),
}

// ── Build ────────────────────────────────────────────────────────────────────

#[derive(Args)]
struct BuildArgs {
    /// Target triple for cross-compilation
    #[arg(long, default_value = DEFAULT_BUILD_TARGET)]
    target: String,

    /// Build in release mode (always true for worker targets)
    #[arg(long, default_value_t = true)]
    release: bool,

    /// Extra flags forwarded to `cargo build`
    #[arg(last = true)]
    cargo_args: Vec<String>,
}

// ── Deploy ───────────────────────────────────────────────────────────────────

#[derive(Args)]
struct DeployArgs {
    /// Comma-separated list of worker host names or IPs
    #[arg(long, value_delimiter = ',', required = true)]
    workers: Vec<String>,

    /// Path to the compiled binary to ship
    #[arg(long, required = true)]
    binary: PathBuf,

    /// Port each worker should listen on
    #[arg(long, default_value_t = 10000)]
    port: u16,

    /// SSH private key for authenticating to workers
    #[arg(long)]
    key: Option<PathBuf>,

    /// SSH user
    #[arg(long, default_value = "ubuntu")]
    user: String,

    /// Skip health-check after deploying
    #[arg(long, default_value_t = false)]
    no_healthcheck: bool,
}

// ── Submit ───────────────────────────────────────────────────────────────────

#[derive(Args)]
struct SubmitArgs {
    /// Comma-separated list of worker host names or IPs
    #[arg(long, value_delimiter = ',')]
    workers: Vec<String>,

    /// Target triple for cross-compilation (passed to `atomic build`)
    #[arg(long, default_value = DEFAULT_BUILD_TARGET)]
    target: String,

    /// Port each worker should listen on
    #[arg(long, default_value_t = 10000)]
    port: u16,

    /// SSH private key for authenticating to workers
    #[arg(long)]
    key: Option<PathBuf>,

    /// SSH user
    #[arg(long, default_value = "ubuntu")]
    user: String,

    /// Driver arguments passed after `--`
    #[arg(last = true)]
    driver_args: Vec<String>,
}

// ── Stop ─────────────────────────────────────────────────────────────────────

#[derive(Args)]
struct StopArgs {
    /// Comma-separated list of worker addresses (host:port). If omitted, reads .atomic/cluster.toml
    #[arg(long, value_delimiter = ',')]
    workers: Vec<String>,
}

// ── Cluster config ────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default)]
struct ClusterConfig {
    workers: Vec<WorkerEntry>,
}

#[derive(Serialize, Deserialize, Clone)]
struct WorkerEntry {
    address: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build(args) => cmd_build(args),
        Commands::Deploy(args) => cmd_deploy(args),
        Commands::Submit(args) => cmd_submit(args),
        Commands::Stop(args) => cmd_stop(args),
    }
}

// ── Build implementation ──────────────────────────────────────────────────────

fn cmd_build(args: BuildArgs) -> Result<()> {
    println!("Building for target: {}", args.target);

    let cross = which_cross()?;
    let mut cmd = Command::new(&cross);
    cmd.arg("build");

    if args.release {
        cmd.arg("--release");
    }

    cmd.arg("--target").arg(&args.target);

    for extra in &args.cargo_args {
        cmd.arg(extra);
    }

    println!("Running: {} {}", cross, format_args_display(&cmd));

    let status = cmd
        .status()
        .with_context(|| format!("failed to run `{cross}`"))?;

    if !status.success() {
        bail!("`{cross}` exited with status {}", status);
    }

    let profile = if args.release { "release" } else { "debug" };
    let bin_dir = PathBuf::from("target").join(&args.target).join(profile);
    println!("Build succeeded. Binaries in: {}", bin_dir.display());
    Ok(())
}

fn which_cross() -> Result<String> {
    // Prefer `cross` on PATH; fall back to `cargo` with a warning.
    if Command::new("cross").arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
        return Ok("cross".to_string());
    }
    eprintln!(
        "Warning: `cross` not found. Using `cargo build` directly — \
        cross-compilation for {DEFAULT_BUILD_TARGET} may fail without a matching toolchain.\n\
        Install cross: cargo install cross"
    );
    Ok("cargo".to_string())
}

fn format_args_display(cmd: &Command) -> String {
    let prog = cmd.get_program().to_string_lossy().to_string();
    let args: Vec<_> = cmd.get_args().map(|a| a.to_string_lossy().to_string()).collect();
    format!("{} {}", prog, args.join(" "))
}

// ── Deploy implementation ─────────────────────────────────────────────────────

fn cmd_deploy(args: DeployArgs) -> Result<()> {
    if !args.binary.exists() {
        bail!("binary not found: {}", args.binary.display());
    }

    let mut deployed: Vec<WorkerEntry> = Vec::new();

    for host in &args.workers {
        println!("Deploying to {}…", host);
        deploy_to_worker(host, &args)?;

        let address = format!("{}:{}", host, args.port);
        if !args.no_healthcheck {
            healthcheck(&address).with_context(|| format!("health-check failed for {}", address))?;
            println!("  ✓ {} is healthy", address);
        }

        deployed.push(WorkerEntry { address });
    }

    save_cluster_config(&deployed)?;
    println!(
        "\nDeployed {} worker(s). Addresses written to {}",
        deployed.len(),
        CLUSTER_CONFIG_PATH
    );
    for w in &deployed {
        println!("  {}", w.address);
    }
    Ok(())
}

fn deploy_to_worker(host: &str, args: &DeployArgs) -> Result<()> {
    let remote_dir = "/tmp/atomic-worker";
    let remote_path = format!("{}/worker", remote_dir);
    let user_host = format!("{}@{}", args.user, host);

    // 1. Create remote directory
    ssh_run(host, &args.user, args.key.as_deref(), &format!("mkdir -p {}", remote_dir))?;

    // 2. SCP binary
    let mut scp = Command::new("scp");
    if let Some(key) = &args.key {
        scp.arg("-i").arg(key);
    }
    scp.args(["-o", "StrictHostKeyChecking=no"])
        .arg(args.binary.as_os_str())
        .arg(format!("{}:{}", user_host, remote_path));

    let status = scp.status().context("failed to run scp")?;
    if !status.success() {
        bail!("scp failed for {}", host);
    }

    // 3. chmod and launch worker in background
    let launch_cmd = format!(
        "chmod +x {path} && nohup {path} --worker --port {port} > /tmp/atomic-worker.log 2>&1 &",
        path = remote_path,
        port = args.port,
    );
    ssh_run(host, &args.user, args.key.as_deref(), &launch_cmd)?;

    Ok(())
}

fn ssh_run(host: &str, user: &str, key: Option<&std::path::Path>, cmd: &str) -> Result<()> {
    let mut ssh = Command::new("ssh");
    if let Some(key) = key {
        ssh.arg("-i").arg(key);
    }
    ssh.args(["-o", "StrictHostKeyChecking=no"])
        .arg(format!("{}@{}", user, host))
        .arg(cmd);

    let status = ssh.status().with_context(|| format!("failed to ssh into {}", host))?;
    if !status.success() {
        bail!("ssh command failed on {}: {}", host, cmd);
    }
    Ok(())
}

/// TCP health-check: connect and read the WorkerCapabilities frame header (4 bytes).
/// If the worker accepts the connection within 10 seconds it is considered healthy.
fn healthcheck(address: &str) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect_timeout(
            &address.parse().with_context(|| format!("invalid address: {}", address))?,
            Duration::from_secs(1),
        ) {
            Ok(mut stream) => {
                // Just connecting is enough — the worker accepts connections once ready.
                // Drain any greeting bytes so the worker doesn't error on our disconnect.
                let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                let mut buf = [0u8; 16];
                let _ = stream.read(&mut buf);
                return Ok(());
            }
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => bail!("could not connect to {}: {}", address, e),
        }
    }
}

// ── Submit implementation ─────────────────────────────────────────────────────

fn cmd_submit(args: SubmitArgs) -> Result<()> {
    // 1. Build
    cmd_build(BuildArgs {
        target: args.target.clone(),
        release: true,
        cargo_args: vec![],
    })?;

    // 2. Locate the built binary (first bin in release profile)
    let bin_dir = PathBuf::from("target").join(&args.target).join("release");
    let binary = find_binary(&bin_dir)
        .with_context(|| format!("no binary found in {}", bin_dir.display()))?;

    // 3. Deploy (if workers given)
    if !args.workers.is_empty() {
        cmd_deploy(DeployArgs {
            workers: args.workers.clone(),
            binary,
            port: args.port,
            key: args.key.clone(),
            user: args.user.clone(),
            no_healthcheck: false,
        })?;
    }

    // 4. Run driver
    let worker_addrs: Vec<String> = if args.workers.is_empty() {
        load_cluster_config()?
            .workers
            .into_iter()
            .map(|w| w.address)
            .collect()
    } else {
        args.workers
            .iter()
            .map(|h| format!("{}:{}", h, args.port))
            .collect()
    };

    let driver_binary = find_binary(&PathBuf::from("target/release"))
        .or_else(|_| find_binary(&PathBuf::from("target/debug")))
        .context("no local driver binary found — run `cargo build` first")?;

    let mut driver_cmd = Command::new(&driver_binary);
    driver_cmd.arg("--driver");
    if !worker_addrs.is_empty() {
        driver_cmd.arg("--workers").arg(worker_addrs.join(","));
    }
    for extra in &args.driver_args {
        driver_cmd.arg(extra);
    }

    println!("Running driver: {}", driver_binary.display());
    let status = driver_cmd.status().context("failed to run driver")?;
    if !status.success() {
        bail!("driver exited with status {}", status);
    }
    Ok(())
}

fn find_binary(dir: &std::path::Path) -> Result<PathBuf> {
    if !dir.exists() {
        bail!("directory does not exist: {}", dir.display());
    }
    let entries = fs::read_dir(dir)?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            // Skip known non-binary extensions
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if matches!(ext, "d" | "rlib" | "rmeta" | "pdb" | "so" | "dylib" | "dll") {
                continue;
            }
            // On Unix, check executable bit
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if entry.metadata().map(|m| m.permissions().mode() & 0o111 != 0).unwrap_or(false) {
                    return Ok(path);
                }
            }
            #[cfg(not(unix))]
            {
                if ext == "exe" || ext.is_empty() {
                    return Ok(path);
                }
            }
        }
    }
    bail!("no executable binary found in {}", dir.display());
}

// ── Stop implementation ───────────────────────────────────────────────────────

fn cmd_stop(args: StopArgs) -> Result<()> {
    let addresses: Vec<String> = if args.workers.is_empty() {
        load_cluster_config()?
            .workers
            .into_iter()
            .map(|w| w.address)
            .collect()
    } else {
        args.workers
    };

    if addresses.is_empty() {
        println!("No workers to stop (no --workers given and {} is empty).", CLUSTER_CONFIG_PATH);
        return Ok(());
    }

    for address in &addresses {
        print!("Stopping {}… ", address);
        match stop_worker(address) {
            Ok(()) => println!("done"),
            Err(e) => println!("warning: {}", e),
        }
    }

    // Clear cluster config
    if PathBuf::from(CLUSTER_CONFIG_PATH).exists() {
        save_cluster_config(&[])?;
    }

    Ok(())
}

/// Send a graceful shutdown frame to a worker.
///
/// The wire protocol sends a 4-byte big-endian length followed by a single
/// byte `0x01` (ShutdownRequest opcode as used by the executor's transport layer).
/// If the worker has already stopped, the connection error is treated as success.
fn stop_worker(address: &str) -> Result<()> {
    let addr = address
        .parse()
        .with_context(|| format!("invalid address: {}", address))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .with_context(|| format!("could not connect to {}", address))?;
    stream.set_write_timeout(Some(Duration::from_secs(3))).ok();

    // Frame: 4-byte BE length (1) + 1-byte opcode (0x01 = shutdown)
    let frame: [u8; 5] = [0, 0, 0, 1, 0x01];
    let _ = stream.write_all(&frame);
    Ok(())
}

// ── Cluster config helpers ────────────────────────────────────────────────────

fn save_cluster_config(workers: &[WorkerEntry]) -> Result<()> {
    let config_path = PathBuf::from(CLUSTER_CONFIG_PATH);
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).context("failed to create .atomic directory")?;
    }
    let config = ClusterConfig { workers: workers.to_vec() };
    let toml = toml::to_string_pretty(&config).context("failed to serialize cluster config")?;
    fs::write(&config_path, toml)
        .with_context(|| format!("failed to write {}", config_path.display()))
}

fn load_cluster_config() -> Result<ClusterConfig> {
    let config_path = PathBuf::from(CLUSTER_CONFIG_PATH);
    if !config_path.exists() {
        return Ok(ClusterConfig::default());
    }
    let text = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    toml::from_str(&text).with_context(|| format!("failed to parse {}", config_path.display()))
}
