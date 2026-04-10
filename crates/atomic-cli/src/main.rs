use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use russh::client::{self, Handle};
use russh::keys::{PrivateKeyWithHashAlg, check_known_hosts_path, load_secret_key};
use russh::ChannelMsg;
use russh_sftp::client::SftpSession;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;

const DEFAULT_BUILD_TARGET: &str = "x86_64-unknown-linux-musl";
const CLUSTER_CONFIG_PATH: &str = ".atomic/cluster.toml";
/// Default remote destination for the shipped binary.
/// Change with --remote-path if /opt is not writable on your workers.
const DEFAULT_REMOTE_PATH: &str = "/opt/atomic/worker";

// ── CLI skeleton ──────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "atomic")]
#[command(about = "Atomic — distributed compute CLI (build, ship, stop)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Cross-compile the project binary for worker deployment
    Build(BuildArgs),
    /// Ship a compiled binary to remote workers over SSH/SFTP
    Ship(ShipArgs),
    /// Build then ship in one step
    Submit(SubmitArgs),
    /// Send a graceful shutdown to all workers in .atomic/cluster.toml
    Stop(StopArgs),
}

// ── Build ─────────────────────────────────────────────────────────────────────

#[derive(Args)]
struct BuildArgs {
    /// Target triple for cross-compilation
    #[arg(long, default_value = DEFAULT_BUILD_TARGET)]
    target: String,
    /// Build in release mode
    #[arg(long, default_value_t = true)]
    release: bool,
    /// Extra flags forwarded to `cargo zigbuild`
    #[arg(last = true)]
    cargo_args: Vec<String>,
}

// ── Ship (formerly Deploy) ────────────────────────────────────────────────────

#[derive(Args)]
struct ShipArgs {
    /// Comma-separated list of worker host names or IPs
    #[arg(long, value_delimiter = ',', required = true)]
    workers: Vec<String>,
    /// Path to the compiled binary to ship
    #[arg(long, required = true)]
    binary: PathBuf,
    /// Absolute path where the binary is written on each worker
    #[arg(long, default_value = DEFAULT_REMOTE_PATH)]
    remote_path: PathBuf,
    /// Port recorded in the cluster config (for later use by the driver)
    #[arg(long, default_value_t = 10000)]
    port: u16,
    /// SSH private key; omit to try ~/.ssh/id_ed25519, id_rsa, id_ecdsa
    #[arg(long)]
    key: Option<PathBuf>,
    /// SSH user on each worker
    #[arg(long, default_value = "ubuntu")]
    user: String,
}

// ── Submit ────────────────────────────────────────────────────────────────────

#[derive(Args)]
struct SubmitArgs {
    #[arg(long, value_delimiter = ',')]
    workers: Vec<String>,
    #[arg(long, default_value = DEFAULT_BUILD_TARGET)]
    target: String,
    #[arg(long, default_value = DEFAULT_REMOTE_PATH)]
    remote_path: PathBuf,
    #[arg(long, default_value_t = 10000)]
    port: u16,
    #[arg(long)]
    key: Option<PathBuf>,
    #[arg(long, default_value = "ubuntu")]
    user: String,
    /// Driver arguments passed after `--`
    #[arg(last = true)]
    driver_args: Vec<String>,
}

// ── Stop ──────────────────────────────────────────────────────────────────────

#[derive(Args)]
struct StopArgs {
    /// Comma-separated list of worker addresses (host:port).
    /// If omitted, reads .atomic/cluster.toml.
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

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build(args) => cmd_build(args),
        Commands::Ship(args) => cmd_ship(args).await,
        Commands::Submit(args) => cmd_submit(args).await,
        Commands::Stop(args) => cmd_stop(args),
    }
}

// ── Build ─────────────────────────────────────────────────────────────────────

fn cmd_build(args: BuildArgs) -> Result<()> {
    ensure_zigbuild()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("zigbuild");
    if args.release {
        cmd.arg("--release");
    }
    cmd.arg("--target").arg(&args.target);
    for extra in &args.cargo_args {
        cmd.arg(extra);
    }

    println!("Running: {}", format_cmd(&cmd));
    let status = cmd.status().context("failed to run `cargo zigbuild`")?;
    if !status.success() {
        bail!("`cargo zigbuild` exited with status {}", status);
    }

    let profile = if args.release { "release" } else { "debug" };
    println!(
        "Build succeeded. Binaries in: {}",
        PathBuf::from("target").join(&args.target).join(profile).display()
    );
    Ok(())
}

/// Ensure `cargo-zigbuild` is installed, attempting `cargo install` if missing.
fn ensure_zigbuild() -> Result<()> {
    if is_available("cargo-zigbuild") {
        return Ok(());
    }
    eprintln!("`cargo-zigbuild` not found — installing…");
    let ok = Command::new("cargo")
        .args(["install", "cargo-zigbuild"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if ok {
        eprintln!("`cargo-zigbuild` installed.");
        return Ok(());
    }
    bail!(
        "`cargo-zigbuild` could not be installed automatically.\n\
        Run:  cargo install cargo-zigbuild\n\
        Then install Zig: https://ziglang.org/download/"
    )
}

// ── Ship ──────────────────────────────────────────────────────────────────────

async fn cmd_ship(args: ShipArgs) -> Result<()> {
    if !args.binary.exists() {
        bail!("binary not found: {}", args.binary.display());
    }

    // Compute SHA-256 locally once — verified on each remote after transfer.
    let binary_data = fs::read(&args.binary)
        .with_context(|| format!("cannot read {}", args.binary.display()))?;
    let local_sha256 = sha256_hex(&binary_data);
    println!("SHA-256: {}", local_sha256);
    println!("Size:    {} bytes", binary_data.len());

    let remote_path = args
        .remote_path
        .to_str()
        .context("remote-path is not valid UTF-8")?
        .to_owned();

    let mut deployed: Vec<WorkerEntry> = Vec::new();

    for host in &args.workers {
        println!("\nShipping to {}…", host);
        ship_to_host(host, &args.user, args.key.as_deref(), &binary_data, &local_sha256, &remote_path)
            .await
            .with_context(|| format!("failed to ship to {}", host))?;
        println!("  ✓ {} — binary at {}", host, remote_path);
        deployed.push(WorkerEntry { address: format!("{}:{}", host, args.port) });
    }

    save_cluster_config(&deployed)?;
    println!(
        "\nShipped to {} worker(s). Addresses written to {}",
        deployed.len(),
        CLUSTER_CONFIG_PATH
    );
    Ok(())
}

/// SSH connect → authenticate → mkdir → SFTP upload → verify SHA-256 → atomic rename.
async fn ship_to_host(
    host: &str,
    user: &str,
    key_path: Option<&Path>,
    binary_data: &[u8],
    expected_sha256: &str,
    remote_path: &str,
) -> Result<()> {
    // ── 1. Connect with host-key verification ─────────────────────────────────
    //
    // Refuses unknown hosts (no StrictHostKeyChecking=no).
    // The caller must have already run `ssh-keyscan -H <host> >> ~/.ssh/known_hosts`.
    let config = Arc::new(client::Config::default());
    let checker = KnownHostsChecker { host: host.to_owned(), port: 22 };
    let mut session = client::connect(config, (host, 22u16), checker)
        .await
        .with_context(|| format!("SSH connect to {} failed", host))?;

    // ── 2. Authenticate ───────────────────────────────────────────────────────
    authenticate(&mut session, user, key_path).await?;

    // ── 3. Create remote directory (exec) ─────────────────────────────────────
    let remote_dir = Path::new(remote_path)
        .parent()
        .and_then(|p| p.to_str())
        .unwrap_or("/opt/atomic");
    exec_checked(&mut session, &format!("mkdir -p {}", shell_quote(remote_dir))).await?;

    // ── 4. Upload to a temp path via SFTP ────────────────────────────────────
    //
    // Writing to `<path>.tmp` means the final destination always contains a
    // complete binary — the rename in step 6 is atomic on POSIX systems.
    let tmp_path = format!("{}.tmp", remote_path);
    {
        let channel = session.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;

        let mut remote_file = sftp
            .create(&tmp_path)
            .await
            .with_context(|| format!("cannot create remote file {}", tmp_path))?;
        remote_file.write_all(binary_data).await?;
        remote_file.shutdown().await?;
        // remote_file dropped — SFTP channel closes here.
    }

    // ── 5. Verify SHA-256 on the remote ──────────────────────────────────────
    //
    // Catches bit-rot, truncated transfers, and any tampering between our
    // SCP upload and the rename.
    let (output, code) =
        exec_output(&mut session, &format!("sha256sum {}", shell_quote(&tmp_path))).await?;
    if code != 0 {
        let _ = exec_output(&mut session, &format!("rm -f {}", shell_quote(&tmp_path))).await;
        bail!("sha256sum failed on {} (exit {})", host, code);
    }
    let remote_sha256 = output.split_whitespace().next().unwrap_or("").trim().to_owned();
    if remote_sha256 != expected_sha256 {
        let _ = exec_output(&mut session, &format!("rm -f {}", shell_quote(&tmp_path))).await;
        bail!(
            "SHA-256 mismatch on {} — transfer corrupted or tampered:\n  expected: {}\n  got:      {}",
            host, expected_sha256, remote_sha256
        );
    }

    // ── 6. Atomic rename + set permissions ────────────────────────────────────
    //
    // chmod before mv: the binary is only visible at the final path once it is
    // both complete and executable.
    exec_checked(
        &mut session,
        &format!(
            "chmod 755 {} && mv {} {}",
            shell_quote(&tmp_path),
            shell_quote(&tmp_path),
            shell_quote(remote_path),
        ),
    )
    .await?;

    session.disconnect(russh::Disconnect::ByApplication, "", "English").await.ok();
    Ok(())
}

// ── SSH host-key verification ─────────────────────────────────────────────────

struct KnownHostsChecker {
    host: String,
    port: u16,
}

impl client::Handler for KnownHostsChecker {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        let known_hosts = ssh_dir()
            .map(|d| d.join("known_hosts"))
            .context("cannot locate ~/.ssh/known_hosts")?;

        if !known_hosts.exists() {
            bail!(
                "~/.ssh/known_hosts does not exist.\n\
                Add the host first:\n  ssh-keyscan -H {} >> ~/.ssh/known_hosts",
                self.host
            );
        }

        match check_known_hosts_path(&self.host, self.port, server_public_key, &known_hosts) {
            // Key found and matches — safe to continue.
            Ok(true) => Ok(true),

            // Host not in known_hosts — refuse rather than auto-accept.
            Ok(false) => bail!(
                "Host '{}' is not in ~/.ssh/known_hosts.\n\
                Add it with:\n  ssh-keyscan -H {} >> ~/.ssh/known_hosts",
                self.host,
                self.host
            ),

            // Key mismatch — warn loudly; this is a potential MITM.
            Err(e) if e.to_string().to_lowercase().contains("changed") => bail!(
                "HOST KEY MISMATCH for '{}'!\n\
                This may indicate a man-in-the-middle attack, or the host was reinstalled.\n\
                If the change is expected, remove the stale entry first:\n\
                  ssh-keygen -R {}\n\
                Then re-add:\n  ssh-keyscan -H {} >> ~/.ssh/known_hosts\n\
                Underlying error: {}",
                self.host,
                self.host,
                self.host,
                e
            ),

            Err(e) => Err(e.into()),
        }
    }
}

// ── SSH authentication ────────────────────────────────────────────────────────

async fn authenticate(
    session: &mut Handle<KnownHostsChecker>,
    user: &str,
    key_path: Option<&Path>,
) -> Result<()> {
    // Explicit key takes priority.
    if let Some(path) = key_path {
        let key = load_secret_key(path, None)
            .with_context(|| format!("cannot load SSH key: {}", path.display()))?;
        let auth_key = PrivateKeyWithHashAlg::new(Arc::new(key), None);
        let result = session
            .authenticate_publickey(user, auth_key)
            .await
            .context("SSH key auth request failed")?;
        if !result.success() {
            bail!("SSH key auth rejected for user '{}' (key: {})", user, path.display());
        }
        return Ok(());
    }

    // Fall back to default key files in preference order.
    let ssh_dir = ssh_dir().context("cannot locate home directory")?;
    for name in &["id_ed25519", "id_ecdsa", "id_rsa"] {
        let path = ssh_dir.join(name);
        if !path.exists() {
            continue;
        }
        let Ok(key) = load_secret_key(&path, None) else {
            continue;
        };
        let auth_key = PrivateKeyWithHashAlg::new(Arc::new(key), None);
        if session.authenticate_publickey(user, auth_key).await.map(|r| r.success()).unwrap_or(false) {
            return Ok(());
        }
    }

    bail!(
        "SSH authentication failed for user '{}'.\n\
        Use --key <path> to specify an SSH private key, or ensure one of\n\
        ~/.ssh/id_ed25519, ~/.ssh/id_ecdsa, ~/.ssh/id_rsa works for this host.",
        user
    )
}

// ── Remote exec helpers ───────────────────────────────────────────────────────

/// Run a remote command; bail if it exits non-zero.
async fn exec_checked(session: &mut Handle<KnownHostsChecker>, cmd: &str) -> Result<()> {
    let (output, code) = exec_output(session, cmd).await?;
    if code != 0 {
        bail!("remote command failed (exit {}):\n  cmd: {}\n  out: {}", code, cmd, output.trim());
    }
    Ok(())
}

/// Run a remote command and return (stdout, exit_code).
async fn exec_output(
    session: &mut Handle<KnownHostsChecker>,
    cmd: &str,
) -> Result<(String, i32)> {
    let mut channel = session
        .channel_open_session()
        .await
        .context("failed to open SSH channel")?;
    channel.exec(true, cmd).await?;

    let mut stdout = Vec::new();
    let mut exit_code = 0i32;
    loop {
        match channel.wait().await {
            Some(ChannelMsg::Data { data }) => stdout.extend_from_slice(&data),
            Some(ChannelMsg::ExitStatus { exit_status }) => {
                exit_code = exit_status as i32;
            }
            // Ignore stderr (ExtendedData) and other control messages.
            Some(ChannelMsg::Eof) | None => break,
            _ => {}
        }
    }

    Ok((String::from_utf8_lossy(&stdout).into_owned(), exit_code))
}

// ── Submit ────────────────────────────────────────────────────────────────────

async fn cmd_submit(args: SubmitArgs) -> Result<()> {
    // 1. Build
    cmd_build(BuildArgs {
        target: args.target.clone(),
        release: true,
        cargo_args: vec![],
    })?;

    // 2. Locate built binary
    let bin_dir = PathBuf::from("target").join(&args.target).join("release");
    let binary = find_binary(&bin_dir)
        .with_context(|| format!("no binary found in {}", bin_dir.display()))?;

    // 3. Ship to workers
    if !args.workers.is_empty() {
        cmd_ship(ShipArgs {
            workers: args.workers.clone(),
            binary,
            remote_path: args.remote_path,
            port: args.port,
            key: args.key.clone(),
            user: args.user.clone(),
        })
        .await?;
    }

    // 4. Run driver locally
    let worker_addrs: Vec<String> = if args.workers.is_empty() {
        load_cluster_config()?
            .workers
            .into_iter()
            .map(|w| w.address)
            .collect()
    } else {
        args.workers.iter().map(|h| format!("{}:{}", h, args.port)).collect()
    };

    let driver_binary = find_binary(&PathBuf::from("target/release"))
        .or_else(|_| find_binary(&PathBuf::from("target/debug")))
        .context("no local driver binary — run `cargo build` first")?;

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

// ── Stop ──────────────────────────────────────────────────────────────────────

fn cmd_stop(args: StopArgs) -> Result<()> {
    let addresses: Vec<String> = if args.workers.is_empty() {
        load_cluster_config()?.workers.into_iter().map(|w| w.address).collect()
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

    if PathBuf::from(CLUSTER_CONFIG_PATH).exists() {
        save_cluster_config(&[])?;
    }
    Ok(())
}

/// Send a graceful shutdown frame to a worker.
fn stop_worker(address: &str) -> Result<()> {
    let addr = address.parse().with_context(|| format!("invalid address: {}", address))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .with_context(|| format!("could not connect to {}", address))?;
    stream.set_write_timeout(Some(Duration::from_secs(3))).ok();
    // Frame: 4-byte BE length (1) + 1-byte opcode (0x01 = shutdown)
    let _ = stream.write_all(&[0, 0, 0, 1, 0x01]);
    Ok(())
}

// ── Utilities ─────────────────────────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Wrap a string in single quotes, escaping any embedded single quotes.
/// Safe for use in remote shell commands that contain paths.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn ssh_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".ssh"))
}

fn is_available(bin: &str) -> bool {
    Command::new(bin).arg("--version").output().map(|o| o.status.success()).unwrap_or(false)
}

fn format_cmd(cmd: &Command) -> String {
    let prog = cmd.get_program().to_string_lossy().to_string();
    let args: Vec<_> = cmd.get_args().map(|a| a.to_string_lossy().to_string()).collect();
    format!("{} {}", prog, args.join(" "))
}

fn find_binary(dir: &Path) -> Result<PathBuf> {
    if !dir.exists() {
        bail!("directory does not exist: {}", dir.display());
    }
    for entry in fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if matches!(ext, "d" | "rlib" | "rmeta" | "pdb" | "so" | "dylib" | "dll") {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if entry.metadata().map(|m| m.permissions().mode() & 0o111 != 0).unwrap_or(false) {
                return Ok(path);
            }
        }
        #[cfg(not(unix))]
        if ext == "exe" || ext.is_empty() {
            return Ok(path);
        }
    }
    bail!("no executable binary found in {}", dir.display())
}

// ── Cluster config ────────────────────────────────────────────────────────────

fn save_cluster_config(workers: &[WorkerEntry]) -> Result<()> {
    let config_path = PathBuf::from(CLUSTER_CONFIG_PATH);
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).context("failed to create .atomic directory")?;
    }
    let toml = toml::to_string_pretty(&ClusterConfig { workers: workers.to_vec() })
        .context("failed to serialize cluster config")?;
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
