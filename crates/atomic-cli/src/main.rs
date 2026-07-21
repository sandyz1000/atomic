use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

mod cluster;
mod commands;
atomic_data::cfg_k8s! {
    mod k8s;
}
mod ssh;

use commands::{cmd_build, cmd_ship, cmd_stop, cmd_submit};

type Result<T, E = CliError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliError {
    // Build
    #[error("`cargo zigbuild` exited with status {0}")]
    BuildFailed(std::process::ExitStatus),

    #[error(
        "`cargo-zigbuild` could not be installed automatically.\n\
        Run:  cargo install cargo-zigbuild\n\
        Then install Zig: https://ziglang.org/download/"
    )]
    ZigbuildInstallFailed,

    #[error("directory does not exist: {0}")]
    DirectoryNotFound(PathBuf),

    #[error("no executable binary found in {0}")]
    NoBinaryFound(PathBuf),

    // Ship
    #[error("binary not found: {0}")]
    BinaryNotFound(PathBuf),

    #[error("remote-path is not valid UTF-8")]
    RemotePathNotUtf8,

    #[error(
        "SHA-256 mismatch on {host} — transfer corrupted or tampered:\n  \
        expected: {expected}\n  got:      {got}"
    )]
    Sha256Mismatch {
        host: String,
        expected: String,
        got: String,
    },

    #[error("sha256sum failed on {host} (exit {code})")]
    RemoteSha256Failed { host: String, code: i32 },

    #[error("remote command failed (exit {code}):\n  cmd: {cmd}\n  out: {output}")]
    RemoteCommandFailed {
        code: i32,
        cmd: String,
        output: String,
    },

    // SSH / auth
    #[error("cannot locate ~/.ssh")]
    NoSshDir,

    #[error("cannot locate home directory")]
    NoHomeDir,

    #[error(
        "~/.ssh/known_hosts does not exist.\n\
        Add the host first:\n  ssh-keyscan -H {host} >> ~/.ssh/known_hosts"
    )]
    KnownHostsMissing { host: String },

    #[error(
        "Host '{host}' is not in ~/.ssh/known_hosts.\n\
        Add it with:\n  ssh-keyscan -H {host} >> ~/.ssh/known_hosts"
    )]
    HostNotKnown { host: String },

    #[error(
        "HOST KEY MISMATCH for '{host}'!\n\
        This may indicate a man-in-the-middle attack, or the host was reinstalled.\n\
        If the change is expected, remove the stale entry first:\n  ssh-keygen -R {host}\n\
        Then re-add:\n  ssh-keyscan -H {host} >> ~/.ssh/known_hosts\n\
        Underlying error: {source}"
    )]
    HostKeyMismatch {
        host: String,
        #[source]
        source: russh::keys::Error,
    },

    #[error("SSH key auth rejected for user '{user}' (key: {key})")]
    SshKeyAuthRejected { user: String, key: PathBuf },

    #[error(
        "SSH authentication failed for user '{user}'.\n\
        Use --key <path> to specify an SSH private key, or ensure one of\n\
        ~/.ssh/id_ed25519, ~/.ssh/id_ecdsa, ~/.ssh/id_rsa works for this host."
    )]
    SshAuthFailed { user: String },

    // Submit / stop
    #[error("driver exited with status {0}")]
    DriverFailed(std::process::ExitStatus),

    #[error("no local driver binary — run `cargo build` first")]
    NoLocalBinary,

    // submit-k8s errors live in their own enum so the whole k8s-only group carries a single
    // feature gate (one `#[cfg]` here) instead of one per variant — variants can't be macro-
    // gated (`cfg_k8s!` is item-position only; see atomic-rust-standards).
    #[cfg(feature = "k8s")]
    #[error(transparent)]
    K8s(#[from] k8s::K8sError),

    // Transparent wrappers
    #[error(transparent)]
    Russh(#[from] russh::Error),

    #[error(transparent)]
    RusshKeys(#[from] russh::keys::Error),

    #[error(transparent)]
    RusshSftp(#[from] russh_sftp::client::error::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    TomlSer(#[from] toml::ser::Error),

    #[error(transparent)]
    TomlDe(#[from] toml::de::Error),

    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),
}

pub(crate) const DEFAULT_BUILD_TARGET: &str = "x86_64-unknown-linux-musl";
/// Default remote destination for the shipped binary.
pub(crate) const DEFAULT_REMOTE_PATH: &str = "/opt/atomic/worker";

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
    /// Run the driver as an ad-hoc Kubernetes Job — no Helm release needed
    #[cfg(feature = "k8s")]
    SubmitK8s(SubmitK8sArgs),
}

#[derive(Args)]
pub(crate) struct BuildArgs {
    #[arg(long, default_value = DEFAULT_BUILD_TARGET)]
    pub(crate) target: String,
    #[arg(long, default_value_t = true)]
    pub(crate) release: bool,
    #[arg(last = true)]
    pub(crate) cargo_args: Vec<String>,
}

#[derive(Args)]
pub(crate) struct ShipArgs {
    #[arg(long, value_delimiter = ',', required = true)]
    pub(crate) workers: Vec<String>,
    #[arg(long, required = true)]
    pub(crate) binary: PathBuf,
    #[arg(long, default_value = DEFAULT_REMOTE_PATH)]
    pub(crate) remote_path: PathBuf,
    #[arg(long, default_value_t = 10000)]
    pub(crate) port: u16,
    #[arg(long)]
    pub(crate) key: Option<PathBuf>,
    #[arg(long, default_value = "ubuntu")]
    pub(crate) user: String,
}

#[derive(Args)]
pub(crate) struct SubmitArgs {
    #[arg(long, value_delimiter = ',')]
    pub(crate) workers: Vec<String>,
    #[arg(long, default_value = DEFAULT_BUILD_TARGET)]
    pub(crate) target: String,
    #[arg(long, default_value = DEFAULT_REMOTE_PATH)]
    pub(crate) remote_path: PathBuf,
    #[arg(long, default_value_t = 10000)]
    pub(crate) port: u16,
    #[arg(long)]
    pub(crate) key: Option<PathBuf>,
    #[arg(long, default_value = "ubuntu")]
    pub(crate) user: String,
    #[arg(last = true)]
    pub(crate) driver_args: Vec<String>,
}

#[derive(Args)]
pub(crate) struct StopArgs {
    #[arg(long, value_delimiter = ',')]
    pub(crate) workers: Vec<String>,
}

atomic_data::cfg_k8s! {
/// Exactly one of `--image` (pre-built, already pushed) or `--binary` (staged to S3,
/// run through the generic `atomic-bootstrap` fetch-and-exec image — no image build).
#[derive(Args)]
#[group(required = true, multiple = false)]
pub(crate) struct ImageSource {
    #[arg(long)]
    pub(crate) image: Option<String>,
    #[arg(long)]
    pub(crate) binary: Option<PathBuf>,
}

#[derive(Args)]
pub(crate) struct SubmitK8sArgs {
    #[command(flatten)]
    pub(crate) source: ImageSource,

    // --binary only: where to stage the binary before the Job can fetch it.
    #[arg(long)]
    pub(crate) s3_bucket: Option<String>,
    #[arg(long)]
    pub(crate) s3_prefix: Option<String>,
    /// Fetch-and-exec image; defaults to the project's published `atomic-bootstrap`.
    #[arg(long)]
    pub(crate) bootstrap_image: Option<String>,

    #[arg(long, default_value = "default")]
    pub(crate) namespace: String,
    #[arg(long)]
    pub(crate) service_account: Option<String>,

    /// Let the submitted driver provision its own per-job worker pods via
    /// `ctx.with_workers(...)` (sets `ATOMIC_ALLOCATOR=kube` on the driver). The
    /// worker count/CPU/memory is a `ResourceProfile` the job's own code constructs —
    /// this flag only makes the Kube allocator available to it.
    #[arg(long)]
    pub(crate) dynamic_workers: bool,
    /// Image for per-job worker pods when `--dynamic-workers` is set. Defaults to
    /// `--image` (workers run the driver's own image) — required with `--binary`,
    /// since the fetch-and-exec bootstrap path only applies to the driver Job.
    #[arg(long)]
    pub(crate) worker_image: Option<String>,
    #[arg(long, default_value_t = 10001)]
    pub(crate) task_port: u16,

    #[arg(long)]
    pub(crate) ttl_seconds_after_finished: Option<i32>,
    #[arg(last = true)]
    pub(crate) job_args: Vec<String>,
}
} // cfg_k8s!

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build(args) => cmd_build(args),
        Commands::Ship(args) => cmd_ship(args).await,
        Commands::Submit(args) => cmd_submit(args).await,
        Commands::Stop(args) => cmd_stop(args),
        #[cfg(feature = "k8s")]
        Commands::SubmitK8s(args) => k8s::cmd_submit_k8s(args).await,
    }
}
