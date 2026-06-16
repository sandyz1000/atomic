use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

mod cluster;
mod commands;
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
