//! Shared CLI shape for every `integration_*` binary in this package: each one
//! is either a worker (`--worker --port N`) or a driver that dispatches to a set
//! of workers (`--driver --workers host:port[,...]`).
//!
//! Centralised here instead of duplicated per binary — every `integration_*.rs`
//! file pulls it in with `#[path = "cli.rs"] mod cli;`.

use std::net::SocketAddrV4;

use clap::Parser;

#[derive(Parser, Debug)]
pub struct IntegrationCli {
    /// Run as a worker, listening on `--port`.
    #[arg(long)]
    pub worker: bool,

    /// Run as a driver, dispatching to `--workers`.
    #[arg(long)]
    pub driver: bool,

    /// Listening port (worker mode).
    #[arg(long)]
    pub port: Option<u16>,

    /// Comma-separated `host:port` worker list (driver mode).
    #[arg(long, value_delimiter = ',')]
    pub workers: Vec<SocketAddrV4>,
}
