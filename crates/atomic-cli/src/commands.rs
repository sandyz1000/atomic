use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::cluster::{WorkerEntry, load_cluster_config, save_cluster_config};
use crate::ssh::{sha256_hex, ship_to_host};
use crate::{BuildArgs, CliError, Result, ShipArgs, StopArgs, SubmitArgs};

pub(crate) fn cmd_build(args: BuildArgs) -> Result<()> {
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
    let status = cmd.status()?;
    if !status.success() {
        return Err(CliError::BuildFailed(status));
    }

    let profile = if args.release { "release" } else { "debug" };
    println!(
        "Build succeeded. Binaries in: {}",
        PathBuf::from("target")
            .join(&args.target)
            .join(profile)
            .display()
    );
    Ok(())
}

pub(crate) async fn cmd_ship(args: ShipArgs) -> Result<()> {
    if !args.binary.exists() {
        return Err(CliError::BinaryNotFound(args.binary));
    }

    let binary_data = fs::read(&args.binary)?;
    let local_sha256 = sha256_hex(&binary_data);
    println!("SHA-256: {}", local_sha256);
    println!("Size:    {} bytes", binary_data.len());

    let remote_path = args
        .remote_path
        .to_str()
        .ok_or(CliError::RemotePathNotUtf8)?
        .to_owned();

    let mut deployed: Vec<WorkerEntry> = Vec::new();

    for host in &args.workers {
        println!("\nShipping to {}…", host);
        ship_to_host(
            host,
            &args.user,
            args.key.as_deref(),
            &binary_data,
            &local_sha256,
            &remote_path,
        )
        .await?;
        println!("  ✓ {} — binary at {}", host, remote_path);
        deployed.push(WorkerEntry {
            address: format!("{}:{}", host, args.port),
        });
    }

    save_cluster_config(&deployed)?;
    println!(
        "\nShipped to {} worker(s). Addresses written to {}",
        deployed.len(),
        crate::cluster::CLUSTER_CONFIG_PATH
    );
    Ok(())
}

pub(crate) async fn cmd_submit(args: SubmitArgs) -> Result<()> {
    cmd_build(BuildArgs {
        target: args.target.clone(),
        release: true,
        cargo_args: vec![],
    })?;

    let bin_dir = PathBuf::from("target").join(&args.target).join("release");
    let binary = find_binary(&bin_dir)?;

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

    let driver_binary = match find_binary(&PathBuf::from("target/release"))
        .or_else(|_| find_binary(&PathBuf::from("target/debug")))
    {
        Ok(b) => b,
        Err(_) => return Err(CliError::NoLocalBinary),
    };

    let mut driver_cmd = Command::new(&driver_binary);
    driver_cmd.arg("--driver");
    if !worker_addrs.is_empty() {
        driver_cmd.arg("--workers").arg(worker_addrs.join(","));
    }
    for extra in &args.driver_args {
        driver_cmd.arg(extra);
    }

    println!("Running driver: {}", driver_binary.display());
    let status = driver_cmd.status()?;
    if !status.success() {
        return Err(CliError::DriverFailed(status));
    }
    Ok(())
}

pub(crate) fn cmd_stop(args: StopArgs) -> Result<()> {
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
        println!(
            "No workers to stop (no --workers given and {} is empty).",
            crate::cluster::CLUSTER_CONFIG_PATH
        );
        return Ok(());
    }

    for address in &addresses {
        print!("Stopping {}… ", address);
        match stop_worker(address) {
            Ok(()) => println!("done"),
            Err(e) => println!("warning: {}", e),
        }
    }

    if PathBuf::from(crate::cluster::CLUSTER_CONFIG_PATH).exists() {
        save_cluster_config(&[])?;
    }
    Ok(())
}

fn stop_worker(address: &str) -> Result<()> {
    let addr = address.parse()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))?;
    stream.set_write_timeout(Some(Duration::from_secs(3))).ok();
    // Frame: 4-byte BE length (1) + 1-byte opcode (0x01 = shutdown)
    let _ = stream.write_all(&[0, 0, 0, 1, 0x01]);
    Ok(())
}

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
    Err(CliError::ZigbuildInstallFailed)
}

fn is_available(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn format_cmd(cmd: &Command) -> String {
    let prog = cmd.get_program().to_string_lossy().to_string();
    let args: Vec<_> = cmd
        .get_args()
        .map(|a| a.to_string_lossy().to_string())
        .collect();
    format!("{} {}", prog, args.join(" "))
}

pub(crate) fn find_binary(dir: &Path) -> Result<PathBuf> {
    if !dir.exists() {
        return Err(CliError::DirectoryNotFound(dir.to_path_buf()));
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
            if entry
                .metadata()
                .map(|m| m.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
            {
                return Ok(path);
            }
        }
        #[cfg(not(unix))]
        if ext == "exe" || ext.is_empty() {
            return Ok(path);
        }
    }
    Err(CliError::NoBinaryFound(dir.to_path_buf()))
}
