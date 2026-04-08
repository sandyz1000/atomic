//! Distributed integration test: real TCP round-trip between driver and worker.
//!
//! Spawns the `integration` example binary twice — once as a worker, once as a
//! driver — and verifies that the driver's JSON output matches the expected values.
//!
//! The test is excluded from `cargo test --workspace` by default because it
//! requires a compiled `integration` binary. Run it with:
//!
//!   cargo test -p atomic-tests test_distributed -- --nocapture

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const WORKER_PORT: u16 = 19201;

fn integration_bin() -> std::path::PathBuf {
    // CARGO_BIN_EXE_integration is a compile-time env var set by Cargo when
    // `integration` is declared as a [[bin]] in this crate's Cargo.toml.
    // Use env!() (compile-time), not std::env::var() (runtime).
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration"))
}

fn start_worker() -> Child {
    Command::new(integration_bin())
        .args(["--worker", "--port", &WORKER_PORT.to_string()])
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn integration worker")
}

fn wait_for_port(port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not come up on port {} within {:?}",
            port,
            timeout
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn distributed_map_and_fold() {
    let mut worker = start_worker();
    wait_for_port(WORKER_PORT, Duration::from_secs(10));

    let driver_out = Command::new(integration_bin())
        .args([
            "--driver",
            "--workers",
            &format!("127.0.0.1:{}", WORKER_PORT),
        ])
        .env("RUST_LOG", "warn")
        .output()
        .expect("failed to run integration driver");

    worker.kill().ok();
    worker.wait().ok();

    assert!(
        driver_out.status.success(),
        "driver failed (exit {:?}):\nstderr: {}",
        driver_out.status,
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value =
        serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
            panic!(
                "driver output is not valid JSON: {e}\nstdout: {}",
                String::from_utf8_lossy(&driver_out.stdout)
            )
        });

    assert_eq!(
        out["doubled"],
        serde_json::json!([2, 4, 6, 8]),
        "map_task(Double) result mismatch"
    );
    assert_eq!(
        out["sum"],
        serde_json::json!(10),
        "fold_task(Add) result mismatch"
    );
}
