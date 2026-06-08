//! Distributed integration tests: real TCP round-trips between driver and workers.
//!
//! Each test spawns integration binary processes — once as a worker, once as a
//! driver — and verifies that the driver's JSON output matches expected values.
//!
//! Run all distributed tests with:
//!
//!   cargo test distributed -- --nocapture

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── Binary helpers ────────────────────────────────────────────────────────────

fn free_port() -> u16 {
    // Bind to port 0 to get an OS-assigned free port, then release it.
    // The worker process will bind the same port immediately after.
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("failed to bind to free port")
        .local_addr()
        .expect("failed to get local addr")
        .port()
}

fn integration_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration"))
}

fn shuffle_wordcount_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration_shuffle_wordcount"))
}

fn multi_stage_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration_multi_stage"))
}

fn fault_tolerance_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration_fault_tolerance"))
}

fn spawn_worker(bin: &std::path::Path, port: u16) -> Child {
    Command::new(bin)
        .args(["--worker", "--port", &port.to_string()])
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn worker on port {port}: {e}"))
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

fn run_driver(bin: &std::path::Path, workers: &[u16]) -> std::process::Output {
    let worker_list = workers
        .iter()
        .map(|p| format!("127.0.0.1:{p}"))
        .collect::<Vec<_>>()
        .join(",");
    Command::new(bin)
        .args(["--driver", "--workers", &worker_list])
        .env("RUST_LOG", "warn")
        .output()
        .unwrap_or_else(|e| panic!("failed to run driver {}: {e}", bin.display()))
}

// ── Test 1: baseline map + fold ───────────────────────────────────────────────

// Distributed tests spawn real child processes over TCP.
// They are marked #[ignore] so `cargo test` skips them by default.
// Run them explicitly with: cargo test -p atomic -- --ignored
// CI runs them in a dedicated job after pre-building the integration binaries.

#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_map_and_fold() {
    let port = free_port();
    let mut worker = spawn_worker(&integration_bin(), port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver(&integration_bin(), &[port]);
    worker.kill().ok();
    worker.wait().ok();

    assert!(
        driver_out.status.success(),
        "driver failed (exit {:?}):\nstderr: {}",
        driver_out.status,
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "driver output is not valid JSON: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    assert_eq!(out["doubled"], serde_json::json!([2, 4, 6, 8]));
    assert_eq!(out["sum"], serde_json::json!(10));
}

// ── Test 2: distributed shuffle word-count ────────────────────────────────────

/// Validates distributed reduce_by_key: tokenize → shuffle-map → reduce.
/// Expected word counts for the corpus "hello world / hello rust / world of rust":
///   hello:2, of:1, rust:2, world:2
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_shuffle_wordcount() {
    let port = free_port();
    let bin = shuffle_wordcount_bin();
    let mut worker = spawn_worker(&bin, port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver(&bin, &[port]);
    worker.kill().ok();
    worker.wait().ok();

    assert!(
        driver_out.status.success(),
        "shuffle wordcount driver failed:\nstderr: {}",
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from shuffle_wordcount: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    assert_eq!(out["hello"], serde_json::json!(2), "hello count mismatch");
    assert_eq!(out["world"], serde_json::json!(2), "world count mismatch");
    assert_eq!(out["rust"],  serde_json::json!(2), "rust count mismatch");
    assert_eq!(out["of"],    serde_json::json!(1), "of count mismatch");
}

// ── Test 3: multi-stage pipeline ──────────────────────────────────────────────

/// Validates a multi-stage pipeline: tokenize → reduce_by_key → sort-by-count.
/// The output must list words ordered by count descending.
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_multi_stage_pipeline() {
    let port = free_port();
    let bin = multi_stage_bin();
    let mut worker = spawn_worker(&bin, port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver(&bin, &[port]);
    worker.kill().ok();
    worker.wait().ok();

    assert!(
        driver_out.status.success(),
        "multi-stage driver failed:\nstderr: {}",
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from multi_stage: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    let sorted = out["sorted_words"].as_array().expect("sorted_words must be array");

    // The first element must have count >= last element (descending by count).
    let first_count = sorted.first().and_then(|v| v[1].as_i64()).unwrap_or(0);
    let last_count  = sorted.last().and_then(|v| v[1].as_i64()).unwrap_or(i64::MAX);
    assert!(
        first_count >= last_count,
        "sorted_words must be in descending count order; first={first_count}, last={last_count}"
    );

    // Total word occurrences: "hello world" + "hello rust" + "world of rust" = 7 tokens.
    let total: i64 = sorted.iter().map(|v| v[1].as_i64().unwrap_or(0)).sum();
    assert_eq!(total, 7, "total word occurrences must be 7");
}

// ── Test 4: fault tolerance ───────────────────────────────────────────────────

/// Validates that the driver completes a job even when one worker is unavailable.
/// Starts one healthy worker and attempts the job with a second (dead) worker
/// listed. The driver should fall back to the healthy worker after retries.
///
/// Bug B8: the driver currently fails during the initial handshake rather than
/// retrying with surviving workers. Error: "timed out waiting for worker (Connection refused)".
/// Fix: skip unreachable workers during handshake and proceed with healthy subset.
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_fault_tolerance_one_dead_worker() {
    let bin = fault_tolerance_bin();
    let healthy_port = free_port();
    let dead_port = free_port();
    // Worker 1: healthy
    let mut worker1 = spawn_worker(&bin, healthy_port);
    wait_for_port(healthy_port, Duration::from_secs(10));
    // Worker 2 (dead_port): NOT started — simulates a dead/unreachable worker.

    let driver_out = run_driver(&bin, &[healthy_port, dead_port]);
    worker1.kill().ok();
    worker1.wait().ok();

    assert!(
        driver_out.status.success(),
        "fault-tolerance driver failed with one dead worker:\nstderr: {}",
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from fault_tolerance: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    // sum(1..=10) = 55
    assert_eq!(out["sum"], serde_json::json!(55), "sum mismatch");

    // doubled elements must each be an even number and the set must have size 10.
    let doubled = out["doubled"].as_array().expect("doubled must be array");
    assert_eq!(doubled.len(), 10, "must have 10 doubled values");
    for v in doubled {
        let n = v.as_i64().unwrap();
        assert!(n % 2 == 0, "doubled value {n} is not even");
    }
}
