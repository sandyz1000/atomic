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

/// All scenarios live in one binary (`integration/main.rs`); a scenario name
/// selects which `run_driver` runs. Worker mode is scenario-agnostic, so every
/// helper below takes the scenario only where it spawns a driver.
fn integration_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration"))
}

/// Spawns the driver as a non-blocking child so the caller can interleave
/// actions (e.g. killing a worker) while the driver process is still running.
fn spawn_driver(scenario: &str, workers: &[u16]) -> Child {
    let worker_list = workers
        .iter()
        .map(|p| format!("127.0.0.1:{p}"))
        .collect::<Vec<_>>()
        .join(",");
    Command::new(integration_bin())
        .args([scenario, "--driver", "--workers", &worker_list])
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn driver ({scenario}): {e}"))
}

fn spawn_worker(port: u16) -> Child {
    Command::new(integration_bin())
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

fn run_driver(scenario: &str, workers: &[u16]) -> std::process::Output {
    let worker_list = workers
        .iter()
        .map(|p| format!("127.0.0.1:{p}"))
        .collect::<Vec<_>>()
        .join(",");
    Command::new(integration_bin())
        .args([scenario, "--driver", "--workers", &worker_list])
        .env("RUST_LOG", "warn")
        .output()
        .unwrap_or_else(|e| panic!("failed to run driver ({scenario}): {e}"))
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
    let mut worker = spawn_worker(port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver("map_fold", &[port]);
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
    let mut worker = spawn_worker(port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver("shuffle_wordcount", &[port]);
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
    assert_eq!(out["rust"], serde_json::json!(2), "rust count mismatch");
    assert_eq!(out["of"], serde_json::json!(1), "of count mismatch");
}

// ── Test 3: multi-stage pipeline ──────────────────────────────────────────────

/// Validates a multi-stage pipeline: tokenize → reduce_by_key → sort-by-count.
/// The output must list words ordered by count descending.
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_multi_stage_pipeline() {
    let port = free_port();
    let mut worker = spawn_worker(port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver("multi_stage", &[port]);
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

    let sorted = out["sorted_words"]
        .as_array()
        .expect("sorted_words must be array");

    // The first element must have count >= last element (descending by count).
    let first_count = sorted.first().and_then(|v| v[1].as_i64()).unwrap_or(0);
    let last_count = sorted
        .last()
        .and_then(|v| v[1].as_i64())
        .unwrap_or(i64::MAX);
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
    let healthy_port = free_port();
    let dead_port = free_port();
    // Worker 1: healthy
    let mut worker1 = spawn_worker(healthy_port);
    wait_for_port(healthy_port, Duration::from_secs(10));
    // Worker 2 (dead_port): NOT started — simulates a dead/unreachable worker.

    let driver_out = run_driver("fault_tolerance", &[healthy_port, dead_port]);
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

// ── Test 5: distributed cache locality ────────────────────────────────────────

/// Builds a `value -> call_id` map from the `[[value, call_id], ...]` JSON array
/// produced by the `cache_locality` scenario.
fn call_id_map(arr: &[serde_json::Value]) -> std::collections::HashMap<i64, i64> {
    arr.iter()
        .map(|pair| (pair[0].as_i64().unwrap(), pair[1].as_i64().unwrap()))
        .collect()
}

/// With no worker failures, a cached RDD's second action must be served
/// entirely from the holding workers' caches — every call id is unchanged.
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_cache_no_recompute() {
    let port_a = free_port();
    let port_b = free_port();
    let mut worker_a = spawn_worker(port_a);
    let mut worker_b = spawn_worker(port_b);
    wait_for_port(port_a, Duration::from_secs(10));
    wait_for_port(port_b, Duration::from_secs(10));

    let driver_out = run_driver("cache_locality", &[port_a, port_b]);
    worker_a.kill().ok();
    worker_a.wait().ok();
    worker_b.kill().ok();
    worker_b.wait().ok();

    assert!(
        driver_out.status.success(),
        "cache_locality driver failed:\nstderr: {}",
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from cache_locality: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    let first = call_id_map(out["first"].as_array().expect("first must be array"));
    let second = call_id_map(out["second"].as_array().expect("second must be array"));
    assert_eq!(first.len(), 4);
    assert_eq!(
        second, first,
        "no worker died — every value must keep its call id (cache hit, no recompute)"
    );
}

/// Killing a worker between the two actions must invalidate only the
/// partitions it held: those get a new call id (recomputed on a survivor)
/// while the surviving worker's own partitions keep their call id (cache hit).
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_cache_recompute_on_worker_death() {
    let port_a = free_port();
    let port_b = free_port();
    let mut worker_a = spawn_worker(port_a);
    let mut worker_b = spawn_worker(port_b);
    wait_for_port(port_a, Duration::from_secs(10));
    wait_for_port(port_b, Duration::from_secs(10));

    let driver = spawn_driver("cache_locality", &[port_a, port_b]);

    // Let the first collect complete, then kill worker B mid-sleep so the
    // second collect must recompute B's partitions on the survivor.
    std::thread::sleep(Duration::from_millis(700));
    worker_b.kill().ok();
    worker_b.wait().ok();

    let output = driver
        .wait_with_output()
        .expect("driver process did not exit");
    worker_a.kill().ok();
    worker_a.wait().ok();

    assert!(
        output.status.success(),
        "cache_locality driver failed after worker death:\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from cache_locality: {e}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    });

    let first = call_id_map(out["first"].as_array().expect("first must be array"));
    let second = call_id_map(out["second"].as_array().expect("second must be array"));
    assert_eq!(first.len(), 4);
    assert_eq!(
        second.len(),
        4,
        "result must still be correct despite the dead worker"
    );

    let mut unchanged = 0;
    let mut changed = 0;
    for (k, v1) in &first {
        let v2 = second
            .get(k)
            .unwrap_or_else(|| panic!("value {k} missing from second collect"));
        if v1 == v2 {
            unchanged += 1;
        } else {
            changed += 1;
        }
    }
    assert!(
        unchanged > 0,
        "expected at least one partition served from cache on the surviving worker"
    );
    assert!(
        changed > 0,
        "expected at least one partition recomputed after its holder died"
    );
}

// ── Test 6: distributed named partitioner ─────────────────────────────────────

/// A `NamedPartitioner` must ship to the worker by name and be honored exactly:
/// 3 output partitions, each containing only keys congruent to its index mod 3.
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_named_partitioner_buckets() {
    let port = free_port();
    let mut worker = spawn_worker(port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver("named_partitioner", &[port]);
    worker.kill().ok();
    worker.wait().ok();

    assert!(
        driver_out.status.success(),
        "named_partitioner driver failed:\nstderr: {}",
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from named_partitioner: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    let parts = out["partitions"]
        .as_array()
        .expect("partitions must be array");
    assert_eq!(
        parts.len(),
        3,
        "expected 3 output partitions — named partitioner must ship to the worker, not degrade to hash"
    );
    for (p, bucket) in parts.iter().enumerate() {
        for pair in bucket.as_array().expect("bucket must be array") {
            let k = pair[0].as_i64().unwrap();
            assert_eq!(
                k.rem_euclid(3) as usize,
                p,
                "key {k} landed in partition {p}, expected {}",
                k.rem_euclid(3)
            );
        }
    }
}

// ── Test 7: distributed sort_by_task ──────────────────────────────────────────

/// `sort_by_task` on a non-pair RDD must produce globally-ordered, non-overlapping
/// partitions across real worker processes with no client-side re-sort.
#[test]
#[ignore = "requires pre-built integration binary and free TCP ports"]
fn distributed_sort_by_task_ordered() {
    let port = free_port();
    let mut worker = spawn_worker(port);
    wait_for_port(port, Duration::from_secs(10));

    let driver_out = run_driver("sort_by_task", &[port]);
    worker.kill().ok();
    worker.wait().ok();

    assert!(
        driver_out.status.success(),
        "sort_by_task driver failed:\nstderr: {}",
        String::from_utf8_lossy(&driver_out.stderr)
    );

    let out: serde_json::Value = serde_json::from_slice(&driver_out.stdout).unwrap_or_else(|e| {
        panic!(
            "invalid JSON from sort_by_task: {e}\nstdout: {}",
            String::from_utf8_lossy(&driver_out.stdout)
        )
    });

    let parts = out["partitions"]
        .as_array()
        .expect("partitions must be array");
    let mut prev_max: Option<i64> = None;
    let mut total = 0usize;
    for part in parts {
        let vals: Vec<i64> = part
            .as_array()
            .expect("partition must be array")
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect();
        total += vals.len();
        for w in vals.windows(2) {
            assert!(w[0] <= w[1], "partition not internally sorted: {vals:?}");
        }
        if let (Some(pm), Some(&first)) = (prev_max, vals.first()) {
            assert!(
                pm <= first,
                "partition boundaries overlap: prev_max={pm} first={first}"
            );
        }
        if let Some(&last) = vals.last() {
            prev_max = Some(last);
        }
    }
    assert_eq!(
        total, 10,
        "expected all 10 elements present across partitions"
    );
}
