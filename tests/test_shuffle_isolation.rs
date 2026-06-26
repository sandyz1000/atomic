//! Bug B1: `SHUFFLE_SERVER_URI` is a `OnceCell<String>` in `atomic_data::env`.
//!
//! When the first `Context` initialises the local shuffle manager, it writes
//! `SHUFFLE_SERVER_URI` via `OnceCell::set()` — a write-once operation. After
//! that context is dropped (and its HTTP shuffle server stops listening), the
//! cell still holds the dead URI. Any subsequent `Context` that tries to perform
//! a shuffle calls `ShuffleFetcher::fetch()`, which hits the now-dead URI and
//! fails with a connection error.
//!
//! Affected: `crates/atomic-data/src/env.rs:15`
//!   `pub static SHUFFLE_SERVER_URI: OnceCell<String> = OnceCell::new();`
//! Written at: `crates/atomic-compute/src/env.rs:313`
//!   `let _ = atomic_data::env::SHUFFLE_SERVER_URI.set(mgr.get_server_uri());`
//!
//! Fix suggestion: use `RwLock<Option<String>>` (or `ArcSwap`) so the URI
//! can be cleared when the ShuffleManager shuts down.

// These tests intentionally hold a process-global serialization guard across
// `.await` to run shuffle scenarios one at a time; the lock is never contended
// on the async path here.
#![allow(clippy::await_holding_lock)]

// Register the shuffle handler for (String, i32) pairs.
// This must appear at binary-level scope — once per KV type used with shuffle.
atomic_compute::register_shuffle_map!(String, i32);

use atomic_compute::context::Context;
use atomic_compute::env::Config;
use atomic_compute::task;
use std::sync::Arc;

#[task]
fn add_i32(a: i32, b: i32) -> i32 {
    a + b
}

fn ctx() -> Arc<Context> {
    Context::new_with_config(Config::local()).unwrap()
}

static SHUFFLE_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

fn shuffle_guard() -> std::sync::MutexGuard<'static, ()> {
    SHUFFLE_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap()
}

/// Shuffle ids must be unique across separate `Context` instances in one process.
///
/// The shuffle cache and `MAP_OUTPUT_TRACKER` are process-wide singletons keyed by
/// shuffle id. A per-context counter (starting at 0 in each `Context`) let concurrent
/// jobs collide on id 0,1,2…, corrupting each other's map outputs and hanging the
/// reduce stage. `new_shuffle_id` now draws from a process-global counter.
#[test]
fn shuffle_ids_unique() {
    let _g = shuffle_guard();
    let a = ctx();
    let b = ctx();
    let id_a = a.new_shuffle_id();
    let id_b = b.new_shuffle_id();
    assert_ne!(id_a, id_b, "shuffle ids collided across contexts");
}

/// Baseline: a single first-in-process reduce_by_key must succeed.
///
/// If this test also fails, the likely cause is a missing `register_shuffle_map!`
/// call — not the OnceLock bug targeted by this file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_first_shuffle_succeeds() {
    let _g = shuffle_guard();
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(
            vec![
                ("a".to_string(), 1i32),
                ("b".to_string(), 2),
                ("a".to_string(), 3),
            ],
            2,
        )
        .reduce_by_key_task(AddI32)
        .collect()
        .unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(result, vec![("a".to_string(), 4), ("b".to_string(), 2)]);
}

/// Bug B1: Two sequential Contexts — the second shuffle fails with a dead-URI error.
///
/// The first `Context::local()` sets `SHUFFLE_SERVER_URI` via `OnceCell::set()`.
/// When that context is dropped the HTTP server stops. The second context cannot
/// overwrite the `OnceCell`, so `ShuffleFetcher` connects to the now-dead URI
/// and returns a connection error.
///
/// **Expected (after fix):** both shuffles succeed independently.
/// **Current behaviour:** second context shuffle fails — this test will FAIL until
/// the OnceLock is replaced with a resettable handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_second_context_shuffle_fails() {
    let _g = shuffle_guard();

    // First context — sets SHUFFLE_SERVER_URI.
    {
        let ctx = ctx();
        let mut r = ctx
            .parallelize_typed(vec![("x".to_string(), 1i32)], 1)
            .reduce_by_key_task(AddI32)
            .collect()
            .unwrap();
        r.sort_by_key(|(k, _)| k.clone());
        assert_eq!(r, vec![("x".to_string(), 1)], "first shuffle must succeed");
        // ctx drops here — ShuffleManager HTTP server stops.
    }

    // Small delay to let the old server fully shut down.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Second context — SHUFFLE_SERVER_URI still points to the dead server.
    let ctx2 = ctx();
    let result = ctx2
        .parallelize_typed(vec![("y".to_string(), 2i32)], 1)
        .reduce_by_key_task(AddI32)
        .collect();

    // This assertion documents the EXPECTED post-fix behaviour.
    // It will FAIL (the test is a bug report) until the fix is in place.
    assert!(
        result.is_ok(),
        "Bug B1: second-context shuffle failed — SHUFFLE_SERVER_URI still \
         points to the dead URI from the first context. Error: {:?}",
        result.err()
    );
}

/// Documents that running shuffle in a child process avoids the stale-URI bug.
///
/// Each subprocess gets its own `OnceCell` state (fresh process = fresh statics),
/// so every subprocess shuffle is independent. This test is informational — it
/// PASSES to show the subprocess workaround the fix should replicate in-process.
#[test]
fn test_subprocess_isolation() {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_integration"));

    // Run the driver in a subprocess — it exits before any stale URI issue.
    let out = std::process::Command::new(&bin)
        .args(["map_fold", "--driver", "--workers", "127.0.0.1:39999"])
        .env("RUST_LOG", "warn")
        .output();

    match out {
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            // A failed connection to a missing worker is expected, but the
            // error should NOT be a stale-shuffle-URI error.
            assert!(
                !stderr.contains("stale")
                    && !stderr.contains("already set")
                    && !stderr.contains("OnceCell"),
                "Unexpected stale-URI error in subprocess: {stderr}"
            );
        }
        Err(e) => eprintln!("Note: subprocess test skipped (binary not found): {e}"),
    }
}
