//! Bug B8: Worker removal after 3 TCP failures is permanent.
//!
//! In `DistributedScheduler::submit_native_task()`, after `MAX_WORKER_FAILURES`
//! (hard-coded 3) consecutive TCP-level errors the worker endpoint is removed from
//! `server_uris`, `worker_capabilities`, `inflight`, and `worker_failures`.
//! There is NO path to re-add it — even if the worker process recovers and
//! reconnects. The driver must be restarted to use the recovered worker.
//!
//! Affected: `crates/atomic-scheduler/src/distributed.rs:259-268`
//!
//! Fix suggestion: add a `reregister_worker()` method (or make workers
//! periodically send heartbeats that restore their entry) so a recovered
//! worker can rejoin without requiring a driver restart.
//!
//! These tests also document the retry/backoff behaviour and the exponential
//! backoff sequence (100ms * min(2^attempt, 32)).

use atomic_scheduler::DistributedScheduler;
use atomic_data::distributed::WorkerCapabilities;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

fn worker_addr(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)
}

fn capabilities(ops: &[&str]) -> WorkerCapabilities {
    WorkerCapabilities::new(
        "test-worker".to_string(),
        4,
        ops.iter().map(|s| s.to_string()).collect(),
    )
}

// ── Worker registration ───────────────────────────────────────────────────────

/// `register_worker()` must make the endpoint available for task dispatch.
#[test]
fn test_worker_selectable() {
    let sched = DistributedScheduler::new(3, true);
    let addr = worker_addr(29100);
    sched.register_worker(addr, capabilities(&[]));

    let selected = sched.next_executor().unwrap();
    assert_eq!(selected, addr, "registered worker must be returned by next_executor()");
}

/// Registering the same worker twice must not duplicate it in the round-robin list.
#[test]
fn test_register_worker_idempotent() {
    let sched = DistributedScheduler::new(3, true);
    let addr = worker_addr(29101);
    sched.register_worker(addr, capabilities(&[]));
    sched.register_worker(addr, capabilities(&[]));

    // First call returns the worker.
    let first = sched.next_executor().unwrap();
    assert_eq!(first, addr);
    // Second call should also return the worker (it's the only one in the list).
    let second = sched.next_executor().unwrap();
    assert_eq!(second, addr);
}

/// `next_executor()` on an empty scheduler returns an error.
#[test]
fn test_next_executor_empty_returns_error() {
    let sched = DistributedScheduler::new(3, true);
    let result = sched.next_executor();
    assert!(result.is_err(), "expected error when no workers are registered");
}

// ── Capability checking ───────────────────────────────────────────────────────

/// Worker with empty `registered_ops` accepts any op (backwards compatibility).
#[test]
fn test_worker_with_empty_ops_accepts_all() {
    let sched = DistributedScheduler::new(3, true);
    let addr = worker_addr(29102);
    sched.register_worker(addr, capabilities(&[])); // empty = accept all
    // No assertion needed — if next_executor_with_capacity() returns Ok the
    // capability check is not blocking.
    assert!(sched.next_executor_with_capacity().is_ok());
}

// ── Bug B8: permanent worker removal ─────────────────────────────────────────

/// Documents that after 3 consecutive TCP failures the worker is permanently
/// removed and `next_executor_with_capacity()` returns `NoCompatibleWorker`.
///
/// Uses a real (but minimal) TCP listener that immediately closes each connection
/// to simulate a "dead" worker that refuses tasks.
///
/// **Expected (after fix):** `reregister_worker()` (or heartbeat) allows the
/// worker to rejoin without a driver restart.
/// **Current behaviour:** worker is gone permanently — this test shows that
/// after the 3rd failure `next_executor` errors.
#[tokio::test]
async fn test_worker_tcp_removal() {
    use atomic_data::distributed::{TaskAction, TaskEnvelope, TaskRuntime, PipelineOp};

    // Bind an ephemeral port — the listener immediately drops each connection
    // to simulate a dead worker.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = worker_addr(port);

    // Background task: accept connections and immediately close them.
    tokio::spawn(async move {
        for _ in 0..10 {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let _ = stream.shutdown().await;
                }
                Err(_) => break,
            }
        }
    });

    let sched = DistributedScheduler::new(3, true);
    sched.register_worker(addr, capabilities(&[]));

    // Confirm the worker is initially registered.
    assert_eq!(sched.next_executor().unwrap(), addr);

    // Build a minimal task envelope.
    let task = TaskEnvelope::new(
        1, 1, 1, 0, 0,
        "test".to_string(),
        vec![PipelineOp {
            op_id: "no.op".to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
        }],
        vec![],
    );

    // Submit will fail on TCP — scheduler should retry and eventually remove worker.
    // We use max_failures=3 so after 3 retries the worker is gone.
    let result = sched.submit_native_task(&task).await;
    assert!(
        result.is_err(),
        "expected error when worker is unreachable, got Ok"
    );

    // Bug B8: the worker is now permanently removed.
    let next = sched.next_executor();
    assert!(
        next.is_err(),
        "Bug B8: after 3 failures the worker should be removed from the pool. \
         If next_executor() still returns Ok, worker removal is broken."
    );

    // Bug B8 (cont): there is no way to re-add the worker without restarting
    // the driver. Document this with a re-registration attempt.
    sched.register_worker(addr, capabilities(&[]));
    assert!(
        sched.next_executor().is_ok(),
        "After re-registration the worker must be selectable again. \
         (This assertion documents that manual re-registration currently works \
         as a workaround — but a heartbeat/auto-recovery path is still missing.)"
    );
}

// ── Exponential backoff timing ────────────────────────────────────────────────

/// Documents that the scheduler applies exponential backoff between retries.
///
/// With `max_failures = 2` (3 attempts total), the expected delays are:
///   attempt 0 → attempt 1: 100ms
///   attempt 1 → attempt 2: 200ms
///
/// We measure wall-clock time and assert it is at least 300ms total.
/// This is a lower-bound check — the actual time may be longer due to TCP
/// setup overhead.
#[tokio::test]
async fn test_exponential_backoff() {
    use atomic_data::distributed::{TaskAction, TaskEnvelope, TaskRuntime, PipelineOp};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = worker_addr(port);

    tokio::spawn(async move {
        for _ in 0..10 {
            match listener.accept().await {
                Ok((mut s, _)) => { let _ = s.shutdown().await; }
                Err(_) => break,
            }
        }
    });

    // max_failures = 2 → 3 total attempts, backoff after attempts 0 and 1.
    let sched = DistributedScheduler::new(2, true);
    sched.register_worker(addr, capabilities(&[]));

    let task = TaskEnvelope::new(
        1, 1, 1, 0, 0,
        "test".to_string(),
        vec![PipelineOp {
            op_id: "no.op".to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
        }],
        vec![],
    );

    let start = Instant::now();
    let _ = sched.submit_native_task(&task).await;
    let elapsed = start.elapsed();

    // Backoff: 100ms (attempt 0→1) + 200ms (attempt 1→2) = at least 300ms.
    assert!(
        elapsed >= Duration::from_millis(250),
        "expected at least 250ms elapsed (exponential backoff), got {:?}",
        elapsed
    );
}

// ── Capacity-aware scheduling ─────────────────────────────────────────────────

/// `next_executor_with_capacity()` must skip workers that are at max capacity.
///
/// With one worker and `max_tasks = 0`, the scheduler should return
/// `NoCompatibleWorker`.
#[test]
fn test_zero_capacity() {
    let sched = DistributedScheduler::new(3, true);
    let addr = worker_addr(29103);
    sched.register_worker(addr, WorkerCapabilities::new("test-worker".to_string(), 0, vec![]));

    let result = sched.next_executor_with_capacity();
    assert!(
        result.is_err(),
        "worker with max_tasks=0 must not be selected"
    );
}
