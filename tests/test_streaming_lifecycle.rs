//! Bugs B9 and B10: `StreamingContext` lifecycle defects.
//!
//! B9 — `stop()` ignores `_stop_sc` and `_gracefully` parameters
//!   (`crates/atomic-streaming/src/context.rs:258`):
//!   Both parameters are prefixed with `_` and not used. Passing
//!   `stop_sc = true` should also shut down the underlying `Context`; passing
//!   `gracefully = true` should wait for the current batch to finish before
//!   stopping. Neither is implemented.
//!
//! B10 — Checkpoint never writes despite `checkpoint()` being called
//!   (`crates/atomic-streaming/src/scheduler/job.rs:132`):
//!   `checkpoint()` creates the directory but the batch loop never serialises
//!   any `Checkpoint` state to disk. A process restart after a failure loses
//!   all streaming progress.
//!
//! Fix B9: implement the `stop_sc` and `gracefully` paths.
//! Fix B10: write a `Checkpoint` file after each batch in `JobScheduler::run_batch_loop()`.

use atomic_compute::context::Context;
use atomic_compute::env::Config;
use atomic_streaming::context::{StreamingContext, StreamingContextState};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

fn compute_ctx() -> Arc<Context> {
    Context::new_with_config(Config::local()).unwrap()
}

// ── State-machine correctness ─────────────────────────────────────────────────

/// Starting a fresh `StreamingContext` succeeds (state: Initialized → Active).
#[test]
fn test_start_active() {
    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(100));

    // Register a no-op output operation so `start()` passes graph validation.
    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});

    ssc.start().unwrap();

    // Stop it right away to avoid background thread leaks.
    ssc.stop(false, false);
}

/// Calling `start()` twice returns `AlreadyStarted` error.
///
/// This verifies the state-machine guard at context.rs:241-242.
#[test]
fn test_double_start() {
    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(100));

    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});

    ssc.start().unwrap();

    let result = ssc.start();
    ssc.stop(false, false);

    assert!(
        result.is_err(),
        "second start() call must return AlreadyStarted, got Ok"
    );
}

/// Calling `start()` after `stop()` returns `AlreadyStopped` error.
///
/// Documents the state machine: Stopped is a terminal state — restart is not
/// possible without creating a new `StreamingContext`.
#[test]
fn test_start_post_stop() {
    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(100));

    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});

    ssc.start().unwrap();
    ssc.stop(false, false);

    let result = ssc.start();
    assert!(
        result.is_err(),
        "start() after stop() must return AlreadyStopped, got Ok"
    );
}

// ── Bug B9: stop() idempotency ────────────────────────────────────────────────

/// `stop()` called twice must be a no-op on the second call (no panic).
///
/// Currently handled by the `if *state == Stopped { return; }` guard at line 260.
/// This test pins that guard as a regression test.
#[test]
fn test_stop_is_idempotent() {
    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(100));

    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});

    ssc.start().unwrap();
    ssc.stop(false, false);
    ssc.stop(false, false); // second stop — must not panic
}

/// B9: `stop(stop_sc=true, gracefully=false)` also shuts down the underlying `Context`.
#[test]
fn test_stop_sc_shutdown() {
    let sc = compute_ctx();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(100));

    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});

    ssc.start().unwrap();
    ssc.stop(true, false); // stop_sc = true → shuts down sc (clears shuffle infrastructure)

    // The shuffle infrastructure is cleared by shutdown(); subsequent shuffle
    // ops on `sc` would need to re-initialize via a new Context.
    assert!(
        atomic_data::env::get_shuffle_server_uri().is_none(),
        "stop(stop_sc=true) must clear the shuffle server URI"
    );
}

// ── Bug B10: checkpoint never writes ─────────────────────────────────────────

/// B10: Verifies that `checkpoint-<ms>` files are written after each batch.
#[test]
fn test_checkpoint_written() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(50));

    // Use a temp directory so we can inspect its contents.
    let dir = tempfile::tempdir().unwrap();
    let checkpoint_path = dir.path().to_path_buf();

    ssc.checkpoint(checkpoint_path.clone());

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, move |_rdd, _t| {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    ssc.start().unwrap();

    // Wait for at least 3 batches to fire.
    let deadline = std::time::Instant::now() + Duration::from_millis(300);
    while counter.load(Ordering::Relaxed) < 3 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    ssc.stop(false, false);

    // Collect checkpoint files written.
    let checkpoint_files: Vec<_> = std::fs::read_dir(&checkpoint_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("checkpoint-")
        })
        .collect();

    // B10 fixed: checkpoint files must be written after each batch.
    assert!(
        !checkpoint_files.is_empty(),
        "expected at least one checkpoint file in {:?}, but directory is empty",
        checkpoint_path
    );
}

/// Verify that `from_checkpoint` restores batch duration from a written checkpoint.
#[test]
fn test_checkpoint_restore() {
    use atomic_streaming::checkpoint::Checkpoint;

    let dir = tempfile::tempdir().unwrap();
    let cp = Checkpoint::new(1_000_000, 100, dir.path().to_string_lossy().as_ref(), Some(999_900));
    cp.write(dir.path()).unwrap();

    let sc = compute_ctx();
    let ssc = StreamingContext::from_checkpoint(sc, dir.path())
        .expect("read ok")
        .expect("checkpoint found");

    assert_eq!(ssc.batch_duration.as_millis(), 100);
}

// ── Batch processing correctness ─────────────────────────────────────────────

/// `foreach_rdd` closure must be invoked for every batch that has data.
///
/// We push 3 RDDs into the queue and verify the closure is called at least 3 times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_foreach_rdd() {
    use std::sync::atomic::{AtomicI32, Ordering};

    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc.clone(), Duration::from_millis(50));

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let stream = ssc.queue_stream(queue.clone(), true);

    // Push 3 pre-built RDDs.
    for i in 0..3 {
        let rdd = sc.parallelize_typed(vec![i as i32], 1);
        queue.lock().push_back(Arc::new(rdd) as Arc<dyn atomic_data::rdd::Rdd<Item = i32>>);
    }

    let sum = Arc::new(AtomicI32::new(0));
    let sum_clone = sum.clone();
    let sc_clone = sc.clone();
    // Clone the runtime handle so the batch-loop thread can enter the tokio context
    // (LocalScheduler calls spawn_blocking which requires a runtime).
    let rt_handle = tokio::runtime::Handle::current();
    ssc.foreach_rdd(stream, move |rdd, _t| {
        let _guard = rt_handle.enter();
        if let Ok(items) = sc_clone.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let batch_sum: i32 = items.into_iter().flatten().sum();
            sum_clone.fetch_add(batch_sum, Ordering::Relaxed);
        }
    });

    ssc.start().unwrap();

    // Wait until the queue is drained (≤ 500ms).
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    while !queue.lock().is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(100)); // let final batch close
    ssc.stop(false, false);

    // 0 + 1 + 2 = 3
    assert_eq!(
        sum.load(Ordering::Relaxed),
        3,
        "expected sum of all batches (0+1+2=3), got {}",
        sum.load(Ordering::Relaxed)
    );
}

// ── Streaming pair ops ────────────────────────────────────────────────────────

atomic_compute::register_shuffle_map!(String, i32);

#[test]
fn test_stream_reduce() {
    use atomic_streaming::dstream::pair::PairDStreamFunctions;
    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(50));

    let queue: Arc<Mutex<VecDeque<Arc<dyn atomic_data::rdd::Rdd<Item = (String, i32)>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    // Push one batch: word count pairs
    let batch_rdd: Arc<dyn atomic_data::rdd::Rdd<Item = (String, i32)>> = Arc::new(
        atomic_compute::rdd::parallel_collection::ParallelCollection::new(
            0,
            vec![
                ("hello".to_string(), 1i32),
                ("world".to_string(), 1),
                ("hello".to_string(), 1),
            ],
            1,
        )
    );
    queue.lock().push_back(batch_rdd);

    let stream = ssc.queue_stream(queue, true);
    let pair_ops = PairDStreamFunctions::new(stream, ssc.clone());

    let result_store: Arc<Mutex<Vec<(String, i32)>>> = Arc::new(Mutex::new(Vec::new()));
    let result_clone = result_store.clone();

    let sc_ref = ssc.sc.clone();
    let reduced = pair_ops.reduce_by_key(|a, b| a + b, 2);
    ssc.foreach_rdd(reduced as Arc<dyn atomic_streaming::dstream::DStream<(String, i32)>>, move |rdd, _t| {
        if let Ok(mut items) = sc_ref.collect_rdd(rdd) {
            items.sort_by_key(|(k, _)| k.clone());
            if !items.is_empty() {
                *result_clone.lock() = items;
            }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    ssc.stop(false, false);

    let results = result_store.lock().clone();
    assert!(!results.is_empty(), "expected reduce_by_key results, got empty");
    let hello = results.iter().find(|(k, _)| k == "hello");
    assert_eq!(hello.map(|(_, v)| v), Some(&2i32), "hello count should be 2");
    let world = results.iter().find(|(k, _)| k == "world");
    assert_eq!(world.map(|(_, v)| v), Some(&1i32), "world count should be 1");
}

/// `await_termination_or_timeout()` must return within the given deadline.
#[test]
fn test_timeout_deadline() {
    let sc = compute_ctx();
    let ssc = StreamingContext::new(sc, Duration::from_millis(1000));

    let queue = Arc::new(Mutex::new(VecDeque::<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>::new()));
    let stream = ssc.queue_stream(queue, true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});
    ssc.start().unwrap();

    let start = std::time::Instant::now();
    ssc.await_termination_or_timeout(Duration::from_millis(150));
    let elapsed = start.elapsed();
    ssc.stop(false, false);

    assert!(
        elapsed < Duration::from_millis(500),
        "await_termination_or_timeout should return within deadline, took {:?}",
        elapsed
    );
}
