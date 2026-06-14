use atomic_compute::context::Context;
use atomic_streaming::context::{StreamingContext, StreamingContextState};
use atomic_streaming::errors::StreamingError;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

/// Shared queue of per-batch RDDs feeding a `queue_stream`.
type RddQueue = Arc<Mutex<VecDeque<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>>>;

fn local_sc() -> Arc<Context> {
    Context::local().unwrap()
}

fn ssc_with_queue(sc: Arc<Context>, batch_ms: u64) -> (Arc<StreamingContext>, RddQueue) {
    let ssc = StreamingContext::new(sc, Duration::from_millis(batch_ms));
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let results_cl = Arc::clone(&results);
    let sc2 = ssc.sc.clone();
    ssc.foreach_rdd(stream, move |rdd, _t| {
        if let Ok(parts) = sc2.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = results_cl.lock();
            for p in parts {
                v.extend(p);
            }
        }
    });
    (ssc, queue)
}

#[test]
fn test_initial_state() {
    let sc = local_sc();
    let ssc = StreamingContext::new(sc, Duration::from_millis(50));
    assert_eq!(ssc.state(), StreamingContextState::Initialized);
}

#[test]
fn test_start_active() {
    let sc = local_sc();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    assert_eq!(ssc.state(), StreamingContextState::Active);
    ssc.stop(false, false);
}

#[test]
fn test_stop_transitions() {
    let sc = local_sc();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    ssc.stop(false, false);
    assert_eq!(ssc.state(), StreamingContextState::Stopped);
}

#[test]
fn test_double_start_err() {
    let sc = local_sc();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    let result = ssc.start();
    ssc.stop(false, false);
    assert!(matches!(result, Err(StreamingError::AlreadyStarted)));
}

#[test]
fn test_restart_err() {
    let sc = local_sc();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    ssc.stop(false, false);
    let result = ssc.start();
    assert!(matches!(result, Err(StreamingError::AlreadyStopped)));
}

#[test]
fn test_stop_unstarted_noop() {
    let sc = local_sc();
    let ssc = StreamingContext::new(sc, Duration::from_millis(50));
    // Just needs to not panic — no output operations, so validate() would fail.
    // Stopping before starting should be a no-op.
    ssc.stop(false, false);
    assert_eq!(ssc.state(), StreamingContextState::Stopped);
}

#[test]
fn test_await_timeout_false() {
    let sc = local_sc();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    let result = ssc
        .await_termination_or_timeout(Duration::from_millis(120))
        .unwrap();
    ssc.stop(false, false);
    assert!(!result, "should return false on timeout, not stopped");
}

#[test]
fn test_await_stopped_true() {
    let sc = local_sc();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    let ssc2 = Arc::clone(&ssc);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(80));
        ssc2.stop(false, false);
    });
    let result = ssc
        .await_termination_or_timeout(Duration::from_millis(500))
        .unwrap();
    assert!(result, "should return true when stopped");
    assert_eq!(ssc.state(), StreamingContextState::Stopped);
}

#[test]
fn test_validate_no_output() {
    let sc = local_sc();
    // No output operations registered — start() must fail validation.
    let ssc = StreamingContext::new(sc, Duration::from_millis(50));
    let result = ssc.start();
    assert!(result.is_err(), "should fail with NoOutputOperations");
}

#[test]
fn test_checkpoint_before_start() {
    let sc = local_sc();
    let td = tempfile::tempdir().unwrap();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.checkpoint(td.path().to_path_buf());
    assert!(ssc.checkpoint_dir.lock().is_some());
    ssc.start().unwrap();
    ssc.stop(false, false);
}

#[test]
fn test_checkpoint_after_start() {
    let sc = local_sc();
    let td = tempfile::tempdir().unwrap();
    let (ssc, _q) = ssc_with_queue(sc, 50);
    ssc.start().unwrap();
    // Calling checkpoint() while Active must not change the dir.
    ssc.checkpoint(td.path().to_path_buf());
    let set = ssc.checkpoint_dir.lock().is_some();
    ssc.stop(false, false);
    assert!(!set, "checkpoint after start should be ignored");
}
