use atomic_streaming::checkpoint::Checkpoint;
use std::time::Duration;

#[test]
fn test_write_and_read_latest_round_trip() {
    let td = tempfile::tempdir().unwrap();
    let cp = Checkpoint::new(1_000, 50, td.path().to_string_lossy().as_ref());
    cp.write(td.path()).unwrap();

    let loaded = Checkpoint::read_latest(td.path()).unwrap().expect("should find a checkpoint");
    assert_eq!(loaded.checkpoint_time_ms, 1_000);
    assert_eq!(loaded.batch_duration_ms, 50);
}

#[test]
fn test_read_latest_returns_none_for_empty_dir() {
    let td = tempfile::tempdir().unwrap();
    let result = Checkpoint::read_latest(td.path()).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_read_latest_returns_none_for_nonexistent_dir() {
    let path = std::path::Path::new("/tmp/atomic_streaming_test_nonexistent_9999");
    let result = Checkpoint::read_latest(path).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_multiple_checkpoints_latest_wins() {
    let td = tempfile::tempdir().unwrap();
    Checkpoint::new(1_000, 50, td.path().to_string_lossy().as_ref())
        .write(td.path())
        .unwrap();
    Checkpoint::new(2_000, 50, td.path().to_string_lossy().as_ref())
        .write(td.path())
        .unwrap();
    Checkpoint::new(3_000, 50, td.path().to_string_lossy().as_ref())
        .write(td.path())
        .unwrap();

    let loaded = Checkpoint::read_latest(td.path()).unwrap().unwrap();
    assert_eq!(loaded.checkpoint_time_ms, 3_000);
}

#[test]
fn test_clean_removes_files_below_threshold() {
    let td = tempfile::tempdir().unwrap();
    for &t in &[1_000u64, 2_000, 3_000, 4_000] {
        Checkpoint::new(t, 50, td.path().to_string_lossy().as_ref())
            .write(td.path())
            .unwrap();
    }
    // Remove files with timestamp < 3_000.
    Checkpoint::clean(td.path(), 3_000).unwrap();

    let files: Vec<_> = std::fs::read_dir(td.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    // Only checkpoints at 3_000 and 4_000 should remain.
    assert_eq!(files.len(), 2);

    let loaded = Checkpoint::read_latest(td.path()).unwrap().unwrap();
    assert_eq!(loaded.checkpoint_time_ms, 4_000);
}

#[test]
fn test_clean_on_empty_dir_is_noop() {
    let td = tempfile::tempdir().unwrap();
    Checkpoint::clean(td.path(), 5_000).unwrap();
    // No error; directory still exists.
    assert!(td.path().exists());
}

#[test]
fn test_checkpoint_written_by_batch_loop() {
    use atomic_compute::context::Context;
    use atomic_streaming::context::StreamingContext;
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::Arc;

    let sc = Context::local().unwrap();
    let td = tempfile::tempdir().unwrap();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn atomic_data::rdd::Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});
    ssc.checkpoint(td.path().to_path_buf());
    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    ssc.stop(false, false);

    // The batch loop should have written at least one checkpoint file.
    let result = Checkpoint::read_latest(td.path()).unwrap();
    assert!(result.is_some(), "batch loop should write checkpoints when dir is set");
}
