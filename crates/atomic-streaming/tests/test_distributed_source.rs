//! Tests for `DistributedSource` trait + `DistributedFileSource` (B4).
//!
//! All tests are `#[ignore]`-free and require no external infrastructure
//! (no Kafka broker, no real worker processes).  The `DistributedInputDStream`
//! uses `Context::dispatch_pipeline` which runs in-process via `NativeBackend`
//! when the context is local.

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::DStream;
use atomic_streaming::dstream::distributed_source::{DistributedFileSource, DistributedSource};
use tempfile::TempDir;

fn local_ssc() -> Arc<StreamingContext> {
    let ctx = Context::local().unwrap();
    StreamingContext::new(ctx, Duration::from_millis(100))
}

fn write_file(dir: &TempDir, name: &str, contents: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "{}", contents.trim_end_matches('\n')).unwrap();
    path
}

/// Files in a temp dir are dispatched via `dispatch_pipeline` (local NativeBackend)
/// and all lines are collected exactly once.
#[test]
fn test_file_source_distributes() {
    let dir = TempDir::new().unwrap();
    write_file(&dir, "a.txt", "hello\nworld");
    write_file(&dir, "b.txt", "foo\nbar");

    let ssc = local_ssc();
    let stream = ssc.distributed_file_stream(dir.path());

    let rdd = stream.compute(1000).expect("should produce an RDD");
    let mut items: Vec<String> = ssc.sc.collect_rdd(rdd).unwrap();
    items.sort();
    assert_eq!(items, vec!["bar", "foo", "hello", "world"]);
}

/// After a successful batch the files are committed; the next `plan_batch`
/// returns zero tasks (no new files).
#[test]
fn test_no_reread_after_commit() {
    let dir = TempDir::new().unwrap();
    write_file(&dir, "only.txt", "line1\nline2");

    let ssc = local_ssc();
    let stream = ssc.distributed_file_stream(dir.path());

    let rdd = stream.compute(1000).unwrap();
    let items: Vec<String> = ssc.sc.collect_rdd(rdd).unwrap();
    assert_eq!(items.len(), 2);

    assert!(
        stream.compute(2000).is_none(),
        "committed files must not be re-read"
    );
}

/// A split not committed (simulates a failed worker) reappears in the next
/// `plan_batch` — demonstrating at-least-once re-planning.
#[test]
fn test_split_replanned() {
    let dir = TempDir::new().unwrap();
    write_file(&dir, "p0.txt", "alpha");
    write_file(&dir, "p1.txt", "beta");

    let source = DistributedFileSource::new(dir.path());

    let tasks_1 = source.plan_batch(1000);
    assert_eq!(tasks_1.len(), 2, "expected one task per file");

    // Partial failure: only commit p0 (identified by descriptor); p1's worker died.
    let p0_task: Vec<_> = tasks_1
        .iter()
        .filter(|t| t.descriptor.ends_with("p0.txt"))
        .cloned()
        .collect();
    assert_eq!(p0_task.len(), 1);
    source.commit(&p0_task);

    let tasks_2 = source.plan_batch(2000);
    assert_eq!(tasks_2.len(), 1, "uncommitted split should be re-planned");
    assert!(tasks_2[0].descriptor.ends_with("p1.txt"));

    // Add a new file; both new file and uncommitted p1 appear.
    write_file(&dir, "p2.txt", "gamma");
    let tasks_3 = source.plan_batch(3000);
    assert_eq!(tasks_3.len(), 2, "new file + uncommitted split = 2 tasks");

    source.commit(&tasks_3);

    let tasks_4 = source.plan_batch(4000);
    assert!(tasks_4.is_empty(), "all files committed — nothing to plan");
}

/// `get_or_compute` returns the same RDD for a repeated batch time (idempotent).
#[test]
fn test_get_or_compute_caches() {
    let dir = TempDir::new().unwrap();
    write_file(&dir, "x.txt", "data");

    let ssc = local_ssc();
    let stream = ssc.distributed_file_stream(dir.path());

    let rdd_a = stream
        .get_or_compute(500)
        .expect("first call must produce RDD");
    let rdd_b = stream
        .get_or_compute(500)
        .expect("second call must return cached RDD");

    let a: Vec<String> = ssc.sc.collect_rdd(rdd_a).unwrap();
    let b: Vec<String> = ssc.sc.collect_rdd(rdd_b).unwrap();
    assert_eq!(a, b);
}

/// An empty directory produces no tasks and `compute` returns `None`.
#[test]
fn test_empty_dir_none() {
    let dir = TempDir::new().unwrap();
    let ssc = local_ssc();
    let stream = ssc.distributed_file_stream(dir.path());
    assert!(stream.compute(1000).is_none());
}
