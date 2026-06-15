//! Bugs B6 and B7: `PartitionStore` and `CachedRdd` correctness issues.
//!
//! B6 — Silent type-mismatch recompute (crates/atomic-compute/src/rdd/cached.rs:134):
//!   `PartitionStore::get::<T>()` downcasts the stored `Box<dyn Any>` to
//!   `Arc<Vec<T>>`. If `T` at read-time differs from `T` at write-time, the
//!   downcast returns `None` silently. `CachedRdd` then recomputes the partition
//!   from scratch with no error or warning. This can hide type-system bugs and
//!   causes unexpected recomputation.
//!
//! B7 — Unbounded `PartitionStore` growth (crates/atomic-data/src/cache/mod.rs):
//!   `PartitionStore` uses a `DashMap` with no eviction policy. Every `cache()`
//!   call adds to the map and nothing is ever removed (except via `remove_rdd`).
//!   Under a workload that repeatedly caches new RDDs the process eventually OOMs.
//!   `BoundedMemoryCache::ensure_free_space()` is a `todo!()` stub.
//!
//! Fix B6: log a warning or return `Err` when the stored type does not match.
//! Fix B7: implement LRU eviction in `PartitionStore` with a configurable cap.

use atomic_compute::context::Context;
use atomic_compute::env::Config;
use atomic_compute::task;
use atomic_data::cache::{PartitionStore, init_partition_cache};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn ctx() -> Arc<Context> {
    Context::new_with_config(Config::local()).unwrap()
}

// ── Bug B6: type-mismatch silent recompute ────────────────────────────────────

/// `PartitionStore::get::<T>()` silently returns `None` when the stored type
/// doesn't match the requested type instead of surfacing an error.
///
/// This test documents the current (silent) behaviour.
/// After the fix, a `Result::Err` or at least a logged warning should appear.
#[tokio::test]
async fn test_type_mismatch_silent() {
    init_partition_cache();
    let store = PartitionStore::new();

    // Write an i32 partition under key (42, 0).
    store.put::<i32>(42, 0, Arc::new(vec![1, 2, 3]));

    // Read it back as String — wrong type.
    let result = store.get::<String>(42, 0);

    // Current behaviour: returns None (silent miss).
    assert!(
        result.is_none(),
        "expected None on type mismatch, got Some — PartitionStore type erasure broken"
    );
    // Bug B6: no warning, no error — the caller has no way to distinguish a
    // genuine cache miss from a type-mismatch miss.
}

/// Round-trip: write and read with the SAME type must succeed.
#[tokio::test]
async fn test_roundtrip_same_type() {
    let store = PartitionStore::new();
    let data = Arc::new(vec![10i32, 20, 30]);
    store.put::<i32>(100, 0, data.clone());
    let retrieved = store.get::<i32>(100, 0);
    assert!(retrieved.is_some(), "same-type get should succeed");
    assert_eq!(*retrieved.unwrap(), vec![10, 20, 30]);
}

// ── Bug B7: unbounded growth ──────────────────────────────────────────────────

/// LRU eviction: inserting more than `cap` partitions must keep the map ≤ cap.
#[tokio::test]
async fn test_unbounded_growth() {
    let cap = 100usize;
    let n = 500usize;
    let store = PartitionStore::with_cap(cap);

    for i in 0..n {
        store.put::<i32>(i, 0, Arc::new(vec![i as i32]));
    }

    assert!(
        store.len() <= cap,
        "expected at most {cap} entries after inserting {n}, got {}",
        store.len()
    );
}

// ── Cache correctness — no recompute on hit ───────────────────────────────────

static COMPUTE_COUNT: AtomicUsize = AtomicUsize::new(0);

// Serializes tests that reset and read COMPUTE_COUNT to prevent parallel interference.
static COUNTER_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
fn counter_guard() -> std::sync::MutexGuard<'static, ()> {
    COUNTER_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap()
}

#[task]
fn count_and_add_one(x: i32) -> i32 {
    COMPUTE_COUNT.fetch_add(1, Ordering::Relaxed);
    x + 1
}

/// `rdd.cache()` must not recompute a partition on the second action.
///
/// We use a global counter incremented inside the task function to detect
/// recomputation. With caching, the counter should increment exactly once per
/// partition element; without it, it would increment again on the second action.
///
/// This test exercises `CachedRdd::compute()` and the `PARTITION_CACHE` hit path.
#[tokio::test]
async fn test_no_recompute() {
    let _g = counter_guard();
    COMPUTE_COUNT.store(0, Ordering::Relaxed);

    let ctx = ctx();
    let data = vec![1i32, 2, 3, 4, 5, 6];
    let n = data.len();
    let rdd = ctx
        .parallelize_typed(data.clone(), 2)
        .map_task(CountAndAddOne)
        .cache();

    // First action — computes all partitions and fills cache.
    let r1 = rdd.collect().unwrap();

    // Second action — must read from cache, NOT recompute.
    let r2 = rdd.collect().unwrap();

    // Both results must be identical (sorted because partition order varies).
    let mut s1 = r1.clone();
    let mut s2 = r2.clone();
    s1.sort();
    s2.sort();
    assert_eq!(s1, s2, "cached results must be identical across actions");

    // The task function should have been called exactly `n` times (first action only).
    let count = COMPUTE_COUNT.load(Ordering::Relaxed);
    assert_eq!(
        count, n,
        "task ran {count} times but should only run {n} times (second action hit cache)"
    );
}

/// `rdd.cache()` followed by two different actions both return the same data.
#[tokio::test]
async fn test_count_collect_agree() {
    let ctx = ctx();
    let data: Vec<i32> = (1..=10).collect();
    let rdd = ctx.parallelize_typed(data.clone(), 3).cache();

    let collected = rdd.collect().unwrap();
    let count = rdd.count().unwrap();

    assert_eq!(collected.len(), count as usize);
    assert_eq!(count, data.len() as u64);
}

/// Cached RDD with map_task in the chain: verifies the whole pipeline is cached.
#[tokio::test]
async fn test_transform_consistency() {
    let _g = counter_guard();
    let ctx = ctx();
    let data = vec![1i32, 2, 3];
    let rdd = ctx
        .parallelize_typed(data, 2)
        .map_task(CountAndAddOne)
        .cache();

    let mut r1 = rdd.collect().unwrap();
    let mut r2 = rdd.collect().unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(
        r1, r2,
        "transformed cached RDD must be stable across actions"
    );
}

// ── PartitionStore remove_rdd ────────────────────────────────────────────────

/// `remove_rdd()` must delete all partitions for that rdd_id.
#[tokio::test]
async fn test_remove_clears() {
    let store = PartitionStore::new();
    for p in 0..4 {
        store.put::<i32>(77, p, Arc::new(vec![p as i32]));
    }

    // Verify all 4 partitions are present.
    for p in 0..4 {
        assert!(
            store.get::<i32>(77, p).is_some(),
            "partition {p} should be present before remove"
        );
    }

    store.remove_rdd(77, 4);

    // After remove, all should be gone.
    for p in 0..4 {
        assert!(
            store.get::<i32>(77, p).is_none(),
            "partition {p} should be absent after remove_rdd"
        );
    }
}

// ── Multiple StorageLevel variants ───────────────────────────────────────────

// ── unpersist() / is_cached() ────────────────────────────────────────────────

#[tokio::test]
async fn test_unpersist_clears_cache() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3], 1).cache();
    let _ = rdd.collect().unwrap();
    assert!(rdd.is_cached(), "RDD should be cached after first collect");
    let rdd = rdd.unpersist();
    assert!(!rdd.is_cached(), "RDD should not be cached after unpersist");
}

#[tokio::test]
async fn test_uncached_before_action() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3], 2).cache();
    assert!(
        !rdd.is_cached(),
        "RDD should not be cached before any action"
    );
}

#[tokio::test]
async fn test_unpersist_then_recompute() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3], 1).cache();
    let _ = rdd.collect().unwrap();
    let rdd = rdd.unpersist();
    let mut result = rdd.collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3]);
}

/// `persist(StorageLevel::MemoryOnly)` is the default and must work identically
/// to `cache()`. All other levels fall back to MemoryOnly per the docs.
#[tokio::test]
async fn test_persist_like_cache() {
    use atomic_data::cache::StorageLevel;
    let ctx = ctx();
    let data = vec![10i32, 20, 30];
    let rdd = ctx
        .parallelize_typed(data.clone(), 1)
        .persist(StorageLevel::MemoryOnly);

    let mut r1 = rdd.collect().unwrap();
    let mut r2 = rdd.collect().unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1, data);
}

#[tokio::test]
async fn test_persist_disk_correct() {
    use atomic_data::cache::StorageLevel;
    let ctx = ctx();
    let data: Vec<i32> = (1..=6).collect();
    let rdd = ctx
        .parallelize_typed(data.clone(), 2)
        .persist(StorageLevel::MemoryAndDisk);
    let mut r1 = rdd.collect().unwrap();
    let mut r2 = rdd.collect().unwrap();
    r1.sort();
    r2.sort();
    assert_eq!(r1, r2);
    assert_eq!(r1, data);
}

#[tokio::test]
async fn test_diskonly_correct() {
    use atomic_data::cache::StorageLevel;
    let ctx = ctx();
    let data: Vec<i32> = (1..=4).collect();
    let rdd = ctx
        .parallelize_typed(data.clone(), 2)
        .persist(StorageLevel::DiskOnly);
    let mut result = rdd.collect().unwrap();
    result.sort();
    assert_eq!(result, data);
}

// ── checkpoint round-trip ─────────────────────────────────────────────────────

/// `checkpoint(dir)` must write partitions under the same id the returned
/// `CheckpointRdd` reads back from. A regression guard: the write path keyed
/// partitions by the *source* RDD's id while the read path used the new
/// `CheckpointRdd` id, so every read-back missed its file and the job hung.
#[tokio::test]
async fn test_checkpoint_roundtrip() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let data: Vec<i32> = (1..=8).collect();

    let checkpointed = ctx
        .parallelize_typed(data.clone(), 3)
        .checkpoint(dir.path().to_string_lossy().as_ref())
        .unwrap();

    // Lineage is truncated — the returned RDD reads straight from disk.
    let mut result = checkpointed.collect().unwrap();
    result.sort();
    assert_eq!(result, data);
}
