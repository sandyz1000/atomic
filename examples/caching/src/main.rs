//! RDD persistence — `cache`, `persist`, `unpersist`, and `checkpoint`.
//!
//! Mirrors Spark's caching example. A global counter inside the mapped task
//! lets us *observe* recomputation: each element computed bumps the counter, so
//! we can prove that a cached RDD is computed once and reused on later actions.
//!
//! Demonstrates:
//! - `cache()` — second action hits the partition store, no recompute.
//! - `unpersist()` — drops cached partitions; the next action recomputes.
//! - `persist_with_disk(MemoryAndDisk)` — spills to disk under memory pressure.
//! - `checkpoint(dir)` — materializes to disk and truncates lineage.
//!
//! # Running locally
//!
//! ```bash
//! cargo run -p caching
//! ```
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_compute::context::Context;
use atomic_compute::task;
use atomic_data::cache::StorageLevel;

/// Counts how many elements the `Square` task has computed this process.
static COMPUTE_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Square a number, recording that a (re)computation happened.
///
/// In local mode the task runs in-process, so the global counter is a faithful
/// measure of how many elements were actually computed vs served from cache.
#[task]
fn square(x: i64) -> i64 {
    COMPUTE_COUNT.fetch_add(1, Ordering::Relaxed);
    x * x
}

fn computed() -> usize {
    COMPUTE_COUNT.load(Ordering::Relaxed)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sc = Context::local()?;
    let data: Vec<i64> = (1..=10).collect();

    // ── cache(): compute once, reuse on subsequent actions ──────────────────
    println!("=== cache() ===");
    let cached = sc
        .parallelize_typed(data.clone(), 4)
        .map_task(Square)
        .cache();

    COMPUTE_COUNT.store(0, Ordering::Relaxed);
    let sum: i64 = cached.collect()?.iter().sum();
    println!(
        "first action  (collect): sum={sum}, elements computed={}",
        computed()
    );

    let count = cached.count()?;
    println!(
        "second action (count):   count={count}, elements computed={} (cache hit, no recompute)",
        computed()
    );
    println!("is_cached() = {}", cached.is_cached());

    // ── unpersist(): drop the cache, next action recomputes ─────────────────
    println!("\n=== unpersist() ===");
    let dropped = cached.unpersist();
    println!(
        "immediately after unpersist: is_cached() = {}",
        dropped.is_cached()
    );
    COMPUTE_COUNT.store(0, Ordering::Relaxed);
    let _ = dropped.collect()?;
    println!(
        "next collect recomputed {} elements (then re-fills the cache)",
        computed()
    );

    // ── persist_with_disk(MemoryAndDisk): spill on memory pressure ──────────
    println!("\n=== persist(MemoryAndDisk) ===");
    let on_disk = sc
        .parallelize_typed(data.clone(), 4)
        .map_task(Square)
        .persist_with_disk(StorageLevel::MemoryAndDisk);
    let top = on_disk.collect()?.into_iter().max().unwrap_or_default();
    println!("max square = {top} (partitions may spill to disk when evicted)");

    // ── checkpoint(dir): materialize + truncate lineage ─────────────────────
    println!("\n=== checkpoint() ===");
    let dir = tempfile::tempdir()?;
    let checkpoint_uri = dir.path().to_string_lossy().to_string();
    let base = sc.parallelize_typed(data, 4).map_task(Square);
    let checkpointed = base.checkpoint(&checkpoint_uri)?;
    let after: Vec<i64> = {
        let mut v = checkpointed.collect()?;
        v.sort();
        v
    };
    println!(
        "checkpointed RDD reads from {checkpoint_uri} with no parent lineage; values = {after:?}"
    );

    Ok(())
}
