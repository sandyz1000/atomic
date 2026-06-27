use atomic_compute::context::Context;
use std::sync::Arc;

// distinct() is shuffle-based (global dedup), so the element wire type must be registered.
atomic_compute::register_shuffle_map!(i32, ());

fn ctx() -> Arc<Context> {
    Context::local().unwrap()
}

// ── union() ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_union_two_rdds() {
    let ctx = ctx();
    let a = ctx.parallelize_typed(vec![1i32, 2, 3], 2);
    let b = ctx.parallelize_typed(vec![4i32, 5, 6], 2);
    let mut result = a.union(b).collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_union_with_empty() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3], 2);
    let empty = ctx.parallelize_typed(Vec::<i32>::new(), 2);
    let mut result = rdd.union(empty).collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_union_preserves_duplicates() {
    let ctx = ctx();
    let a = ctx.parallelize_typed(vec![1i32, 2], 1);
    let b = ctx.parallelize_typed(vec![2i32, 3], 1);
    let mut result = a.union(b).collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 2, 3]);
}

// ── zip() ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zip_pairs() {
    let ctx = ctx();
    let a = ctx.parallelize_typed(vec![10i32, 20, 30], 1);
    let b = ctx.parallelize_typed(vec![1i32, 2, 3], 1);
    let result = a.zip(b).collect().unwrap();
    assert_eq!(result, vec![(10, 1), (20, 2), (30, 3)]);
}

// ── distinct() ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_distinct_removes_duplicates() {
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(vec![1i32, 2, 2, 3, 3, 3], 2)
        .distinct()
        .collect()
        .unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_distinct_already_unique() {
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(vec![10i32, 20, 30], 2)
        .distinct()
        .collect()
        .unwrap();
    result.sort();
    assert_eq!(result, vec![10, 20, 30]);
}

/// The same value placed in *different* partitions must collapse to one — the global
/// guarantee the old per-partition dedup got wrong. `[1,1,1,1]` over 4 partitions puts
/// one `1` in each; only a shuffle yields a single `1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_distinct_cross_partition() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 1, 1, 1], 4)
        .distinct()
        .collect()
        .unwrap();
    assert_eq!(result, vec![1]);
}

// ── subtract() ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_subtract() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3, 4], 2);
    let other = ctx.parallelize_typed(vec![2i32, 4], 2);
    let mut result = rdd.subtract(other).collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 3]);
}

#[tokio::test]
async fn test_subtract_empty_other() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3], 2);
    let empty = ctx.parallelize_typed(Vec::<i32>::new(), 2);
    let mut result = rdd.subtract(empty).collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3]);
}

// ── intersection() ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_intersection() {
    let ctx = ctx();
    let a = ctx.parallelize_typed(vec![1i32, 2, 3], 2);
    let b = ctx.parallelize_typed(vec![2i32, 3, 4], 2);
    let mut result = a.intersection(b).collect().unwrap();
    result.sort();
    assert_eq!(result, vec![2, 3]);
}

// ── cartesian() ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cartesian_product() {
    let ctx = ctx();
    let a = ctx.parallelize_typed(vec![1i32, 2], 1);
    let b = ctx.parallelize_typed(vec![10i32, 20], 1);
    let result = a.cartesian(b).collect().unwrap();
    assert_eq!(
        result.len(),
        4,
        "cartesian product of 2×2 should have 4 elements"
    );
    assert!(result.contains(&(1, 10)));
    assert!(result.contains(&(1, 20)));
    assert!(result.contains(&(2, 10)));
    assert!(result.contains(&(2, 20)));
}
