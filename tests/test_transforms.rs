use atomic_compute::{context::Context, rdd::typed::TypedRdd};
use std::sync::{Arc, Mutex};

atomic_compute::register_shuffle_map!(String, i32);
atomic_compute::register_shuffle_map!(usize, i32);

fn ctx() -> Arc<Context> {
    Context::local().unwrap()
}

static SHUFFLE_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

fn shuffle_guard() -> std::sync::MutexGuard<'static, ()> {
    SHUFFLE_LOCK.get_or_init(|| std::sync::Mutex::new(())).lock().unwrap()
}

// ── coalesce() ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_coalesce_reduces_partitions() {
    let ctx = ctx();
    let rdd = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5, 6, 7, 8], 4)
        .coalesce(2, false);
    assert_eq!(rdd.num_partitions(), 2);
    let mut result = rdd.collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

#[tokio::test]
async fn test_coalesce_noop() {
    let ctx = ctx();
    // CoalescedRdd clamps to the existing partition count when target > num_partitions
    let rdd = ctx
        .parallelize_typed(vec![1i32, 2, 3], 2)
        .coalesce(10, false);
    assert!(rdd.num_partitions() <= 10);
    let mut result = rdd.collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3]);
}

// ── repartition() ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_repartition() {
    let ctx = ctx();
    let rdd = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4], 2)
        .repartition(4);
    // repartition(n) = coalesce(n, shuffle=true); all elements preserved
    let mut result = rdd.collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3, 4]);
}

// ── num_partitions ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_num_partitions_getter() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![1i32, 2, 3, 4, 5], 3);
    assert_eq!(rdd.num_partitions(), 3);
}

// ── map_partitions() ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_map_partitions() {
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 3)
        .map_partitions(|iter| iter)
        .collect()
        .unwrap();
    result.sort();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_map_partitions_idx() {
    let ctx = ctx();
    let num_partitions = 3usize;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = Arc::clone(&seen);
    ctx.parallelize_typed(vec![0i32; 9], num_partitions)
        .map_partitions_with_index(move |idx, iter| {
            seen2.lock().unwrap().push(idx);
            iter
        })
        .collect()
        .unwrap();
    let mut indices = seen.lock().unwrap().clone();
    indices.sort();
    assert_eq!(indices, vec![0, 1, 2]);
}

// ── glom() ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_glom_partition_structure() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5, 6], 3)
        .glom()
        .collect()
        .unwrap();
    // 3 partitions → 3 inner vecs
    assert_eq!(result.len(), 3);
    // All elements preserved
    let mut all: Vec<i32> = result.into_iter().flatten().collect();
    all.sort();
    assert_eq!(all, vec![1, 2, 3, 4, 5, 6]);
}

// ── range() ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_range_sum() {
    let ctx = ctx();
    // range(1, 10, step=1, partitions=2) → [1..=10]
    let rdd = TypedRdd::new(ctx.range(1, 10, 1, 2), ctx.clone());
    let sum = rdd
        .aggregate(0u64, |acc, x| acc + x, |a, b| a + b)
        .unwrap();
    assert_eq!(sum, 55);
}

// ── cache() ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cache_consistent() {
    let ctx = ctx();
    let rdd = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 3)
        .cache();
    let first = rdd.collect().unwrap();
    let second = rdd.collect().unwrap();
    assert_eq!(first, second);
}

#[tokio::test]
async fn test_cache_count_matches() {
    let ctx = ctx();
    let rdd = ctx.parallelize_typed(vec![10i32, 20, 30], 2).cache();
    let c1 = rdd.count().unwrap();
    let c2 = rdd.count().unwrap();
    assert_eq!(c1, 3);
    assert_eq!(c1, c2);
}

// ── sample() ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sample_fraction_bounds() {
    let ctx = ctx();
    let data: Vec<i32> = (0..1000).collect();
    let result = ctx
        .parallelize_typed(data, 4)
        .sample(false, 0.5)
        .collect()
        .unwrap();
    // With 1000 elements and fraction 0.5, expect roughly 500.
    // Wide bounds to keep the test non-flaky.
    assert!(
        result.len() >= 100 && result.len() <= 900,
        "sampled len {} not in [100, 900]",
        result.len()
    );
}

// ── sort_by() ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sort_by_descending() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![3i32, 1, 4, 1, 5], 2)
        .sort_by(|x| *x, false)
        .collect()
        .unwrap();
    assert_eq!(result, vec![5, 4, 3, 1, 1]);
}

#[tokio::test]
async fn test_sort_by_ascending() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![3i32, 1, 4, 1, 5], 2)
        .sort_by(|x| *x, true)
        .collect()
        .unwrap();
    assert_eq!(result, vec![1, 1, 3, 4, 5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_partitions_pair_reduce() {
    let _g = shuffle_guard();
    let ctx = ctx();
    // Each partition emits word-count pairs; reduce_by_key sums them up.
    let data = vec![
        "hello world hello".to_string(),
        "world world foo".to_string(),
    ];
    let rdd = ctx.parallelize_typed(data, 2);
    let mut result = rdd
        .map_partitions_to_pair(|_idx, iter| {
            Box::new(iter.flat_map(|line| {
                line.split_whitespace()
                    .map(|w| (w.to_string(), 1i32))
                    .collect::<Vec<_>>()
            })) as Box<dyn Iterator<Item = (String, i32)>>
        })
        .reduce_by_key(|a, b| a + b)
        .collect()
        .unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(
        result,
        vec![
            ("foo".to_string(), 1),
            ("hello".to_string(), 2),
            ("world".to_string(), 3),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repartition_shuffle_redistributes() {
    let _g = shuffle_guard();
    let ctx = ctx();
    // 1000 items in 2 partitions (skewed: 500 each). Repartition to 5.
    // All 5 output partitions must be non-empty and total count must be preserved.
    let data: Vec<i32> = (0..1000).collect();
    let rdd = ctx.parallelize_typed(data, 2);
    let partitions = rdd.repartition_shuffle(5).collect_partitions().unwrap();
    assert_eq!(partitions.len(), 5, "should have 5 output partitions");
    let total: usize = partitions.iter().map(|p| p.len()).sum();
    assert_eq!(total, 1000, "all elements must be preserved");
    for (i, part) in partitions.iter().enumerate() {
        assert!(!part.is_empty(), "partition {i} should be non-empty after shuffle repartition");
    }
}
