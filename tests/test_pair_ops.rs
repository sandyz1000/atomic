// `task_fn!(|x| -> T { .. })` requires block bodies (return-typed closures);
// clippy's unused_braces is a false positive on these macro inputs.
#![allow(unused_braces)]

use atomic_compute::context::Context;
use atomic_compute::task;
use std::sync::Arc;

fn ctx() -> Arc<Context> {
    Context::local().unwrap()
}

#[task]
fn add_i32(a: i32, b: i32) -> i32 {
    a + b
}

fn word_pairs(ctx: &Arc<Context>) -> atomic_compute::rdd::typed::TypedRdd<(String, i32)> {
    ctx.parallelize_typed(
        vec![
            ("a".to_string(), 1i32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
            ("b".to_string(), 4),
            ("c".to_string(), 5),
        ],
        2,
    )
}

// ── reduce_by_key() ───────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reduce_by_key_sum() {
    let ctx = ctx();
    let mut result = word_pairs(&ctx)
        .reduce_by_key_task(AddI32)
        .collect()
        .unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(
        result,
        vec![
            ("a".to_string(), 4),
            ("b".to_string(), 6),
            ("c".to_string(), 5),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reduce_by_key_empty() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(Vec::<(String, i32)>::new(), 2)
        .reduce_by_key_task(AddI32)
        .collect()
        .unwrap();
    assert!(result.is_empty());
}

// ── group_by_key() ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_key() {
    let ctx = ctx();
    let mut result = word_pairs(&ctx).group_by_key().collect().unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    for (_, vs) in &mut result {
        let mut vs_sorted = vs.to_vec();
        vs_sorted.sort();
        *vs = vs_sorted;
    }
    assert_eq!(result[0], ("a".to_string(), vec![1, 3]));
    assert_eq!(result[1], ("b".to_string(), vec![2, 4]));
    assert_eq!(result[2], ("c".to_string(), vec![5]));
}

// ── map_values() ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_map_values_doubles() {
    let ctx = ctx();
    let mut result = word_pairs(&ctx)
        .map_task(atomic_compute::task_fn!(
            |kv: (String, i32)| -> (String, i32) { (kv.0, kv.1 * 2) }
        ))
        .collect()
        .unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(
        result,
        vec![
            ("a".to_string(), 2),
            ("a".to_string(), 6),
            ("b".to_string(), 4),
            ("b".to_string(), 8),
            ("c".to_string(), 10),
        ]
    );
}

// ── keys() / values() ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_keys() {
    let ctx = ctx();
    let mut result = word_pairs(&ctx).keys().collect().unwrap();
    result.sort();
    assert_eq!(
        result,
        vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
            "b".to_string(),
            "c".to_string()
        ]
    );
}

#[tokio::test]
async fn test_values() {
    let ctx = ctx();
    let mut result = word_pairs(&ctx).values().collect().unwrap();
    result.sort();
    assert_eq!(result, vec![1i32, 2, 3, 4, 5]);
}

// ── count_by_key() ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_by_key() {
    let ctx = ctx();
    let result = word_pairs(&ctx).count_by_key().unwrap();
    assert_eq!(result.get("a"), Some(&2u64));
    assert_eq!(result.get("b"), Some(&2u64));
    assert_eq!(result.get("c"), Some(&1u64));
}

// ── lookup() ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_lookup_found() {
    let ctx = ctx();
    let mut result = word_pairs(&ctx).lookup(&"a".to_string()).unwrap();
    result.sort();
    assert_eq!(result, vec![1i32, 3]);
}

#[tokio::test]
async fn test_lookup_not_found() {
    let ctx = ctx();
    let result = word_pairs(&ctx).lookup(&"z".to_string()).unwrap();
    assert!(result.is_empty());
}

// ── join() / left_outer_join() ────────────────────────────────────────────────
// join and cogroup now use shuffle infrastructure — guard against parallel interference.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_inner() {
    let ctx = ctx();
    let left = ctx.parallelize_typed(vec![("a".to_string(), 1i32), ("b".to_string(), 2)], 2);
    let right = ctx.parallelize_typed(vec![("a".to_string(), 10i32), ("c".to_string(), 30)], 2);
    let result = left.join(right).collect().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], ("a".to_string(), (1, 10)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_no_overlap() {
    let ctx = ctx();
    let left = ctx.parallelize_typed(vec![("a".to_string(), 1i32)], 1);
    let right = ctx.parallelize_typed(vec![("b".to_string(), 2i32)], 1);
    let result = left.join(right).collect().unwrap();
    assert!(result.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_left_outer_join() {
    let ctx = ctx();
    let left = ctx.parallelize_typed(vec![("a".to_string(), 1i32), ("b".to_string(), 2)], 2);
    let right = ctx.parallelize_typed(vec![("a".to_string(), 10i32)], 1);
    let mut result = left.left_outer_join(right).collect().unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(result[0], ("a".to_string(), (1, Some(10))));
    assert_eq!(result[1], ("b".to_string(), (2, None)));
}

/// Right outer join: every right key is preserved; a missing left key yields `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_right_outer_join() {
    let ctx = ctx();
    let left = ctx.parallelize_typed(vec![("a".to_string(), 1i32)], 1);
    let right = ctx.parallelize_typed(vec![("a".to_string(), 10i32), ("c".to_string(), 30)], 2);
    let mut result = left.right_outer_join(right).collect().unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    // "a" matches on both sides; "c" exists only on the right → left is None.
    assert_eq!(result[0], ("a".to_string(), (Some(1), 10)));
    assert_eq!(result[1], ("c".to_string(), (None, 30)));
}

/// Full outer join: keys from either side survive; the absent side is `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_full_outer_join() {
    let ctx = ctx();
    let left = ctx.parallelize_typed(vec![("a".to_string(), 1i32), ("b".to_string(), 2)], 2);
    let right = ctx.parallelize_typed(vec![("a".to_string(), 10i32), ("c".to_string(), 30)], 2);
    let mut result = left.full_outer_join(right).collect().unwrap();
    result.sort_by_key(|(k, _)| k.clone());
    // a → both, b → left only, c → right only.
    assert_eq!(result[0], ("a".to_string(), (Some(1), Some(10))));
    assert_eq!(result[1], ("b".to_string(), (Some(2), None)));
    assert_eq!(result[2], ("c".to_string(), (None, Some(30))));
}

// ── Range partitioner ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_partitioner_assign() {
    use atomic_data::partitioner::Partitioner;
    use std::any::Any;
    // bounds = [3, 7]: partition 0 = keys < 3, partition 1 = 3..7, partition 2 = >= 7
    let p = Partitioner::range(vec![3i32, 7i32], true);
    assert_eq!(p.get_num_of_partitions(), 3);
    assert_eq!(p.get_partition(&1i32 as &dyn Any), 0);
    assert_eq!(p.get_partition(&3i32 as &dyn Any), 1);
    assert_eq!(p.get_partition(&6i32 as &dyn Any), 1);
    assert_eq!(p.get_partition(&7i32 as &dyn Any), 2);
    assert_eq!(p.get_partition(&100i32 as &dyn Any), 2);
}

#[tokio::test]
async fn test_range_sort_global() {
    let ctx = ctx();
    let data: Vec<(i32, i32)> = vec![
        (5, 50),
        (1, 10),
        (3, 30),
        (7, 70),
        (2, 20),
        (6, 60),
        (4, 40),
    ];
    let sorted = ctx
        .parallelize_typed(data, 3)
        .sort_by_key_range(3, true)
        .collect()
        .unwrap();
    let keys: Vec<i32> = sorted.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6, 7]);
}

// ── sort_by_key() ─────────────────────────────────────────────────────────────
// sort_by_key now uses shuffle (sample → range partition → local sort); needs multi-thread + guard.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sort_by_key_ascending() {
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(
            vec![
                ("b".to_string(), 2i32),
                ("a".to_string(), 1),
                ("c".to_string(), 3),
            ],
            2,
        )
        .sort_by_key(true)
        .collect()
        .unwrap();
    // collect() preserves partition order (globally sorted), but sort for safety with small data.
    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(
        result,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sort_by_key_descending() {
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(
            vec![
                ("b".to_string(), 2i32),
                ("a".to_string(), 1),
                ("c".to_string(), 3),
            ],
            2,
        )
        .sort_by_key(false)
        .collect()
        .unwrap();
    result.sort_by_key(|(k, _)| std::cmp::Reverse(k.clone()));
    assert_eq!(
        result,
        vec![
            ("c".to_string(), 3),
            ("b".to_string(), 2),
            ("a".to_string(), 1),
        ]
    );
}

/// The sort-shuffle k-way merge must emit globally-ordered output straight from
/// `collect()` (no client re-sort), and the lazy merge must drive a streaming
/// consumer like `count()`. Reverse-ordered input across 4 partitions forces a
/// real multi-run merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sort_shuffle_globally_ordered() {
    let ctx = ctx();
    let data: Vec<(i32, i32)> = (0..40).rev().map(|k| (k, k * 10)).collect();

    let asc: Vec<i32> = ctx
        .parallelize_typed(data.clone(), 4)
        .sort_by_key(true)
        .collect()
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(
        asc.windows(2).all(|w| w[0] <= w[1]),
        "ascending not globally ordered: {asc:?}"
    );
    assert_eq!((asc.first(), asc.last()), (Some(&0), Some(&39)));

    let desc: Vec<i32> = ctx
        .parallelize_typed(data.clone(), 4)
        .sort_by_key(false)
        .collect()
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(
        desc.windows(2).all(|w| w[0] >= w[1]),
        "descending not globally ordered: {desc:?}"
    );

    // Lazy consumer: count() pulls the merge without materializing the output.
    let n = ctx
        .parallelize_typed(data, 4)
        .sort_by_key(true)
        .count()
        .unwrap();
    assert_eq!(n, 40);
}

// ── key_by() ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_key_by() {
    let ctx = ctx();
    let mut result = ctx
        .parallelize_typed(vec![1i32, 2, 3], 1)
        .map_task(atomic_compute::task_fn!(|x: i32| -> (i32, i32) {
            (x % 2, x)
        }))
        .collect()
        .unwrap();
    result.sort_by_key(|(k, _)| *k);
    assert!(result.contains(&(0, 2)));
    assert!(result.contains(&(1, 1)));
    assert!(result.contains(&(1, 3)));
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2A: Local shuffle E2E tests
//
// These tests exercise the full shuffle pipeline in local mode:
// map → shuffle-map stage → HTTP fetch → reduce stage.
// All require isolated shuffle state and use shuffle_guard() + #[ignore].
//
// register_shuffle_map!(String, i32) must be present in this binary.
// ─────────────────────────────────────────────────────────────────────────────

// Register the shuffle handler for (String, i32) at binary scope.
atomic_compute::register_shuffle_map!(String, i32);
// (i32, i32) is used by the sort-shuffle ordering test.
atomic_compute::register_shuffle_map!(i32, i32);

// Tasks used by Phase 2A shuffle tests.
#[task]
fn tokenize_line(line: String) -> Vec<(String, i32)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1i32))
        .collect()
}

#[task]
fn bucket_by_parity(x: i32) -> (String, i32) {
    (
        if x % 2 == 0 {
            "even".to_string()
        } else {
            "odd".to_string()
        },
        x,
    )
}

/// Classic word-count pipeline: tokenize → pair → reduce_by_key.
/// This is the canonical correctness benchmark for the local shuffle path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_word_count() {
    let ctx = ctx();

    let lines = vec![
        "hello world".to_string(),
        "hello rust".to_string(),
        "world of rust".to_string(),
    ];

    let mut result = ctx
        .parallelize_typed(lines, 2)
        .flat_map_task(TokenizeLine)
        .reduce_by_key_task(AddI32)
        .collect()
        .unwrap();

    result.sort_by_key(|(k, _): &(String, i32)| k.clone());
    assert_eq!(
        result,
        vec![
            ("hello".to_string(), 2),
            ("of".to_string(), 1),
            ("rust".to_string(), 2),
            ("world".to_string(), 2),
        ]
    );
}

/// `reduce_by_key` after a `map` transformation (chained pipeline → shuffle).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reduce_chained() {
    let ctx = ctx();

    // Map: each i32 → ("bucket", value) based on even/odd
    let mut result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5, 6], 2)
        .map_task(BucketByParity)
        .reduce_by_key_task(AddI32)
        .collect()
        .unwrap();

    result.sort_by_key(|(k, _): &(String, i32)| k.clone());
    assert_eq!(
        result,
        vec![("even".to_string(), 12), ("odd".to_string(), 9)]
    );
}

/// `group_by_key` across 4 partitions — all partitions must contribute.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_many_partitions() {
    let ctx = ctx();

    let pairs = vec![
        ("x".to_string(), 1i32),
        ("y".to_string(), 2i32),
        ("x".to_string(), 3i32),
        ("z".to_string(), 4i32),
        ("y".to_string(), 5i32),
        ("x".to_string(), 6i32),
        ("z".to_string(), 7i32),
        ("y".to_string(), 8i32),
    ];
    let mut result = ctx
        .parallelize_typed(pairs, 4) // spread across 4 partitions
        .group_by_key()
        .collect()
        .unwrap();

    result.sort_by_key(|(k, _)| k.clone());

    // Sort the inner Vec so assertions are deterministic.
    let mut result: Vec<(String, Vec<i32>)> = result
        .into_iter()
        .map(|(k, mut v)| {
            v.sort();
            (k, v)
        })
        .collect();
    result.sort_by_key(|(k, _)| k.clone());

    assert_eq!(result[0], ("x".to_string(), vec![1, 3, 6]));
    assert_eq!(result[1], ("y".to_string(), vec![2, 5, 8]));
    assert_eq!(result[2], ("z".to_string(), vec![4, 7]));
}

/// Partitions that contribute no keys for a given bucket must not produce output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reduce_empty() {
    let ctx = ctx();

    // Only 2 unique keys but 6 partitions → 4 partitions are empty.
    let pairs = vec![
        ("alpha".to_string(), 1i32),
        ("beta".to_string(), 10i32),
        ("alpha".to_string(), 2i32),
    ];
    let mut result = ctx
        .parallelize_typed(pairs, 6)
        .reduce_by_key_task(AddI32)
        .collect()
        .unwrap();

    result.sort_by_key(|(k, _)| k.clone());
    assert_eq!(
        result,
        vec![("alpha".to_string(), 3), ("beta".to_string(), 10)]
    );
}

// ── cogroup() ────────────────────────────────────────────────────────────────
// cogroup uses shuffle — needs registration for each (K, V) type combination used.
atomic_compute::register_shuffle_map!(String, u32);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cogroup_basic() {
    let ctx = ctx();
    let rdd1 = ctx.parallelize_typed(
        vec![
            ("a".to_string(), 1i32),
            ("b".to_string(), 2),
            ("a".to_string(), 3),
        ],
        2,
    );
    let rdd2 = ctx.parallelize_typed(vec![("a".to_string(), 10u32), ("c".to_string(), 30)], 2);
    let mut result = rdd1.cogroup(rdd2).collect().unwrap();
    result.sort_by_key(|(k, _, _)| k.clone());

    // "a" appears in both: v1s=[1,3] or [3,1], v2s=[10]
    let a = result.iter().find(|(k, _, _)| k == "a").unwrap();
    let mut a_v1 = a.1.clone();
    a_v1.sort();
    assert_eq!(a_v1, vec![1, 3]);
    assert_eq!(a.2, vec![10u32]);

    // "b" only in rdd1
    let b = result.iter().find(|(k, _, _)| k == "b").unwrap();
    assert_eq!(b.1, vec![2]);
    assert!(b.2.is_empty());

    // "c" only in rdd2
    let c = result.iter().find(|(k, _, _)| k == "c").unwrap();
    assert!(c.1.is_empty());
    assert_eq!(c.2, vec![30u32]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cogroup_empty_sides() {
    let ctx = ctx();
    let rdd1 = ctx.parallelize_typed(vec![("x".to_string(), 1i32)], 1);
    let rdd2 = ctx.parallelize_typed(vec![("y".to_string(), 2i32)], 1);
    let mut result = rdd1.cogroup(rdd2).collect().unwrap();
    result.sort_by_key(|(k, _, _)| k.clone());
    assert_eq!(result.len(), 2);
    let x = result.iter().find(|(k, _, _)| k == "x").unwrap();
    assert_eq!(x.1, vec![1]);
    assert!(x.2.is_empty());
    let y = result.iter().find(|(k, _, _)| k == "y").unwrap();
    assert!(y.1.is_empty());
    assert_eq!(y.2, vec![2]);
}

#[tokio::test]
async fn test_flat_map_values() {
    let ctx = ctx();
    // ("a", 3) → [("a",1),("a",2),("a",3)]   ("b", 2) → [("b",1),("b",2)]
    let data = vec![("a".to_string(), 3u32), ("b".to_string(), 2u32)];
    let rdd = ctx.parallelize_typed(data, 1);
    let mut result = rdd
        .flat_map_task(atomic_compute::task_fn!(
            |kv: (String, u32)| -> Vec<(String, u32)> {
                (1..=kv.1).map(|u| (kv.0.clone(), u)).collect()
            }
        ))
        .collect()
        .unwrap();
    result.sort();
    assert_eq!(
        result,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2),
            ("a".to_string(), 3),
            ("b".to_string(), 1),
            ("b".to_string(), 2),
        ]
    );
}

// ── aggregate_by_key_task(): accumulator type C != value type V ──────────────
// The shuffle ships the value wire type (String, f64); the accumulator (f64, u64)
// is produced on the reduce side and never crosses the shuffle as a key-value pair.
atomic_compute::register_shuffle_map!(String, f64);

/// Lift one rating into a (sum, count) accumulator.
#[task]
fn to_sum_count(r: f64) -> (f64, u64) {
    (r, 1)
}

/// Merge two (sum, count) accumulators component-wise — associative and commutative.
#[task]
fn add_sum_count(a: (f64, u64), b: (f64, u64)) -> (f64, u64) {
    (a.0 + b.0, a.1 + b.1)
}

/// Mean rating per key: lift each value into the (sum, count) monoid, merge, divide.
/// This is the canonical `C != V` case `reduce_by_key_task` cannot express.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_aggregate_by_key_mean() {
    let ctx = ctx();
    // a → [1, 3]  (mean 2.0),  b → [2, 4, 6]  (mean 4.0),  c → [5]  (mean 5.0)
    let ratings = ctx.parallelize_typed(
        vec![
            ("a".to_string(), 1.0f64),
            ("b".to_string(), 2.0),
            ("a".to_string(), 3.0),
            ("b".to_string(), 4.0),
            ("c".to_string(), 5.0),
            ("b".to_string(), 6.0),
        ],
        3,
    );

    let mut means = ratings
        .aggregate_by_key_task(ToSumCount, AddSumCount, 4)
        .map_task(atomic_compute::task_fn!(
            |kv: (String, (f64, u64))| -> (String, f64) { (kv.0, kv.1.0 / kv.1.1 as f64) }
        ))
        .collect()
        .unwrap();
    means.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    assert_eq!(
        means,
        vec![
            ("a".to_string(), 2.0),
            ("b".to_string(), 4.0),
            ("c".to_string(), 5.0),
        ]
    );
}

// ── reduce_by_key_locally_task(): driver-side reduce, no shuffle ──────────────

/// Reduce per key into a `HashMap` on the driver without a shuffle. Map-side
/// combine per partition, then merge the partial maps on the driver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reduce_by_key_locally() {
    let ctx = ctx();
    let totals = word_pairs(&ctx).reduce_by_key_locally_task(AddI32).unwrap();

    // word_pairs: a→[1,3]=4, b→[2,4]=6, c→[5]=5
    let mut entries: Vec<(String, i32)> = totals.into_iter().collect();
    entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    assert_eq!(
        entries,
        vec![
            ("a".to_string(), 4),
            ("b".to_string(), 6),
            ("c".to_string(), 5),
        ]
    );
}

// ── repartition_and_sort(): secondary sort, one shuffle ──────────────────────
// i64 value wire type avoids colliding with the i32/String/u32/f64 handlers above.
atomic_compute::register_sort_shuffle_map!(i64, i64);

/// Places an `i64` key into `key % n` — ships to workers by name.
struct ModPartitioner {
    n: usize,
}

impl atomic_data::partitioner::CustomPartitioner for ModPartitioner {
    fn num_partitions(&self) -> usize {
        self.n
    }
    fn get_partition_for_key(&self, key: &dyn std::any::Any) -> usize {
        let k = key.downcast_ref::<i64>().copied().unwrap_or(0);
        k.rem_euclid(self.n as i64) as usize
    }
}

impl atomic_data::partitioner::NamedPartitioner for ModPartitioner {
    const NAME: &'static str = "pair_ops_test_mod";
    fn create(n: usize) -> Self {
        ModPartitioner { n }
    }
}

atomic_compute::register_partitioner!(ModPartitioner);

/// Each output partition holds only keys `≡ p (mod n)` and is internally key-sorted —
/// the partitioner placement and the within-partition order both hold in one shuffle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repartition_and_sort() {
    let ctx = ctx();
    let pairs = ctx.parallelize_typed(
        vec![
            (7i64, 70i64),
            (1, 10),
            (4, 40),
            (2, 20),
            (5, 50),
            (0, 0),
            (3, 30),
            (6, 60),
        ],
        3,
    );

    let parts = pairs
        .repartition_and_sort(ModPartitioner { n: 3 }, true)
        .collect_partitions()
        .unwrap();

    assert_eq!(
        parts.len(),
        3,
        "must honor the partitioner's partition count"
    );
    for (p, bucket) in parts.iter().enumerate() {
        let keys: Vec<i64> = bucket.iter().map(|(k, _)| *k).collect();
        // Internally key-sorted.
        assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "partition {p} not key-sorted: {keys:?}"
        );
        // Every key lands in its `key % 3` partition.
        for k in &keys {
            assert_eq!(
                k.rem_euclid(3) as usize,
                p,
                "key {k} landed in partition {p}"
            );
        }
    }
}
