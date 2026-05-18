use atomic_compute::context::Context;
use std::sync::{Arc, Mutex};

fn ctx() -> Arc<Context> {
    Context::local().unwrap()
}

// ── first() ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_first_nonempty() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![10i32, 20, 30], 2)
        .first()
        .unwrap();
    assert_eq!(result, 10);
}

#[tokio::test]
async fn test_first_empty_errors() {
    let ctx = ctx();
    let result = ctx.parallelize_typed(Vec::<i32>::new(), 2).first();
    assert!(result.is_err(), "first() on empty RDD should return Err");
}

// ── is_empty() ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_is_empty_true() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(Vec::<i32>::new(), 2)
        .is_empty()
        .unwrap();
    assert!(result);
}

#[tokio::test]
async fn test_is_empty_false() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3], 2)
        .is_empty()
        .unwrap();
    assert!(!result);
}

// ── aggregate() ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_aggregate_sum() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5, 6], 4)
        .aggregate(0i32, |acc, x| acc + x, |a, b| a + b)
        .unwrap();
    assert_eq!(result, 21);
}

#[tokio::test]
async fn test_aggregate_count() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![10i32, 20, 30, 40], 2)
        .aggregate(0i32, |acc, _x| acc + 1, |a, b| a + b)
        .unwrap();
    assert_eq!(result, 4);
}

// ── max() / min() ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_max_numeric() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![3i32, 1, 4, 1, 5, 9, 2, 6], 3)
        .max()
        .unwrap();
    assert_eq!(result, Some(9));
}

#[tokio::test]
async fn test_min_numeric() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![3i32, 1, 4, 1, 5, 9, 2, 6], 3)
        .min()
        .unwrap();
    assert_eq!(result, Some(1));
}

#[tokio::test]
async fn test_max_empty() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(Vec::<i32>::new(), 2)
        .max()
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_min_empty() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(Vec::<i32>::new(), 2)
        .min()
        .unwrap();
    assert_eq!(result, None);
}

// ── top() / take_ordered() ────────────────────────────────────────────────────

#[tokio::test]
async fn test_top_k() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![3i32, 1, 4, 1, 5, 9, 2, 6], 3)
        .top(3)
        .unwrap();
    assert_eq!(result, vec![9, 6, 5]);
}

#[tokio::test]
async fn test_top_zero() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3], 2)
        .top(0)
        .unwrap();
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_take_ordered() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![3i32, 1, 4, 1, 5, 9, 2, 6], 3)
        .take_ordered(3)
        .unwrap();
    assert_eq!(result, vec![1, 1, 2]);
}

// ── for_each() / for_each_partition() ─────────────────────────────────────────

#[tokio::test]
async fn test_for_each_accumulates() {
    let ctx = ctx();
    let counter = Arc::new(Mutex::new(0i32));
    let counter2 = Arc::clone(&counter);
    ctx.parallelize_typed(vec![1i32, 2, 3, 4, 5], 3)
        .for_each(move |_x| {
            *counter2.lock().unwrap() += 1;
        })
        .unwrap();
    assert_eq!(*counter.lock().unwrap(), 5);
}

#[tokio::test]
async fn test_for_each_partition() {
    let ctx = ctx();
    let num_partitions = 3usize;
    let counter = Arc::new(Mutex::new(0usize));
    let counter2 = Arc::clone(&counter);
    ctx.parallelize_typed(vec![1i32, 2, 3, 4, 5, 6], num_partitions)
        .for_each_partition(move |_iter| {
            *counter2.lock().unwrap() += 1;
        })
        .unwrap();
    assert_eq!(*counter.lock().unwrap(), num_partitions);
}
