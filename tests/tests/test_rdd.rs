use atomic_compute::context::Context;
use std::sync::Arc;

fn ctx() -> Arc<Context> {
    Context::local().unwrap()
}

#[tokio::test]
async fn test_parallelize_and_collect() {
    let ctx = ctx();
    let data = vec![1i32, 2, 3, 4];
    let result = ctx.parallelize_typed(data.clone(), 2).collect().unwrap();
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_map() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4], 2)
        .map(|x| x * 2)
        .collect()
        .unwrap();
    assert_eq!(result, vec![2, 4, 6, 8]);
}

#[tokio::test]
async fn test_filter() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 2)
        .filter(|x| x % 2 == 0)
        .collect()
        .unwrap();
    assert_eq!(result, vec![2, 4]);
}

#[tokio::test]
async fn test_flat_map() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3], 2)
        .flat_map(|x| Box::new(vec![x, x * 10].into_iter()))
        .collect()
        .unwrap();
    assert_eq!(result, vec![1, 10, 2, 20, 3, 30]);
}

#[tokio::test]
async fn test_fold() {
    let ctx = ctx();
    let sum = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 2)
        .fold(0i32, |a, b| a + b)
        .unwrap();
    assert_eq!(sum, 15);
}

#[tokio::test]
async fn test_count() {
    let ctx = ctx();
    let n = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5, 6], 3)
        .count()
        .unwrap();
    assert_eq!(n, 6);
}

#[tokio::test]
async fn test_take() {
    let ctx = ctx();
    let taken = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5, 6], 3)
        .take(3)
        .unwrap();
    assert_eq!(taken.len(), 3);
}

#[tokio::test]
async fn test_take_more_than_available() {
    let ctx = ctx();
    let taken = ctx
        .parallelize_typed(vec![1i32, 2, 3], 2)
        .take(10)
        .unwrap();
    assert_eq!(taken.len(), 3);
}

#[tokio::test]
async fn test_count_by_value() {
    let ctx = ctx();
    let counts = ctx
        .parallelize_typed(vec![1i32, 2, 1, 3, 2, 3, 3], 2)
        .count_by_value()
        .unwrap();
    assert_eq!(counts[&1], 2);
    assert_eq!(counts[&2], 2);
    assert_eq!(counts[&3], 3);
}

#[tokio::test]
async fn test_chained_map_filter_fold() {
    let ctx = ctx();
    // double → keep positives → sum
    let result = ctx
        .parallelize_typed(vec![-2i32, -1, 0, 1, 2, 3], 2)
        .map(|x| x * 2)
        .filter(|x| *x > 0)
        .fold(0i32, |a, b| a + b)
        .unwrap();
    assert_eq!(result, 2 + 4 + 6);
}

#[tokio::test]
async fn test_empty_rdd() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(Vec::<i32>::new(), 2)
        .collect()
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_single_partition() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![10i32, 20, 30], 1)
        .map(|x| x + 1)
        .collect()
        .unwrap();
    assert_eq!(result, vec![11, 21, 31]);
}

#[tokio::test]
async fn test_more_partitions_than_elements() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2], 8)
        .map(|x| x * 3)
        .collect()
        .unwrap();
    let mut sorted = result;
    sorted.sort();
    assert_eq!(sorted, vec![3, 6]);
}
