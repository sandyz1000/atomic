use atomic_compute::{context::Context, task, task_fn};
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
async fn test_map_task() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4], 2)
        .map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
        .collect()
        .unwrap();
    assert_eq!(result, vec![2, 4, 6, 8]);
}

#[tokio::test]
async fn test_filter_task() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 2)
        .filter_task(task_fn!(|x: i32| -> bool { x % 2 == 0 }))
        .collect()
        .unwrap();
    assert_eq!(result, vec![2, 4]);
}

#[tokio::test]
async fn test_flat_map_task() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3], 2)
        .flat_map_task(task_fn!(|x: i32| -> Vec<i32> { vec![x, x * 10] }))
        .collect()
        .unwrap();
    assert_eq!(result, vec![1, 10, 2, 20, 3, 30]);
}

#[tokio::test]
async fn test_fold_task() {
    let ctx = ctx();
    let sum = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 2)
        .fold_task(0i32, task_fn!(|a: i32, b: i32| a + b))
        .unwrap();
    assert_eq!(sum, 15);
}

#[tokio::test]
async fn test_reduce_task() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4, 5], 2)
        .reduce_task(task_fn!(|a: i32, b: i32| a + b))
        .unwrap();
    assert_eq!(result, Some(15));
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
    // double → keep positives → sum, all via task pipeline
    #[task]
    fn double_i32(x: i32) -> i32 { x * 2 }
    #[task]
    fn is_positive(x: i32) -> bool { x > 0 }

    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![-2i32, -1, 0, 1, 2, 3], 2)
        .map_task(DoubleI32)
        .filter_task(IsPositive)
        .fold_task(0i32, task_fn!(|a: i32, b: i32| a + b))
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
        .map_task(task_fn!(|x: i32| -> i32 { x + 1 }))
        .collect()
        .unwrap();
    assert_eq!(result, vec![11, 21, 31]);
}

#[tokio::test]
async fn test_more_partitions_than_elements() {
    let ctx = ctx();
    let result = ctx
        .parallelize_typed(vec![1i32, 2], 8)
        .map_task(task_fn!(|x: i32| -> i32 { x * 3 }))
        .collect()
        .unwrap();
    let mut sorted = result;
    sorted.sort();
    assert_eq!(sorted, vec![3, 6]);
}
