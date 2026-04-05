use atomic_compute::context::Context;
use std::sync::Arc;

fn ctx() -> Arc<Context> {
    Context::local().unwrap()
}

#[tokio::test]
async fn test_count_by_value_words() {
    let ctx = ctx();
    let words: Vec<String> = vec![
        "apple", "banana", "apple", "cherry", "banana", "apple",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect();

    let counts = ctx
        .parallelize_typed(words, 2)
        .count_by_value()
        .unwrap();

    assert_eq!(counts["apple"], 3);
    assert_eq!(counts["banana"], 2);
    assert_eq!(counts["cherry"], 1);
}

#[tokio::test]
async fn test_word_count_flat_map() {
    let ctx = ctx();
    let lines = vec![
        "to be or not to be".to_string(),
        "that is the question".to_string(),
    ];

    let counts = ctx
        .parallelize_typed(lines, 1)
        .flat_map(|line| {
            Box::new(
                line.split_whitespace()
                    .map(|w| w.to_string())
                    .collect::<Vec<_>>()
                    .into_iter(),
            )
        })
        .count_by_value()
        .unwrap();

    assert_eq!(counts["to"], 2);
    assert_eq!(counts["be"], 2);
    assert_eq!(counts["not"], 1);
    assert_eq!(counts["or"], 1);
}

#[tokio::test]
async fn test_map_to_pairs_then_fold() {
    let ctx = ctx();
    let data = vec![1i32, 2, 3, 4, 5];

    let sum_of_squares = ctx
        .parallelize_typed(data, 2)
        .map(|x| x * x)
        .fold(0i32, |acc, x| acc + x)
        .unwrap();

    assert_eq!(sum_of_squares, 1 + 4 + 9 + 16 + 25);
}

#[tokio::test]
async fn test_count_by_value_empty() {
    let ctx = ctx();
    let counts = ctx
        .parallelize_typed(Vec::<i32>::new(), 2)
        .count_by_value()
        .unwrap();
    assert!(counts.is_empty());
}

#[tokio::test]
async fn test_count_by_value_single_value() {
    let ctx = ctx();
    let counts = ctx
        .parallelize_typed(vec![42i32; 100], 4)
        .count_by_value()
        .unwrap();
    assert_eq!(counts[&42], 100);
}
