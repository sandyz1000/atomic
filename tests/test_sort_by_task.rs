//! Distributed-capable `sort_by_task` — sort a non-pair RDD by a registered
//! key-value task (`T -> (K, T)`) instead of a driver-collecting closure.

use atomic_compute::context::Context;
use atomic_compute::task;

// Key-value task: pair each word with its length as the sort key.
#[task]
fn by_len(word: String) -> (usize, String) {
    (word.len(), word)
}

// The (K, T) sort shuffle must be registered for the ordered path.
atomic_compute::register_sort_shuffle_map!(usize, String);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sorts_by_key_task() {
    let ctx = Context::local().unwrap();
    let words: Vec<String> = ["pear", "fig", "banana", "kiwi", "a"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let sorted = ctx
        .parallelize_typed(words, 3)
        .sort_by_task(ByLen, true)
        .collect()
        .unwrap();

    let lens: Vec<usize> = sorted.iter().map(|w| w.len()).collect();
    assert!(
        lens.windows(2).all(|w| w[0] <= w[1]),
        "not sorted by length: {sorted:?}"
    );
    assert_eq!(sorted.first().map(String::as_str), Some("a"));
    assert_eq!(sorted.last().map(String::as_str), Some("banana"));
}
