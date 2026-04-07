/// Key-value grouping — demonstrates pair RDD operations.
///
/// Shows: reduce_by_key, group_by_key, map_values, count_by_key.
///
/// **Local-only example**: `reduce_by_key` and `group_by_key` use closure-based
/// aggregation functions that always run on the driver's local scheduler. They
/// cannot dispatch work to remote workers. Use `Context::local()` here.
///
/// For distributed grouping, use task-based pipelines with explicit key extraction
/// and `flat_map_task` / `map_task` to prepare key-value pairs on workers, then
/// aggregate on the driver.
///
/// Run locally:
///   cargo run -p group_by
use atomic_compute::context::Context;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    // --- Example 1: word count via reduce_by_key (1 partition → global result) ---
    let words: Vec<(String, i32)> = vec![
        ("rust".to_string(), 1i32),
        ("atomic".to_string(), 1),
        ("rust".to_string(), 1),
        ("spark".to_string(), 1),
        ("atomic".to_string(), 1),
        ("rust".to_string(), 1),
        ("distributed".to_string(), 1),
        ("spark".to_string(), 1),
    ];

    let word_counts = ctx
        .parallelize_typed(words, 1) // single partition for correct global reduce
        .reduce_by_key(|a, b| a + b)
        .collect()?;

    println!("=== Word counts (reduce_by_key) ===");
    let mut sorted = word_counts.clone();
    sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (word, count) in &sorted {
        println!("  {:20} {}", word, count);
    }

    // --- Example 2: group_by_key (1 partition → global result) ---
    let scores: Vec<(String, i32)> = vec![
        ("alice".to_string(), 90),
        ("bob".to_string(), 75),
        ("alice".to_string(), 85),
        ("carol".to_string(), 92),
        ("bob".to_string(), 88),
        ("carol".to_string(), 78),
    ];

    let grouped = ctx
        .parallelize_typed(scores, 1) // single partition for correct global grouping
        .group_by_key()
        .collect()?;

    println!("\n=== Score groups (group_by_key) ===");
    let mut grouped_sorted = grouped;
    grouped_sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (name, scores) in &grouped_sorted {
        let mut s = scores.clone();
        s.sort();
        println!("  {:10} {:?}", name, s);
    }

    // --- Example 3: map_values — transform values, keep keys ---
    let averages = ctx
        .parallelize_typed(
            vec![
                ("alice".to_string(), vec![90i32, 85]),
                ("bob".to_string(), vec![75, 88]),
                ("carol".to_string(), vec![92, 78]),
            ],
            2,
        )
        .map_values(|scores| scores.iter().sum::<i32>() as f64 / scores.len() as f64)
        .collect()?;

    println!("\n=== Averages (map_values) ===");
    let mut avgs_sorted = averages;
    avgs_sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (name, avg) in &avgs_sorted {
        println!("  {:10} {:.1}", name, avg);
    }

    // --- Example 4: count_by_key (driver-side merge, works across all partitions) ---
    let department_employees: Vec<(String, String)> = vec![
        ("engineering".to_string(), "alice".to_string()),
        ("engineering".to_string(), "bob".to_string()),
        ("sales".to_string(), "carol".to_string()),
        ("engineering".to_string(), "dave".to_string()),
        ("sales".to_string(), "eve".to_string()),
        ("hr".to_string(), "frank".to_string()),
    ];

    let dept_counts = ctx
        .parallelize_typed(department_employees, 3) // multiple partitions — count_by_key merges on driver
        .count_by_key()?;

    println!("\n=== Department headcount (count_by_key, 3 partitions) ===");
    let mut dc: Vec<_> = dept_counts.into_iter().collect();
    dc.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (dept, count) in &dc {
        println!("  {:20} {}", dept, count);
    }

    Ok(())
}
