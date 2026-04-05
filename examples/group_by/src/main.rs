/// Key-value grouping — demonstrates pair RDD operations.
///
/// Shows: reduce_by_key (single-partition), group_by_key, map_values, count_by_key.
///
/// Note: `reduce_by_key` and `group_by_key` are narrow partition-local transformations.
/// For globally correct results without a shuffle they are shown here on a single
/// partition. `count_by_key` performs a driver-side merge and is correct across
/// any number of partitions.
///
/// Run locally:
///   cargo run --manifest-path examples/group_by/Cargo.toml
///
/// Run distributed (after `atomic deploy`):
///   ATOMIC_DEPLOYMENT_MODE=distributed ATOMIC_IS_DRIVER=true \
///   ATOMIC_SLAVES=host1@10.0.0.1 \
///   cargo run --manifest-path examples/group_by/Cargo.toml
use atomic_compute::context::Context;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    // --- Example 1: word count via reduce_by_key (1 partition → global result) ---
    let words = vec![
        ("rust", 1i32), ("atomic", 1), ("rust", 1), ("spark", 1),
        ("atomic", 1), ("rust", 1), ("distributed", 1), ("spark", 1),
    ];

    let word_counts = ctx
        .parallelize_typed(words, 1)       // single partition for correct global reduce
        .reduce_by_key(|a, b| a + b)
        .collect()?;

    println!("=== Word counts (reduce_by_key) ===");
    let mut sorted = word_counts.clone();
    sorted.sort_by_key(|(w, _)| *w);
    for (word, count) in &sorted {
        println!("  {:20} {}", word, count);
    }

    // --- Example 2: group_by_key (1 partition → global result) ---
    let scores: Vec<(&str, i32)> = vec![
        ("alice", 90), ("bob", 75), ("alice", 85),
        ("carol", 92), ("bob", 88), ("carol", 78),
    ];

    let grouped = ctx
        .parallelize_typed(scores, 1)      // single partition for correct global grouping
        .group_by_key()
        .collect()?;

    println!("\n=== Score groups (group_by_key) ===");
    let mut grouped_sorted = grouped;
    grouped_sorted.sort_by_key(|(name, _)| *name);
    for (name, scores) in &grouped_sorted {
        let mut s = scores.clone();
        s.sort();
        println!("  {:10} {:?}", name, s);
    }

    // --- Example 3: map_values — transform values, keep keys ---
    let averages = ctx
        .parallelize_typed(
            vec![
                ("alice", vec![90i32, 85]),
                ("bob",   vec![75, 88]),
                ("carol", vec![92, 78]),
            ],
            2,
        )
        .map_values(|scores| scores.iter().sum::<i32>() as f64 / scores.len() as f64)
        .collect()?;

    println!("\n=== Averages (map_values) ===");
    let mut avgs_sorted = averages;
    avgs_sorted.sort_by_key(|(name, _)| *name);
    for (name, avg) in &avgs_sorted {
        println!("  {:10} {:.1}", name, avg);
    }

    // --- Example 4: count_by_key (driver-side merge, works across all partitions) ---
    let department_employees: Vec<(&str, &str)> = vec![
        ("engineering", "alice"), ("engineering", "bob"),
        ("sales", "carol"), ("engineering", "dave"),
        ("sales", "eve"), ("hr", "frank"),
    ];

    let dept_counts = ctx
        .parallelize_typed(department_employees, 3)  // multiple partitions — count_by_key merges on driver
        .count_by_key()?;

    println!("\n=== Department headcount (count_by_key, 3 partitions) ===");
    let mut dc: Vec<_> = dept_counts.into_iter().collect();
    dc.sort_by_key(|(dept, _)| *dept);
    for (dept, count) in &dc {
        println!("  {:20} {}", dept, count);
    }

    Ok(())
}
