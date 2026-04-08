/// Word count — canonical Atomic example.
///
/// Demonstrates: parallelize, flat_map_task, map_task (task pipeline),
/// and driver-side aggregation via HashMap.
///
/// # Running locally
///
/// ```bash
/// cargo run -p wordcount
/// ```
///
/// # Running distributed (two terminals)
///
/// Terminal 1 — start a worker:
/// ```bash
/// cargo build -p wordcount --release
/// ./target/release/wordcount --worker --port 10001
/// ```
///
/// Terminal 2 — run the driver:
/// ```bash
/// ./target/release/wordcount --workers 127.0.0.1:10001
/// ```
use std::collections::HashMap;

use atomic_compute::app::AtomicApp;
use atomic_compute::task;

/// Split a line into words, lower-cased and stripped of punctuation.
///
/// Registered in `TASK_REGISTRY` at link time. Workers execute this via the
/// compiled dispatch handler — only the data travels over the wire, not the code.
#[task]
fn tokenize(line: String) -> Vec<String> {
    line.split_whitespace()
        .map(|w| w.to_lowercase().trim_matches(|c: char| !c.is_alphabetic()).to_string())
        .filter(|w| !w.is_empty())
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // AtomicApp::build() reads CLI flags:
    //   --worker [--port N]    → starts worker loop, never returns
    //   --workers addr:port,...→ distributed driver
    //   (no flags)             → local driver
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;

    let lines = vec![
        "hello world".to_string(),
        "hello atomic".to_string(),
        "atomic is fast".to_string(),
        "world is big".to_string(),
    ];

    // Pipeline runs on workers in distributed mode:
    //   flat_map_task(Tokenize) — lines → words  (remote)
    //   map_task                — word → (word,1) (remote, pipelined)
    //   collect                 — bring (word,1) pairs to driver
    let pairs: Vec<(String, u32)> = ctx
        .parallelize_typed(lines, 2)
        .flat_map_task(Tokenize)
        .map_task(atomic_compute::task_fn!(|w: String| -> (String, u32) { (w, 1) }))
        .collect()?;

    // Aggregate on driver.
    let mut counts: HashMap<String, u64> = HashMap::new();
    for (word, n) in pairs {
        *counts.entry(word).or_insert(0) += n as u64;
    }

    let mut results: Vec<_> = counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    println!("Word counts (sorted by frequency):");
    for (word, count) in &results {
        println!("  {:<20} {}", word, count);
    }

    Ok(())
}
