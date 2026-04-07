/// Word count using `#[task]` functions on distributed workers.
///
/// # Running locally (single process)
///
/// ```bash
/// cargo run -p task_wordcount
/// ```
///
/// # Running distributed (two terminals)
///
/// Terminal 1 — start two workers:
/// ```bash
/// cargo build -p task_wordcount --release
/// ./target/release/task_wordcount --worker --port 10001 &
/// ./target/release/task_wordcount --worker --port 10002 &
/// ```
///
/// Terminal 2 — run the driver, pointing at those workers:
/// ```bash
/// ./target/release/task_wordcount \
///   --driver \
///   --workers 127.0.0.1:10001,127.0.0.1:10002
/// ```
use std::collections::HashMap;

use atomic_compute::app::AtomicApp;
use atomic_compute::task;

// ── Task functions ─────────────────────────────────────────────────────────────
//
// Each function is registered in TASK_REGISTRY at link time.
// Workers receive a TaskEnvelope with the op_id and execute the registered handler
// directly — only data travels over the wire, not function code.

/// Split a line into lowercase words, stripping punctuation.
///
/// `flat_map_task(Tokenize)` sends each partition's lines to a worker; the
/// worker tokenises them and returns a flat list of words.
#[task]
fn tokenize(line: String) -> Vec<String> {
    line.split_whitespace()
        .map(|w| w.to_lowercase().trim_matches(|c: char| !c.is_alphabetic()).to_string())
        .filter(|w| !w.is_empty())
        .collect()
}

/// Pair each word with a count of 1.
///
/// `map_task(PairOne)` runs on the same worker immediately after tokenization —
/// both ops are pipelined into a single `TaskEnvelope` per partition.
#[task]
fn pair_one(word: String) -> (String, u32) {
    (word, 1)
}

// ── Dataset ───────────────────────────────────────────────────────────────────

fn corpus() -> Vec<String> {
    vec![
        "To be or not to be that is the question".to_string(),
        "Whether tis nobler in the mind to suffer".to_string(),
        "The slings and arrows of outrageous fortune".to_string(),
        "Or to take arms against a sea of troubles".to_string(),
        "And by opposing end them to die to sleep".to_string(),
        "No more and by a sleep to say we end".to_string(),
        "The heartache and the thousand natural shocks".to_string(),
        "That flesh is heir to tis a consummation".to_string(),
    ]
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // AtomicApp::build() reads CLI flags:
    //   --worker [--port N] [--local-ip ADDR]  → starts worker loop, never returns
    //   --workers addr:port,...                  → distributed driver
    //   (no flags)                               → local driver
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;

    let lines = corpus();
    let num_partitions = 4;

    println!("Processing {} lines across {} partitions...\n", lines.len(), num_partitions);

    // Pipeline:
    //   1. parallelize       — split lines into `num_partitions` partitions
    //   2. flat_map_task     — workers tokenize each partition: lines → words
    //   3. map_task          — workers pair each word: word → (word, 1)
    //   4. collect           — driver receives all (word, 1) pairs
    //
    // In distributed mode, steps 2 and 3 are pipelined into a single
    // TaskEnvelope per partition, so each worker receives lines, runs both
    // tasks, and returns (word, 1) pairs — one round-trip per partition.
    let pairs: Vec<(String, u32)> = ctx
        .parallelize_typed(lines, num_partitions)
        .flat_map_task(Tokenize)
        .map_task(PairOne)
        .collect()?;

    // Aggregate word counts on the driver.
    let mut word_counts: HashMap<String, u64> = HashMap::new();
    for (word, n) in pairs {
        *word_counts.entry(word).or_insert(0) += n as u64;
    }

    // Sort by count descending, then word alphabetically.
    let mut results: Vec<(String, u64)> = word_counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    println!("{:<20} COUNT", "WORD");
    println!("{}", "-".repeat(28));
    for (word, count) in &results {
        if !word.is_empty() {
            println!("{:<20} {}", word, count);
        }
    }

    let unique = results.iter().filter(|(w, _)| !w.is_empty()).count();
    let total: u64 = results.iter().map(|(_, c)| c).sum();
    println!("\nUnique words: {}  |  Total tokens: {}", unique, total);

    Ok(())
}
