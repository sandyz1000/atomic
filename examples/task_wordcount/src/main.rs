/// Word count using `#[task]` functions on distributed workers.
///
/// This is the canonical distributed compute example. It demonstrates:
/// - `flat_map` with a `#[task]` tokenizer — each line → Vec<String>
/// - `map` with a `#[task]` keyer — each word → (word, 1)
/// - `reduce_by_key` with a `#[task]` combiner — merge counts per key
///
/// # One binary, two roles
///
/// The same compiled binary runs as either driver or worker depending on flags:
///
/// ```
/// task_wordcount --worker --port 10001   ← worker mode: listens for tasks
/// task_wordcount --driver                 ← driver mode: runs the job
/// ```
///
/// In local mode (no flags), it runs everything in-process.
///
/// # Running locally
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
/// Terminal 2 — run the driver:
/// ```bash
/// ATOMIC_DEPLOYMENT_MODE=distributed \
/// ATOMIC_IS_DRIVER=true \
/// ATOMIC_SLAVES="user@127.0.0.1,user@127.0.0.1" \
///   ./target/release/task_wordcount --driver
/// ```
///
/// # Deploying to real workers with `atomic deploy`
///
/// ```bash
/// # Cross-compile for Linux workers
/// cargo run -p atomic-cli -- build --target x86_64-unknown-linux-musl
///
/// # Ship binary to workers, start them, record addresses to .atomic/cluster.toml
/// cargo run -p atomic-cli -- deploy \
///   --workers 10.0.0.1,10.0.0.2,10.0.0.3 \
///   --binary ./target/x86_64-unknown-linux-musl/release/task_wordcount \
///   --port 10001 \
///   --key ~/.ssh/id_rsa
///
/// # Run driver (reads worker list from .atomic/cluster.toml automatically)
/// ATOMIC_DEPLOYMENT_MODE=distributed ATOMIC_IS_DRIVER=true \
///   ./target/release/task_wordcount --driver
///
/// # Shut down all workers when done
/// cargo run -p atomic-cli -- stop
/// ```
use atomic_compute::app::AtomicApp;
use atomic_compute::task;

// ── Task functions ─────────────────────────────────────────────────────────────
//
// Each is registered in the TASK_REGISTRY at link time under an op_id derived
// from the module path, e.g. "task_wordcount::tokenize".
//
// Workers receive a TaskEnvelope { op_id: "task_wordcount::tokenize", action: FlatMap, data: ... }
// and call the generated __atomic_dispatch_tokenize handler directly — no network
// round-trip for the function code itself, only for the data.

/// Split a line into lowercase words, stripping punctuation.
///
/// Registered as: `TaskAction::FlatMap` on `Vec<String>` partition → `Vec<String>`
#[task]
fn tokenize(line: String) -> Vec<String> {
    line.split_whitespace()
        .map(|w| w.to_lowercase().trim_matches(|c: char| !c.is_alphabetic()).to_string())
        .filter(|w| !w.is_empty())
        .collect()
}

/// Pair each word with a count of 1.
///
/// Registered as: `TaskAction::Map` on `String` → `(String, u32)`
#[task]
fn pair_one(word: String) -> (String, u32) {
    (word, 1)
}

/// Merge two word counts (used in reduce_by_key).
///
/// Registered as: `TaskAction::Reduce` on `(String, u32)` partition
#[task]
fn merge_counts(a: (String, u32), b: (String, u32)) -> (String, u32) {
    (a.0, a.1 + b.1)
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
    // AtomicApp::build() checks CLI args:
    //   --worker [--port N]  → starts executor loop (never reaches code below)
    //   --driver / no flags  → returns Arc<Context> via driver_context()
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;

    let lines = corpus();
    let num_partitions = 4;

    println!("Processing {} lines across {} partitions...\n", lines.len(), num_partitions);

    // Pipeline:
    //   1. parallelize — distribute lines across partitions
    //   2. flat_map    — tokenize: each line → Vec<String> words  [uses #[task] fn]
    //   3. count_by_value — driver-side HashMap merge, correct across all partitions
    //
    // `tokenize` is a `#[task]` Vec-returning function → registered for TaskAction::FlatMap.
    // `count_by_value` collects per-partition HashMaps and merges them on the driver.
    let word_counts = ctx
        .parallelize_typed(lines, num_partitions)
        .flat_map(|line| Box::new(tokenize(line).into_iter()))
        .count_by_value()?;

    // Sort by count descending, then word alphabetically for stable output.
    let mut results: Vec<(String, u64)> = word_counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    println!("{:<20} {}", "WORD", "COUNT");
    println!("{}", "-".repeat(28));
    for (word, count) in &results {
        if !word.is_empty() {
            println!("{:<20} {}", word, count);
        }
    }

    // count total unique words and total tokens
    let unique = results.iter().filter(|(w, _)| !w.is_empty()).count();
    let total: u64 = results.iter().map(|(_, c)| c).sum();
    println!("\nUnique words: {}  |  Total tokens: {}", unique, total);

    Ok(())
}
