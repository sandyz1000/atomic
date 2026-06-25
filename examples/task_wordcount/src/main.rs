/// Word count using `#[task]` functions on distributed workers.
///
/// This binary has two roles — driver (producer) and worker (consumer) — selected
/// by CLI flags, exactly like rusty-celery's `Produce`/`Consume` subcommands.
///
/// # Running as worker (consumer)
///
/// ```bash
/// cargo build -p task_wordcount --release
/// ./target/release/task_wordcount --worker --port 10001
/// ./target/release/task_wordcount --worker --port 10002
/// ```
///
/// # Running as driver (producer)
///
/// Local (single process):
/// ```bash
/// cargo run -p task_wordcount
/// ```
///
/// Distributed — point the driver at running workers:
/// ```bash
/// ./target/release/task_wordcount --workers 127.0.0.1:10001,127.0.0.1:10002
/// ```
/// (`--driver` is also accepted as an explicit alias for the driver role.)
///
/// # Notes on distributed shuffle
///
/// `reduce_by_key` partitions `(word, count)` pairs by key hash and reduces each
/// output partition independently — no full data movement to the driver.
/// In local mode the shuffle writes to an in-process `DashMapShuffleCache` and
/// the result is fetched by the local `ShuffleManager` HTTP server.
/// In distributed mode each worker must register its `ShuffleManager` URI with
/// the driver's `MapOutputTracker` — see ROADMAP.md P0 for the remaining work.
use atomic_compute::app::{AppRole, AtomicApp};
use atomic_compute::task;

// Register the shuffle-map handler for `(String, u32)`.
// This links the type-specific partition-write function into the binary so that
// workers can materialise shuffle buckets when `reduce_by_key` fires.
atomic_compute::register_shuffle_map!(String, u32);

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
        .map(|w| {
            w.to_lowercase()
                .trim_matches(|c: char| !c.is_alphabetic())
                .to_string()
        })
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

/// Sum two word counts — the keyed-reduction merge, dispatched by the shuffle.
#[task]
fn add_u32(a: u32, b: u32) -> u32 {
    a + b
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
    // build() parses CLI flags and returns — role is stored in app.role.
    // Match on it to branch explicitly, mirroring celery's Consume/Produce pattern.
    //
    //   --worker [--port N] [--local-ip ADDR]   → AppRole::Worker  → run_worker()
    //   --driver / --workers addr:port,...       → AppRole::Driver  → driver_context()
    //   (no flags)                               → AppRole::Driver  → local driver
    let app = AtomicApp::build().await?;

    let ctx = match app.role {
        AppRole::Worker { .. } => {
            app.run_worker(); // executor: dispatch TaskEnvelopes via TASK_REGISTRY — never returns
        }
        AppRole::Driver => app.driver_context()?,
    };

    let lines = corpus();
    let num_partitions = 4;

    println!(
        "Processing {} lines across {} partitions...\n",
        lines.len(),
        num_partitions
    );

    // Pipeline:
    //   1. parallelize       — split lines into `num_partitions` partitions
    //   2. flat_map_task     — workers tokenize each partition: lines → words
    //   3. map_task          — workers pair each word: word → (word, 1)
    //   4. reduce_by_key     — shuffle by key hash; each partition reduces its key range
    //   5. collect           — driver receives the fully-reduced (word, count) pairs
    //
    // In distributed mode, steps 2–3 are pipelined into a single TaskEnvelope per
    // partition. The reduce_by_key shuffle then moves only reduced data — not all raw
    // pairs — back to the driver.
    let mut word_counts: Vec<(String, u32)> = ctx
        .parallelize_typed(lines, num_partitions)
        .flat_map_task(Tokenize)
        .map_task(PairOne)
        .reduce_by_key_task(AddU32)
        .collect()?;

    // Sort by count descending, then word alphabetically.
    word_counts.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    println!("{:<20} COUNT", "WORD");
    println!("{}", "-".repeat(28));
    for (word, count) in &word_counts {
        if !word.is_empty() {
            println!("{:<20} {}", word, count);
        }
    }

    let unique = word_counts.iter().filter(|(w, _)| !w.is_empty()).count();
    let total: u64 = word_counts.iter().map(|(_, c)| *c as u64).sum();
    println!("\nUnique words: {}  |  Total tokens: {}", unique, total);

    Ok(())
}
