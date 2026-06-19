/// Word count via distributed shuffle (`reduce_by_key`) — benchmark counterpart
/// to `examples/wordcount` (which aggregates on the driver instead).
///
/// Generates a synthetic corpus in memory, runs
/// `flat_map_task(Tokenize) -> map_task(PairOne) -> reduce_by_key -> collect`,
/// and reports wall-clock time. See `benchmarks/spark/wordcount_benchmark.py`
/// for the Spark side and `benchmarks/README.md` for methodology.
///
/// ```bash
/// cargo run --release -p bench_wordcount
/// ```
use std::time::Instant;

use atomic_compute::app::AtomicApp;
use atomic_compute::task;

atomic_compute::register_shuffle_map!(String, u64);

const VOCAB_SIZE: usize = 5_000;
const WORDS_PER_LINE: usize = 20;
const TOTAL_WORDS: usize = 3_000_000;
const NUM_PARTITIONS: usize = 12;

#[task]
fn tokenize(line: String) -> Vec<String> {
    line.split_whitespace().map(|w| w.to_string()).collect()
}

#[task]
fn pair_one(word: String) -> (String, u64) {
    (word, 1)
}

/// Same synthetic-corpus layout used by `benchmarks/spark/wordcount_benchmark.py`:
/// `num_lines = TOTAL_WORDS / WORDS_PER_LINE` lines, each word a cyclic index
/// into a `VOCAB_SIZE`-word vocabulary.
fn generate_corpus() -> Vec<String> {
    let num_lines = TOTAL_WORDS / WORDS_PER_LINE;
    (0..num_lines)
        .map(|line_idx| {
            (0..WORDS_PER_LINE)
                .map(|w| format!("word{}", (line_idx * WORDS_PER_LINE + w) % VOCAB_SIZE))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let setup_start = Instant::now();
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;
    let lines = generate_corpus();
    let num_lines = lines.len();
    let setup_elapsed = setup_start.elapsed();

    let job_start = Instant::now();
    let counts: Vec<(String, u64)> = ctx
        .parallelize_typed(lines, NUM_PARTITIONS)
        .flat_map_task(Tokenize)
        .map_task(PairOne)
        .reduce_by_key(|a, b| a + b)
        .collect()?;
    let job_elapsed = job_start.elapsed();

    let distinct_keys = counts.len();
    let total_count: u64 = counts.iter().map(|(_, c)| *c).sum();

    println!(
        "RESULT engine=atomic workload=wordcount total_words={} lines={} vocab={} \
         partitions={} distinct_keys={} total_count={} setup_ms={} job_ms={} total_ms={}",
        TOTAL_WORDS,
        num_lines,
        VOCAB_SIZE,
        NUM_PARTITIONS,
        distinct_keys,
        total_count,
        setup_elapsed.as_millis(),
        job_elapsed.as_millis(),
        (setup_elapsed + job_elapsed).as_millis(),
    );

    Ok(())
}
