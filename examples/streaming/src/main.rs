//! Micro-batch streaming word count — `atomic-streaming` example.
//!
//! Mirrors Spark Streaming's `NetworkWordCount`, but driven by a
//! `queue_stream` instead of a live TCP socket so the run is deterministic and
//! CI-friendly. Each queued RDD is one "batch" of input lines; the DStream
//! pipeline tokenizes, pairs, and the output operation aggregates per batch.
//!
//! Pipeline:
//! ```text
//! queue_stream(lines)
//!   └─ FlatMappedDStream(tokenize)  lines → words
//!        └─ MappedDStream(pair)     word  → (word, 1)
//!             └─ foreach_rdd        collect + count per batch, print
//! ```
//!
//! # Running locally
//!
//! ```bash
//! cargo run -p streaming
//! ```
//!
//! To stream from a real socket instead, swap `queue_stream` for
//! `ssc.socket_text_stream("127.0.0.1", 9999)` and `nc -lk 9999` lines into it.
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::DStream;
use atomic_streaming::dstream::mapped::{FlatMappedDStream, MappedDStream};
use parking_lot::Mutex;

// Streaming-created RDDs use a high, non-colliding id range.
static NEXT_ID: AtomicUsize = AtomicUsize::new(0x9000_0000);
fn next_id() -> usize {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Split a line into lowercase alphabetic words.
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sc = Context::local()?;
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(200));

    // Three batches of input lines — pushed onto the queue up front.
    let batches = [
        vec!["the quick brown fox", "the lazy dog"],
        vec!["the fox jumps", "quick quick fox"],
        vec!["lazy dog sleeps", "the end"],
    ];
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = String>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    for batch in batches {
        let lines: Vec<String> = batch.into_iter().map(String::from).collect();
        queue
            .lock()
            .push_back(sc.parallelize_typed(lines, 2).into_rdd());
    }

    // Build the DStream pipeline: lines → words → (word, 1).
    let lines = ssc.queue_stream(Arc::clone(&queue), true) as Arc<dyn DStream<String>>;
    let words = Arc::new(FlatMappedDStream::new(next_id(), lines, tokenize));
    let pairs = Arc::new(MappedDStream::new(next_id(), words, |w: String| (w, 1u32)));

    // Output operation: per batch, collect the pairs and print counts.
    let batch_no = Arc::new(AtomicUsize::new(0));
    let sc_out = Arc::clone(&sc);
    let batch_no_out = Arc::clone(&batch_no);
    ssc.foreach_rdd(pairs, move |rdd, time_ms| {
        let parts = match sc_out.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<(String, u32)>>())
        {
            Ok(parts) => parts,
            Err(e) => {
                eprintln!("batch job failed: {e}");
                return;
            }
        };
        let mut counts: HashMap<String, u32> = HashMap::new();
        for part in parts {
            for (word, n) in part {
                *counts.entry(word).or_insert(0) += n;
            }
        }
        if counts.is_empty() {
            return;
        }
        let n = batch_no_out.fetch_add(1, Ordering::Relaxed) + 1;
        let mut rows: Vec<_> = counts.into_iter().collect();
        rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        println!("--- batch {n} (t={time_ms}ms) ---");
        for (word, count) in rows {
            println!("  {word:<8} {count}");
        }
    });

    println!("Starting streaming context (3 batches, 200ms each)...\n");
    ssc.start()?;
    // Let the batch loop drain the queue, then stop.
    std::thread::sleep(Duration::from_millis(900));
    ssc.stop(false, false);
    println!("\nStreaming context stopped.");

    Ok(())
}
