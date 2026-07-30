//! Scenario: opt-in map-side pre-combine for `aggregate_by_key_task` (the `C != V` case).
//!
//! Mean rating per movie: each rating `f64` is lifted into a `(sum, count)` accumulator and
//! merged component-wise. A staged `_task` pipeline (`flat_map_task`) precedes the shuffle and
//! `register_combine_lift!(String, f64, (f64, u64))` is present, so in distributed mode
//! a `CombineByKey`-lift step runs on each worker before the shuffle write: it lifts
//! `f64 -> (f64, u64)`, groups by movie, and folds each group — so the shuffle then carries
//! pre-combined `(String, (f64, u64))` pairs and the reduce side merges them via
//! `merge_combiners` alone.
//!
//! This is the reduce-side-branching path (`map_side_combined`), so it is the case with the
//! most correctness risk. Uses distinct types so it never touches the plain-wordcount
//! scenarios sharing the binary.
//!
//! Input "a:4 b:1" / "a:6 b:2" / "a:5 b:3":
//!   a = (4+6+5)/3 = 5.0,  b = (1+2+3)/3 = 2.0
//! Expected output: {"a":5.0,"b":2.0}

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

// Fallback (non-combined) shuffle handler for (String, f64), plus the lift-combine handler
// (which also registers the post-combine (String, (f64, u64)) shuffle handler).
atomic_compute::register_shuffle_map!(String, f64);
atomic_compute::register_combine_lift!(String, f64, (f64, u64));

#[task]
fn parse_rating(line: String) -> Vec<(String, f64)> {
    line.split_whitespace()
        .filter_map(|tok| {
            let (movie, score) = tok.split_once(':')?;
            let score: f64 = score.parse().ok()?;
            Some((movie.to_string(), score))
        })
        .collect()
}

#[task]
fn to_sum_count(r: f64) -> (f64, u64) {
    (r, 1)
}

#[task]
fn add_sum_count(a: (f64, u64), b: (f64, u64)) -> (f64, u64) {
    (a.0 + b.0, a.1 + b.1)
}

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let lines = vec![
        "a:4 b:1".to_string(),
        "a:6 b:2".to_string(),
        "a:5 b:3".to_string(),
    ];

    let sums = ctx
        .parallelize_typed(lines, 2)
        .flat_map_task(ParseRating) // staged pipeline precedes the shuffle
        .aggregate_by_key_task(ToSumCount, AddSumCount, 4) // map-side CombineByKey-lift
        .collect()?; // Vec<(String, (f64, u64))>

    let mut means: Vec<(String, f64)> = sums
        .into_iter()
        .map(|(movie, (sum, count))| (movie, sum / count as f64))
        .collect();
    means.sort_by(|a, b| a.0.cmp(&b.0));

    let map: serde_json::Map<String, serde_json::Value> = means
        .into_iter()
        .filter_map(|(k, mean)| serde_json::Number::from_f64(mean).map(|n| (k, n.into())))
        .collect();
    println!("{}", serde_json::Value::Object(map));
    Ok(())
}
