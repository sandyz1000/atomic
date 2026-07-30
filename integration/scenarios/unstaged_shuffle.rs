//! Scenario: `.take()` on an un-staged (no `_task`) RDD with a shuffle dependency.
//!
//! Validates the fix for a gap found while unifying local/distributed execution:
//! `collect()` (backing `.take()`, `.for_each()`, etc.) previously skipped the
//! `run_pending_shuffle_stages` call `.collect()` already made, so this exact shape
//! (`group_by_key()` — a shuffle with no preceding `_task` step — followed by an action
//! other than `.collect()`) hard-errored under distributed mode: the reduce side tried to
//! fetch shuffle output that was never dispatched/registered.
//!
//! Expected output: {"a":[1,3],"b":[2]} (order of values within a group is not guaranteed,
//! so the test sorts them before comparing)

use atomic_compute::context::Context;
use std::error::Error;
use std::sync::Arc;

atomic_compute::register_shuffle_map!(String, i32);

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let pairs = vec![
        ("a".to_string(), 1i32),
        ("b".to_string(), 2i32),
        ("a".to_string(), 3i32),
    ];

    // No `_task` methods anywhere in this chain — `.staged` stays `None` all the way to
    // `.take()`, so the shuffle dispatch has to come from `run_pending_shuffle_stages`
    // rather than a staged pipeline's own `EngineAction::ShuffleMap` step. Checks actual
    // group contents (not just a count) so this also validates the aggregation-on-reduce-side
    // fix: `group_by_key`'s shuffle write path is the generic, non-aggregating one in
    // distributed mode, so a broken reduce-side aggregate would produce wrong values, not
    // just a wrong count.
    let mut grouped = ctx.parallelize_typed(pairs, 2).group_by_key().take(10)?;
    grouped.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    for (_, vs) in &mut grouped {
        vs.sort();
    }

    let map: serde_json::Map<String, serde_json::Value> = grouped
        .into_iter()
        .map(|(k, vs)| (k, serde_json::json!(vs)))
        .collect();
    println!("{}", serde_json::Value::Object(map));
    Ok(())
}
