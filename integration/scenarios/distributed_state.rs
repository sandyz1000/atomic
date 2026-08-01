//! Scenario: distributed stateful-streaming state-shard routing across two workers.
//!
//! Runs a windowed aggregation with `.distributed(2)` over a real two-worker
//! cluster, feeding two micro-batches. The keyed state is sharded and each batch's
//! partials are merged into the worker-resident `WORKER_STATE_STORE` via
//! `MergeState` tasks. Correctness across batches depends on each shard routing
//! back to the same worker every batch (`pin_state_shard` report-back affinity):
//! window-0/user-`a` is seen in both batches, so its running count only reaches 2
//! if the shard's second-batch merge lands on the worker that holds its first-batch
//! state. Without per-shard routing, `next_executor_server` could place the second
//! batch on a cold worker and the count would come back 1.
//!
//! Expected output: {"cells":[[0,"a",2],[1000,"b",1]]}

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::sink::MemorySink;
use atomic_structured::source::QueueSource;
use atomic_structured::{Agg, OutputMode, StreamingDataFrame, Trigger};

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn batch(ts: &[i64], users: &[&str], amount: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(users.to_vec())),
            Arc::new(Int64Array::from(amount.to_vec())),
        ],
    )
    .expect("batch build")
}

/// Emitted cells from the final batch as sorted `(window_start, user, count)`.
fn cells(sink: &MemorySink) -> Vec<(i64, String, i64)> {
    let last = sink.batches().into_iter().next_back().expect("no emission");
    let ws = last
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("window col");
    let user = last
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("user col");
    let cnt = last
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count col");
    let mut out: Vec<(i64, String, i64)> = (0..last.num_rows())
        .map(|i| (ws.value(i), user.value(i).to_string(), cnt.value(i)))
        .collect();
    out.sort();
    out
}

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[100], &["a"], &[10])],
            vec![batch(&[200, 1500], &["a", "b"], &[20, 5])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    // Batch/trigger intervals are wide enough for a real TCP round-trip to the
    // workers on each micro-batch.
    let ssc = StreamingContext::new(ctx.clone(), Duration::from_millis(200));

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt"), Agg::sum("amount", "total")])
        .distributed(2)
        .write_stream()
        .output_mode(OutputMode::Complete)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(200)))
        .format(sink.clone())
        .start(&ssc)?;
    std::thread::sleep(Duration::from_millis(1500));
    q.stop();

    let cells = cells(&sink);
    println!("{}", serde_json::json!({ "cells": cells }));
    Ok(())
}
