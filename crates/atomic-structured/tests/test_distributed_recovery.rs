//! Part 3 / D3 — distributed state checkpoint + recovery.
//!
//! Each `MergeState` task checkpoints its shard's post-merge state under the
//! query's checkpoint dir and, on a cold shard (no in-memory state — e.g. after a
//! worker restart), reloads from there before merging. This test simulates a
//! restart by clearing the process-global `WORKER_STATE_STORE` between runs and
//! asserts the window counts accumulate across the restart.
//!
//! It lives in its own test binary so clearing the global store cannot race with
//! the other distributed-state tests (separate process = separate store).

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
    ]))
}

fn cells(sink: &MemorySink) -> Vec<(i64, String, i64)> {
    let last = sink.batches().into_iter().next_back().expect("no emission");
    let ws = last
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let user = last
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let cnt = last
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut out: Vec<(i64, String, i64)> = (0..last.num_rows())
        .map(|i| (ws.value(i), user.value(i).to_string(), cnt.value(i)))
        .collect();
    out.sort();
    out
}

#[test]
fn distributed_recovers_state() {
    let ckpt = tempfile::tempdir().unwrap();

    let run = |events: &[i64]| -> Arc<MemorySink> {
        let users: Vec<&str> = events.iter().map(|_| "a").collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(events.to_vec())),
                Arc::new(StringArray::from(users)),
            ],
        )
        .unwrap();
        let source = Arc::new(QueueSource::from_batches(schema(), vec![vec![batch]]));
        let sink = Arc::new(MemorySink::new());
        let ssc = StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50));
        StreamingDataFrame::read_stream(source)
            .window("ts", Duration::from_millis(1000))
            .group_by(&["user"])
            .aggregate(vec![Agg::count("cnt")])
            .distributed(2)
            .write_stream()
            .output_mode(OutputMode::Complete)
            .trigger(Trigger::Once)
            .checkpoint(ckpt.path())
            .format(sink.clone())
            .start(&ssc)
            .unwrap();
        sink
    };

    // First run: two events land in window 0 for user "a".
    let s1 = run(&[100, 200]);
    assert_eq!(cells(&s1), vec![(0, "a".to_string(), 2)]);

    // Simulate a worker restart: the in-memory shard state is gone, but the
    // per-shard checkpoint files remain.
    atomic_data::state_store::worker_state_store().clear();

    // Second run with the same checkpoint dir: cold shards reload from disk, so the
    // new event accumulates onto the recovered count (2 + 1 = 3).
    let s2 = run(&[300]);
    assert_eq!(cells(&s2), vec![(0, "a".to_string(), 3)]);
}
