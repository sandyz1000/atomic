//! Part 3 / D1 — distributed (sharded) windowed state.
//!
//! Runs the windowed aggregation with `.distributed(N)`, which shards the keyed
//! state across `N` shards and merges each batch's partials via `MergeState` tasks
//! into the worker-resident `WORKER_STATE_STORE`. In local mode the shards share
//! the in-process store, so these tests exercise the full route — sharding,
//! dispatch, per-shard merge/emit, and output reassembly — and must match the
//! driver-local windowed result.

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
    .unwrap()
}

/// Sort emitted cells as `(window_start, user, count)` for deterministic asserts.
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
fn distributed_windowed_counts_match() {
    // Same data and query as the driver-local windowed Complete test, but with
    // `.distributed(4)`: the result must be identical regardless of sharding.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[100], &["a"], &[10])],
            vec![batch(&[200, 1500], &["a", "b"], &[20, 5])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50));

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt"), Agg::sum("amount", "total")])
        .distributed(4)
        .write_stream()
        .output_mode(OutputMode::Complete)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(350));
    q.stop();

    // window-0/a cnt=2 (ts 100,200), window-1000/b cnt=1 (ts 1500).
    assert_eq!(
        cells(&sink),
        vec![(0, "a".to_string(), 2), (1000, "b".to_string(), 1)]
    );
}

#[test]
fn distributed_update_emits_changed() {
    // Update mode emits the cells changed each batch. The final emission is from
    // the second batch: window-0/a bumped to 2 and the new window-1000/b at 1.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[100], &["a"], &[10])],
            vec![batch(&[200, 1500], &["a", "b"], &[20, 5])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50));

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt")])
        .distributed(3)
        .write_stream()
        .output_mode(OutputMode::Update)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(350));
    q.stop();

    assert_eq!(
        cells(&sink),
        vec![(0, "a".to_string(), 2), (1000, "b".to_string(), 1)]
    );
}

#[test]
fn distributed_single_shard_matches() {
    // A single shard must behave exactly like the driver-local engine.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![vec![batch(&[100, 200, 300], &["a", "b", "a"], &[1, 1, 1])]],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50));

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt")])
        .distributed(1)
        .write_stream()
        .output_mode(OutputMode::Complete)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(250));
    q.stop();

    // All in window 0: a cnt=2, b cnt=1.
    assert_eq!(
        cells(&sink),
        vec![(0, "a".to_string(), 2), (0, "b".to_string(), 1)]
    );
}
