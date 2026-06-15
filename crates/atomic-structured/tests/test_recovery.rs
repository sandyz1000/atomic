//! 4c — Complete output mode and crash-recovery without double-emission.

use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::sink::{FileSink, MemorySink};
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

fn streaming_ctx() -> Arc<StreamingContext> {
    StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50))
}

fn aggs() -> Vec<Agg> {
    vec![Agg::count("cnt"), Agg::sum("amount", "total")]
}

#[test]
fn complete_emits_all() {
    // Complete mode re-emits the entire result table each batch; the final
    // emission must contain every (window, user) cell with its cumulative value.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[100], &["a"], &[10])],
            vec![batch(&[200, 1500], &["a", "b"], &[20, 5])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx();

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(aggs())
        .write_stream()
        .output_mode(OutputMode::Complete)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(350));
    q.stop();

    // Last emitted batch is the full table: window-0/a cnt=2, window-1000/b cnt=1.
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
    let mut cells: Vec<(i64, String, i64)> = (0..last.num_rows())
        .map(|i| (ws.value(i), user.value(i).to_string(), cnt.value(i)))
        .collect();
    cells.sort();
    assert_eq!(
        cells,
        vec![(0, "a".to_string(), 2), (1000, "b".to_string(), 1)]
    );
}

#[test]
fn recovery_no_double_emit() {
    // A FileSink uses deterministic part-{epoch} names. After a restart that
    // restores state, re-processing the same epoch overwrites its part file
    // rather than producing a duplicate — so the output dir has one part per epoch.
    let state_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();

    let run = |out: &std::path::Path| {
        let source = Arc::new(QueueSource::from_batches(
            schema(),
            vec![vec![batch(&[100, 200], &["a", "a"], &[1, 1])]],
        ));
        let ssc = streaming_ctx();
        StreamingDataFrame::read_stream(source)
            .window("ts", Duration::from_millis(1000))
            .group_by(&["user"])
            .aggregate(aggs())
            .write_stream()
            .output_mode(OutputMode::Complete)
            .trigger(Trigger::Once)
            .checkpoint(state_dir.path())
            .format(Arc::new(FileSink::new(out)))
            .start(&ssc)
            .unwrap();
    };

    run(out_dir.path()); // first run
    run(out_dir.path()); // restart: restores state, re-emits epoch 0 (overwrites part-0)

    let parts: Vec<_> = std::fs::read_dir(out_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("part-"))
        .collect();
    assert_eq!(
        parts.len(),
        1,
        "expected a single part file (no double-emit)"
    );
}
