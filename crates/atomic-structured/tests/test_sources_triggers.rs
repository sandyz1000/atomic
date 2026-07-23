//! Tier B: rate source, AvailableNow trigger, and query naming.

use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::sink::MemorySink;
use atomic_structured::source::{QueueSource, RateSource};
use atomic_structured::{OutputMode, StreamingDataFrame, Trigger};

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn kv_batch(users: &[&str], amounts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        kv_schema(),
        vec![
            Arc::new(StringArray::from(users.to_vec())),
            Arc::new(Int64Array::from(amounts.to_vec())),
        ],
    )
    .unwrap()
}

fn ssc() -> Arc<StreamingContext> {
    StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50))
}

#[test]
fn test_rate_source() {
    // 3 rows/batch; one Once batch → 3 rows with a `value` column.
    let source = Arc::new(RateSource::new(3));
    let sink = Arc::new(MemorySink::new());
    let q = StreamingDataFrame::read_stream(source)
        .sql("SELECT value FROM input")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::Once)
        .format(sink.clone())
        .start(&ssc())
        .unwrap();
    q.await_termination().unwrap();
    assert_eq!(sink.row_count(), 3);
}

#[test]
fn test_file_source() {
    use atomic_structured::FileStreamSource;
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    // Two CSV files in the watched directory.
    for (fname, rows) in [("a.csv", "x\n1\n2\n"), ("b.csv", "x\n3\n")] {
        let mut f = std::fs::File::create(dir.path().join(fname)).unwrap();
        f.write_all(rows.as_bytes()).unwrap();
    }
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let source = Arc::new(FileStreamSource::csv(dir.path(), schema, true));
    let sink = Arc::new(MemorySink::new());
    let q = StreamingDataFrame::read_stream(source)
        .sql("SELECT x FROM input")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::AvailableNow)
        .format(sink.clone())
        .start(&ssc())
        .unwrap();
    q.await_termination().unwrap();
    // a.csv (2 rows) + b.csv (1 row) = 3 rows, read once each.
    assert_eq!(sink.row_count(), 3);
}

#[test]
fn test_available_now() {
    // Three queued batches; AvailableNow drains all of them, then stops.
    let source = Arc::new(QueueSource::from_batches(
        kv_schema(),
        vec![
            vec![kv_batch(&["a"], &[1])],
            vec![kv_batch(&["b"], &[2])],
            vec![kv_batch(&["c"], &[3])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let q = StreamingDataFrame::read_stream(source)
        .sql("SELECT user FROM input")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::AvailableNow)
        .format(sink.clone())
        .start(&ssc())
        .unwrap();
    q.await_termination().unwrap();
    // All three batches processed in one drain.
    assert_eq!(sink.row_count(), 3);
}

#[test]
fn test_dedup_watermark() {
    // Schema with an event-time column for the watermark.
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("user", DataType::Utf8, false),
    ]));
    let row = |ts: &[i64], u: &[&str]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(StringArray::from(u.to_vec())),
            ],
        )
        .unwrap()
    };
    // Batch 1: a, b, a(dup). Batch 2: b(dup within watermark), c.
    let source = Arc::new(QueueSource::from_batches(
        schema.clone(),
        vec![
            vec![row(&[10, 11, 12], &["a", "b", "a"])],
            vec![row(&[13, 14], &["b", "c"])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let q = StreamingDataFrame::read_stream(source)
        .with_watermark("ts", Duration::from_millis(100))
        .drop_duplicates_within_watermark(&["user"])
        .output_mode(OutputMode::Append)
        .trigger(Trigger::AvailableNow)
        .format(sink.clone())
        .start(&ssc())
        .unwrap();
    q.await_termination().unwrap();
    // Distinct users a, b, c → 3 rows (the two duplicates dropped).
    assert_eq!(sink.row_count(), 3);
}

#[test]
fn test_map_groups_state() {
    use atomic_structured::map_groups_state::GroupStateTimeout;
    use atomic_structured::state::GroupVal;

    // Input (user, amount); running per-user total emitted each batch.
    let source = Arc::new(QueueSource::from_batches(
        kv_schema(),
        vec![
            vec![kv_batch(&["a", "a", "b"], &[1, 2, 10])],
            vec![kv_batch(&["a", "b"], &[3, 20])],
        ],
    ));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
    ]));
    let sink = Arc::new(MemorySink::new());
    let q = StreamingDataFrame::read_stream(source)
        .map_groups_with_state::<i64, _>(
            &["user"],
            out_schema,
            GroupStateTimeout::None,
            |key, rows, state| {
                let GroupVal::Str(user) = &key[0] else {
                    return vec![];
                };
                // Column 1 is `amount`; sum it into the running total.
                let batch_sum: i64 = rows
                    .iter()
                    .map(|r| match &r[1] {
                        GroupVal::Int(v) => *v,
                        _ => 0,
                    })
                    .sum();
                let total = state.get().copied().unwrap_or(0) + batch_sum;
                state.update(total);
                vec![vec![GroupVal::Str(user.clone()), GroupVal::Int(total)]]
            },
        )
        .output_mode(OutputMode::Append)
        .trigger(Trigger::AvailableNow)
        .format(sink.clone())
        .start(&ssc())
        .unwrap();
    q.await_termination().unwrap();
    // Batch 1 emits a=3, b=10; batch 2 emits a=6, b=30 → 4 output rows.
    assert_eq!(sink.row_count(), 4);
}

#[test]
fn test_query_name() {
    let source = Arc::new(QueueSource::from_batches(
        kv_schema(),
        vec![vec![kv_batch(&["a"], &[1])]],
    ));
    let sink = Arc::new(MemorySink::new());
    let q = StreamingDataFrame::read_stream(source)
        .sql("SELECT user FROM input")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::Once)
        .query_name("my_stream")
        .format(sink)
        .start(&ssc())
        .unwrap();
    assert_eq!(q.name(), Some("my_stream"));
}
