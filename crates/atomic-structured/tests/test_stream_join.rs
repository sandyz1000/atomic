//! C3 — stream-stream join engine tests.

use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::sink::MemorySink;
use atomic_structured::source::QueueSource;
use atomic_structured::{JoinType, OutputMode, StreamingDataFrame, Trigger};

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

fn left_row(ts: i64, id: &str, amount: i64) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts])),
            Arc::new(StringArray::from(vec![id])),
            Arc::new(Int64Array::from(vec![amount])),
        ],
    )
    .unwrap()
}

fn right_row(ts: i64, id: &str, label: &str) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts])),
            Arc::new(StringArray::from(vec![id])),
            Arc::new(StringArray::from(vec![label])),
        ],
    )
    .unwrap()
}

fn ssc() -> Arc<StreamingContext> {
    StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50))
}

/// Helper: collect (left_id, right_id_or_null) pairs from the joined output.
fn joined_ids(sink: &MemorySink) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    for b in sink.batches() {
        // Schema: ts, id (left), amount, ts_right, id_right, label_right
        // "id" is at index 1, "id_right" is at index 4.
        let left_id = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        // Column 4 is "id_right" (right side id renamed to avoid duplicate).
        let right_col = b.column(4);
        let right_id = right_col.as_any().downcast_ref::<StringArray>();
        for i in 0..b.num_rows() {
            let lid = left_id.value(i).to_string();
            let rid = right_id.and_then(|a| {
                if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i).to_string())
                }
            });
            out.push((lid, rid));
        }
    }
    out
}

/// Inner join: matching rows within `time_bound` are emitted; rows outside the
/// bound are not joined (they stay buffered until evicted by watermark).
#[test]
fn test_inner_join_matches() {
    // Left: (ts=100, id="x"), (ts=500, id="y")
    // Right: (ts=150, id="x"), (ts=700, id="y")   <- y is out of bound (500 vs 700 = 200 > 100)
    let left = Arc::new(QueueSource::from_batches(
        left_schema(),
        vec![vec![left_row(100, "x", 10)], vec![left_row(500, "y", 20)]],
    ));
    let right = Arc::new(QueueSource::from_batches(
        right_schema(),
        vec![
            vec![right_row(150, "x", "match")],
            vec![right_row(700, "y", "nomatch")],
        ],
    ));
    let sink = Arc::new(MemorySink::new());

    let q = StreamingDataFrame::read_stream(left)
        .with_watermark("ts", Duration::from_millis(0))
        .join_stream(
            right,
            "id",
            "id",
            JoinType::Inner,
            Duration::from_millis(100),
        )
        .output_mode(OutputMode::Append)
        .format(sink.clone())
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .start(&ssc())
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    q.stop();

    let pairs = joined_ids(&sink);
    assert!(
        pairs
            .iter()
            .any(|(l, r)| l == "x" && r.as_deref() == Some("x")),
        "inner join must emit (x, x) pair; got {:?}",
        pairs
    );
    assert!(
        !pairs
            .iter()
            .any(|(l, r)| l == "y" && r.as_deref() == Some("y")),
        "y pair is outside time bound and must not be joined; got {:?}",
        pairs
    );
}

/// Left outer join: unmatched left rows are emitted (with null right side)
/// once the watermark advances past `row_time + time_bound`.
#[test]
fn test_outer_emits_unmatched() {
    // Left: (ts=100, id="z") — no matching right row.
    // Advance watermark to 300 > 100 + 100 = 200, so z is emitted as unmatched.
    let left = Arc::new(QueueSource::from_batches(
        left_schema(),
        vec![
            vec![left_row(100, "z", 42)],
            vec![left_row(300, "w", 0)], // advance watermark
        ],
    ));
    let right = Arc::new(QueueSource::from_batches(
        right_schema(),
        vec![
            vec![], // empty — no match for z
            vec![right_row(300, "w", "match")],
        ],
    ));
    let sink = Arc::new(MemorySink::new());

    let q = StreamingDataFrame::read_stream(left)
        .with_watermark("ts", Duration::from_millis(0))
        .join_stream(
            right,
            "id",
            "id",
            JoinType::LeftOuter,
            Duration::from_millis(100),
        )
        .output_mode(OutputMode::Append)
        .format(sink.clone())
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .start(&ssc())
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    q.stop();

    let pairs = joined_ids(&sink);
    assert!(
        pairs.iter().any(|(l, r)| l == "z" && r.is_none()),
        "left outer must emit (z, null) for unmatched row; got {:?}",
        pairs
    );
}
