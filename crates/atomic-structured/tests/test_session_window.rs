//! C2 — session window engine tests.

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

fn event(ts: i64, user: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![ts])),
            Arc::new(StringArray::from(vec![user])),
        ],
    )
    .unwrap()
}

fn ssc() -> Arc<StreamingContext> {
    StreamingContext::new(Context::local().unwrap(), Duration::from_millis(50))
}

/// Extract `(session_start, session_end, user, count)` rows from the sink.
fn session_rows(sink: &MemorySink) -> Vec<(i64, i64, String, i64)> {
    let mut out = Vec::new();
    for b in sink.batches() {
        // Schema: session_start, session_end, user (group), cnt
        let starts = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ends = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let users = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((
                starts.value(i),
                ends.value(i),
                users.value(i).to_string(),
                counts.value(i),
            ));
        }
    }
    out
}

/// A bridging event merges two otherwise-separate sessions into one.
///
/// All three events arrive in the same batch so the bridge is processed before the
/// watermark can finalize either proto-session:
///   t=10  → session [10,10]
///   t=50  → 50 > 40 (=10+gap), so new session [50,50]
///   t=35  → 35 ∈ [10−30, 10+30] AND [50−30, 50+30]: bridges both → merged [10,50] cnt=3
///
/// Watermark advances to 200 in the next batch, which finalizes [10,50] (80 ≤ 200).
#[test]
fn test_session_merges() {
    let gap = Duration::from_millis(30);
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            // Bridge event in the same batch as the two proto-sessions.
            vec![event(10, "a"), event(50, "a"), event(35, "a")],
            vec![event(200, "a")], // advance watermark to finalize
        ],
    ));
    let sink = Arc::new(MemorySink::new());

    let q = StreamingDataFrame::read_stream(source)
        .with_watermark("ts", Duration::from_millis(0))
        .session_window("ts", gap)
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt")])
        .write_stream()
        .output_mode(OutputMode::Append)
        .format(sink.clone())
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .start(&ssc())
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    q.stop();

    let rows = session_rows(&sink);
    let merged: Vec<_> = rows.iter().filter(|(_, _, u, _)| u == "a").collect();
    // Should see exactly one finalized session for "a" with count >= 3.
    let total_count: i64 = merged.iter().map(|(_, _, _, c)| c).sum();
    assert_eq!(
        total_count, 3,
        "bridging event should merge sessions → count=3; got {rows:?}"
    );
    // The merged session should span [10, 50].
    let spans_correct = merged.iter().any(|(s, e, _, _)| *s == 10 && *e == 50);
    assert!(
        spans_correct,
        "merged session must span [10,50]; got {rows:?}"
    );
}

/// Two events separated by more than `gap` produce two distinct sessions.
///
/// Timeline (gap=30ms):
///   t=10  user "b"  → session1 [10,10]
///   t=60  user "b"  → 60 > 10+30=40 → new session2 [60,60]
///   t=200 user "b"  → watermark advances; both sessions finalized (10+30=40≤200; 60+30=90≤200)
#[test]
fn test_session_gap_splits() {
    let gap = Duration::from_millis(30);
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![event(10, "b")],
            vec![event(60, "b")],
            vec![event(200, "b")], // watermark pump
        ],
    ));
    let sink = Arc::new(MemorySink::new());

    let q = StreamingDataFrame::read_stream(source)
        .with_watermark("ts", Duration::from_millis(0))
        .session_window("ts", gap)
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt")])
        .write_stream()
        .output_mode(OutputMode::Append)
        .format(sink.clone())
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .start(&ssc())
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    q.stop();

    let rows = session_rows(&sink);
    let b_rows: Vec<_> = rows.iter().filter(|(_, _, u, _)| u == "b").collect();
    assert_eq!(
        b_rows.len(),
        2,
        "gap > threshold must produce 2 separate sessions; got {rows:?}"
    );
    assert!(
        b_rows
            .iter()
            .any(|(s, e, _, c)| *s == 10 && *e == 10 && *c == 1),
        "first session must be [10,10] cnt=1; got {rows:?}"
    );
    assert!(
        b_rows
            .iter()
            .any(|(s, e, _, c)| *s == 60 && *e == 60 && *c == 1),
        "second session must be [60,60] cnt=1; got {rows:?}"
    );
}
