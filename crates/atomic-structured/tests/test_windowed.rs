//! 4b — event-time windowed aggregation: watermark, state merge, output modes,
//! and checkpoint recovery.

use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::sink::MemorySink;
use atomic_structured::source::QueueSource;
use atomic_structured::{Agg, OutputMode, StreamingDataFrame, Trigger};

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
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

/// Extract `(window_start, user, cnt, total)` rows from a sink's emissions.
fn rows(sink: &MemorySink) -> Vec<(i64, String, i64, f64)> {
    let mut out = Vec::new();
    for b in sink.batches() {
        let ws = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let user = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let cnt = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let total = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((
                ws.value(i),
                user.value(i).to_string(),
                cnt.value(i),
                total.value(i),
            ));
        }
    }
    out
}

fn aggs() -> Vec<Agg> {
    vec![Agg::count("cnt"), Agg::sum("amount", "total")]
}

#[test]
fn merges_window_counts() {
    // Two batches land in window 0 (user a) plus window 1000 (user b).
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[100, 200], &["a", "a"], &[10, 20])],
            vec![batch(&[300, 1500], &["a", "b"], &[5, 100])],
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx();

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(aggs())
        .write_stream()
        .output_mode(OutputMode::Update)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(350));
    q.stop();

    let r = rows(&sink);
    // (window 0, user a) merges to cnt=3, total=35 across both batches.
    let best_a = r
        .iter()
        .filter(|(w, u, _, _)| *w == 0 && u == "a")
        .map(|(_, _, c, t)| (*c, *t))
        .max_by_key(|(c, _)| *c)
        .expect("no window-0 user-a emission");
    assert_eq!(best_a, (3, 35.0));
    assert!(
        r.iter()
            .any(|(w, u, c, t)| *w == 1000 && u == "b" && *c == 1 && *t == 100.0)
    );
}

#[test]
fn drops_late_data() {
    // Batch 1 advances the watermark to 5000; batch 2's window-0 row is late.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[5000], &["a"], &[10])],
            vec![batch(&[100], &["a"], &[99])], // window 0 ends at 1000 <= watermark 5000 → late
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx();

    let q = StreamingDataFrame::read_stream(source)
        .with_watermark("ts", Duration::from_millis(0))
        .window("ts", Duration::from_millis(1000))
        .aggregate(vec![Agg::count("cnt"), Agg::sum("amount", "total")])
        .write_stream()
        .output_mode(OutputMode::Update)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    q.stop();

    // No grouping → user column is absent; reuse rows() not possible. Inspect window only.
    let windows: Vec<i64> = sink
        .batches()
        .iter()
        .flat_map(|b| {
            let ws = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..b.num_rows()).map(|i| ws.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert!(windows.contains(&5000), "window 5000 should be present");
    assert!(!windows.contains(&0), "late window 0 must be dropped");
}

#[test]
fn appends_final_windows() {
    // Append emits a window once the watermark passes its end, exactly once.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[100, 200], &["a", "a"], &[1, 1])], // window 0
            vec![batch(&[1200], &["a"], &[1])],             // advances wm to 1200 → finalize win 0
            vec![batch(&[2500], &["a"], &[1])], // advances wm to 2500 → finalize win 1000
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx();

    let q = StreamingDataFrame::read_stream(source)
        .with_watermark("ts", Duration::from_millis(0))
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(aggs())
        .write_stream()
        .output_mode(OutputMode::Append)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(450));
    q.stop();

    let r = rows(&sink);
    let win0: Vec<_> = r.iter().filter(|(w, ..)| *w == 0).collect();
    assert_eq!(win0.len(), 1, "window 0 emitted exactly once");
    assert_eq!((win0[0].2, win0[0].3), (2, 2.0)); // cnt=2, total=2
    assert_eq!(r.iter().filter(|(w, ..)| *w == 1000).count(), 1);
}

#[test]
fn recovers_from_checkpoint() {
    let dir = tempfile::tempdir().unwrap();

    // First run: count 2 rows in window 0 (user a), checkpoint, terminate.
    {
        let source = Arc::new(QueueSource::from_batches(
            schema(),
            vec![vec![batch(&[100, 200], &["a", "a"], &[10, 20])]],
        ));
        let ssc = streaming_ctx();
        StreamingDataFrame::read_stream(source)
            .window("ts", Duration::from_millis(1000))
            .group_by(&["user"])
            .aggregate(aggs())
            .write_stream()
            .output_mode(OutputMode::Update)
            .trigger(Trigger::Once)
            .checkpoint(dir.path())
            .format(Arc::new(MemorySink::new()))
            .start(&ssc)
            .unwrap();
    }

    // Second run: restores cnt=2, then merges 3 more → cnt=5.
    let sink = Arc::new(MemorySink::new());
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![vec![batch(&[100, 200, 300], &["a", "a", "a"], &[1, 1, 1])]],
    ));
    let ssc = streaming_ctx();
    StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(aggs())
        .write_stream()
        .output_mode(OutputMode::Update)
        .trigger(Trigger::Once)
        .checkpoint(dir.path())
        .format(sink.clone())
        .start(&ssc)
        .unwrap();

    let r = rows(&sink);
    let win0_a = r
        .iter()
        .find(|(w, u, ..)| *w == 0 && u == "a")
        .expect("missing cell");
    assert_eq!(win0_a.2, 5, "checkpoint did not restore prior count");
}

// ── C1: Sliding windows ───────────────────────────────────────────────────────

/// W=10ms S=5ms: each event fans out to ceil(W/S)=2 overlapping windows.
/// t=7 → [0,10) and [5,15); t=12 → [5,15) and [10,20).
/// Window [5,15) receives both events.
#[test]
fn test_sliding_overlaps() {
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![vec![batch(&[7, 12], &["a", "a"], &[1, 1])]],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx();

    StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(10))
        .slide(Duration::from_millis(5))
        .group_by(&["user"])
        .aggregate(aggs())
        .write_stream()
        .output_mode(OutputMode::Update)
        .format(sink.clone())
        .trigger(Trigger::Once)
        .start(&ssc)
        .unwrap();

    let mut r = rows(&sink);
    r.sort_by_key(|(ws, _, _, _)| *ws);

    // Each of the three covering windows must appear.
    assert!(
        r.iter().any(|(ws, _, cnt, _)| *ws == 0 && *cnt == 1),
        "window [0,10)"
    );
    assert!(
        r.iter().any(|(ws, _, cnt, _)| *ws == 5 && *cnt == 2),
        "window [5,15) both events"
    );
    assert!(
        r.iter().any(|(ws, _, cnt, _)| *ws == 10 && *cnt == 1),
        "window [10,20)"
    );
}

/// A late event (past watermark) is dropped in sliding mode just as in tumbling.
/// Batch 1: t=20 (wm → 20).  Batch 2: t=3 (windows [0,5),[0,10) both have end ≤ 20 → late).
#[test]
fn test_sliding_late_drop() {
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&[20], &["a"], &[1])],
            vec![batch(&[3], &["a"], &[99])], // late: covering windows close before wm=20
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx();

    let q = StreamingDataFrame::read_stream(source)
        .with_watermark("ts", Duration::from_millis(0))
        .window("ts", Duration::from_millis(10))
        .slide(Duration::from_millis(5))
        .group_by(&["user"])
        .aggregate(aggs())
        .write_stream()
        .output_mode(OutputMode::Update)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    q.stop();

    let r = rows(&sink);
    // t=3 covers windows [0,5) end=5 and [0,10) end=10 (actually wait: W=10ms S=5ms)
    // t=3: k_min = (3-10)/5_floor + 1 = -2+1 = -1 → filter(>=0): k=0
    //        k_max = 3/5 = 0 → only window k=0 → [0, 10)
    // With wm=20, window [0,10) end=10 <= 20 → late → amount=99 must NOT appear.
    let sum_99: Vec<_> = r.iter().filter(|(_, _, _, total)| *total == 99.0).collect();
    assert!(
        sum_99.is_empty(),
        "late row (total=99) must be dropped, got {:?}",
        r
    );
}
