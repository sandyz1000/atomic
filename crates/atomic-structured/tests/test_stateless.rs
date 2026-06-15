//! 4a — stateless continuous queries: per-batch SQL projection/filter to sinks.

use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::sink::{FileSink, MemorySink};
use atomic_structured::source::QueueSource;
use atomic_structured::{OutputMode, StreamingDataFrame, Trigger};

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn batch(users: &[&str], amounts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(users.to_vec())),
            Arc::new(Int64Array::from(amounts.to_vec())),
        ],
    )
    .unwrap()
}

fn streaming_ctx(batch_ms: u64) -> Arc<StreamingContext> {
    let sc = Context::local().unwrap();
    StreamingContext::new(sc, Duration::from_millis(batch_ms))
}

const FILTER_SQL: &str = "SELECT user, amount FROM input WHERE amount > 100";

#[test]
fn once_runs_one_batch() {
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![vec![batch(&["a", "b"], &[150, 50])]],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx(50);

    let q = StreamingDataFrame::read_stream(source)
        .sql(FILTER_SQL)
        .output_mode(OutputMode::Append)
        .trigger(Trigger::Once)
        .format(sink.clone())
        .start(&ssc)
        .unwrap();
    q.await_termination().unwrap();

    // Only the row with amount > 100 (a=150) survives.
    assert_eq!(sink.row_count(), 1);
}

#[test]
fn streams_filtered_rows() {
    // Three batches; rows with amount > 100 across all of them should reach the sink.
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![
            vec![batch(&["a", "b"], &[150, 50])],  // keep a
            vec![batch(&["c", "d"], &[200, 300])], // keep c, d
            vec![batch(&["e", "f"], &[10, 999])],  // keep f
        ],
    ));
    let sink = Arc::new(MemorySink::new());
    let ssc = streaming_ctx(50);

    let q = StreamingDataFrame::read_stream(source)
        .sql(FILTER_SQL)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(50)))
        .format(sink.clone())
        .start(&ssc)
        .unwrap();

    std::thread::sleep(Duration::from_millis(450));
    q.stop();

    assert_eq!(sink.row_count(), 4, "expected 4 rows with amount > 100");
}

#[test]
fn file_sink_writes_parquet() {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(QueueSource::from_batches(
        schema(),
        vec![vec![batch(&["a", "b", "c"], &[150, 50, 250])]],
    ));
    let ssc = streaming_ctx(50);

    let q = StreamingDataFrame::read_stream(source)
        .sql(FILTER_SQL)
        .trigger(Trigger::Once)
        .format(Arc::new(FileSink::new(dir.path())))
        .start(&ssc)
        .unwrap();
    q.await_termination().unwrap();

    let part = dir.path().join("part-0.parquet");
    assert!(part.exists(), "parquet part file not written");

    // Read it back: 2 rows survive the filter (a=150, c=250).
    let file = std::fs::File::open(&part).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 2);
}
