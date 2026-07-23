//! Streaming sources — produce the new rows for each micro-batch.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use datafusion::arrow::array::{Int64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

/// A source of micro-batch input. Each tick the engine calls [`next_batch`] to
/// get the rows that arrived since the previous tick.
///
/// [`next_batch`]: StreamSource::next_batch
pub trait StreamSource: Send + Sync {
    /// Arrow schema of the rows this source produces (the `input` table schema).
    fn schema(&self) -> SchemaRef;

    /// Rows for the batch at `time_ms` — may be empty when no data arrived.
    fn next_batch(&self, time_ms: u64) -> Vec<RecordBatch>;

    /// Start any background work (e.g. a socket/Kafka consumer). Default no-op.
    fn start(&self) {}

    /// Stop background work and release resources. Default no-op.
    fn stop(&self) {}

    /// Called after the sink has successfully written the batch for `epoch`.
    /// Sources that track an external read position (e.g. `KafkaDirectSource`
    /// offsets) advance their committed position here — this gives at-least-once
    /// delivery (re-read on failure before this call, never silently skipped).
    /// Default no-op.
    fn post_batch_commit(&self, _epoch: u64) {}

    crate::cfg_kafka! {
    /// Consumed offsets for the last batch, suitable for committing inside a Kafka
    /// producer transaction (exactly-once). Returns `None` for all non-Kafka sources
    /// and for Kafka sources before any batch has been consumed.
    ///
    /// When the engine detects both a transactional [`Sink`] and a source that returns
    /// `Some` here, it routes the batch through `add_batch_with_offsets` instead of
    /// `add_batch + post_batch_commit`, ensuring source offsets and output records are
    /// committed atomically.
    fn pending_offsets(&self) -> Option<crate::kafka::OffsetCommit> {
        None
    }
    } // cfg_kafka!
}

/// A deterministic in-memory queue of pre-built batches. The primary test source:
/// each `next_batch` pops one entry, so one queue entry == one micro-batch.
pub struct QueueSource {
    schema: SchemaRef,
    queue: Mutex<VecDeque<Vec<RecordBatch>>>,
}

impl QueueSource {
    /// Empty queue with the given schema; feed it with [`push`](Self::push).
    pub fn new(schema: SchemaRef) -> Self {
        QueueSource {
            schema,
            queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Pre-load all micro-batches up front (one outer entry per batch).
    pub fn from_batches(schema: SchemaRef, batches: Vec<Vec<RecordBatch>>) -> Self {
        QueueSource {
            schema,
            queue: Mutex::new(batches.into()),
        }
    }

    /// Enqueue one micro-batch's worth of rows.
    pub fn push(&self, batch: Vec<RecordBatch>) {
        self.queue.lock().push_back(batch);
    }

    /// Number of batches still queued.
    pub fn remaining(&self) -> usize {
        self.queue.lock().len()
    }
}

impl StreamSource for QueueSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&self, _time_ms: u64) -> Vec<RecordBatch> {
        self.queue.lock().pop_front().unwrap_or_default()
    }
}

/// A synthetic source emitting a fixed number of rows per batch, each with a monotonically
/// increasing `value` (i64) and the batch's `timestamp` (ms). Mirrors Spark's `rate` source —
/// useful for load testing and demos.
pub struct RateSource {
    rows_per_batch: usize,
    next_value: AtomicI64,
    schema: SchemaRef,
}

impl RateSource {
    /// Emit `rows_per_batch` rows on every tick.
    pub fn new(rows_per_batch: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
        RateSource {
            rows_per_batch,
            next_value: AtomicI64::new(0),
            schema,
        }
    }
}

impl StreamSource for RateSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&self, time_ms: u64) -> Vec<RecordBatch> {
        if self.rows_per_batch == 0 {
            return vec![];
        }
        let start = self
            .next_value
            .fetch_add(self.rows_per_batch as i64, Ordering::Relaxed);
        let values: Vec<i64> = (start..start + self.rows_per_batch as i64).collect();
        let ts = TimestampMillisecondArray::from(vec![time_ms as i64; self.rows_per_batch]);
        let val = Int64Array::from(values);
        match RecordBatch::try_new(self.schema.clone(), vec![Arc::new(ts), Arc::new(val)]) {
            Ok(b) => vec![b],
            Err(_) => vec![],
        }
    }
}

/// Watches a directory for new CSV files and emits each newly-seen file's rows as a micro-batch.
/// Mirrors Spark's file source. Files already read are remembered by name, so a file is processed
/// once; already-present files at start-up are read on the first tick.
pub struct FileStreamSource {
    dir: PathBuf,
    schema: SchemaRef,
    has_header: bool,
    seen: Mutex<HashSet<String>>,
}

impl FileStreamSource {
    /// Read CSV files under `dir` with the given `schema`. `has_header` skips the first line of
    /// each file.
    pub fn csv(dir: impl Into<PathBuf>, schema: SchemaRef, has_header: bool) -> Self {
        FileStreamSource {
            dir: dir.into(),
            schema,
            has_header,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Read one CSV file into batches, or an empty vec on any read error (the file is skipped and
    /// not retried, matching the "process each file once" contract).
    fn read_file(&self, path: &std::path::Path) -> Vec<RecordBatch> {
        let Ok(file) = std::fs::File::open(path) else {
            return vec![];
        };
        let reader = datafusion::arrow::csv::ReaderBuilder::new(self.schema.clone())
            .with_header(self.has_header)
            .build(file);
        match reader {
            Ok(r) => r.filter_map(Result::ok).collect(),
            Err(_) => vec![],
        }
    }
}

impl StreamSource for FileStreamSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&self, _time_ms: u64) -> Vec<RecordBatch> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return vec![];
        };
        // Collect new file names deterministically (sorted) so batch order is stable.
        let mut new_files: Vec<PathBuf> = Vec::new();
        {
            let seen = self.seen.lock();
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !name.is_empty() && !seen.contains(name) {
                    new_files.push(path);
                }
            }
        }
        new_files.sort();

        let mut out = Vec::new();
        let mut seen = self.seen.lock();
        for path in new_files {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                seen.insert(name.to_string());
            }
            out.extend(self.read_file(&path));
        }
        out
    }
}
