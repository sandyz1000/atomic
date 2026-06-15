//! Streaming sources — produce the new rows for each micro-batch.

use std::collections::VecDeque;

use datafusion::arrow::datatypes::SchemaRef;
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
