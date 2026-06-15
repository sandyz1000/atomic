//! Streaming sinks — where each batch's results are emitted.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use crate::errors::{StructuredError, StructuredResult};

/// Destination for the per-batch query results.
pub trait Sink: Send + Sync {
    /// Emit the result `batches` for micro-batch `epoch`. Empty input is a no-op
    /// for most sinks.
    fn add_batch(&self, epoch: u64, batches: &[RecordBatch]) -> StructuredResult<()>;
}

/// Collects all emitted batches in memory — the primary test sink. Cheaply
/// cloneable (shared inner buffer); pass an `Arc<MemorySink>` to the query and
/// keep a handle to read results.
#[derive(Default)]
pub struct MemorySink {
    batches: Mutex<Vec<RecordBatch>>,
}

impl MemorySink {
    pub fn new() -> Self {
        MemorySink::default()
    }

    /// All batches emitted so far, in order.
    pub fn batches(&self) -> Vec<RecordBatch> {
        self.batches.lock().clone()
    }

    /// Total number of rows emitted so far.
    pub fn row_count(&self) -> usize {
        self.batches.lock().iter().map(RecordBatch::num_rows).sum()
    }

    /// Drop all collected batches.
    pub fn clear(&self) {
        self.batches.lock().clear();
    }
}

impl Sink for MemorySink {
    fn add_batch(&self, _epoch: u64, batches: &[RecordBatch]) -> StructuredResult<()> {
        let mut buf = self.batches.lock();
        for b in batches {
            if b.num_rows() > 0 {
                buf.push(b.clone());
            }
        }
        Ok(())
    }
}

/// Pretty-prints each non-empty batch to stdout (like Spark's `console` sink).
pub struct ConsoleSink {
    name: String,
}

impl ConsoleSink {
    pub fn new(name: impl Into<String>) -> Self {
        ConsoleSink { name: name.into() }
    }
}

impl Sink for ConsoleSink {
    fn add_batch(&self, epoch: u64, batches: &[RecordBatch]) -> StructuredResult<()> {
        let non_empty: Vec<RecordBatch> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        if non_empty.is_empty() {
            return Ok(());
        }
        println!("------ {} batch {epoch} ------", self.name);
        let table = datafusion::arrow::util::pretty::pretty_format_batches(&non_empty)
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        println!("{table}");
        Ok(())
    }
}

/// Writes each non-empty batch to `{dir}/part-{epoch}.parquet`. Deterministic
/// part names make re-emission after recovery idempotent (overwrite same file).
pub struct FileSink {
    dir: PathBuf,
}

impl FileSink {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        FileSink { dir: dir.into() }
    }
}

impl Sink for FileSink {
    fn add_batch(&self, epoch: u64, batches: &[RecordBatch]) -> StructuredResult<()> {
        use datafusion::parquet::arrow::ArrowWriter;

        let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
        let Some(first) = non_empty.first() else {
            return Ok(());
        };

        std::fs::create_dir_all(&self.dir).map_err(|e| StructuredError::Sink(e.to_string()))?;
        let path = self.dir.join(format!("part-{epoch}.parquet"));
        let file =
            std::fs::File::create(&path).map_err(|e| StructuredError::Sink(e.to_string()))?;

        let mut writer = ArrowWriter::try_new(file, first.schema(), None)
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        for b in &non_empty {
            writer
                .write(b)
                .map_err(|e| StructuredError::Sink(e.to_string()))?;
        }
        writer
            .close()
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        Ok(())
    }
}

/// Helper: wrap a concrete sink into the shared trait object the query holds.
pub fn shared<S: Sink + 'static>(sink: S) -> Arc<dyn Sink> {
    Arc::new(sink)
}
