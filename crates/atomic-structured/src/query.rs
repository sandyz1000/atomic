//! Execution glue: the `BatchEngine` abstraction (stateless or windowed), the
//! `OutputOperation` that runs an engine + sink per batch, and the user-facing
//! `StreamingQuery` handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_sql::context::AtomicSqlContext;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::{OutputOperation, StreamingJob};
use atomic_streaming::errors::StreamingError;
use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::{StructuredError, StructuredResult};
use crate::sink::Sink;
use crate::source::StreamSource;

// Output-op ids for structured queries live in their own high range.
static NEXT_OP_ID: AtomicUsize = AtomicUsize::new(0xA000_0000);

fn next_op_id() -> usize {
    NEXT_OP_ID.fetch_add(1, Ordering::Relaxed)
}

/// Computes the rows to emit for one micro-batch. Stateless and windowed engines
/// both implement this; the output op is agnostic to which.
pub(crate) trait BatchEngine: Send + Sync {
    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>>;

    /// Called after the sink has successfully written the batch's output.
    /// Forwards to the source so it can advance any tracked read position
    /// (e.g. Kafka offsets) only once delivery is confirmed. Default no-op.
    fn post_commit(&self, _epoch: u64) {}

    crate::cfg_kafka! {
    /// Consumed offsets for the last batch from a Kafka source, ready to be
    /// committed inside a producer transaction. Returns `None` for non-Kafka sources
    /// and before the first batch. Delegates to the underlying `StreamSource`.
    fn pending_offsets(&self) -> Option<crate::kafka::OffsetCommit> {
        None
    }
    } // cfg_kafka!
}

/// Stateless engine (4a): register the batch as `input`, run the SQL, emit the
/// result rows directly.
pub(crate) struct StatelessEngine {
    pub(crate) sql_ctx: AtomicSqlContext,
    pub(crate) runtime: tokio::runtime::Runtime,
    pub(crate) source: Arc<dyn StreamSource>,
    pub(crate) query: String,
}

impl BatchEngine for StatelessEngine {
    fn post_commit(&self, epoch: u64) {
        self.source.post_batch_commit(epoch);
    }

    crate::cfg_kafka! {
    fn pending_offsets(&self) -> Option<crate::kafka::OffsetCommit> {
        self.source.pending_offsets()
    }
    } // cfg_kafka!

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let batches = self.source.next_batch(epoch);
        if batches.iter().map(|b| b.num_rows()).sum::<usize>() == 0 {
            return Ok(vec![]);
        }
        let _ = self.sql_ctx.deregister_table("input");
        self.sql_ctx
            .register_batches("input", batches)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;

        let query = self.query.clone();
        self.runtime.block_on(async {
            let df = self
                .sql_ctx
                .sql(&query)
                .await
                .map_err(|e| StructuredError::Sql(e.to_string()))?;
            df.collect()
                .await
                .map_err(|e| StructuredError::Sql(e.to_string()))
        })
    }
}

/// Pairs an engine with a sink — the unit run once per micro-batch.
pub(crate) struct QueryRunner {
    engine: Arc<dyn BatchEngine>,
    sink: Arc<dyn Sink>,
}

impl QueryRunner {
    pub(crate) fn new(engine: Arc<dyn BatchEngine>, sink: Arc<dyn Sink>) -> Arc<Self> {
        Arc::new(QueryRunner { engine, sink })
    }

    pub(crate) fn run_batch(&self, epoch: u64) -> StructuredResult<()> {
        let out = self.engine.process(epoch)?;

        // Exactly-once path: when the source exposes pending Kafka offsets, commit them
        // inside the sink's producer transaction instead of in a separate `post_commit`
        // call. This makes source-offset advancement and output-record visibility atomic —
        // a crash-restart neither re-produces the batch nor skips the source messages.
        // Offsets already committed inside the transaction — do NOT call post_commit.
        if self.commit_kafka_offsets(epoch, &out)? {
            return Ok(());
        }

        self.sink.add_batch(epoch, &out)?;
        self.engine.post_commit(epoch);
        Ok(())
    }

    crate::cfg_kafka! {
        /// Returns `true` if handled via a Kafka exactly-once transaction (caller must
        /// not also call `post_commit`); `false` when there were no pending offsets.
        fn commit_kafka_offsets(&self, epoch: u64, out: &[RecordBatch]) -> StructuredResult<bool> {
            let Some(offsets) = self.engine.pending_offsets() else {
                return Ok(false);
            };
            self.sink.add_batch_with_offsets(epoch, out, &offsets)?;
            Ok(true)
        }
    }
    crate::cfg_not_kafka! {
        fn commit_kafka_offsets(&self, _epoch: u64, _out: &[RecordBatch]) -> StructuredResult<bool> {
            Ok(false)
        }
    }
}

/// `OutputOperation` that runs the query once per batch via its `QueryRunner`.
pub(crate) struct QueryOutputOp {
    runner: Arc<QueryRunner>,
    op_id: usize,
}

impl QueryOutputOp {
    pub(crate) fn new(runner: Arc<QueryRunner>) -> Self {
        QueryOutputOp {
            runner,
            op_id: next_op_id(),
        }
    }
}

impl OutputOperation for QueryOutputOp {
    fn generate_job(&self, time_ms: u64) -> Option<StreamingJob> {
        let runner = self.runner.clone();
        Some(StreamingJob::new(time_ms, self.op_id, move || {
            runner
                .run_batch(time_ms)
                .map_err(|e| StreamingError::Internal(e.to_string()))
        }))
    }
}

/// A running structured streaming query — control handle.
pub struct StreamingQuery {
    ssc: Option<Arc<StreamingContext>>,
    source: Arc<dyn StreamSource>,
    runner: Arc<QueryRunner>,
}

impl StreamingQuery {
    pub(crate) fn running(
        ssc: Arc<StreamingContext>,
        source: Arc<dyn StreamSource>,
        runner: Arc<QueryRunner>,
    ) -> Self {
        StreamingQuery {
            ssc: Some(ssc),
            source,
            runner,
        }
    }

    pub(crate) fn completed(source: Arc<dyn StreamSource>, runner: Arc<QueryRunner>) -> Self {
        StreamingQuery {
            ssc: None,
            source,
            runner,
        }
    }

    /// Block until the query is stopped. Returns immediately for a `Once` query.
    pub fn await_termination(&self) -> StructuredResult<()> {
        if let Some(ssc) = &self.ssc {
            ssc.await_termination()
                .map_err(|e| StructuredError::Source(e.to_string()))?;
        }
        Ok(())
    }

    /// Stop the query and release the source.
    pub fn stop(&self) {
        self.source.stop();
        if let Some(ssc) = &self.ssc {
            ssc.stop(false, false);
        }
    }

    /// Run one batch synchronously (used by `Trigger::Once`).
    pub(crate) fn run_once(&self) -> StructuredResult<()> {
        self.runner.run_batch(0)
    }
}
