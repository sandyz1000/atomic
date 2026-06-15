//! The running query: a `Runner` (per-batch engine), the `OutputOperation` that
//! wires it into the streaming context, and the user-facing `StreamingQuery` handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_sql::context::AtomicSqlContext;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::{OutputOperation, StreamingJob};
use atomic_streaming::errors::StreamingError;

use crate::errors::{StructuredError, StructuredResult};
use crate::sink::Sink;
use crate::source::StreamSource;

// Output-op ids for structured queries live in their own high range.
static NEXT_OP_ID: AtomicUsize = AtomicUsize::new(0xA000_0000);

pub(crate) fn next_op_id() -> usize {
    NEXT_OP_ID.fetch_add(1, Ordering::Relaxed)
}

/// Per-batch execution engine: register the batch as `input`, run the query,
/// emit the result to the sink. Shared (`Arc`) by the output op.
pub(crate) struct Runner {
    pub(crate) sql_ctx: AtomicSqlContext,
    pub(crate) runtime: tokio::runtime::Runtime,
    pub(crate) source: Arc<dyn StreamSource>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) query: String,
}

impl Runner {
    /// Run one micro-batch. Stateless (4a): empty input yields empty output.
    pub(crate) fn process_batch(&self, epoch: u64) -> StructuredResult<()> {
        let batches = self.source.next_batch(epoch);
        if batches.iter().map(|b| b.num_rows()).sum::<usize>() == 0 {
            return Ok(());
        }

        // Re-register the per-batch input table (drop the previous batch's first).
        let _ = self.sql_ctx.deregister_table("input");
        self.sql_ctx
            .register_batches("input", batches)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;

        let query = self.query.clone();
        let out = self.runtime.block_on(async {
            let df = self
                .sql_ctx
                .sql(&query)
                .await
                .map_err(|e| StructuredError::Sql(e.to_string()))?;
            df.collect()
                .await
                .map_err(|e| StructuredError::Sql(e.to_string()))
        })?;

        self.sink.add_batch(epoch, &out)
    }
}

/// `OutputOperation` that runs the query's `Runner` once per batch.
pub(crate) struct QueryOutputOp {
    runner: Arc<Runner>,
    op_id: usize,
}

impl QueryOutputOp {
    pub(crate) fn new(runner: Arc<Runner>) -> Self {
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
                .process_batch(time_ms)
                .map_err(|e| StreamingError::Internal(e.to_string()))
        }))
    }
}

/// A running structured streaming query — control handle.
pub struct StreamingQuery {
    ssc: Option<Arc<StreamingContext>>,
    source: Arc<dyn StreamSource>,
    runner: Arc<Runner>,
}

impl StreamingQuery {
    /// For a context-driven (ProcessingTime) query: holds the running context.
    pub(crate) fn running(
        ssc: Arc<StreamingContext>,
        source: Arc<dyn StreamSource>,
        runner: Arc<Runner>,
    ) -> Self {
        StreamingQuery {
            ssc: Some(ssc),
            source,
            runner,
        }
    }

    /// For a `Trigger::Once` query that already ran its single batch.
    pub(crate) fn completed(source: Arc<dyn StreamSource>, runner: Arc<Runner>) -> Self {
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
        self.runner.process_batch(0)
    }
}
