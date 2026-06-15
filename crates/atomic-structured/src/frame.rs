//! The `StreamingDataFrame` / `StreamWriter` builder — the public entry point.

use std::sync::Arc;

use atomic_sql::context::AtomicSqlContext;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::OutputOperation;

use crate::errors::{StructuredError, StructuredResult};
use crate::query::{QueryOutputOp, Runner, StreamingQuery};
use crate::sink::Sink;
use crate::source::StreamSource;
use crate::{OutputMode, Trigger};

/// A continuous query under construction: a source plus a SQL transformation.
pub struct StreamingDataFrame {
    source: Arc<dyn StreamSource>,
    query: Option<String>,
}

impl StreamingDataFrame {
    /// Begin a query reading from `source`.
    pub fn read_stream(source: Arc<dyn StreamSource>) -> Self {
        StreamingDataFrame {
            source,
            query: None,
        }
    }

    /// Set the per-batch SQL. The source is queryable as the table `input`.
    pub fn sql(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Transition to the output/sink builder.
    pub fn write_stream(self) -> StreamWriter {
        StreamWriter {
            source: self.source,
            query: self.query,
            mode: OutputMode::default(),
            trigger: Trigger::default(),
            sink: None,
        }
    }
}

/// Configures output mode / trigger / sink and starts the query.
pub struct StreamWriter {
    source: Arc<dyn StreamSource>,
    query: Option<String>,
    mode: OutputMode,
    trigger: Trigger,
    sink: Option<Arc<dyn Sink>>,
}

impl StreamWriter {
    /// How results are emitted (4a supports `Append` only).
    pub fn output_mode(mut self, mode: OutputMode) -> Self {
        self.mode = mode;
        self
    }

    /// When micro-batches fire.
    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.trigger = trigger;
        self
    }

    /// The destination sink.
    pub fn format(mut self, sink: Arc<dyn Sink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Start the query against `ssc`. For `Trigger::ProcessingTime` this registers
    /// an output op and starts the context's batch loop; for `Trigger::Once` it
    /// runs exactly one batch synchronously and returns a terminated query.
    pub fn start(self, ssc: &Arc<StreamingContext>) -> StructuredResult<StreamingQuery> {
        let query = self
            .query
            .ok_or_else(|| StructuredError::Unsupported("no query: call .sql(..)".into()))?;
        let sink = self
            .sink
            .ok_or_else(|| StructuredError::Sink("no sink: call .format(..)".into()))?;
        // 4a is stateless: only Append. Update/Complete need the state store (4b/4c).
        if self.mode != OutputMode::Append {
            return Err(StructuredError::Unsupported(format!(
                "output mode {:?} requires stateful aggregation (4b/4c)",
                self.mode
            )));
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| StructuredError::Source(e.to_string()))?;
        let sql_ctx = AtomicSqlContext::with_compute(ssc.sc.clone());
        let runner = Arc::new(Runner {
            sql_ctx,
            runtime,
            source: self.source.clone(),
            sink,
            query,
        });

        self.source.start();

        match self.trigger {
            Trigger::Once => {
                let q = StreamingQuery::completed(self.source.clone(), runner);
                q.run_once()?;
                self.source.stop();
                Ok(q)
            }
            Trigger::ProcessingTime(_) => {
                let op = Arc::new(QueryOutputOp::new(runner.clone()));
                ssc.graph
                    .lock()
                    .add_output_stream(op as Arc<dyn OutputOperation>);
                ssc.start()
                    .map_err(|e| StructuredError::Source(e.to_string()))?;
                Ok(StreamingQuery::running(
                    ssc.clone(),
                    self.source.clone(),
                    runner,
                ))
            }
        }
    }
}
