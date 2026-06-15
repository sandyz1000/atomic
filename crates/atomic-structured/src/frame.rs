//! The `StreamingDataFrame` builder — public entry point for both the stateless
//! (`.sql`) and windowed (`.window().group_by().aggregate()`) query paths.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use atomic_sql::context::AtomicSqlContext;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::OutputOperation;

use crate::errors::{StructuredError, StructuredResult};
use crate::query::{BatchEngine, QueryOutputOp, QueryRunner, StatelessEngine, StreamingQuery};
use crate::sink::Sink;
use crate::source::StreamSource;
use crate::state::{Agg, ensure_mergeable};
use crate::windowed::{WindowedEngine, WindowedSpec};
use crate::{OutputMode, Trigger};

/// A continuous query under construction.
pub struct StreamingDataFrame {
    source: Arc<dyn StreamSource>,
    watermark: Option<(String, u64)>,
}

impl StreamingDataFrame {
    /// Begin a query reading from `source`.
    pub fn read_stream(source: Arc<dyn StreamSource>) -> Self {
        StreamingDataFrame {
            source,
            watermark: None,
        }
    }

    /// Declare an event-time watermark: rows older than `max(col) - delay` are late.
    /// `col` must be an epoch-millisecond (`Int64`) column.
    pub fn with_watermark(mut self, col: &str, delay: Duration) -> Self {
        self.watermark = Some((col.to_string(), delay.as_millis() as u64));
        self
    }

    /// Stateless path: a per-batch SQL transform over the `input` table.
    pub fn sql(self, query: impl Into<String>) -> StreamWriter {
        StreamWriter::new(self.source, Plan::Stateless(query.into()), self.watermark)
    }

    /// Windowed path: tumbling event-time window of `size` on the epoch-ms `time_col`.
    pub fn window(self, time_col: &str, size: Duration) -> WindowedBuilder {
        WindowedBuilder {
            source: self.source,
            watermark: self.watermark,
            time_col: time_col.to_string(),
            window_size_ms: size.as_millis() as u64,
            group_cols: Vec::new(),
            aggs: Vec::new(),
        }
    }
}

/// Builds a windowed aggregation query.
pub struct WindowedBuilder {
    source: Arc<dyn StreamSource>,
    watermark: Option<(String, u64)>,
    time_col: String,
    window_size_ms: u64,
    group_cols: Vec<String>,
    aggs: Vec<Agg>,
}

impl WindowedBuilder {
    /// Grouping key columns (in addition to the window).
    pub fn group_by(mut self, cols: &[&str]) -> Self {
        self.group_cols = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    /// The aggregates to compute per (window, group) cell.
    pub fn aggregate(mut self, aggs: Vec<Agg>) -> Self {
        self.aggs = aggs;
        self
    }

    /// Finish building and move to the output/sink stage.
    pub fn write_stream(self) -> StreamWriter {
        StreamWriter::new(
            self.source,
            Plan::Windowed {
                time_col: self.time_col,
                window_size_ms: self.window_size_ms,
                group_cols: self.group_cols,
                aggs: self.aggs,
            },
            self.watermark,
        )
    }
}

enum Plan {
    Stateless(String),
    Windowed {
        time_col: String,
        window_size_ms: u64,
        group_cols: Vec<String>,
        aggs: Vec<Agg>,
    },
}

/// Configures output mode / trigger / sink / checkpoint and starts the query.
pub struct StreamWriter {
    source: Arc<dyn StreamSource>,
    plan: Plan,
    watermark: Option<(String, u64)>,
    mode: OutputMode,
    trigger: Trigger,
    sink: Option<Arc<dyn Sink>>,
    checkpoint_dir: Option<PathBuf>,
}

impl StreamWriter {
    fn new(source: Arc<dyn StreamSource>, plan: Plan, watermark: Option<(String, u64)>) -> Self {
        StreamWriter {
            source,
            plan,
            watermark,
            mode: OutputMode::default(),
            trigger: Trigger::default(),
            sink: None,
            checkpoint_dir: None,
        }
    }

    pub fn output_mode(mut self, mode: OutputMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.trigger = trigger;
        self
    }

    pub fn format(mut self, sink: Arc<dyn Sink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Directory where windowed state + watermark are checkpointed (and restored from).
    pub fn checkpoint(mut self, dir: impl Into<PathBuf>) -> Self {
        self.checkpoint_dir = Some(dir.into());
        self
    }

    /// Start the query against `ssc`.
    pub fn start(self, ssc: &Arc<StreamingContext>) -> StructuredResult<StreamingQuery> {
        let sink = self
            .sink
            .ok_or_else(|| StructuredError::Sink("no sink: call .format(..)".into()))?;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| StructuredError::Source(e.to_string()))?;
        let sql_ctx = AtomicSqlContext::with_compute(ssc.sc.clone());

        let engine: Arc<dyn BatchEngine> = match self.plan {
            Plan::Stateless(query) => {
                // Stateless queries are Append-only; aggregation needs the windowed path.
                if self.mode != OutputMode::Append {
                    return Err(StructuredError::Unsupported(format!(
                        "output mode {:?} requires the windowed aggregation path",
                        self.mode
                    )));
                }
                Arc::new(StatelessEngine {
                    sql_ctx,
                    runtime,
                    source: self.source.clone(),
                    query,
                })
            }
            Plan::Windowed {
                time_col,
                window_size_ms,
                group_cols,
                aggs,
            } => {
                ensure_mergeable(&aggs)?;
                let spec = WindowedSpec {
                    time_col,
                    window_size_ms,
                    watermark_delay_ms: self.watermark.as_ref().map(|(_, d)| *d),
                    group_cols,
                    aggs,
                    mode: self.mode,
                };
                Arc::new(WindowedEngine::new(
                    sql_ctx,
                    runtime,
                    self.source.clone(),
                    spec,
                    self.checkpoint_dir,
                )?)
            }
        };

        let runner = QueryRunner::new(engine, sink);
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
