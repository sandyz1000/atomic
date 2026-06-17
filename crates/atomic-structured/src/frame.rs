//! The `StreamingDataFrame` builder — public entry point for both the stateless
//! (`.sql`) and windowed (`.window().group_by().aggregate()`) query paths.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use atomic_sql::context::AtomicSqlContext;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::OutputOperation;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::distributed_state::DistributedStateEngine;
use crate::errors::{StructuredError, StructuredResult};
use crate::query::{BatchEngine, QueryOutputOp, QueryRunner, StatelessEngine, StreamingQuery};
use crate::session_window::{DistributedSessionEngine, SessionEngine, SessionSpec};
use crate::sink::Sink;
use crate::source::StreamSource;
use crate::state::{Agg, ensure_mergeable};
use crate::stream_join::{DistributedJoinEngine, JoinType, StreamJoinEngine, StreamJoinSpec};
use crate::windowed::{WindowedEngine, WindowedSpec};
use crate::{OutputMode, Trigger};

/// Stable (FNV-1a) query id from the checkpoint dir, so shard `state_id`s and
/// their checkpoint files line up across restarts.
fn stable_query_id(checkpoint_dir: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in checkpoint_dir.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

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

    /// Session window on `time_col` with inactivity `gap`.  Events within `gap` of each
    /// other coalesce into one session; a wider gap starts a new session.
    pub fn session_window(self, time_col: &str, gap: Duration) -> SessionBuilder {
        SessionBuilder {
            source: self.source,
            watermark: self.watermark,
            time_col: time_col.to_string(),
            gap_ms: gap.as_millis() as u64,
            group_cols: Vec::new(),
            aggs: Vec::new(),
            distributed_shards: None,
        }
    }

    /// Stream-stream equi-join on `left_key` = `right_key`, bounded by `time_bound`.
    /// Returns a `StreamWriter` directly (output mode is always Append for joins).
    pub fn join_stream(
        self,
        right: Arc<dyn StreamSource>,
        left_key: &str,
        right_key: &str,
        join_type: JoinType,
        time_bound: Duration,
    ) -> StreamWriter {
        StreamWriter::new(
            self.source,
            Plan::Join {
                right,
                left_key: left_key.to_string(),
                right_key: right_key.to_string(),
                join_type,
                time_bound_ms: time_bound.as_millis() as u64,
                watermark: self.watermark.clone(),
                distributed_shards: None,
            },
            self.watermark,
        )
    }

    /// Windowed path: tumbling event-time window of `size` on the epoch-ms `time_col`.
    /// Chain `.slide(step)` on the returned builder for a sliding window.
    pub fn window(self, time_col: &str, size: Duration) -> WindowedBuilder {
        WindowedBuilder {
            source: self.source,
            watermark: self.watermark,
            time_col: time_col.to_string(),
            window_size_ms: size.as_millis() as u64,
            slide_ms: None,
            group_cols: Vec::new(),
            aggs: Vec::new(),
            distributed_shards: None,
        }
    }
}

/// Builds a windowed aggregation query.
pub struct WindowedBuilder {
    source: Arc<dyn StreamSource>,
    watermark: Option<(String, u64)>,
    time_col: String,
    window_size_ms: u64,
    slide_ms: Option<u64>,
    group_cols: Vec<String>,
    aggs: Vec<Agg>,
    distributed_shards: Option<u32>,
}

impl WindowedBuilder {
    /// Sliding step for overlapping windows.  Must be < `size`; omitting this
    /// gives a tumbling window (slide == size).
    pub fn slide(mut self, step: Duration) -> Self {
        self.slide_ms = Some(step.as_millis() as u64);
        self
    }

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

    /// Distribute the keyed window state across `num_shards` shards instead of
    /// keeping it in one driver-local store. Each batch's partials are routed by a
    /// stable key hash to a `MergeState` task; the shard's state persists on the
    /// owning worker (in local mode, the in-process shared store). Use this when the
    /// window/group key space is large enough that driver-local state is a concern.
    pub fn distributed(mut self, num_shards: u32) -> Self {
        self.distributed_shards = Some(num_shards.max(1));
        self
    }

    /// Finish building and move to the output/sink stage.
    pub fn write_stream(self) -> StreamWriter {
        StreamWriter::new(
            self.source,
            Plan::Windowed {
                time_col: self.time_col,
                window_size_ms: self.window_size_ms,
                slide_ms: self.slide_ms,
                group_cols: self.group_cols,
                aggs: self.aggs,
                distributed_shards: self.distributed_shards,
            },
            self.watermark,
        )
    }
}

/// Builds a session window aggregation query.
pub struct SessionBuilder {
    source: Arc<dyn StreamSource>,
    watermark: Option<(String, u64)>,
    time_col: String,
    gap_ms: u64,
    group_cols: Vec<String>,
    aggs: Vec<Agg>,
    distributed_shards: Option<u32>,
}

impl SessionBuilder {
    pub fn group_by(mut self, cols: &[&str]) -> Self {
        self.group_cols = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn aggregate(mut self, aggs: Vec<Agg>) -> Self {
        self.aggs = aggs;
        self
    }

    /// Shard the per-group session state across `num_shards` shards (see
    /// [`WindowedBuilder::distributed`]). Each batch's events route by a stable
    /// group-key hash to a `MergeState` task instead of one driver-local store.
    pub fn distributed(mut self, num_shards: u32) -> Self {
        self.distributed_shards = Some(num_shards.max(1));
        self
    }

    pub fn write_stream(self) -> StreamWriter {
        StreamWriter::new(
            self.source,
            Plan::Session {
                time_col: self.time_col,
                gap_ms: self.gap_ms,
                group_cols: self.group_cols,
                aggs: self.aggs,
                distributed_shards: self.distributed_shards,
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
        slide_ms: Option<u64>,
        group_cols: Vec<String>,
        aggs: Vec<Agg>,
        distributed_shards: Option<u32>,
    },
    Join {
        right: Arc<dyn StreamSource>,
        left_key: String,
        right_key: String,
        join_type: JoinType,
        time_bound_ms: u64,
        /// Watermark hint — same value passed to StreamWriter; used to set up WatermarkTracker.
        watermark: Option<(String, u64)>,
        distributed_shards: Option<u32>,
    },
    Session {
        time_col: String,
        gap_ms: u64,
        group_cols: Vec<String>,
        aggs: Vec<Agg>,
        distributed_shards: Option<u32>,
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

    /// Shard a stream-stream join's buffer state across `num_shards` (see
    /// [`WindowedBuilder::distributed`]). Joins have no intermediate builder, so this
    /// is set on the writer; it applies only to the `join_stream` path.
    pub fn distributed(mut self, num_shards: u32) -> Self {
        if let Plan::Join {
            distributed_shards, ..
        } = &mut self.plan
        {
            *distributed_shards = Some(num_shards.max(1));
        }
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
                slide_ms,
                group_cols,
                aggs,
                distributed_shards,
            } => {
                ensure_mergeable(&aggs)?;
                let spec = WindowedSpec {
                    time_col,
                    window_size_ms,
                    slide_ms,
                    watermark_delay_ms: self.watermark.as_ref().map(|(_, d)| *d),
                    group_cols,
                    aggs,
                    mode: self.mode,
                };
                match distributed_shards {
                    Some(n) => {
                        // Distributed state lives in the sharded worker stores, so the
                        // inner engine's local store is unused (checkpoint = None); the
                        // checkpoint is driven per-shard by the merge tasks instead.
                        let windowed =
                            WindowedEngine::new(sql_ctx, runtime, self.source.clone(), spec, None)?;
                        let ckpt = self
                            .checkpoint_dir
                            .as_ref()
                            .map(|p| p.to_string_lossy().into_owned());
                        // The query id must be stable across restarts when checkpointing
                        // (shard state_ids and their checkpoint files are keyed by it), so
                        // derive it from the checkpoint dir. Without a checkpoint, a unique
                        // per-process counter is enough.
                        let query_id = match &ckpt {
                            Some(dir) => stable_query_id(dir),
                            None => {
                                static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
                                NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
                            }
                        };
                        Arc::new(DistributedStateEngine::new(
                            windowed,
                            ssc.sc.clone(),
                            n,
                            query_id,
                            ckpt,
                        ))
                    }
                    None => Arc::new(WindowedEngine::new(
                        sql_ctx,
                        runtime,
                        self.source.clone(),
                        spec,
                        self.checkpoint_dir,
                    )?),
                }
            }
            Plan::Join {
                right,
                left_key,
                right_key,
                join_type,
                time_bound_ms,
                watermark,
                distributed_shards,
            } => {
                let left_schema = self.source.schema();
                let right_schema = right.schema();
                let left_time_col = watermark
                    .as_ref()
                    .map(|(c, _)| c.clone())
                    .unwrap_or_default();
                let right_time_col = left_time_col.clone();
                let watermark_delay_ms = watermark.as_ref().map(|(_, d)| *d).unwrap_or(0);
                let spec = StreamJoinSpec {
                    left_key_col: left_key,
                    right_key_col: right_key,
                    join_type,
                    time_bound_ms,
                    left_time_col,
                    right_time_col,
                    left_schema,
                    right_schema,
                    watermark_delay_ms,
                };
                right.start();
                match distributed_shards {
                    Some(n) => {
                        let ckpt = self
                            .checkpoint_dir
                            .as_ref()
                            .map(|p| p.to_string_lossy().into_owned());
                        let query_id = match &ckpt {
                            Some(dir) => stable_query_id(dir),
                            None => {
                                static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
                                NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
                            }
                        };
                        let inner = StreamJoinEngine::new(self.source.clone(), right, spec, None)?;
                        Arc::new(DistributedJoinEngine::new(
                            inner,
                            ssc.sc.clone(),
                            n,
                            query_id,
                            ckpt,
                        ))
                    }
                    None => Arc::new(StreamJoinEngine::new(
                        self.source.clone(),
                        right,
                        spec,
                        self.checkpoint_dir,
                    )?),
                }
            }
            Plan::Session {
                time_col,
                gap_ms,
                group_cols,
                aggs,
                distributed_shards,
            } => {
                ensure_mergeable(&aggs)?;
                let spec = SessionSpec {
                    time_col,
                    gap_ms,
                    watermark_delay_ms: self.watermark.as_ref().map(|(_, d)| *d),
                    group_cols,
                    aggs,
                    mode: self.mode,
                };
                match distributed_shards {
                    Some(n) => {
                        let ckpt = self
                            .checkpoint_dir
                            .as_ref()
                            .map(|p| p.to_string_lossy().into_owned());
                        let query_id = match &ckpt {
                            Some(dir) => stable_query_id(dir),
                            None => {
                                static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
                                NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
                            }
                        };
                        let inner = SessionEngine::new(self.source.clone(), spec, None)?;
                        Arc::new(DistributedSessionEngine::new(
                            inner,
                            ssc.sc.clone(),
                            n,
                            query_id,
                            ckpt,
                        ))
                    }
                    None => Arc::new(SessionEngine::new(
                        self.source.clone(),
                        spec,
                        self.checkpoint_dir,
                    )?),
                }
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
