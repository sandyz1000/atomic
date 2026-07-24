use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{Field, Schema};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;

use atomic_streaming::context::StreamingContext;
use atomic_structured::frame::{SessionBuilder, StreamWriter, StreamingDataFrame, WindowedBuilder};
use atomic_structured::sink::{ConsoleSink, FileSink, MemorySink, Sink};
use atomic_structured::source::{FileStreamSource, RateSource};
use atomic_structured::state::Agg;
use atomic_structured::{OutputMode, Trigger};

use crate::sql::{batches_to_json_rows, parse_arrow_type};

fn err(msg: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(msg.to_string())
}

fn consumed() -> napi::Error {
    err("builder already consumed by a previous call")
}

/// One aggregate as a plain object: `{ kind, col?, output }`. `kind` is
/// `count`/`sum`/`min`/`max`/`avg`; `col` is omitted for `count`.
#[napi(object)]
pub struct AggSpec {
    pub kind: String,
    pub col: Option<String>,
    pub output: String,
}

fn spec_to_agg(s: &AggSpec) -> Result<Agg> {
    let col = || s.col.clone().ok_or_else(|| err("aggregate needs a `col`"));
    Ok(match s.kind.to_ascii_lowercase().as_str() {
        "count" => Agg::count(&s.output),
        "sum" => Agg::sum(&col()?, &s.output),
        "min" => Agg::min(&col()?, &s.output),
        "max" => Agg::max(&col()?, &s.output),
        "avg" => Agg::avg(&col()?, &s.output),
        other => return Err(err(format!("unknown aggregate kind: {other}"))),
    })
}

fn specs_to_aggs(specs: &[AggSpec]) -> Result<Vec<Agg>> {
    specs.iter().map(spec_to_agg).collect()
}

/// Static constructors for `AggSpec` objects, mirroring the Python `Agg` factory.
#[napi(js_name = "Agg")]
pub struct JsAgg;

#[napi]
impl JsAgg {
    #[napi]
    pub fn count(output: String) -> AggSpec {
        AggSpec {
            kind: "count".into(),
            col: None,
            output,
        }
    }

    #[napi]
    pub fn sum(col: String, output: String) -> AggSpec {
        AggSpec {
            kind: "sum".into(),
            col: Some(col),
            output,
        }
    }

    #[napi]
    pub fn min(col: String, output: String) -> AggSpec {
        AggSpec {
            kind: "min".into(),
            col: Some(col),
            output,
        }
    }

    #[napi]
    pub fn max(col: String, output: String) -> AggSpec {
        AggSpec {
            kind: "max".into(),
            col: Some(col),
            output,
        }
    }

    #[napi]
    pub fn avg(col: String, output: String) -> AggSpec {
        AggSpec {
            kind: "avg".into(),
            col: Some(col),
            output,
        }
    }
}

/// Entry point for structured (continuous) streaming queries.
#[napi(js_name = "StructuredStreamingContext")]
pub struct JsStructuredContext {
    ssc: Arc<StreamingContext>,
}

#[napi]
impl JsStructuredContext {
    #[napi(constructor)]
    pub fn new(batch_secs: Option<f64>) -> Result<Self> {
        let sc = atomic_compute::context::Context::new().map_err(err)?;
        let ssc = StreamingContext::new(sc, Duration::from_secs_f64(batch_secs.unwrap_or(1.0)));
        Ok(Self { ssc })
    }

    /// A source that emits `rowsPerBatch` synthetic rows (`timestamp`, `value`) each tick.
    #[napi]
    pub fn rate_stream(&self, rows_per_batch: u32) -> JsStreamingDataFrame {
        let source = Arc::new(RateSource::new(rows_per_batch as usize));
        JsStreamingDataFrame::wrap(StreamingDataFrame::read_stream(source))
    }

    /// A source reading new CSV files under `dir`. `schema` is an ordered array of
    /// `[columnName, arrowType]` pairs; `hasHeader` skips each file's first line.
    #[napi]
    pub fn csv_stream(
        &self,
        dir: String,
        schema: Vec<(String, String)>,
        has_header: bool,
    ) -> Result<JsStreamingDataFrame> {
        let fields = schema
            .iter()
            .map(|(n, t)| Ok(Field::new(n, parse_arrow_type(t)?, true)))
            .collect::<Result<Vec<_>>>()?;
        let arrow_schema = Arc::new(Schema::new(fields));
        let source = Arc::new(FileStreamSource::csv(dir, arrow_schema, has_header));
        Ok(JsStreamingDataFrame::wrap(StreamingDataFrame::read_stream(
            source,
        )))
    }
}

/// A continuous query under construction (before an aggregation or SQL step).
#[napi(js_name = "StreamingDataFrame")]
pub struct JsStreamingDataFrame {
    inner: Mutex<Option<StreamingDataFrame>>,
}

impl JsStreamingDataFrame {
    fn wrap(df: StreamingDataFrame) -> Self {
        Self {
            inner: Mutex::new(Some(df)),
        }
    }

    fn take(&self) -> Result<StreamingDataFrame> {
        self.inner.lock().take().ok_or_else(consumed)
    }
}

#[napi]
impl JsStreamingDataFrame {
    /// Declare an event-time watermark on `col` (epoch-ms), tolerating `delayMs` of lateness.
    #[napi]
    pub fn with_watermark(&self, col: String, delay_ms: u32) -> Result<JsStreamingDataFrame> {
        Ok(JsStreamingDataFrame::wrap(self.take()?.with_watermark(
            &col,
            Duration::from_millis(delay_ms as u64),
        )))
    }

    /// Stateless per-batch SQL over the `input` table.
    #[napi]
    pub fn sql(&self, query: String) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.sql(query)))
    }

    /// Tumbling event-time window of `sizeMs` on the epoch-ms `timeCol`.
    #[napi]
    pub fn window(&self, time_col: String, size_ms: u32) -> Result<JsWindowedBuilder> {
        Ok(JsWindowedBuilder::wrap(
            self.take()?
                .window(&time_col, Duration::from_millis(size_ms as u64)),
        ))
    }

    /// Session window on `timeCol` with an inactivity `gapMs`.
    #[napi]
    pub fn session_window(&self, time_col: String, gap_ms: u32) -> Result<JsSessionBuilder> {
        Ok(JsSessionBuilder::wrap(self.take()?.session_window(
            &time_col,
            Duration::from_millis(gap_ms as u64),
        )))
    }

    /// Drop duplicate rows keyed by `keyCols`, bounded by the declared watermark.
    #[napi]
    pub fn drop_duplicates_within_watermark(
        &self,
        key_cols: Vec<String>,
    ) -> Result<JsStreamWriter> {
        let refs: Vec<&str> = key_cols.iter().map(String::as_str).collect();
        Ok(JsStreamWriter::wrap(
            self.take()?.drop_duplicates_within_watermark(&refs),
        ))
    }
}

/// Builds a windowed aggregation query.
#[napi(js_name = "WindowedBuilder")]
pub struct JsWindowedBuilder {
    inner: Mutex<Option<WindowedBuilder>>,
}

impl JsWindowedBuilder {
    fn wrap(b: WindowedBuilder) -> Self {
        Self {
            inner: Mutex::new(Some(b)),
        }
    }

    fn take(&self) -> Result<WindowedBuilder> {
        self.inner.lock().take().ok_or_else(consumed)
    }
}

#[napi]
impl JsWindowedBuilder {
    /// Convert the tumbling window into a sliding window advancing by `stepMs`.
    #[napi]
    pub fn slide(&self, step_ms: u32) -> Result<JsWindowedBuilder> {
        Ok(JsWindowedBuilder::wrap(
            self.take()?.slide(Duration::from_millis(step_ms as u64)),
        ))
    }

    /// Group each window by `cols` before aggregating.
    #[napi]
    pub fn group_by(&self, cols: Vec<String>) -> Result<JsWindowedBuilder> {
        let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        Ok(JsWindowedBuilder::wrap(self.take()?.group_by(&refs)))
    }

    /// Set the aggregates emitted per window.
    #[napi]
    pub fn aggregate(&self, aggs: Vec<AggSpec>) -> Result<JsWindowedBuilder> {
        let aggs = specs_to_aggs(&aggs)?;
        Ok(JsWindowedBuilder::wrap(self.take()?.aggregate(aggs)))
    }

    /// Shard window state across `numShards` distributed tasks.
    #[napi]
    pub fn distributed(&self, num_shards: u32) -> Result<JsWindowedBuilder> {
        Ok(JsWindowedBuilder::wrap(
            self.take()?.distributed(num_shards),
        ))
    }

    /// Finish the builder, returning a writer.
    #[napi]
    pub fn write_stream(&self) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.write_stream()))
    }
}

/// Builds a session-window aggregation query.
#[napi(js_name = "SessionBuilder")]
pub struct JsSessionBuilder {
    inner: Mutex<Option<SessionBuilder>>,
}

impl JsSessionBuilder {
    fn wrap(b: SessionBuilder) -> Self {
        Self {
            inner: Mutex::new(Some(b)),
        }
    }

    fn take(&self) -> Result<SessionBuilder> {
        self.inner.lock().take().ok_or_else(consumed)
    }
}

#[napi]
impl JsSessionBuilder {
    /// Group each session by `cols` before aggregating.
    #[napi]
    pub fn group_by(&self, cols: Vec<String>) -> Result<JsSessionBuilder> {
        let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        Ok(JsSessionBuilder::wrap(self.take()?.group_by(&refs)))
    }

    /// Set the aggregates emitted per session.
    #[napi]
    pub fn aggregate(&self, aggs: Vec<AggSpec>) -> Result<JsSessionBuilder> {
        let aggs = specs_to_aggs(&aggs)?;
        Ok(JsSessionBuilder::wrap(self.take()?.aggregate(aggs)))
    }

    /// Shard session state across `numShards` distributed tasks.
    #[napi]
    pub fn distributed(&self, num_shards: u32) -> Result<JsSessionBuilder> {
        Ok(JsSessionBuilder::wrap(self.take()?.distributed(num_shards)))
    }

    /// Finish the builder, returning a writer.
    #[napi]
    pub fn write_stream(&self) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.write_stream()))
    }
}

/// A streaming output sink.
#[napi(js_name = "Sink")]
pub struct JsSink {
    memory: Option<Arc<MemorySink>>,
    sink: Arc<dyn Sink>,
}

#[napi]
impl JsSink {
    /// In-memory sink; read collected rows with `rows()`.
    #[napi(factory)]
    pub fn memory() -> Self {
        let mem = Arc::new(MemorySink::new());
        JsSink {
            memory: Some(mem.clone()),
            sink: mem,
        }
    }

    /// Print each batch to stdout under `name`.
    #[napi(factory)]
    pub fn console(name: String) -> Self {
        JsSink {
            memory: None,
            sink: Arc::new(ConsoleSink::new(name)),
        }
    }

    /// Write each batch as a file under `dir`.
    #[napi(factory)]
    pub fn file(dir: String) -> Self {
        JsSink {
            memory: None,
            sink: Arc::new(FileSink::new(dir)),
        }
    }

    /// Rows collected so far (memory sink only), as an array of objects.
    #[napi]
    pub fn rows(&self) -> Vec<serde_json::Value> {
        match &self.memory {
            Some(mem) => batches_to_json_rows(&mem.batches()),
            None => Vec::new(),
        }
    }

    /// Total rows collected so far (memory sink only).
    #[napi]
    pub fn row_count(&self) -> u32 {
        self.memory
            .as_ref()
            .map(|m| m.row_count() as u32)
            .unwrap_or(0)
    }
}

/// Configures and starts a streaming query.
#[napi(js_name = "StreamWriter")]
pub struct JsStreamWriter {
    inner: Mutex<Option<StreamWriter>>,
}

impl JsStreamWriter {
    fn wrap(w: StreamWriter) -> Self {
        Self {
            inner: Mutex::new(Some(w)),
        }
    }

    fn take(&self) -> Result<StreamWriter> {
        self.inner.lock().take().ok_or_else(consumed)
    }
}

#[napi]
impl JsStreamWriter {
    /// Output mode: `append`, `update`, or `complete`.
    #[napi]
    pub fn output_mode(&self, mode: String) -> Result<JsStreamWriter> {
        let m = match mode.to_ascii_lowercase().as_str() {
            "append" => OutputMode::Append,
            "update" => OutputMode::Update,
            "complete" => OutputMode::Complete,
            other => return Err(err(format!("unknown output mode: {other}"))),
        };
        Ok(JsStreamWriter::wrap(self.take()?.output_mode(m)))
    }

    /// Name this query.
    #[napi]
    pub fn query_name(&self, name: String) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.query_name(name)))
    }

    /// Trigger: `processing_time` (needs `intervalMs`), `once`, or `available_now`.
    #[napi]
    pub fn trigger(&self, kind: String, interval_ms: Option<u32>) -> Result<JsStreamWriter> {
        let t = match kind.to_ascii_lowercase().as_str() {
            "processing_time" => {
                let ms =
                    interval_ms.ok_or_else(|| err("processing_time trigger needs intervalMs"))?;
                Trigger::ProcessingTime(Duration::from_millis(ms as u64))
            }
            "once" => Trigger::Once,
            "available_now" => Trigger::AvailableNow,
            other => return Err(err(format!("unknown trigger: {other}"))),
        };
        Ok(JsStreamWriter::wrap(self.take()?.trigger(t)))
    }

    /// Route output to `sink`.
    #[napi]
    pub fn format(&self, sink: &JsSink) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.format(sink.sink.clone())))
    }

    /// Directory where windowed state + watermark are checkpointed.
    #[napi]
    pub fn checkpoint(&self, dir: String) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.checkpoint(dir)))
    }

    /// Shard a stream-stream join's buffer state across `numShards`.
    #[napi]
    pub fn distributed(&self, num_shards: u32) -> Result<JsStreamWriter> {
        Ok(JsStreamWriter::wrap(self.take()?.distributed(num_shards)))
    }

    /// Start the query on `ctx`, returning a control handle.
    #[napi]
    pub fn start(&self, ctx: &JsStructuredContext) -> Result<JsStreamingQuery> {
        let query = self.take()?.start(&ctx.ssc).map_err(err)?;
        Ok(JsStreamingQuery { inner: query })
    }
}

/// A running structured streaming query — control handle.
#[napi(js_name = "StreamingQuery")]
pub struct JsStreamingQuery {
    inner: atomic_structured::query::StreamingQuery,
}

#[napi]
impl JsStreamingQuery {
    /// The user-assigned query name, if any.
    #[napi]
    pub fn name(&self) -> Option<String> {
        self.inner.name().map(str::to_string)
    }

    /// Block until the query is stopped.
    #[napi]
    pub fn await_termination(&self) -> Result<()> {
        self.inner.await_termination().map_err(err)
    }

    /// Stop the query and release the source.
    #[napi]
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// The number of micro-batches processed so far.
    #[napi]
    pub fn epoch(&self) -> u32 {
        self.inner.epoch() as u32
    }

    /// A snapshot of the most recent progress: `{ epoch, lastBatchMs }`.
    #[napi]
    pub fn last_progress(&self) -> serde_json::Value {
        let p = self.inner.last_progress();
        serde_json::json!({
            "epoch": p.epoch,
            "lastBatchMs": p.last_batch_ms,
        })
    }
}
