use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{Field, Schema};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use atomic_streaming::context::StreamingContext;
use atomic_structured::frame::{SessionBuilder, StreamWriter, StreamingDataFrame, WindowedBuilder};
use atomic_structured::sink::{ConsoleSink, FileSink, MemorySink, Sink};
use atomic_structured::source::{FileStreamSource, RateSource};
use atomic_structured::state::Agg;
use atomic_structured::{OutputMode, Trigger};

use crate::sql::{batches_to_py_list, parse_arrow_type};

fn runtime_err(msg: impl std::fmt::Display) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(msg.to_string())
}

fn consumed_err() -> PyErr {
    runtime_err("builder already consumed by a previous call")
}

/// Entry point for structured (continuous) streaming queries.
///
/// ```python
/// ctx = StructuredStreamingContext(batch_secs=1.0)
/// q = (ctx.rate_stream(rows_per_batch=5)
///         .window("timestamp", size_ms=1000)
///         .aggregate([Agg.count("n")])
///         .write_stream()
///         .output_mode("complete")
///         .trigger("available_now")
///         .format(Sink.memory())
///         .start(ctx))
/// ```
#[pyclass(name = "StructuredStreamingContext")]
pub struct PyStructuredContext {
    ssc: Arc<StreamingContext>,
}

#[pymethods]
impl PyStructuredContext {
    #[new]
    #[pyo3(signature = (batch_secs = 1.0))]
    pub fn new(batch_secs: f64) -> PyResult<Self> {
        let sc = atomic_compute::context::Context::new().map_err(runtime_err)?;
        let ssc = StreamingContext::new(sc, Duration::from_secs_f64(batch_secs));
        Ok(Self { ssc })
    }

    /// A source that emits `rows_per_batch` synthetic rows (`timestamp`, `value`) each tick.
    pub fn rate_stream(&self, rows_per_batch: usize) -> PyStreamingDataFrame {
        let source = Arc::new(RateSource::new(rows_per_batch));
        PyStreamingDataFrame::wrap(StreamingDataFrame::read_stream(source))
    }

    /// A source that reads new CSV files under `dir`. `schema` is an ordered list of
    /// `(column_name, arrow_type)` pairs; `has_header` skips each file's first line.
    pub fn csv_stream(
        &self,
        dir: &str,
        schema: Vec<(String, String)>,
        has_header: bool,
    ) -> PyResult<PyStreamingDataFrame> {
        let fields = schema
            .iter()
            .map(|(n, t)| Ok(Field::new(n, parse_arrow_type(t)?, true)))
            .collect::<PyResult<Vec<_>>>()?;
        let arrow_schema = Arc::new(Schema::new(fields));
        let source = Arc::new(FileStreamSource::csv(dir, arrow_schema, has_header));
        Ok(PyStreamingDataFrame::wrap(StreamingDataFrame::read_stream(
            source,
        )))
    }
}

/// A continuous query under construction (before an aggregation or SQL step).
#[pyclass(name = "StreamingDataFrame")]
pub struct PyStreamingDataFrame {
    inner: Mutex<Option<StreamingDataFrame>>,
}

impl PyStreamingDataFrame {
    fn wrap(df: StreamingDataFrame) -> Self {
        Self {
            inner: Mutex::new(Some(df)),
        }
    }

    fn take(&self) -> PyResult<StreamingDataFrame> {
        self.inner.lock().take().ok_or_else(consumed_err)
    }
}

#[pymethods]
impl PyStreamingDataFrame {
    /// Declare an event-time watermark on `col` (epoch-ms), tolerating `delay_ms` of lateness.
    pub fn with_watermark(&self, col: &str, delay_ms: u64) -> PyResult<PyStreamingDataFrame> {
        let df = self.take()?;
        Ok(PyStreamingDataFrame::wrap(
            df.with_watermark(col, Duration::from_millis(delay_ms)),
        ))
    }

    /// Stateless per-batch SQL over the `input` table.
    pub fn sql(&self, query: &str) -> PyResult<PyStreamWriter> {
        let df = self.take()?;
        Ok(PyStreamWriter::wrap(df.sql(query)))
    }

    /// Tumbling event-time window of `size_ms` on the epoch-ms `time_col`.
    pub fn window(&self, time_col: &str, size_ms: u64) -> PyResult<PyWindowedBuilder> {
        let df = self.take()?;
        Ok(PyWindowedBuilder::wrap(
            df.window(time_col, Duration::from_millis(size_ms)),
        ))
    }

    /// Session window on `time_col` with an inactivity `gap_ms`.
    pub fn session_window(&self, time_col: &str, gap_ms: u64) -> PyResult<PySessionBuilder> {
        let df = self.take()?;
        Ok(PySessionBuilder::wrap(
            df.session_window(time_col, Duration::from_millis(gap_ms)),
        ))
    }

    /// Drop duplicate rows keyed by `key_cols`, bounded by the declared watermark.
    pub fn drop_duplicates_within_watermark(
        &self,
        key_cols: Vec<String>,
    ) -> PyResult<PyStreamWriter> {
        let df = self.take()?;
        let refs: Vec<&str> = key_cols.iter().map(String::as_str).collect();
        Ok(PyStreamWriter::wrap(
            df.drop_duplicates_within_watermark(&refs),
        ))
    }
}

/// Builds a windowed aggregation query.
#[pyclass(name = "WindowedBuilder")]
pub struct PyWindowedBuilder {
    inner: Mutex<Option<WindowedBuilder>>,
}

impl PyWindowedBuilder {
    fn wrap(b: WindowedBuilder) -> Self {
        Self {
            inner: Mutex::new(Some(b)),
        }
    }

    fn take(&self) -> PyResult<WindowedBuilder> {
        self.inner.lock().take().ok_or_else(consumed_err)
    }
}

#[pymethods]
impl PyWindowedBuilder {
    /// Convert the tumbling window into a sliding window advancing by `step_ms`.
    pub fn slide(&self, step_ms: u64) -> PyResult<PyWindowedBuilder> {
        Ok(PyWindowedBuilder::wrap(
            self.take()?.slide(Duration::from_millis(step_ms)),
        ))
    }

    /// Group each window by `cols` before aggregating.
    pub fn group_by(&self, cols: Vec<String>) -> PyResult<PyWindowedBuilder> {
        let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        Ok(PyWindowedBuilder::wrap(self.take()?.group_by(&refs)))
    }

    /// Set the aggregates emitted per window.
    pub fn aggregate(&self, py: Python<'_>, aggs: Vec<Py<PyAgg>>) -> PyResult<PyWindowedBuilder> {
        let aggs: Vec<Agg> = aggs.iter().map(|a| a.borrow(py).inner.clone()).collect();
        Ok(PyWindowedBuilder::wrap(self.take()?.aggregate(aggs)))
    }

    /// Shard window state across `num_shards` distributed tasks.
    pub fn distributed(&self, num_shards: u32) -> PyResult<PyWindowedBuilder> {
        Ok(PyWindowedBuilder::wrap(
            self.take()?.distributed(num_shards),
        ))
    }

    /// Finish the builder, returning a writer.
    pub fn write_stream(&self) -> PyResult<PyStreamWriter> {
        Ok(PyStreamWriter::wrap(self.take()?.write_stream()))
    }
}

/// Builds a session-window aggregation query.
#[pyclass(name = "SessionBuilder")]
pub struct PySessionBuilder {
    inner: Mutex<Option<SessionBuilder>>,
}

impl PySessionBuilder {
    fn wrap(b: SessionBuilder) -> Self {
        Self {
            inner: Mutex::new(Some(b)),
        }
    }

    fn take(&self) -> PyResult<SessionBuilder> {
        self.inner.lock().take().ok_or_else(consumed_err)
    }
}

#[pymethods]
impl PySessionBuilder {
    /// Group each session by `cols` before aggregating.
    pub fn group_by(&self, cols: Vec<String>) -> PyResult<PySessionBuilder> {
        let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        Ok(PySessionBuilder::wrap(self.take()?.group_by(&refs)))
    }

    /// Set the aggregates emitted per session.
    pub fn aggregate(&self, py: Python<'_>, aggs: Vec<Py<PyAgg>>) -> PyResult<PySessionBuilder> {
        let aggs: Vec<Agg> = aggs.iter().map(|a| a.borrow(py).inner.clone()).collect();
        Ok(PySessionBuilder::wrap(self.take()?.aggregate(aggs)))
    }

    /// Shard session state across `num_shards` distributed tasks.
    pub fn distributed(&self, num_shards: u32) -> PyResult<PySessionBuilder> {
        Ok(PySessionBuilder::wrap(self.take()?.distributed(num_shards)))
    }

    /// Finish the builder, returning a writer.
    pub fn write_stream(&self) -> PyResult<PyStreamWriter> {
        Ok(PyStreamWriter::wrap(self.take()?.write_stream()))
    }
}

/// A mergeable aggregate: `kind(input_col) AS output_col`.
#[pyclass(name = "Agg")]
pub struct PyAgg {
    inner: Agg,
}

#[pymethods]
impl PyAgg {
    /// `COUNT(*) AS output`.
    #[staticmethod]
    pub fn count(output: &str) -> Self {
        PyAgg {
            inner: Agg::count(output),
        }
    }

    /// `SUM(col) AS output`.
    #[staticmethod]
    pub fn sum(col: &str, output: &str) -> Self {
        PyAgg {
            inner: Agg::sum(col, output),
        }
    }

    /// `MIN(col) AS output`.
    #[staticmethod]
    pub fn min(col: &str, output: &str) -> Self {
        PyAgg {
            inner: Agg::min(col, output),
        }
    }

    /// `MAX(col) AS output`.
    #[staticmethod]
    pub fn max(col: &str, output: &str) -> Self {
        PyAgg {
            inner: Agg::max(col, output),
        }
    }

    /// `AVG(col) AS output`.
    #[staticmethod]
    pub fn avg(col: &str, output: &str) -> Self {
        PyAgg {
            inner: Agg::avg(col, output),
        }
    }
}

/// A streaming output sink.
#[pyclass(name = "Sink")]
pub struct PySink {
    memory: Option<Arc<MemorySink>>,
    sink: Arc<dyn Sink>,
}

#[pymethods]
impl PySink {
    /// In-memory sink; read collected rows with `rows()`.
    #[staticmethod]
    pub fn memory() -> Self {
        let mem = Arc::new(MemorySink::new());
        PySink {
            memory: Some(mem.clone()),
            sink: mem,
        }
    }

    /// Print each batch to stdout under `name`.
    #[staticmethod]
    pub fn console(name: &str) -> Self {
        PySink {
            memory: None,
            sink: Arc::new(ConsoleSink::new(name)),
        }
    }

    /// Write each batch as a file under `dir`.
    #[staticmethod]
    pub fn file(dir: &str) -> Self {
        PySink {
            memory: None,
            sink: Arc::new(FileSink::new(dir)),
        }
    }

    /// Rows collected so far (memory sink only), as a list of dicts.
    pub fn rows(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.memory {
            Some(mem) => batches_to_py_list(py, &mem.batches()),
            None => batches_to_py_list(py, &[]),
        }
    }

    /// Total rows collected so far (memory sink only).
    pub fn row_count(&self) -> usize {
        self.memory.as_ref().map(|m| m.row_count()).unwrap_or(0)
    }
}

/// Configures and starts a streaming query.
#[pyclass(name = "StreamWriter")]
pub struct PyStreamWriter {
    inner: Mutex<Option<StreamWriter>>,
}

impl PyStreamWriter {
    fn wrap(w: StreamWriter) -> Self {
        Self {
            inner: Mutex::new(Some(w)),
        }
    }

    fn take(&self) -> PyResult<StreamWriter> {
        self.inner.lock().take().ok_or_else(consumed_err)
    }
}

#[pymethods]
impl PyStreamWriter {
    /// Output mode: `append`, `update`, or `complete`.
    pub fn output_mode(&self, mode: &str) -> PyResult<PyStreamWriter> {
        let m = match mode.to_ascii_lowercase().as_str() {
            "append" => OutputMode::Append,
            "update" => OutputMode::Update,
            "complete" => OutputMode::Complete,
            other => return Err(runtime_err(format!("unknown output mode: {other}"))),
        };
        Ok(PyStreamWriter::wrap(self.take()?.output_mode(m)))
    }

    /// Name this query.
    pub fn query_name(&self, name: &str) -> PyResult<PyStreamWriter> {
        Ok(PyStreamWriter::wrap(self.take()?.query_name(name)))
    }

    /// Trigger: `processing_time` (needs `interval_ms`), `once`, or `available_now`.
    #[pyo3(signature = (kind, interval_ms = None))]
    pub fn trigger(&self, kind: &str, interval_ms: Option<u64>) -> PyResult<PyStreamWriter> {
        let t = match kind.to_ascii_lowercase().as_str() {
            "processing_time" => {
                let ms = interval_ms
                    .ok_or_else(|| runtime_err("processing_time trigger needs interval_ms"))?;
                Trigger::ProcessingTime(Duration::from_millis(ms))
            }
            "once" => Trigger::Once,
            "available_now" => Trigger::AvailableNow,
            other => return Err(runtime_err(format!("unknown trigger: {other}"))),
        };
        Ok(PyStreamWriter::wrap(self.take()?.trigger(t)))
    }

    /// Route output to `sink`.
    pub fn format(&self, sink: &PySink) -> PyResult<PyStreamWriter> {
        Ok(PyStreamWriter::wrap(self.take()?.format(sink.sink.clone())))
    }

    /// Directory where windowed state + watermark are checkpointed.
    pub fn checkpoint(&self, dir: &str) -> PyResult<PyStreamWriter> {
        Ok(PyStreamWriter::wrap(self.take()?.checkpoint(dir)))
    }

    /// Shard a stream-stream join's buffer state across `num_shards`.
    pub fn distributed(&self, num_shards: u32) -> PyResult<PyStreamWriter> {
        Ok(PyStreamWriter::wrap(self.take()?.distributed(num_shards)))
    }

    /// Start the query on `ctx`, returning a control handle.
    pub fn start(&self, ctx: &PyStructuredContext) -> PyResult<PyStreamingQuery> {
        let query = self.take()?.start(&ctx.ssc).map_err(runtime_err)?;
        Ok(PyStreamingQuery { inner: query })
    }
}

/// A running structured streaming query — control handle.
#[pyclass(name = "StreamingQuery")]
pub struct PyStreamingQuery {
    inner: atomic_structured::query::StreamingQuery,
}

#[pymethods]
impl PyStreamingQuery {
    /// The user-assigned query name, if any.
    pub fn name(&self) -> Option<String> {
        self.inner.name().map(str::to_string)
    }

    /// Block until the query is stopped.
    pub fn await_termination(&self) -> PyResult<()> {
        self.inner.await_termination().map_err(runtime_err)
    }

    /// Stop the query and release the source.
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// The number of micro-batches processed so far.
    pub fn epoch(&self) -> usize {
        self.inner.epoch()
    }

    /// A snapshot of the most recent progress: `{epoch, last_batch_ms}`.
    pub fn last_progress(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let p = self.inner.last_progress();
        let d = PyDict::new(py);
        d.set_item("epoch", p.epoch)?;
        d.set_item("last_batch_ms", p.last_batch_ms)?;
        Ok(d.into_any().unbind())
    }
}
