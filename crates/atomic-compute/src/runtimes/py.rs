/// PyO3 thread-pool for Python task execution.
///
/// Each [`PyWorker`] is a dedicated OS thread that holds a PyO3 interpreter
/// context. Workers communicate with their callers via synchronous Rust channels
/// (zero-capacity rendezvous), so no tokio runtime is required in the pool.
///
/// ## GIL behaviour
///
/// Standard CPython has a process-wide GIL: only one thread runs Python bytecode
/// at a time regardless of the number of workers. Round-robin dispatch across
/// multiple workers gives true concurrency for I/O-bound tasks (the GIL is released
/// during I/O). CPU-bound tasks serialize through the GIL — the same limitation as
/// any embedded CPython approach. The architecture is forward-compatible with
/// Python 3.13 free-threaded builds, which remove the GIL entirely.
///
/// ## Why not subprocess?
///
/// The old design spawned `python3` as a subprocess and communicated over
/// length-prefixed JSON frames on stdin/stdout. This required `base64`-encoding
/// pickled function bytes, managing child-process lifecycle, and handling IPC
/// framing errors. The PyO3 thread-pool replaces all of that with direct in-process
/// calls — no subprocess, no pipe, no JSON framing overhead.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use thiserror::Error;

use crate::error::{ComputeError, ComputeResult};
use atomic_data::distributed::PythonTaskPayload;

#[derive(Debug, Error)]
pub enum PythonTaskError {
    #[error("Python error: {0}")]
    Py(#[from] pyo3::PyErr),
    #[error("payload decode: {0}")]
    PayloadDecode(#[from] serde_json::Error),
    #[error("data conversion: {0}")]
    Convert(#[from] pythonize::PythonizeError),
    #[error("worker channel closed")]
    ChannelClosed,
    #[error("tool source contains an embedded NUL byte")]
    InvalidSource,
    #[error("tool source has no top-level `run(args)` function")]
    MissingRunFn,
}

impl From<PythonTaskError> for ComputeError {
    fn from(e: PythonTaskError) -> Self {
        ComputeError::InvalidPayload(e.to_string())
    }
}

struct PyTask {
    fn_bytes: Vec<u8>,
    data: Vec<u8>,
    reply: mpsc::SyncSender<Result<Vec<u8>, PythonTaskError>>,
}

struct PyWorker {
    task_tx: mpsc::SyncSender<PyTask>,
    _thread: std::thread::JoinHandle<()>,
}

impl PyWorker {
    /// Execute one pickled Python task against a JSON-encoded partition.
    ///
    /// Called from within `Python::attach` so `py` is the live GIL token.
    /// `pythonize`/`depythonize` convert between `serde_json::Value` and Python
    /// objects in Rust — the only Python import needed is `cloudpickle`/`pickle`.
    fn run_task(py: Python<'_>, fn_bytes: &[u8], data: &[u8]) -> Result<Vec<u8>, PythonTaskError> {
        // Unpickle the user's task — cloudpickle is the only Python import needed.
        let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
        let fn_obj = pickle.call_method1("loads", (PyBytes::new(py, fn_bytes),))?;

        // Deserialise partition bytes → serde_json::Value → Python object (no json module).
        let value: serde_json::Value = serde_json::from_slice(data)?;
        let partition = pythonize::pythonize(py, &value)?;

        let result = fn_obj.call1((partition,))?;

        // Convert Python result → serde_json::Value → bytes (no json module).
        let out: serde_json::Value = pythonize::depythonize(&result)?;
        Ok(serde_json::to_vec(&out)?)
    }

    fn spawn() -> Self {
        let (task_tx, task_rx) = mpsc::sync_channel::<PyTask>(0);
        let thread = std::thread::spawn(move || {
            for task in task_rx {
                let result = Python::attach(|py| Self::run_task(py, &task.fn_bytes, &task.data));
                let _ = task.reply.send(result);
            }
        });
        Self {
            task_tx,
            _thread: thread,
        }
    }

    fn execute(&self, fn_bytes: Vec<u8>, data: Vec<u8>) -> Result<Vec<u8>, PythonTaskError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        self.task_tx
            .send(PyTask {
                fn_bytes,
                data,
                reply: reply_tx,
            })
            .map_err(|_| PythonTaskError::ChannelClosed)?;
        reply_rx
            .recv()
            .map_err(|_| PythonTaskError::ChannelClosed)?
    }
}

/// Pool of [`PyWorker`] threads. Round-robin dispatch minimises contention on
/// the per-worker rendezvous channel.
pub struct PyWorkerPool {
    workers: Vec<PyWorker>,
    next: AtomicUsize,
}

impl PyWorkerPool {
    pub fn new(pool_size: usize) -> Self {
        // Ensure Python is initialized before worker threads call attach().
        Python::initialize();
        Self {
            workers: (0..pool_size).map(|_| PyWorker::spawn()).collect(),
            next: AtomicUsize::new(0),
        }
    }

    pub fn execute(&self, fn_bytes: Vec<u8>, data: Vec<u8>) -> Result<Vec<u8>, PythonTaskError> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[idx].execute(fn_bytes, data)
    }
}

/// Run a Python tool's source against one JSON-text argument and return one
/// JSON-text result.
///
/// Unlike [`PyWorkerPool::execute`] (which unpickles a closure shipped from the
/// driver), `source` is raw Python text defining a top-level `run(args)` function —
/// the same convention `atomic-nlq`'s `ToolRegistry::Python(String)` tools already use
/// (see `crates/atomic-nlq/tests/test_context.rs`). Used by `agent_step` tool dispatch
/// (`TOOL_CALL:` handling) for tools resolved into `AgentStepPayload.resolved_tools`.
pub fn run_tool_call(source: &str, args_json: &str) -> Result<String, PythonTaskError> {
    Python::initialize();
    Python::attach(|py| {
        let code = std::ffi::CString::new(source).map_err(|_| PythonTaskError::InvalidSource)?;
        let module = PyModule::from_code(py, &code, c"<agent_tool>", c"agent_tool")?;
        let run_fn = module
            .getattr("run")
            .map_err(|_| PythonTaskError::MissingRunFn)?;

        let value: serde_json::Value = serde_json::from_str(args_json)?;
        let args_obj = pythonize::pythonize(py, &value)?;
        let result = run_fn.call1((args_obj,))?;

        let out: serde_json::Value = pythonize::depythonize(&result)?;
        Ok(serde_json::to_string(&out)?)
    })
}

/// [`OpDispatcher`] for `TaskRuntime::Python` ops.
///
/// Owns a [`PyWorkerPool`] and forwards pickled task bytes + partition data to it.
/// Constructed by [`ComputeEngine::default`]; the pool starts on first construction.
pub(crate) struct PythonDispatcher {
    pool: Arc<PyWorkerPool>,
}

impl PythonDispatcher {
    pub(crate) fn new(pool_size: usize) -> Self {
        Self {
            pool: Arc::new(PyWorkerPool::new(pool_size)),
        }
    }

    fn dispatch_impl(
        &self,
        op: &atomic_data::distributed::PipelineOp,
        data: &[u8],
    ) -> Result<Vec<u8>, PythonTaskError> {
        let spec: PythonTaskPayload = serde_json::from_slice(&op.payload)?;
        self.pool.execute(spec.fn_bytes, data.to_vec())
    }
}

impl crate::runtimes::OpDispatcher for PythonDispatcher {
    fn dispatch(
        &self,
        op: &atomic_data::distributed::PipelineOp,
        _partition_id: usize,
        data: &[u8],
    ) -> ComputeResult<Vec<u8>> {
        Ok(self.dispatch_impl(op, data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_map_roundtrip() {
        // Skip if cloudpickle or python3 not available.
        let output = std::process::Command::new("python3")
            .args([
                "-c",
                "import cloudpickle, sys; sys.stdout.buffer.write(cloudpickle.dumps(lambda p: [x*2 for x in p]))",
            ])
            .output();

        let fn_bytes = match output {
            Ok(o) if o.status.success() && !o.stdout.is_empty() => o.stdout,
            _ => {
                eprintln!("skipping pool_map_roundtrip: cloudpickle not available");
                return;
            }
        };

        let pool = PyWorkerPool::new(2);
        let data = b"[1, 2, 3]".to_vec();
        let result = pool.execute(fn_bytes, data).expect("map succeeded");
        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![2, 4, 6]);
    }
}
