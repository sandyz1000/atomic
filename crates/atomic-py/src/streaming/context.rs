use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;

use super::batch_queue::PyBatchQueue;
use super::dstream::{PyDStream, PyDStreamInner};
use super::engine::{call_output_fn, compute_batch};

struct OutputOp {
    stream: Arc<PyDStreamInner>,
    func: Py<PyAny>,
}

/// Per-`updateStateByKey` operator state: each entry maps a serialized key to its serialized
/// state, one map per stateful DStream.
type StateStores = Arc<Mutex<Vec<HashMap<String, Vec<u8>>>>>;

#[pyclass(name = "StreamingContext")]
pub struct PyStreamingContext {
    batch_secs: f64,
    output_ops: Arc<Mutex<Vec<OutputOp>>>,
    state_stores: StateStores,
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    checkpoint_dir: Option<PathBuf>,
}

#[pymethods]
impl PyStreamingContext {
    #[new]
    pub fn new(batch_secs: f64) -> Self {
        Self {
            batch_secs,
            output_ops: Arc::new(Mutex::new(Vec::new())),
            state_stores: Arc::new(Mutex::new(Vec::new())),
            stop_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            thread_handle: Mutex::new(None),
            checkpoint_dir: None,
        }
    }

    /// Enable checkpointing: the current state is written to `dir` after each
    /// `run_one_batch()` call. The directory is created if it does not exist.
    pub fn checkpoint(&mut self, dir: &str) -> PyResult<()> {
        let path = PathBuf::from(dir);
        std::fs::create_dir_all(&path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("checkpoint dir: {e}")))?;
        self.checkpoint_dir = Some(path);
        Ok(())
    }

    /// Restore a StreamingContext from the latest checkpoint written to `dir`.
    ///
    /// Returns `None` if no checkpoint exists at `dir`. The caller must
    /// re-register all DStreams and output operations on the returned context;
    /// the saved state stores are pre-loaded so `updateStateByKey` resumes
    /// from where it left off.
    #[staticmethod]
    pub fn from_checkpoint(dir: &str) -> PyResult<Option<PyStreamingContext>> {
        let path = PathBuf::from(dir).join("checkpoint.json");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("read checkpoint: {e}")))?;
        let data: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("parse checkpoint: {e}"))
        })?;
        let batch_secs = data["batch_secs"].as_f64().unwrap_or(1.0);
        let raw_stores = data["state_stores"].as_array().cloned().unwrap_or_default();
        let state_stores: Vec<HashMap<String, Vec<u8>>> = raw_stores
            .iter()
            .map(|store| {
                store
                    .as_object()
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| {
                                let bytes: Vec<u8> = v
                                    .as_array()
                                    .unwrap_or(&vec![])
                                    .iter()
                                    .filter_map(|b| b.as_u64().map(|n| n as u8))
                                    .collect();
                                (k.clone(), bytes)
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect();
        Ok(Some(PyStreamingContext {
            batch_secs,
            output_ops: Arc::new(Mutex::new(Vec::new())),
            state_stores: Arc::new(Mutex::new(state_stores)),
            stop_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            thread_handle: Mutex::new(None),
            checkpoint_dir: Some(PathBuf::from(dir)),
        }))
    }

    pub fn socket_text_stream(&self, host: &str, port: u16) -> PyDStream {
        PyDStream {
            inner: Arc::new(PyDStreamInner::Socket {
                host: host.to_string(),
                port,
            }),
            is_pair: false,
        }
    }

    pub fn text_file_stream(&self, directory: &str) -> PyDStream {
        PyDStream {
            inner: Arc::new(PyDStreamInner::File {
                directory: directory.to_string(),
            }),
            is_pair: false,
        }
    }

    pub fn test_queue_stream(&self) -> (PyDStream, PyBatchQueue) {
        let queue: Arc<Mutex<VecDeque<Vec<Py<PyAny>>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let dstream = PyDStream {
            inner: Arc::new(PyDStreamInner::Queue {
                queue: Arc::clone(&queue),
            }),
            is_pair: false,
        };
        (dstream, PyBatchQueue { queue })
    }

    pub fn test_pair_queue_stream(&self) -> (PyDStream, PyBatchQueue) {
        let queue: Arc<Mutex<VecDeque<Vec<Py<PyAny>>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let dstream = PyDStream {
            inner: Arc::new(PyDStreamInner::Queue {
                queue: Arc::clone(&queue),
            }),
            is_pair: true,
        };
        (dstream, PyBatchQueue { queue })
    }

    pub fn foreach_rdd(
        &self,
        _py: Python<'_>,
        stream: &PyDStream,
        func: Py<PyAny>,
    ) -> PyResult<()> {
        let op_idx = {
            let mut ops = self.output_ops.lock();
            let idx = ops.len();
            ops.push(OutputOp {
                stream: Arc::clone(&stream.inner),
                func,
            });
            idx
        };
        // If restored from checkpoint, state_stores may already have an entry for
        // this op index; only push a fresh empty map when one isn't present.
        let mut stores = self.state_stores.lock();
        if stores.len() <= op_idx {
            stores.push(HashMap::new());
        }
        Ok(())
    }

    /// Run exactly one batch tick synchronously (for deterministic tests).
    pub fn run_one_batch(&self, py: Python<'_>) -> PyResult<()> {
        let locked_ops = self.output_ops.lock();
        let mut locked_states = self.state_stores.lock();
        for (idx, op) in locked_ops.iter().enumerate() {
            let state_store = &mut locked_states[idx];
            let elements = compute_batch(py, &op.stream, state_store)?;
            call_output_fn(py, &op.func, elements)?;
        }
        drop(locked_states);
        drop(locked_ops);
        self.write_checkpoint_if_enabled()?;
        Ok(())
    }

    fn write_checkpoint_if_enabled(&self) -> PyResult<()> {
        let Some(ref dir) = self.checkpoint_dir else {
            return Ok(());
        };
        let stores = self.state_stores.lock();
        let serialisable: Vec<serde_json::Value> = stores
            .iter()
            .map(|store| {
                let obj: serde_json::Map<String, serde_json::Value> = store
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            serde_json::Value::Array(
                                v.iter().map(|b| serde_json::json!(*b)).collect(),
                            ),
                        )
                    })
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();
        let data = serde_json::json!({
            "batch_secs": self.batch_secs,
            "state_stores": serialisable,
        });
        let tmp = dir.join("checkpoint.json.tmp");
        let final_path = dir.join("checkpoint.json");
        std::fs::write(&tmp, data.to_string())
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("write checkpoint: {e}")))?;
        std::fs::rename(&tmp, &final_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("rename checkpoint: {e}")))
    }

    /// Start the background batch loop (pyo3 0.28: use `Python::attach` for GIL).
    pub fn start(&self, _py: Python<'_>) -> PyResult<()> {
        let ops = Arc::clone(&self.output_ops);
        let state_stores = Arc::clone(&self.state_stores);
        let stop_flag = Arc::clone(&self.stop_flag);
        let batch_ms = (self.batch_secs * 1000.0) as u64;

        let handle = std::thread::spawn(move || {
            loop {
                if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(batch_ms));
                if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                Python::attach(|py| {
                    let locked_ops = ops.lock();
                    let mut locked_states = state_stores.lock();
                    for (idx, op) in locked_ops.iter().enumerate() {
                        let state_store = &mut locked_states[idx];
                        match compute_batch(py, &op.stream, state_store) {
                            Ok(elements) => {
                                let _ = call_output_fn(py, &op.func, elements);
                            }
                            Err(e) => {
                                eprintln!("PyStreamingContext batch error: {}", e);
                            }
                        }
                    }
                });
            }
        });

        *self.thread_handle.lock() = Some(handle);
        Ok(())
    }

    pub fn stop(&self) {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn await_termination_or_timeout(&self, timeout_secs: f64) -> bool {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs_f64(timeout_secs);
        loop {
            if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                return true;
            }
            if start.elapsed() >= timeout {
                self.stop();
                return false;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
