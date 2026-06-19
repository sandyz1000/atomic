/// Streaming bindings for Python — Phase 4.4
///
/// All DStream element types are `Py<PyAny>` (pyo3 0.28 API).
/// The batch loop runs either synchronously (for tests, via `run_one_batch()`)
/// or in a background thread (via `start()`).
///
/// pyo3 0.28 API notes:
/// - `Py<T>::call1(py, args)` returns `PyResult<Py<PyAny>>` (not `Bound`).
/// - `Bound<T>::unbind()` converts `Bound` → `Py<T>`.
/// - `Py<T>::bind(py)` returns `&Bound<'_, T>`.
/// - `Python::attach(f)` acquires the GIL (replaces `Python::with_gil`).
/// - Do NOT call `.bind(py)` on an already-`Bound` value.
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyIterator, PyList, PyTuple};

// Output/transform functions are stored as live `Py<PyAny>` references rather than
// pickled bytes: every DStream op here runs in-process (synchronously in
// `run_one_batch`, or via `Python::attach` in the background thread spawned by
// `start()`) — nothing crosses a process boundary. Pickling would round-trip the
// closure *by value* (cloudpickle captures free variables at dump time), silently
// detaching callbacks like `lambda b: results.extend(b)` from the caller's live
// `results` list. Holding the `Py<PyAny>` directly keeps the original object alive.
enum PyStreamTransform {
    Map(Py<PyAny>),
    Filter(Py<PyAny>),
    FlatMap(Py<PyAny>),
    ReduceByKey(Py<PyAny>),
    GroupByKey,
    Join(Arc<PyDStreamInner>),
    LeftOuterJoin(Arc<PyDStreamInner>),
    UpdateStateByKey(Py<PyAny>),
    MapValues(Py<PyAny>),
}

enum PyDStreamInner {
    Queue {
        queue: Arc<Mutex<VecDeque<Vec<Py<PyAny>>>>>,
    },
    Socket {
        #[allow(dead_code)]
        host: String,
        #[allow(dead_code)]
        port: u16,
    },
    File {
        directory: String,
    },
    Transform {
        parent: Arc<PyDStreamInner>,
        op: PyStreamTransform,
    },
    Windowed {
        parent: Arc<PyDStreamInner>,
        window_ms: u64,
        // (arrival_time, batch_elements) — pruned on each compute
        buffer: Mutex<VecDeque<(Instant, Vec<Py<PyAny>>)>>,
    },
}

#[pyclass(name = "DStream")]
pub struct PyDStream {
    inner: Arc<PyDStreamInner>,
    pub is_pair: bool,
}

#[pymethods]
impl PyDStream {
    pub fn map(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Map(func),
            }),
            is_pair: false,
        })
    }

    pub fn filter(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Filter(func),
            }),
            is_pair: self.is_pair,
        })
    }

    pub fn flat_map(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::FlatMap(func),
            }),
            is_pair: false,
        })
    }

    pub fn reduce_by_key(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "reduce_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::ReduceByKey(func),
            }),
            is_pair: true,
        })
    }

    pub fn group_by_key(&self) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "group_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::GroupByKey,
            }),
            is_pair: true,
        })
    }

    pub fn join(&self, other: &PyDStream) -> PyResult<PyDStream> {
        if !self.is_pair || !other.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "join requires pair DStreams",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Join(Arc::clone(&other.inner)),
            }),
            is_pair: true,
        })
    }

    pub fn left_outer_join(&self, other: &PyDStream) -> PyResult<PyDStream> {
        if !self.is_pair || !other.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "left_outer_join requires pair DStreams",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::LeftOuterJoin(Arc::clone(&other.inner)),
            }),
            is_pair: true,
        })
    }

    pub fn update_state_by_key(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "update_state_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::UpdateStateByKey(func),
            }),
            is_pair: true,
        })
    }

    pub fn map_values(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "map_values requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::MapValues(func),
            }),
            is_pair: true,
        })
    }

    /// Return a new DStream that unions the parent's batches over a sliding window.
    ///
    /// `window_ms` — how far back (in milliseconds) to include batches.
    /// `slide_ms`  — accepted for API compatibility; in this in-process model every
    ///               `run_one_batch()` call advances the window by one tick.
    pub fn window(&self, window_ms: u64, _slide_ms: u64) -> PyDStream {
        PyDStream {
            inner: Arc::new(PyDStreamInner::Windowed {
                parent: Arc::clone(&self.inner),
                window_ms,
                buffer: Mutex::new(VecDeque::new()),
            }),
            is_pair: self.is_pair,
        }
    }
}

#[pyclass(name = "BatchQueue")]
pub struct PyBatchQueue {
    queue: Arc<Mutex<VecDeque<Vec<Py<PyAny>>>>>,
}

#[pymethods]
impl PyBatchQueue {
    pub fn push(&self, py: Python<'_>, batch: Py<PyAny>) -> PyResult<()> {
        let list = batch.bind(py).cast::<PyList>()?;
        let items: Vec<Py<PyAny>> = list.iter().map(|x| x.unbind()).collect();
        self.queue.lock().push_back(items);
        Ok(())
    }
}

struct OutputOp {
    stream: Arc<PyDStreamInner>,
    func: Py<PyAny>,
}

fn compute_batch(
    py: Python<'_>,
    inner: &PyDStreamInner,
    state_store: &mut HashMap<String, Vec<u8>>,
) -> PyResult<Vec<Py<PyAny>>> {
    match inner {
        PyDStreamInner::Queue { queue } => {
            let mut q = queue.lock();
            Ok(q.pop_front().unwrap_or_default())
        }
        PyDStreamInner::Socket { .. } => Ok(vec![]),
        PyDStreamInner::File { directory } => {
            let mut lines: Vec<Py<PyAny>> = Vec::new();
            if let Ok(dir) = std::fs::read_dir(directory) {
                for entry in dir.flatten() {
                    if let Ok(content) = std::fs::read_to_string(entry.path()) {
                        for line in content.lines() {
                            lines.push(line.to_string().into_pyobject(py)?.into_any().unbind());
                        }
                    }
                }
            }
            Ok(lines)
        }
        PyDStreamInner::Transform { parent, op } => {
            let parent_elems = compute_batch(py, parent, state_store)?;
            apply_transform(py, parent_elems, op, state_store)
        }
        PyDStreamInner::Windowed {
            parent,
            window_ms,
            buffer,
        } => {
            let batch = compute_batch(py, parent, state_store)?;
            let now = Instant::now();
            let cutoff = Duration::from_millis(*window_ms);
            let mut buf = buffer.lock();
            buf.push_back((now, batch));
            buf.retain(|(ts, _)| now.duration_since(*ts) < cutoff);
            Ok(buf
                .iter()
                .flat_map(|(_, elems)| elems.iter().map(|e| e.clone_ref(py)))
                .collect())
        }
    }
}

/// Build a Python tuple `(a, b)` from two `Py<PyAny>`.
fn py_tuple2(py: Python<'_>, a: &Py<PyAny>, b: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    Ok(PyTuple::new(py, [a.bind(py), b.bind(py)])?
        .into_any()
        .unbind())
}

fn apply_transform(
    py: Python<'_>,
    elements: Vec<Py<PyAny>>,
    op: &PyStreamTransform,
    state_store: &mut HashMap<String, Vec<u8>>,
) -> PyResult<Vec<Py<PyAny>>> {
    match op {
        PyStreamTransform::Map(f) => elements
            .iter()
            .map(|e| f.call1(py, (e.bind(py),)))
            .collect(),

        PyStreamTransform::Filter(f) => elements
            .iter()
            .filter_map(
                |e| match f.call1(py, (e.bind(py),)).and_then(|r| r.is_truthy(py)) {
                    Ok(true) => Some(Ok(e.clone_ref(py))),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect(),

        PyStreamTransform::FlatMap(f) => {
            let mut result = Vec::new();
            for e in &elements {
                let iter_result = f.call1(py, (e.bind(py),))?;
                let iter = PyIterator::from_object(iter_result.bind(py))?;
                for item in iter {
                    result.push(item?.unbind());
                }
            }
            Ok(result)
        }

        PyStreamTransform::ReduceByKey(f) => {
            let mut groups: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();
            for e in &elements {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?.unbind();
                let v = pair.get_item(1)?.unbind();
                let mut found = false;
                for (ek, acc) in &mut groups {
                    if ek.bind(py).eq(k.bind(py))? {
                        *acc = f.call1(py, (acc.bind(py), v.bind(py)))?;
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, v));
                }
            }
            groups.iter().map(|(k, v)| py_tuple2(py, k, v)).collect()
        }

        PyStreamTransform::GroupByKey => {
            let mut groups: Vec<(Py<PyAny>, Vec<Py<PyAny>>)> = Vec::new();
            for e in &elements {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?.unbind();
                let v = pair.get_item(1)?.unbind();
                let mut found = false;
                for (ek, vals) in &mut groups {
                    if ek.bind(py).eq(k.bind(py))? {
                        vals.push(v.clone_ref(py));
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, vec![v]));
                }
            }
            groups
                .iter()
                .map(|(k, vals)| {
                    let vlist: Py<PyAny> = PyList::new(py, vals.iter().map(|v| v.bind(py)))?
                        .into_any()
                        .unbind();
                    py_tuple2(py, k, &vlist)
                })
                .collect()
        }

        PyStreamTransform::Join(right_inner) => {
            let mut dummy: HashMap<String, Vec<u8>> = HashMap::new();
            let right_elems = compute_batch(py, right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le.bind(py).cast::<PyTuple>()?;
                let lk = lp.get_item(0)?.unbind();
                let lv = lp.get_item(1)?.unbind();
                for re in &right_elems {
                    let rp = re.bind(py).cast::<PyTuple>()?;
                    let rk = rp.get_item(0)?.unbind();
                    let rv = rp.get_item(1)?.unbind();
                    if lk.bind(py).eq(rk.bind(py))? {
                        let vpair = py_tuple2(py, &lv, &rv)?;
                        result.push(py_tuple2(py, &lk, &vpair)?);
                    }
                }
            }
            Ok(result)
        }

        PyStreamTransform::LeftOuterJoin(right_inner) => {
            let mut dummy: HashMap<String, Vec<u8>> = HashMap::new();
            let right_elems = compute_batch(py, right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le.bind(py).cast::<PyTuple>()?;
                let lk = lp.get_item(0)?.unbind();
                let lv = lp.get_item(1)?.unbind();
                let mut matched = false;
                for re in &right_elems {
                    let rp = re.bind(py).cast::<PyTuple>()?;
                    let rk = rp.get_item(0)?.unbind();
                    let rv = rp.get_item(1)?.unbind();
                    if lk.bind(py).eq(rk.bind(py))? {
                        let vpair = py_tuple2(py, &lv, &rv)?;
                        result.push(py_tuple2(py, &lk, &vpair)?);
                        matched = true;
                    }
                }
                if !matched {
                    let none_val: Py<PyAny> = py.None();
                    let vpair = py_tuple2(py, &lv, &none_val)?;
                    result.push(py_tuple2(py, &lk, &vpair)?);
                }
            }
            Ok(result)
        }

        PyStreamTransform::UpdateStateByKey(f) => {
            let pickle = PyModule::import(py, "pickle")?;

            // Group new values by key (repr as map key).
            let mut new_vals: Vec<(String, Py<PyAny>, Vec<Py<PyAny>>)> = Vec::new();
            for e in &elements {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?;
                let v = pair.get_item(1)?.unbind();
                let k_str = k.repr()?.to_string();
                let mut found = false;
                for (ks, _, vals) in &mut new_vals {
                    if *ks == k_str {
                        vals.push(v.clone_ref(py));
                        found = true;
                        break;
                    }
                }
                if !found {
                    new_vals.push((k_str, k.unbind(), vec![v]));
                }
            }

            // All keys: new keys + state-only keys.
            let mut all_keys: Vec<(String, Py<PyAny>)> = new_vals
                .iter()
                .map(|(ks, k, _)| (ks.clone(), k.clone_ref(py)))
                .collect();
            for k_str in state_store.keys() {
                if !new_vals.iter().any(|(ks, _, _)| ks == k_str) {
                    let py_k: Py<PyAny> = k_str.clone().into_pyobject(py)?.into_any().unbind();
                    all_keys.push((k_str.clone(), py_k));
                }
            }

            let mut result = Vec::new();
            for (key_str, key_obj) in &all_keys {
                let new_values_for_key: &[Py<PyAny>] = new_vals
                    .iter()
                    .find(|(ks, _, _)| ks == key_str)
                    .map(|(_, _, v)| v.as_slice())
                    .unwrap_or(&[]);
                let old_state: Py<PyAny> = if let Some(state_bytes) = state_store.get(key_str) {
                    pickle
                        .call_method1("loads", (PyBytes::new(py, state_bytes),))?
                        .unbind()
                } else {
                    py.None()
                };
                let new_vals_list = PyList::new(py, new_values_for_key.iter().map(|v| v.bind(py)))?;
                let new_state: Py<PyAny> =
                    f.call1(py, (new_vals_list.into_any(), old_state.bind(py)))?;
                if new_state.is_none(py) {
                    state_store.remove(key_str);
                } else {
                    let serialized: Vec<u8> = pickle
                        .call_method1("dumps", (new_state.bind(py),))?
                        .extract()?;
                    state_store.insert(key_str.clone(), serialized);
                    result.push(py_tuple2(py, key_obj, &new_state)?);
                }
            }
            Ok(result)
        }

        PyStreamTransform::MapValues(f) => elements
            .iter()
            .map(|e| {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?.unbind();
                let new_v: Py<PyAny> = f.call1(py, (pair.get_item(1)?,))?;
                py_tuple2(py, &k, &new_v)
            })
            .collect(),
    }
}

fn call_output_fn(py: Python<'_>, func: &Py<PyAny>, elements: Vec<Py<PyAny>>) -> PyResult<()> {
    let list = PyList::new(py, elements.iter().map(|e| e.bind(py)))?;
    func.call1(py, (list.into_any(),))?;
    Ok(())
}

#[pyclass(name = "StreamingContext")]
pub struct PyStreamingContext {
    batch_secs: f64,
    output_ops: Arc<Mutex<Vec<OutputOp>>>,
    state_stores: Arc<Mutex<Vec<HashMap<String, Vec<u8>>>>>,
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
