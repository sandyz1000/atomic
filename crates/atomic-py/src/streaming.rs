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
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyIterator, PyList, PyTuple};


fn pickle_fn(py: Python<'_>, func: &Py<PyAny>) -> PyResult<Vec<u8>> {
    let pickle = PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
    pickle
        .call_method1("dumps", (func.bind(py),))?
        .extract::<Vec<u8>>()
}

fn unpickle_fn(py: Python<'_>, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    let pickle = PyModule::import(py, "pickle")?;
    Ok(pickle
        .call_method1("loads", (PyBytes::new(py, bytes),))?
        .unbind())
}


#[derive(Clone)]
enum PyStreamTransform {
    Map(Vec<u8>),
    Filter(Vec<u8>),
    FlatMap(Vec<u8>),
    ReduceByKey(Vec<u8>),
    GroupByKey,
    Join(Arc<PyDStreamInner>),
    LeftOuterJoin(Arc<PyDStreamInner>),
    UpdateStateByKey(Vec<u8>),
    MapValues(Vec<u8>),
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
}


#[pyclass(name = "DStream")]
pub struct PyDStream {
    inner: Arc<PyDStreamInner>,
    pub is_pair: bool,
}

#[pymethods]
impl PyDStream {
    pub fn map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Map(pickle_fn(py, &func)?),
            }),
            is_pair: false,
        })
    }

    pub fn filter(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Filter(pickle_fn(py, &func)?),
            }),
            is_pair: self.is_pair,
        })
    }

    pub fn flat_map(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::FlatMap(pickle_fn(py, &func)?),
            }),
            is_pair: false,
        })
    }

    pub fn reduce_by_key(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "reduce_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::ReduceByKey(pickle_fn(py, &func)?),
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

    pub fn update_state_by_key(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "update_state_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::UpdateStateByKey(pickle_fn(py, &func)?),
            }),
            is_pair: true,
        })
    }

    pub fn map_values(&self, py: Python<'_>, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "map_values requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::MapValues(pickle_fn(py, &func)?),
            }),
            is_pair: true,
        })
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
    func_bytes: Vec<u8>,
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
        PyStreamTransform::Map(func_bytes) => {
            let f = unpickle_fn(py, func_bytes)?;
            elements
                .iter()
                .map(|e| f.call1(py, (e.bind(py),)))
                .collect()
        }

        PyStreamTransform::Filter(func_bytes) => {
            let f = unpickle_fn(py, func_bytes)?;
            elements
                .iter()
                .filter_map(
                    |e| match f.call1(py, (e.bind(py),)).and_then(|r| r.is_truthy(py)) {
                        Ok(true) => Some(Ok(e.clone_ref(py))),
                        Ok(false) => None,
                        Err(err) => Some(Err(err)),
                    },
                )
                .collect()
        }

        PyStreamTransform::FlatMap(func_bytes) => {
            let f = unpickle_fn(py, func_bytes)?;
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

        PyStreamTransform::ReduceByKey(func_bytes) => {
            let f = unpickle_fn(py, func_bytes)?;
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

        PyStreamTransform::UpdateStateByKey(func_bytes) => {
            let f = unpickle_fn(py, func_bytes)?;
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

        PyStreamTransform::MapValues(func_bytes) => {
            let f = unpickle_fn(py, func_bytes)?;
            elements
                .iter()
                .map(|e| {
                    let pair = e.bind(py).cast::<PyTuple>()?;
                    let k = pair.get_item(0)?.unbind();
                    let new_v: Py<PyAny> = f.call1(py, (pair.get_item(1)?,))?;
                    py_tuple2(py, &k, &new_v)
                })
                .collect()
        }
    }
}

fn call_output_fn(py: Python<'_>, func_bytes: &[u8], elements: Vec<Py<PyAny>>) -> PyResult<()> {
    let f = unpickle_fn(py, func_bytes)?;
    let list = PyList::new(py, elements.iter().map(|e| e.bind(py)))?;
    f.call1(py, (list.into_any(),))?;
    Ok(())
}


#[pyclass(name = "StreamingContext")]
pub struct PyStreamingContext {
    batch_secs: f64,
    output_ops: Arc<Mutex<Vec<OutputOp>>>,
    state_stores: Arc<Mutex<Vec<HashMap<String, Vec<u8>>>>>,
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
    thread_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
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
        }
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

    pub fn foreach_rdd(&self, py: Python<'_>, stream: &PyDStream, func: Py<PyAny>) -> PyResult<()> {
        let func_bytes = pickle_fn(py, &func)?;
        self.output_ops.lock().push(OutputOp {
            stream: Arc::clone(&stream.inner),
            func_bytes,
        });
        self.state_stores.lock().push(HashMap::new());
        Ok(())
    }

    /// Run exactly one batch tick synchronously (for deterministic tests).
    pub fn run_one_batch(&self, py: Python<'_>) -> PyResult<()> {
        let locked_ops = self.output_ops.lock();
        let mut locked_states = self.state_stores.lock();
        for (idx, op) in locked_ops.iter().enumerate() {
            let state_store = &mut locked_states[idx];
            let elements = compute_batch(py, &op.stream, state_store)?;
            call_output_fn(py, &op.func_bytes, elements)?;
        }
        Ok(())
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
                                let _ = call_output_fn(py, &op.func_bytes, elements);
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
