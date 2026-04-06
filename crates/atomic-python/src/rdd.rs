use std::collections::HashMap;
use std::sync::Arc;

use atomic_data::distributed::{PipelineOp, PythonUdfPayload, TaskAction};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyIterator, PyList, PyTuple};

/// Accumulated lazy ops for distributed execution.
struct StagedPyPipeline {
    /// JSON-encoded partition data (`Vec<serde_json::Value>` per partition).
    source_partitions: Vec<Vec<u8>>,
    /// Ordered ops to dispatch to workers.
    ops: Vec<PipelineOp>,
}

/// An in-memory distributed dataset (RDD) of Python objects.
///
/// In **local mode** all transformations execute eagerly in the calling thread.
/// In **distributed mode** `map`/`filter`/`flat_map`/`fold` build a lazy pipeline
/// that is dispatched to workers as a single `TaskEnvelope` per partition when an
/// action (`collect`, `count`, `fold`, `reduce`) is called.
///
/// # Example (local, no env vars needed)
/// ```python
/// import atomic
/// ctx = atomic.Context()
/// result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x * 2).collect()
/// # [2, 4, 6, 8]
/// ```
#[pyclass(name = "Rdd")]
pub struct PyRdd {
    pub elements: Vec<Py<PyAny>>,
    pub num_partitions: usize,
    /// Always-present reference to the compute context for dispatch.
    pub context: Arc<atomic_compute::context::Context>,
    /// `Some(...)` when distributed mode has started staging ops.
    staged: Option<StagedPyPipeline>,
}

impl PyRdd {
    pub fn from_data(
        _py: Python,
        elements: Vec<Py<PyAny>>,
        num_partitions: usize,
        context: Arc<atomic_compute::context::Context>,
    ) -> Self {
        Self { elements, num_partitions, context, staged: None }
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// Pickle a Python callable and return the bytes.
    fn pickle_fn(py: Python, f: &Py<PyAny>) -> PyResult<Vec<u8>> {
        let pickle = PyModule::import(py, "pickle")?;
        let bytes_obj: Bound<'_, PyAny> = pickle.call_method1("dumps", (f.bind(py),))?;
        Ok(bytes_obj.extract::<Vec<u8>>()?)
    }

    /// JSON-encode the elements split into `num_partitions` chunks.
    fn encode_source_partitions(&self, py: Python) -> PyResult<Vec<Vec<u8>>> {
        let json_mod = PyModule::import(py, "json")?;
        let total = self.elements.len();
        let np = self.num_partitions.max(1);
        let chunk_size = (total + np - 1) / np; // ceiling division

        let mut partitions = Vec::with_capacity(np);
        for chunk in self.elements.chunks(chunk_size.max(1)) {
            let py_list = PyList::new(py, chunk.iter().map(|e| e.bind(py).clone()))?;
            let json_bytes: Bound<'_, PyAny> = json_mod.call_method1("dumps", (py_list,))?;
            let json_str: String = json_bytes.extract()?;
            partitions.push(json_str.into_bytes());
        }
        // Pad with empty partitions if needed
        while partitions.len() < np {
            partitions.push(b"[]".to_vec());
        }
        Ok(partitions)
    }

    /// Stage a new `PipelineOp`, encoding source partitions on first op.
    fn push_op(&mut self, py: Python, op: PipelineOp) -> PyResult<()> {
        if let Some(ref mut staged) = self.staged {
            staged.ops.push(op);
        } else {
            let source_partitions = self.encode_source_partitions(py)?;
            self.staged = Some(StagedPyPipeline {
                source_partitions,
                ops: vec![op],
            });
        }
        Ok(())
    }

    /// Build a Python UDF op and push it into the staged pipeline.
    fn stage_python_udf(&mut self, py: Python, f: &Py<PyAny>, action: &str) -> PyResult<()> {
        let fn_bytes = Self::pickle_fn(py, f)?;
        let payload_struct = PythonUdfPayload {
            action: action.to_string(),
            fn_bytes,
            zero_bytes: vec![],
        };
        let payload = serde_json::to_vec(&payload_struct)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let op = PipelineOp {
            op_id: "atomic::udf::python".to_string(),
            action: TaskAction::PythonUdf,
            payload,
        };
        self.push_op(py, op)
    }

    /// Collect results from distributed dispatch: JSON bytes → Python list.
    fn collect_distributed(py: Python, result_bytes: Vec<Vec<u8>>) -> PyResult<Py<PyAny>> {
        let json_mod = PyModule::import(py, "json")?;
        let all = PyList::empty(py);
        for bytes in result_bytes {
            let json_str = std::str::from_utf8(&bytes)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let items: Bound<'_, PyAny> = json_mod.call_method1("loads", (json_str,))?;
            for item in items.try_iter()? {
                all.append(item?)?;
            }
        }
        Ok(all.into_any().unbind())
    }
}

#[pymethods]
impl PyRdd {
    // ── Transformations ──────────────────────────────────────────────────────

    /// Apply `f` to each element, returning a new RDD.
    pub fn map(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            self.stage_python_udf(py, &f, "map")?;
            // Return self with updated staged pipeline (move semantics emulated via clone of non-data fields)
            return Ok(self.take_as_new(py));
        }
        // Local eager
        let elements = self
            .elements
            .iter()
            .map(|item| f.call1(py, (item,)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Keep only elements for which `f` returns truthy.
    pub fn filter(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            self.stage_python_udf(py, &f, "filter")?;
            return Ok(self.take_as_new(py));
        }
        // Local eager
        let elements = self
            .elements
            .iter()
            .filter_map(|item| {
                let keep = f.call1(py, (item,)).and_then(|v| v.is_truthy(py));
                match keep {
                    Ok(true) => Some(Ok(item.clone_ref(py))),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each element and flatten the results (f must return an iterable).
    pub fn flat_map(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            self.stage_python_udf(py, &f, "flat_map")?;
            return Ok(self.take_as_new(py));
        }
        // Local eager
        let mut elements = Vec::new();
        for item in &self.elements {
            let result = f.call1(py, (item,))?;
            let iter = PyIterator::from_object(result.bind(py))?;
            for val in iter {
                elements.push(val?.unbind());
            }
        }
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` only to the value in each `(key, value)` pair.
    pub fn map_values(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            self.stage_python_udf(py, &f, "map_values")?;
            return Ok(self.take_as_new(py));
        }
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let pair = item.bind(py).cast::<PyTuple>()?;
                if pair.len() != 2 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "map_values requires elements to be 2-tuples",
                    ));
                }
                let key = pair.get_item(0)?.unbind();
                let new_val = f.call1(py, (pair.get_item(1)?,))?;
                Ok(PyTuple::new(py, [key, new_val])?.unbind().into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each value in `(key, value)` pairs and flatten.
    pub fn flat_map_values(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            self.stage_python_udf(py, &f, "flat_map_values")?;
            return Ok(self.take_as_new(py));
        }
        let mut elements = Vec::new();
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "flat_map_values requires elements to be 2-tuples",
                ));
            }
            let key = pair.get_item(0)?.unbind();
            let iter_result = f.call1(py, (pair.get_item(1)?,))?;
            let iter_obj = PyIterator::from_object(iter_result.bind(py))?;
            for val in iter_obj {
                let new_pair = PyTuple::new(py, [key.clone_ref(py), val?.unbind()])?.unbind().into_any();
                elements.push(new_pair);
            }
        }
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Produce `(f(element), element)` pairs.
    pub fn key_by(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            self.stage_python_udf(py, &f, "key_by")?;
            return Ok(self.take_as_new(py));
        }
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let key = f.call1(py, (item,))?;
                Ok(PyTuple::new(py, [key, item.clone_ref(py)])?.unbind().into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Group elements by `f(element)`.
    pub fn group_by(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        self.key_by(py, f)?.group_by_key(py)
    }

    /// Group `(key, value)` pairs by key.
    pub fn group_by_key(&self, py: Python) -> PyResult<PyRdd> {
        let mut groups: HashMap<String, (Py<PyAny>, Vec<Py<PyAny>>)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "group_by_key requires elements to be 2-tuples",
                ));
            }
            let key_obj = pair.get_item(0)?.unbind();
            let val_obj = pair.get_item(1)?.unbind();
            let key_str = key_obj.bind(py).repr()?.to_string();
            let entry = groups.entry(key_str.clone()).or_insert_with(|| {
                order.push(key_str);
                (key_obj.clone_ref(py), Vec::new())
            });
            entry.1.push(val_obj);
        }

        let elements = order
            .iter()
            .map(|k| {
                let (key, vals) = &groups[k];
                let py_list = PyList::new(py, vals.iter().map(|v| v.bind(py)))?.into_any().unbind();
                Ok(PyTuple::new(py, [key.bind(py), py_list.bind(py)])?.unbind().into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Aggregate values with the same key using `f(acc, value) -> acc`.
    pub fn reduce_by_key(&self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        let mut accum: HashMap<String, (Py<PyAny>, Py<PyAny>)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "reduce_by_key requires elements to be 2-tuples",
                ));
            }
            let key_obj = pair.get_item(0)?.unbind();
            let val_obj = pair.get_item(1)?.unbind();
            let key_str = key_obj.bind(py).repr()?.to_string();

            match accum.get_mut(&key_str) {
                Some((_, acc)) => {
                    let new_acc = f.call1(py, (acc.clone_ref(py), val_obj))?;
                    *acc = new_acc;
                }
                None => {
                    order.push(key_str.clone());
                    accum.insert(key_str, (key_obj, val_obj));
                }
            }
        }

        let elements = order
            .iter()
            .map(|k| {
                let (key, val) = &accum[k];
                Ok(PyTuple::new(py, [key.bind(py).clone(), val.bind(py).clone()])?.unbind().into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Merge two RDDs with the same element type into one.
    pub fn union(&self, py: Python, other: &PyRdd) -> PyRdd {
        let mut elements: Vec<Py<PyAny>> = self.elements.iter().map(|e| e.clone_ref(py)).collect();
        elements.extend(other.elements.iter().map(|e| e.clone_ref(py)));
        let partitions = self.num_partitions + other.num_partitions;
        PyRdd::from_data(py, elements, partitions, Arc::clone(&self.context))
    }

    /// Zip two RDDs element-wise into an RDD of `(a, b)` tuples.
    pub fn zip(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        if self.elements.len() != other.elements.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "zip requires equal-length RDDs: {} vs {}",
                self.elements.len(),
                other.elements.len()
            )));
        }
        let elements = self
            .elements
            .iter()
            .zip(other.elements.iter())
            .map(|(a, b)| {
                Ok(PyTuple::new(py, [a.bind(py).clone(), b.bind(py).clone()])?.unbind().into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(py, elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Compute the Cartesian product of two RDDs.
    pub fn cartesian(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        let mut elements = Vec::new();
        for a in &self.elements {
            for b in &other.elements {
                elements.push(
                    PyTuple::new(py, [a.bind(py).clone(), b.bind(py).clone()])?.unbind().into_any(),
                );
            }
        }
        let partitions = self.num_partitions * other.num_partitions.max(1);
        Ok(PyRdd::from_data(py, elements, partitions, Arc::clone(&self.context)))
    }

    /// Coalesce into `num_partitions` logical partitions.
    pub fn coalesce(&self, py: Python, num_partitions: usize) -> PyRdd {
        PyRdd {
            elements: self.elements.iter().map(|e| e.clone_ref(py)).collect(),
            num_partitions: num_partitions.max(1),
            context: Arc::clone(&self.context),
            staged: None,
        }
    }

    /// Alias for `coalesce`.
    pub fn repartition(&self, py: Python, num_partitions: usize) -> PyRdd {
        self.coalesce(py, num_partitions)
    }

    // ── Actions ──────────────────────────────────────────────────────────────

    /// Return all elements as a Python list.
    ///
    /// In distributed mode with a staged pipeline, dispatches to workers and
    /// returns the combined results.
    pub fn collect(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            if let Some(ref staged) = self.staged {
                let result_bytes = self.context
                    .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                return Self::collect_distributed(py, result_bytes);
            }
        }
        // Local eager: return elements as a list
        Ok(PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))
            .unwrap()
            .into_any()
            .unbind())
    }

    /// Return the number of elements.
    pub fn count(&mut self, py: Python) -> PyResult<usize> {
        if self.context.is_distributed() {
            if self.staged.is_some() {
                let list = self.collect(py)?;
                return Ok(list.bind(py).len()?);
            }
        }
        Ok(self.elements.len())
    }

    /// Return the first element.
    pub fn first(&self, py: Python) -> PyResult<Py<PyAny>> {
        self.elements
            .first()
            .map(|e| Ok(e.clone_ref(py)))
            .unwrap_or_else(|| {
                Err(pyo3::exceptions::PyStopIteration::new_err("RDD is empty"))
            })
    }

    /// Return the first `n` elements as a Python list.
    pub fn take(&self, py: Python, n: usize) -> Py<PyAny> {
        PyList::new(
            py,
            self.elements.iter().take(n).map(|e| e.bind(py).clone()),
        )
        .unwrap()
        .into_any()
        .unbind()
    }

    /// Aggregate all elements using `f(acc, element) -> acc`.
    ///
    /// In distributed mode this dispatches the reduce to workers (each returns a
    /// per-partition scalar), then combines those scalars on the driver.
    pub fn reduce(&mut self, py: Python, f: Py<PyAny>) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            let fn_bytes = Self::pickle_fn(py, &f)?;
            let payload_struct = PythonUdfPayload {
                action: "reduce".to_string(),
                fn_bytes,
                zero_bytes: vec![],
            };
            let payload = serde_json::to_vec(&payload_struct)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let op = PipelineOp {
                op_id: "atomic::udf::python".to_string(),
                action: TaskAction::PythonUdf,
                payload,
            };
            self.push_op(py, op)?;

            let staged = self.staged.as_ref().unwrap();
            let result_bytes = self.context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Each non-empty partition returned [scalar]. Combine on driver.
            let json_mod = PyModule::import(py, "json")?;
            let mut acc: Option<Bound<'_, PyAny>> = None;
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let items: Bound<'_, PyAny> = json_mod.call_method1("loads", (json_str,))?;
                // items is [] (empty partition) or [scalar]
                let length: usize = items.len()?;
                if length == 0 {
                    continue;
                }
                let scalar = items.get_item(0)?;
                acc = Some(match acc {
                    None => scalar,
                    Some(a) => f.call1(py, (a, scalar))?.bind(py).clone(),
                });
            }
            return acc
                .map(|a| Ok(a.unbind()))
                .unwrap_or_else(|| Err(pyo3::exceptions::PyValueError::new_err("reduce on empty RDD")));
        }
        // Local eager
        let mut iter = self.elements.iter();
        let first = iter.next().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("reduce on empty RDD")
        })?;
        let mut acc = first.clone_ref(py);
        for item in iter {
            acc = f.call1(py, (acc, item.clone_ref(py)))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial `zero` value using `f(acc, element) -> acc`.
    ///
    /// In distributed mode this dispatches the fold to workers.
    pub fn fold(&mut self, py: Python, zero: Py<PyAny>, f: Py<PyAny>) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            // Pickle zero and fn, push a fold op, dispatch, combine results on driver.
            let fn_bytes = Self::pickle_fn(py, &f)?;
            let zero_bytes = {
                let pickle = PyModule::import(py, "pickle")?;
                let b: Bound<'_, PyAny> = pickle.call_method1("dumps", (zero.bind(py),))?;
                b.extract::<Vec<u8>>()?
            };
            let payload_struct = PythonUdfPayload {
                action: "fold".to_string(),
                fn_bytes,
                zero_bytes: zero_bytes.clone(),
            };
            let payload = serde_json::to_vec(&payload_struct)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let op = PipelineOp {
                op_id: "atomic::udf::python".to_string(),
                action: TaskAction::PythonUdf,
                payload,
            };
            self.push_op(py, op)?;

            let staged = self.staged.as_ref().unwrap();
            let result_bytes = self.context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Each partition returned [scalar]. Combine scalars using f on driver.
            let json_mod = PyModule::import(py, "json")?;
            let pickle = PyModule::import(py, "pickle")?;
            let zero_again = pickle.call_method1("loads", (PyBytes::new(py, &zero_bytes),))?;
            let mut acc: Bound<'_, PyAny> = zero_again;
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let items: Bound<'_, PyAny> = json_mod.call_method1("loads", (json_str,))?;
                // items is [scalar] — take the first
                let scalar = items.get_item(0)?;
                acc = f.call1(py, (acc, scalar))?.bind(py).clone();
            }
            return Ok(acc.unbind());
        }
        // Local eager
        let mut acc = zero;
        for item in &self.elements {
            acc = f.call1(py, (acc, item.clone_ref(py)))?;
        }
        Ok(acc)
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn __len__(&self) -> usize {
        self.elements.len()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Rdd(count={}, num_partitions={}, distributed={})",
            self.elements.len(),
            self.num_partitions,
            self.context.is_distributed(),
        )
    }
}

impl PyRdd {
    /// Transfer the staged pipeline state to a new PyRdd (after staging an op).
    /// The new RDD has an empty `elements` (data is now in source_partitions).
    fn take_as_new(&mut self, _py: Python) -> PyRdd {
        PyRdd {
            elements: Vec::new(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: self.staged.take(),
        }
    }
}
