use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_data::distributed::{PipelineOp, PythonUdfPayload, TaskAction, TaskRuntime};
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
/// import atomic_compute
/// ctx = atomic_compute.Context()
/// result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x * 2).collect()
/// # [2, 4, 6, 8]
/// ```
#[pyclass(name = "Rdd")]
pub struct PyRdd {
    pub elements: Vec<Py<PyAny>>,
    pub num_partitions: usize,
    /// Always-present reference to the compute context for dispatch.
    pub context: Arc<Context>,
    /// `Some(...)` when distributed mode has started staging ops.
    staged: Option<StagedPyPipeline>,
}

impl PyRdd {
    pub fn from_data(
        _py: Python,
        elements: Vec<Py<PyAny>>,
        num_partitions: usize,
        context: Arc<Context>,
    ) -> Self {
        Self {
            elements,
            num_partitions,
            context,
            staged: None,
        }
    }

    /// Pickle a callable using cloudpickle (if available) or stdlib pickle.
    ///
    /// cloudpickle is required for lambdas and closures; the worker subprocess
    /// already prefers it.  Stdlib pickle only works for top-level named functions.
    fn pickle_fn(py: Python, f: &Py<PyAny>) -> PyResult<Vec<u8>> {
        let pickle =
            PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
        let bytes_obj: Bound<'_, PyAny> = pickle.call_method1("dumps", (f.bind(py),))?;
        Ok(bytes_obj.extract::<Vec<u8>>()?)
    }

    /// Wrap an element-level function `f` into a partition-level function using
    /// `eval(expr, scope)` where `scope = {"_f": f}` (and optionally `"_z": zero`).
    ///
    /// The worker calls `partition_fn(partition: list) -> list`, so every function
    /// we send must be partition-level.  Using default-argument capture (`_f=_f`)
    /// makes the lambda self-contained and picklable by cloudpickle.
    ///
    /// Python's `eval` automatically inserts `__builtins__` into `scope` when it
    /// is absent, so list comprehensions and built-ins work inside the lambda body.
    fn make_partition_wrapper<'py>(
        py: Python<'py>,
        expr: &str,
        f: &Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = pyo3::types::PyDict::new(py);
        scope.set_item("_f", f.bind(py))?;
        let builtins = PyModule::import(py, "builtins")?;
        builtins.getattr("eval")?.call1((expr, &scope))
    }

    /// Like `make_partition_wrapper` but also captures a `zero` value as `_z`.
    fn make_partition_wrapper_with_zero<'py>(
        py: Python<'py>,
        expr: &str,
        f: &Py<PyAny>,
        zero: &Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = pyo3::types::PyDict::new(py);
        scope.set_item("_f", f.bind(py))?;
        scope.set_item("_z", zero.bind(py))?;
        let builtins = PyModule::import(py, "builtins")?;
        builtins.getattr("eval")?.call1((expr, &scope))
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
    fn stage_python_udf(
        &mut self,
        py: Python,
        partition_fn_bytes: Vec<u8>,
        action: TaskAction,
    ) -> PyResult<()> {
        let payload_struct = PythonUdfPayload {
            fn_bytes: partition_fn_bytes,
            zero_bytes: vec![],
        };
        let payload = serde_json::to_vec(&payload_struct)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let op = PipelineOp {
            op_id: "atomic::udf::python".to_string(),
            action,
            runtime: TaskRuntime::Python,
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
    /// Apply `f` to each element, returning a new RDD.
    pub fn map(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [_f(x) for x in partition]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;
            return Ok(self.take_as_new(py));
        }
        // Local eager
        let elements = self
            .elements
            .iter()
            .map(|item| f.call1(py, (item,)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Keep only elements for which `f` returns truthy.
    pub fn filter(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [x for x in partition if _f(x)]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Filter)?;
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
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` to each element and flatten the results (f must return an iterable).
    pub fn flat_map(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [y for x in partition for y in _f(x)]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::FlatMap)?;
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
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` only to the value in each `(key, value)` pair.
    pub fn map_values(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            // Partition-level wrapper: preserves the key, applies f to the value only.
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [(p[0], _f(p[1])) for p in partition]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;
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
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` to each value in `(key, value)` pairs and flatten.
    pub fn flat_map_values(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [(p[0], v) for p in partition for v in _f(p[1])]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::FlatMap)?;
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
                let new_pair = PyTuple::new(py, [key.clone_ref(py), val?.unbind()])?
                    .unbind()
                    .into_any();
                elements.push(new_pair);
            }
        }
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Produce `(f(element), element)` pairs.
    pub fn key_by(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [(_f(x), x) for x in partition]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;
            return Ok(self.take_as_new(py));
        }
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let key = f.call1(py, (item,))?;
                Ok(PyTuple::new(py, [key, item.clone_ref(py)])?
                    .unbind()
                    .into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Group elements by `f(element)`.
    pub fn group_by(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        self.key_by(py, f)?.group_by_key(py)
    }

    /// Group `(key, value)` pairs by key.
    pub fn group_by_key(&self, py: Python) -> PyResult<PyRdd> {
        // Use Python equality (==) rather than repr strings to group keys correctly.
        // This is O(n²) but correct for all hashable Python objects.
        let mut groups: Vec<(Py<PyAny>, Vec<Py<PyAny>>)> = Vec::new();

        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "group_by_key requires elements to be 2-tuples",
                ));
            }
            let key_obj = pair.get_item(0)?.unbind();
            let val_obj = pair.get_item(1)?.unbind();

            let mut found = false;
            for (existing_key, vals) in &mut groups {
                if existing_key.bind(py).eq(key_obj.bind(py))? {
                    vals.push(val_obj.clone_ref(py));
                    found = true;
                    break;
                }
            }
            if !found {
                groups.push((key_obj, vec![val_obj]));
            }
        }

        let elements = groups
            .iter()
            .map(|(key, vals)| {
                let py_list = PyList::new(py, vals.iter().map(|v| v.bind(py)))?
                    .into_any()
                    .unbind();
                Ok(PyTuple::new(py, [key.bind(py), py_list.bind(py)])?
                    .unbind()
                    .into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Aggregate values with the same key using `f(acc, value) -> acc`.
    pub fn reduce_by_key(&self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        // Use Python equality (==) for key comparison, not repr strings.
        let mut groups: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new(); // (key, accumulated_val)

        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "reduce_by_key requires elements to be 2-tuples",
                ));
            }
            let key_obj = pair.get_item(0)?.unbind();
            let val_obj = pair.get_item(1)?.unbind();

            let mut found = false;
            for (existing_key, acc) in &mut groups {
                if existing_key.bind(py).eq(key_obj.bind(py))? {
                    let new_acc = f.call1(py, (acc.clone_ref(py), val_obj.clone_ref(py)))?;
                    *acc = new_acc;
                    found = true;
                    break;
                }
            }
            if !found {
                groups.push((key_obj, val_obj));
            }
        }

        let elements = groups
            .iter()
            .map(|(key, val)| {
                Ok(
                    PyTuple::new(py, [key.bind(py).clone(), val.bind(py).clone()])?
                        .unbind()
                        .into_any(),
                )
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
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
                Ok(PyTuple::new(py, [a.bind(py).clone(), b.bind(py).clone()])?
                    .unbind()
                    .into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Compute the Cartesian product of two RDDs.
    pub fn cartesian(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        let mut elements = Vec::new();
        for a in &self.elements {
            for b in &other.elements {
                elements.push(
                    PyTuple::new(py, [a.bind(py).clone(), b.bind(py).clone()])?
                        .unbind()
                        .into_any(),
                );
            }
        }
        let partitions = self.num_partitions * other.num_partitions.max(1);
        Ok(PyRdd::from_data(
            py,
            elements,
            partitions,
            Arc::clone(&self.context),
        ))
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

    /// Return all elements as a Python list.
    ///
    /// In distributed mode with a staged pipeline, dispatches to workers and
    /// returns the combined results.
    pub fn collect(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            if let Some(ref staged) = self.staged {
                let result_bytes = self
                    .context
                    .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                return Self::collect_distributed(py, result_bytes);
            }
        }
        // Local eager: return elements as a list
        Ok(
            PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))
                .unwrap()
                .into_any()
                .unbind(),
        )
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
            .unwrap_or_else(|| Err(pyo3::exceptions::PyStopIteration::new_err("RDD is empty")))
    }

    /// Return the first `n` elements as a Python list.
    pub fn take(&self, py: Python, n: usize) -> Py<PyAny> {
        PyList::new(py, self.elements.iter().take(n).map(|e| e.bind(py).clone()))
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
            // Partition-level reduce: each partition returns [] (empty) or [scalar].
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [] if not partition else [__import__('functools').reduce(_f, partition)]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            let payload_struct = PythonUdfPayload {
                fn_bytes,
                zero_bytes: vec![],
            };
            let payload = serde_json::to_vec(&payload_struct)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let op = PipelineOp {
                op_id: "atomic::udf::python".to_string(),
                action: TaskAction::Reduce,
                runtime: TaskRuntime::Python,
                payload,
            };
            self.push_op(py, op)?;

            let staged = self.staged.as_ref().unwrap();
            let result_bytes = self
                .context
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
            return acc.map(|a| Ok(a.unbind())).unwrap_or_else(|| {
                Err(pyo3::exceptions::PyValueError::new_err(
                    "reduce on empty RDD",
                ))
            });
        }
        // Local eager
        let mut iter = self.elements.iter();
        let first = iter
            .next()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("reduce on empty RDD"))?;
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
            // Partition-level fold: embed both _f and _z into the wrapper via default args.
            // Each partition returns [accumulated_scalar]; driver combines using f starting
            // from zero.  zero_bytes is kept for the driver-side combination step.
            let wrapper = Self::make_partition_wrapper_with_zero(
                py,
                "lambda partition, _f=_f, _z=_z: [__import__('functools').reduce(_f, partition, _z)]",
                &f,
                &zero,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            // zero_bytes is only used by the driver combiner below — store it separately.
            let zero_bytes = Self::pickle_fn(py, &zero)?;
            let payload_struct = PythonUdfPayload {
                fn_bytes,
                zero_bytes: vec![], // zero embedded in wrapper; zero_bytes unused by worker
            };
            let payload = serde_json::to_vec(&payload_struct)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let op = PipelineOp {
                op_id: "atomic::udf::python".to_string(),
                action: TaskAction::Fold,
                runtime: TaskRuntime::Python,
                payload,
            };
            self.push_op(py, op)?;

            let staged = self.staged.as_ref().unwrap();
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Each partition returned [scalar]. Combine scalars using f on driver from zero.
            let json_mod = PyModule::import(py, "json")?;
            let pickle =
                PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
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

    /// Remove duplicate elements.
    pub fn distinct(&self, py: Python) -> PyResult<PyRdd> {
        let mut seen = std::collections::HashSet::new();
        let elements = self
            .elements
            .iter()
            .filter(|e| {
                let key = e.bind(py).repr().map(|r| r.to_string()).unwrap_or_default();
                seen.insert(key)
            })
            .map(|e| e.clone_ref(py))
            .collect();
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Return elements in `self` that are not in `other`.
    pub fn subtract(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        let other_set: std::collections::HashSet<String> = other
            .elements
            .iter()
            .map(|e| e.bind(py).repr().map(|r| r.to_string()).unwrap_or_default())
            .collect();
        let elements = self
            .elements
            .iter()
            .filter(|e| {
                let key = e.bind(py).repr().map(|r| r.to_string()).unwrap_or_default();
                !other_set.contains(&key)
            })
            .map(|e| e.clone_ref(py))
            .collect();
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Return elements present in both `self` and `other` (no duplicates).
    pub fn intersection(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        let other_set: std::collections::HashSet<String> = other
            .elements
            .iter()
            .map(|e| e.bind(py).repr().map(|r| r.to_string()).unwrap_or_default())
            .collect();
        let mut seen = std::collections::HashSet::new();
        let elements = self
            .elements
            .iter()
            .filter(|e| {
                let key = e.bind(py).repr().map(|r| r.to_string()).unwrap_or_default();
                other_set.contains(&key) && seen.insert(key)
            })
            .map(|e| e.clone_ref(py))
            .collect();
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` to each logical partition (Python list of elements), returning a flattened RDD.
    pub fn map_partitions(&self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = (total + np - 1) / np;
        let mut elements = Vec::new();
        for chunk in self.elements.chunks(chunk_size.max(1)) {
            let py_list = PyList::new(py, chunk.iter().map(|e| e.bind(py).clone()))?;
            let result = f.call1(py, (py_list,))?;
            let iter = pyo3::types::PyIterator::from_object(result.bind(py))?;
            for val in iter {
                elements.push(val?.unbind());
            }
        }
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Extract the key from each `(key, value)` tuple.
    pub fn keys(&self, py: Python) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let pair = item.bind(py).cast::<PyTuple>()?;
                if pair.len() < 1 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "keys requires elements to be non-empty tuples",
                    ));
                }
                Ok(pair.get_item(0)?.unbind())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Extract the value from each `(key, value)` tuple.
    pub fn values(&self, py: Python) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let pair = item.bind(py).cast::<PyTuple>()?;
                if pair.len() < 2 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "values requires elements to be 2-tuples",
                    ));
                }
                Ok(pair.get_item(1)?.unbind())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Return all values associated with `key` in a pair RDD.
    pub fn lookup(&self, py: Python, key: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let vals = PyList::empty(py);
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                continue;
            }
            if pair.get_item(0)?.eq(key.bind(py))? {
                vals.append(pair.get_item(1)?)?;
            }
        }
        Ok(vals.into_any().unbind())
    }

    /// Apply `f` to each element for side effects.
    pub fn for_each(&self, py: Python, f: Py<PyAny>) -> PyResult<()> {
        for item in &self.elements {
            f.call1(py, (item.clone_ref(py),))?;
        }
        Ok(())
    }

    /// Apply `f` to each logical partition for side effects.
    pub fn for_each_partition(&self, py: Python, f: Py<PyAny>) -> PyResult<()> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = (total + np - 1) / np;
        for chunk in self.elements.chunks(chunk_size.max(1)) {
            let py_list = PyList::new(py, chunk.iter().map(|e| e.bind(py).clone()))?;
            f.call1(py, (py_list,))?;
        }
        Ok(())
    }

    /// Count occurrences of each distinct element. Returns a Python dict keyed by items.
    pub fn count_by_value(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = pyo3::types::PyDict::new(py);
        for item in &self.elements {
            let key = item.bind(py);
            let current: i64 = dict
                .get_item(key.clone())?
                .map(|v| v.extract::<i64>().unwrap_or(0))
                .unwrap_or(0);
            dict.set_item(key, current + 1)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Count elements per key in a pair RDD. Returns a Python dict keyed by key objects.
    pub fn count_by_key(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = pyo3::types::PyDict::new(py);
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() < 1 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "count_by_key requires elements to be non-empty tuples",
                ));
            }
            let key = pair.get_item(0)?;
            let current: i64 = dict
                .get_item(key.clone())?
                .map(|v| v.extract::<i64>().unwrap_or(0))
                .unwrap_or(0);
            dict.set_item(key, current + 1)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Return True if the RDD has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Return the maximum element. Optional `key` function `f(x) => comparable`.
    pub fn max(&self, py: Python, key: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        if self.elements.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("max on empty RDD"));
        }
        let mut result = self.elements[0].clone_ref(py);
        let mut result_key = match &key {
            Some(f) => Some(f.call1(py, (result.clone_ref(py),))?),
            None => None,
        };
        for item in &self.elements[1..] {
            let item_key = match &key {
                Some(f) => Some(f.call1(py, (item.clone_ref(py),))?),
                None => None,
            };
            let is_greater = match (&item_key, &result_key) {
                (Some(ik), Some(rk)) => ik.bind(py).gt(rk.bind(py))?,
                _ => item.bind(py).gt(result.bind(py))?,
            };
            if is_greater {
                result = item.clone_ref(py);
                result_key = item_key;
            }
        }
        Ok(result)
    }

    /// Return the minimum element. Optional `key` function `f(x) => comparable`.
    pub fn min(&self, py: Python, key: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        if self.elements.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("min on empty RDD"));
        }
        let mut result = self.elements[0].clone_ref(py);
        let mut result_key = match &key {
            Some(f) => Some(f.call1(py, (result.clone_ref(py),))?),
            None => None,
        };
        for item in &self.elements[1..] {
            let item_key = match &key {
                Some(f) => Some(f.call1(py, (item.clone_ref(py),))?),
                None => None,
            };
            let is_less = match (&item_key, &result_key) {
                (Some(ik), Some(rk)) => ik.bind(py).lt(rk.bind(py))?,
                _ => item.bind(py).lt(result.bind(py))?,
            };
            if is_less {
                result = item.clone_ref(py);
                result_key = item_key;
            }
        }
        Ok(result)
    }

    /// Return the top `n` elements (largest first). Optional `key` function.
    pub fn top(&self, py: Python, n: usize, key: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let mut indexed: Vec<(Py<PyAny>, Py<PyAny>)> = self
            .elements
            .iter()
            .map(|item| {
                let k = match &key {
                    Some(f) => f.call1(py, (item.clone_ref(py),))?,
                    None => item.clone_ref(py),
                };
                Ok((item.clone_ref(py), k))
            })
            .collect::<PyResult<_>>()?;
        // Sort descending by key
        let mut sort_error: Option<pyo3::PyErr> = None;
        indexed.sort_by(|(_, ka), (_, kb)| {
            if sort_error.is_some() {
                return std::cmp::Ordering::Equal;
            }
            match kb.bind(py).lt(ka.bind(py)) {
                Ok(true) => std::cmp::Ordering::Less,
                Ok(false) => match ka.bind(py).lt(kb.bind(py)) {
                    Ok(true) => std::cmp::Ordering::Greater,
                    Ok(false) => std::cmp::Ordering::Equal,
                    Err(e) => {
                        sort_error = Some(e);
                        std::cmp::Ordering::Equal
                    }
                },
                Err(e) => {
                    sort_error = Some(e);
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = sort_error {
            return Err(e);
        }
        let result: Vec<_> = indexed
            .into_iter()
            .take(n)
            .map(|(item, _)| item.bind(py).clone())
            .collect();
        Ok(PyList::new(py, result)?.into_any().unbind())
    }

    /// Return the `n` smallest elements. Optional `key` function.
    pub fn take_ordered(
        &self,
        py: Python,
        n: usize,
        key: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let mut indexed: Vec<(Py<PyAny>, Py<PyAny>)> = self
            .elements
            .iter()
            .map(|item| {
                let k = match &key {
                    Some(f) => f.call1(py, (item.clone_ref(py),))?,
                    None => item.clone_ref(py),
                };
                Ok((item.clone_ref(py), k))
            })
            .collect::<PyResult<_>>()?;
        // Sort ascending by key
        let mut sort_error: Option<pyo3::PyErr> = None;
        indexed.sort_by(|(_, ka), (_, kb)| {
            if sort_error.is_some() {
                return std::cmp::Ordering::Equal;
            }
            match ka.bind(py).lt(kb.bind(py)) {
                Ok(true) => std::cmp::Ordering::Less,
                Ok(false) => match kb.bind(py).lt(ka.bind(py)) {
                    Ok(true) => std::cmp::Ordering::Greater,
                    Ok(false) => std::cmp::Ordering::Equal,
                    Err(e) => {
                        sort_error = Some(e);
                        std::cmp::Ordering::Equal
                    }
                },
                Err(e) => {
                    sort_error = Some(e);
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = sort_error {
            return Err(e);
        }
        let result: Vec<_> = indexed
            .into_iter()
            .take(n)
            .map(|(item, _)| item.bind(py).clone())
            .collect();
        Ok(PyList::new(py, result)?.into_any().unbind())
    }

    /// Write each element as a line to `path`.
    ///
    /// Accepts a local file path or, when built with the `s3` feature, an
    /// S3 URI (`s3://bucket/prefix`).  S3 writes upload a single `part-0`
    /// object under the given prefix.
    pub fn save_as_text_file(&self, py: Python, path: String) -> PyResult<()> {
        #[cfg(feature = "s3")]
        if path.starts_with("s3://") {
            use atomic_compute::io::s3::s3_impl::{S3Uri, write_text};
            let s3uri = S3Uri::parse(&path).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "save_as_text_file: invalid S3 URI: {path}"
                ))
            })?;
            let content: String = self
                .elements
                .iter()
                .map(|item| {
                    let s = item
                        .bind(py)
                        .str()
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    format!("{s}\n")
                })
                .collect();
            let key = format!("{}/part-0", s3uri.key.trim_end_matches('/'));
            write_text(&s3uri.bucket, &key, content)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;
            return Ok(());
        }
        #[cfg(not(feature = "s3"))]
        if path.starts_with("s3://") {
            return Err(pyo3::exceptions::PyIOError::new_err(
                "save_as_text_file: s3:// URIs require the 's3' feature flag",
            ));
        }
        use std::io::Write;
        let mut file = std::fs::File::create(&path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        for item in &self.elements {
            let line = item.bind(py).str()?.to_string();
            writeln!(file, "{}", line)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Two-phase aggregation: `seq_fn(acc, elem)` within partitions,
    /// `comb_fn(acc, acc)` across partition results.
    pub fn aggregate(
        &self,
        py: Python,
        zero: Py<PyAny>,
        seq_fn: Py<PyAny>,
        comb_fn: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = (total + np - 1) / np;

        let mut partition_results = Vec::new();
        for chunk in self.elements.chunks(chunk_size.max(1)) {
            let mut acc = zero.clone_ref(py);
            for item in chunk {
                acc = seq_fn.call1(py, (acc, item.clone_ref(py)))?;
            }
            partition_results.push(acc);
        }

        let mut result = zero;
        for part_acc in partition_results {
            result = comb_fn.call1(py, (result, part_acc))?;
        }
        Ok(result)
    }

    /// Inner join two pair RDDs on their keys.
    ///
    /// Both RDDs must contain `(key, value)` 2-tuples.
    /// Emits `(key, (left_val, right_val))` for every matching key pair.
    #[pyo3(signature = (other))]
    pub fn join(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        // Collect (key, val) pairs from both sides
        let mut left_pairs: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "join requires elements to be 2-tuples",
                ));
            }
            left_pairs.push((pair.get_item(0)?.unbind(), pair.get_item(1)?.unbind()));
        }
        let mut right_pairs: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();
        for item in &other.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "join requires elements to be 2-tuples",
                ));
            }
            right_pairs.push((pair.get_item(0)?.unbind(), pair.get_item(1)?.unbind()));
        }

        let mut elements: Vec<Py<PyAny>> = Vec::new();
        for (lk, lv) in &left_pairs {
            for (rk, rv) in &right_pairs {
                if lk.bind(py).eq(rk.bind(py))? {
                    let inner = PyTuple::new(py, [lv.bind(py).clone(), rv.bind(py).clone()])?
                        .unbind()
                        .into_any();
                    let outer = PyTuple::new(py, [lk.bind(py).clone(), inner.bind(py).clone()])?
                        .unbind()
                        .into_any();
                    elements.push(outer);
                }
            }
        }
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Left outer join two pair RDDs on their keys.
    ///
    /// Unmatched left keys emit `(key, (left_val, None))`.
    #[pyo3(signature = (other))]
    pub fn left_outer_join(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        let mut left_pairs: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "left_outer_join requires elements to be 2-tuples",
                ));
            }
            left_pairs.push((pair.get_item(0)?.unbind(), pair.get_item(1)?.unbind()));
        }
        let mut right_pairs: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();
        for item in &other.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "left_outer_join requires elements to be 2-tuples",
                ));
            }
            right_pairs.push((pair.get_item(0)?.unbind(), pair.get_item(1)?.unbind()));
        }

        let mut elements: Vec<Py<PyAny>> = Vec::new();
        for (lk, lv) in &left_pairs {
            let mut matched = false;
            for (rk, rv) in &right_pairs {
                if lk.bind(py).eq(rk.bind(py))? {
                    let inner = PyTuple::new(py, [lv.bind(py).clone(), rv.bind(py).clone()])?
                        .unbind()
                        .into_any();
                    let outer = PyTuple::new(py, [lk.bind(py).clone(), inner.bind(py).clone()])?
                        .unbind()
                        .into_any();
                    elements.push(outer);
                    matched = true;
                }
            }
            if !matched {
                let none_val: Py<PyAny> = py.None();
                let inner = PyTuple::new(py, [lv.bind(py).clone(), none_val.bind(py).clone()])?
                    .unbind()
                    .into_any();
                let outer = PyTuple::new(py, [lk.bind(py).clone(), inner.bind(py).clone()])?
                    .unbind()
                    .into_any();
                elements.push(outer);
            }
        }
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Sort elements. Optional `key_fn` extracts a comparison key; `ascending` defaults to `true`.
    #[pyo3(signature = (key_fn=None, ascending=None))]
    pub fn sort_by(
        &self,
        py: Python,
        key_fn: Option<Py<PyAny>>,
        ascending: Option<bool>,
    ) -> PyResult<PyRdd> {
        let asc = ascending.unwrap_or(true);

        // Build (element, key) pairs
        let mut indexed: Vec<(Py<PyAny>, Py<PyAny>)> = self
            .elements
            .iter()
            .map(|item| {
                let k = match &key_fn {
                    Some(f) => f.call1(py, (item.clone_ref(py),))?,
                    None => item.clone_ref(py),
                };
                Ok((item.clone_ref(py), k))
            })
            .collect::<PyResult<_>>()?;

        let mut sort_error: Option<pyo3::PyErr> = None;
        indexed.sort_by(|(_, ka), (_, kb)| {
            if sort_error.is_some() {
                return std::cmp::Ordering::Equal;
            }
            // For ascending: ka < kb  ⟹  Less
            // For descending: kb < ka ⟹  Less
            let (a, b) = if asc {
                (ka.bind(py), kb.bind(py))
            } else {
                (kb.bind(py), ka.bind(py))
            };
            match a.lt(b.clone()) {
                Ok(true) => std::cmp::Ordering::Less,
                Ok(false) => match b.lt(a) {
                    Ok(true) => std::cmp::Ordering::Greater,
                    Ok(false) => std::cmp::Ordering::Equal,
                    Err(e) => {
                        sort_error = Some(e);
                        std::cmp::Ordering::Equal
                    }
                },
                Err(e) => {
                    sort_error = Some(e);
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = sort_error {
            return Err(e);
        }

        let elements = indexed
            .into_iter()
            .map(|(item, _)| item)
            .collect::<Vec<_>>();
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Sort a pair RDD by key. `ascending` defaults to `true`.
    ///
    /// Each element must be a `(key, value)` 2-tuple; sorting is performed on the key.
    #[pyo3(signature = (ascending=None))]
    pub fn sort_by_key(&self, py: Python, ascending: Option<bool>) -> PyResult<PyRdd> {
        // Validate all elements are 2-tuples first
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "sort_by_key requires elements to be 2-tuples",
                ));
            }
        }
        // Build a Python lambda `lambda p: p[0]` as the key extractor
        let builtins = PyModule::import(py, "builtins")?;
        let key_fn = builtins
            .getattr("eval")?
            .call1(("lambda p: p[0]",))?
            .unbind();
        self.sort_by(py, Some(key_fn), ascending)
    }

    /// Return a Python list of sublists — one sublist per logical partition.
    ///
    /// `[[partition_0_elements], [partition_1_elements], ...]`
    pub fn glom(&self, py: Python) -> PyResult<Py<PyAny>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();

        let outer = PyList::empty(py);

        if total == 0 {
            // All partitions are empty
            for _ in 0..np {
                outer.append(PyList::empty(py))?;
            }
            return Ok(outer.into_any().unbind());
        }

        let chunk_size = (total + np - 1) / np; // ceiling division
        let mut emitted = 0usize;
        for chunk in self.elements.chunks(chunk_size.max(1)) {
            let inner = PyList::new(py, chunk.iter().map(|e| e.bind(py).clone()))?;
            outer.append(inner)?;
            emitted += 1;
        }
        // Pad with empty sublists for any remaining logical partitions
        while emitted < np {
            outer.append(PyList::empty(py))?;
            emitted += 1;
        }
        Ok(outer.into_any().unbind())
    }

    /// No-op in local mode: return a clone of this RDD (same elements, same partitions).
    pub fn cache(&self, py: Python) -> PyRdd {
        PyRdd {
            elements: self.elements.iter().map(|e| e.clone_ref(py)).collect(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: None,
        }
    }

    /// No-op in local mode: return a clone of this RDD (same elements, same partitions).
    pub fn persist(&self, py: Python) -> PyRdd {
        self.cache(py)
    }

    /// No-op in local mode: return a clone of this RDD (same elements, same partitions).
    pub fn unpersist(&self, py: Python) -> PyRdd {
        self.cache(py)
    }

    /// Materialize elements to `{path}/checkpoint.pkl` using Python pickle, then return
    /// a new RDD with the same elements (lineage is conceptually truncated).
    ///
    /// Write is atomic: data goes to `{path}/checkpoint.pkl.tmp` first, then renamed.
    #[pyo3(signature = (path))]
    pub fn checkpoint(&self, py: Python, path: &str) -> PyResult<PyRdd> {
        use std::io::Write;

        // Create directory
        std::fs::create_dir_all(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        // Serialize elements list with pickle
        let pickle =
            PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
        let py_list = PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))?;
        let bytes_obj = pickle.call_method1("dumps", (py_list,))?;
        let bytes: Vec<u8> = bytes_obj.extract()?;

        // Atomic write: tmp → rename
        let tmp_path = format!("{}/checkpoint.pkl.tmp", path);
        let final_path = format!("{}/checkpoint.pkl", path);
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        f.write_all(&bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        drop(f);
        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        // Return new RDD with same elements (lineage truncated conceptually)
        Ok(PyRdd {
            elements: self.elements.iter().map(|e| e.clone_ref(py)).collect(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: None,
        })
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn __len__(&self) -> usize {
        self.elements.len()
    }

    /// Iterate over elements: `for x in rdd`.
    pub fn __iter__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))?;
        let iter = PyIterator::from_object(list.as_any())?;
        Ok(iter.into_any().unbind())
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
