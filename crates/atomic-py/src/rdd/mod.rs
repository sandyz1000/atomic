use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_data::distributed::{PipelineOp, PythonUdfPayload, TaskAction, TaskRuntime};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

mod actions;
mod agent;
mod errors;
mod pair_ops;
mod sort;
mod transforms;

use errors::PyUdfStageError;

/// Verify that `f` survives the same pickle round-trip distributed-mode staging
/// performs. Distributed `Context` construction requires real reachable workers,
/// so this is the only way to exercise `pickle_fn`'s round-trip check from a test
/// without standing up a live cluster.
#[pyfunction(name = "_verify_picklable")]
pub(crate) fn verify_picklable(py: Python<'_>, f: Py<PyAny>) -> PyResult<()> {
    PyRdd::pickle_fn(py, &f)?;
    Ok(())
}

#[derive(Clone)]
struct StagedPyPipeline {
    source_partitions: Vec<Vec<u8>>,
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
    pub context: Arc<Context>,
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

    /// Pickle a callable using cloudpickle (if available) or stdlib pickle, then
    /// verify it round-trips through `loads` before returning.  A UDF that pickles
    /// but refuses to load (captures an open file, a C-extension handle, a thread
    /// lock, …) is caught here at staging time rather than failing on every worker.
    fn pickle_fn(py: Python, f: &Py<PyAny>) -> PyResult<Vec<u8>> {
        let pickle =
            PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
        let bytes: Vec<u8> = pickle.call_method1("dumps", (f.bind(py),))?.extract()?;
        pickle
            .call_method1("loads", (PyBytes::new(py, &bytes),))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    PyUdfStageError::Unpicklable(e.to_string()).to_string(),
                )
            })?;
        Ok(bytes)
    }

    /// Wrap an element-level function `f` into a partition-level function.
    ///
    /// The worker calls `partition_fn(partition: list) -> list`. Using default-argument
    /// capture (`_f=_f`) makes the lambda self-contained and picklable by cloudpickle.
    /// Python's `eval` inserts `__builtins__` automatically so list comprehensions work.
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

    fn encode_source_partitions(&self, py: Python) -> PyResult<Vec<Vec<u8>>> {
        let json_mod = PyModule::import(py, "json")?;
        let total = self.elements.len();
        let np = self.num_partitions.max(1);
        let chunk_size = (total + np - 1) / np;

        let mut partitions = Vec::with_capacity(np);
        for chunk in self.elements.chunks(chunk_size.max(1)) {
            let py_list = PyList::new(py, chunk.iter().map(|e| e.bind(py).clone()))?;
            let json_bytes: Bound<'_, PyAny> = json_mod.call_method1("dumps", (py_list,))?;
            let json_str: String = json_bytes.extract()?;
            partitions.push(json_str.into_bytes());
        }
        while partitions.len() < np {
            partitions.push(b"[]".to_vec());
        }
        Ok(partitions)
    }

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

    fn take_as_new(&mut self, _py: Python) -> PyRdd {
        PyRdd {
            elements: Vec::new(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: self.staged.take(),
        }
    }
}
