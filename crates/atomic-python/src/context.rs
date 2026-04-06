use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::rdd::PyRdd;

/// The Atomic execution context.
///
/// Entry point for creating RDDs. In **local mode** (default) transformations run
/// eagerly in the calling thread. In **distributed mode** (set `VEGA_DEPLOYMENT_MODE=distributed`)
/// the context connects to remote workers and dispatches pipeline ops over TCP.
///
/// # Local mode
/// ```python
/// import atomic
/// ctx = atomic.Context()
/// result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x + 1).collect()
/// # [2, 3, 4, 5]
/// ```
///
/// # Distributed mode
/// ```python
/// import os
/// os.environ["VEGA_DEPLOYMENT_MODE"] = "distributed"
/// os.environ["VEGA_LOCAL_IP"] = "127.0.0.1"
/// # Workers are listed in ~/hosts.conf or via VEGA_SLAVES env var.
/// import atomic
/// ctx = atomic.Context()
/// result = ctx.parallelize(range(100), num_partitions=4).map(lambda x: x * 2).collect()
/// ```
#[pyclass(name = "Context")]
pub struct PyContext {
    pub inner: Arc<atomic_compute::context::Context>,
    default_parallelism: usize,
}

#[pymethods]
impl PyContext {
    #[new]
    #[pyo3(signature = (default_parallelism=None))]
    pub fn new(default_parallelism: Option<usize>) -> PyResult<Self> {
        let inner = atomic_compute::context::Context::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let parallelism = default_parallelism.unwrap_or_else(num_cpus);
        Ok(Self { inner, default_parallelism: parallelism })
    }

    /// Distribute a Python list as an RDD across `num_partitions` partitions.
    ///
    /// # Example
    /// ```python
    /// rdd = ctx.parallelize([1, 2, 3, 4], num_partitions=2)
    /// ```
    #[pyo3(signature = (data, num_partitions=None))]
    pub fn parallelize(
        &self,
        py: Python,
        data: &Bound<'_, PyList>,
        num_partitions: Option<usize>,
    ) -> PyResult<PyRdd> {
        let elements: Vec<Py<PyAny>> = data.iter().map(|item| item.unbind()).collect();
        let partitions = num_partitions.unwrap_or(self.default_parallelism).max(1);
        Ok(PyRdd::from_data(py, elements, partitions, Arc::clone(&self.inner)))
    }

    /// Create an RDD from lines of a text file.
    pub fn text_file(&self, py: Python, path: &str) -> PyResult<PyRdd> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let elements: Vec<Py<PyAny>> = content
            .lines()
            .map(|line| line.to_string().into_pyobject(py).unwrap().into_any().unbind())
            .collect();
        let partitions = self.default_parallelism.max(1);
        Ok(PyRdd::from_data(py, elements, partitions, Arc::clone(&self.inner)))
    }

    /// Create an RDD of integers in `[start, end)` with optional `step`.
    #[pyo3(signature = (start, end, step=None, num_partitions=None))]
    pub fn range(
        &self,
        py: Python,
        start: i64,
        end: i64,
        step: Option<i64>,
        num_partitions: Option<usize>,
    ) -> PyResult<PyRdd> {
        let step = step.unwrap_or(1);
        if step == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err("step cannot be zero"));
        }
        let elements: Vec<Py<PyAny>> = (start..end)
            .step_by(step.unsigned_abs() as usize)
            .filter(|&x| if step > 0 { x < end } else { x > end })
            .map(|i| i.into_pyobject(py).unwrap().into_any().unbind())
            .collect();
        let partitions = num_partitions.unwrap_or(self.default_parallelism).max(1);
        Ok(PyRdd::from_data(py, elements, partitions, Arc::clone(&self.inner)))
    }

    pub fn default_parallelism(&self) -> usize {
        self.default_parallelism
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
}
