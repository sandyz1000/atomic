use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::rdd::PyRdd;

/// The Atomic execution context.
///
/// Entry point for creating RDDs. In **local mode** (default) transformations run
/// eagerly in the calling thread. In **distributed mode** (set `ATOMIC_DEPLOYMENT_MODE=distributed`)
/// the context connects to remote workers and dispatches pipeline ops over TCP.
///
/// # Local mode
/// ```python
/// import atomic_compute
/// ctx = atomic_compute.Context()
/// result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x + 1).collect()
/// # [2, 3, 4, 5]
/// ```
///
/// # Distributed mode
/// ```python
/// import os
/// os.environ["ATOMIC_DEPLOYMENT_MODE"] = "distributed"
/// os.environ["ATOMIC_LOCAL_IP"] = "127.0.0.1"
/// # Workers are listed in ~/hosts.conf or via ATOMIC_SLAVES env var.
/// import atomic_compute
/// ctx = atomic_compute.Context()
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
        Ok(Self {
            inner,
            default_parallelism: parallelism,
        })
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
        Ok(PyRdd::from_data(
            py,
            elements,
            partitions,
            Arc::clone(&self.inner),
        ))
    }

    /// Create an RDD from lines of a text file or S3 object.
    ///
    /// Accepts local paths (`/path/to/file`, `file:///path`) and, when built
    /// with the `s3` feature, S3 URIs (`s3://bucket/key`).  A directory path
    /// or S3 prefix produces one partition per file/object.
    pub fn text_file(&self, py: Python, path: &str) -> PyResult<PyRdd> {
        let lines: Vec<String> = self
            .inner
            .text_file(path)
            .collect()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let elements: Vec<Py<PyAny>> = lines
            .into_iter()
            .map(|line| line.into_pyobject(py).unwrap().into_any().unbind())
            .collect();
        let partitions = self.default_parallelism.max(1);
        Ok(PyRdd::from_data(
            py,
            elements,
            partitions,
            Arc::clone(&self.inner),
        ))
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
            return Err(pyo3::exceptions::PyValueError::new_err(
                "step cannot be zero",
            ));
        }
        // `(start..end)` is empty when start >= end, so descending ranges need explicit generation.
        let abs_step = step.unsigned_abs() as usize;
        let mut cur = start;
        let mut raw: Vec<i64> = Vec::new();
        if step > 0 {
            while cur < end {
                raw.push(cur);
                cur += step;
            }
        } else {
            while cur > end {
                raw.push(cur);
                cur -= abs_step as i64;
            }
        }
        let elements: Vec<Py<PyAny>> = raw
            .into_iter()
            .map(|i| i.into_pyobject(py).unwrap().into_any().unbind())
            .collect();
        let partitions = num_partitions.unwrap_or(self.default_parallelism).max(1);
        Ok(PyRdd::from_data(
            py,
            elements,
            partitions,
            Arc::clone(&self.inner),
        ))
    }

    pub fn default_parallelism(&self) -> usize {
        self.default_parallelism
    }

    /// Broadcast a read-only value to all tasks.
    ///
    /// The value is serialized with cloudpickle (falling back to stdlib
    /// `pickle`) on the driver and deserialized on demand when `value()` is
    /// called on the `BroadcastVar`.
    ///
    /// # Example
    /// ```python
    /// bv = ctx.broadcast({"threshold": 10})
    /// rdd.filter(lambda x: x > bv.value()["threshold"]).collect()
    /// ```
    pub fn broadcast(
        &self,
        py: Python<'_>,
        value: Py<PyAny>,
    ) -> PyResult<crate::distributed_vars::PyBroadcastVar> {
        // Try cloudpickle first (supports lambdas); fall back to stdlib pickle.
        let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
        let bytes: Vec<u8> = pickle.call_method1("dumps", (value,))?.extract()?;
        Ok(crate::distributed_vars::PyBroadcastVar::new(bytes))
    }

    /// Create a mutable accumulator with an initial (zero) value.
    ///
    /// Supports `int`, `float`, `str`, `bool`, and `list` as the initial
    /// value.  `add()` increments the accumulator; `reset()` restores the
    /// initial value.
    ///
    /// # Example
    /// ```python
    /// acc = ctx.accumulator(0)
    /// ctx.parallelize([1, 2, 3]).for_each(lambda x: acc.add(x))
    /// assert acc.value() == 6
    /// ```
    #[pyo3(signature = (zero, merge_fn=None))]
    pub fn accumulator(
        &self,
        py: Python<'_>,
        zero: Py<PyAny>,
        merge_fn: Option<Py<PyAny>>,
    ) -> PyResult<crate::distributed_vars::PyAccumulator> {
        let initial = crate::distributed_vars::pythonobj_to_json(py, &zero)?;
        Ok(crate::distributed_vars::PyAccumulator::new(
            initial, merge_fn,
        ))
    }

    /// Stop the context and release resources.
    ///
    /// In distributed mode, sends a graceful-shutdown signal to every worker.
    /// Safe to call multiple times.
    pub fn stop(&self) {
        self.inner.stop();
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
}
