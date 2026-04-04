use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::rdd::PyRdd;

/// The Atomic execution context.
///
/// Entry point for creating RDDs. Each `Context` tracks a logical execution
/// environment — currently in-process (local thread) for Python tasks, with
/// Docker dispatch available via `DockerStub`.
///
/// # Example
/// ```python
/// import atomic
///
/// ctx = atomic.Context()
/// result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x + 1).collect()
/// # [2, 3, 4, 5]
/// ```
#[pyclass(name = "Context")]
pub struct PyContext {
    /// Number of default partitions when not specified by the caller.
    default_parallelism: usize,
}

#[pymethods]
impl PyContext {
    #[new]
    #[pyo3(signature = (default_parallelism=None))]
    pub fn new(default_parallelism: Option<usize>) -> Self {
        Self {
            default_parallelism: default_parallelism.unwrap_or_else(num_cpus),
        }
    }

    /// Distribute a Python list as an RDD, optionally across `num_partitions` partitions.
    ///
    /// Elements are stored in-memory. Partitions are logical — local execution
    /// processes all elements in the calling thread.
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
        Ok(PyRdd::from_data(py, elements, partitions))
    }

    /// Create an RDD from lines of a text file.
    ///
    /// Each line becomes one element (trailing newline stripped).
    ///
    /// # Example
    /// ```python
    /// lines = ctx.text_file("words.txt")
    /// words = lines.flat_map(lambda line: line.split())
    /// ```
    pub fn text_file(&self, py: Python, path: &str) -> PyResult<PyRdd> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let elements: Vec<Py<PyAny>> = content
            .lines()
            .map(|line| line.to_string().into_pyobject(py).unwrap().into_any().unbind())
            .collect();
        let partitions = self.default_parallelism.max(1);
        Ok(PyRdd::from_data(py, elements, partitions))
    }

    /// Create an RDD of integers in `[start, end)` with optional `step`.
    ///
    /// # Example
    /// ```python
    /// rdd = ctx.range(0, 100, step=2)
    /// ```
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
        Ok(PyRdd::from_data(py, elements, partitions))
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
