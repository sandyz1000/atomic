use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pyclass(name = "BatchQueue")]
pub struct PyBatchQueue {
    pub(crate) queue: Arc<Mutex<VecDeque<Vec<Py<PyAny>>>>>,
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
