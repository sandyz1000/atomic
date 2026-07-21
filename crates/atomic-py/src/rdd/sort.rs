use std::sync::Arc;

use atomic_data::distributed::TaskAction;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::PyRdd;

#[pymethods]
impl PyRdd {
    /// Return the top `n` elements (largest first). Optional `key` function.
    #[pyo3(signature = (n, key=None))]
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
    #[pyo3(signature = (n, key=None))]
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

    /// Sort elements. Optional `key_fn` extracts a comparison key; `ascending` defaults to `true`.
    #[pyo3(signature = (key_fn=None, ascending=None))]
    pub fn sort_by(
        &self,
        py: Python,
        key_fn: Option<Py<PyAny>>,
        ascending: Option<bool>,
    ) -> PyResult<PyRdd> {
        let asc = ascending.unwrap_or(true);

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
    /// In distributed mode each worker sorts its partition; the driver k-way merges
    /// the N sorted streams in O(N log P). Not a globally-distributed sort (no range
    /// partitioner / shuffle), but removes the O(N log N) bottleneck on the driver.
    #[pyo3(signature = (ascending=None))]
    pub fn sort_by_key(&mut self, py: Python, ascending: Option<bool>) -> PyResult<PyRdd> {
        let asc = ascending.unwrap_or(true);

        if self.context.is_distributed() {
            // sort_by_key is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let builtins = PyModule::import(py, "builtins")?;
            // Partition-level sort lambda: sorts by key (index 0), direction controlled by asc.
            // Rust's bool Display ("true"/"false") is not valid Python — spell out the literal.
            let asc_literal = if asc { "True" } else { "False" };
            let sort_lambda = format!(
                "lambda partition, _asc={asc_literal}: \
                 sorted(partition, key=lambda x: x[0], reverse=not _asc)"
            );
            let sort_fn = builtins.getattr("eval")?.call1((sort_lambda.as_str(),))?;
            let fn_bytes = Self::pickle_fn(py, &sort_fn.unbind())?;
            self.stage_python_task(py, fn_bytes, TaskAction::Map)?;

            let staged = self.staged.as_ref().unwrap();
            let (source_partitions, ops) = (staged.source_partitions.clone(), staged.steps.clone());
            self.staged = saved_staged;
            let result_bytes = self
                .context
                .dispatch_pipeline(source_partitions, ops)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Decode N sorted partition lists
            let json_mod = PyModule::import(py, "json")?;
            let mut sorted_parts: Vec<Vec<Py<PyAny>>> = Vec::with_capacity(result_bytes.len());
            for bytes in &result_bytes {
                let json_str = std::str::from_utf8(bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let items: Vec<Py<PyAny>> = json_mod
                    .call_method1("loads", (json_str,))?
                    .try_iter()?
                    .map(|x| Ok(x?.unbind()))
                    .collect::<PyResult<_>>()?;
                sorted_parts.push(items);
            }

            // K-way merge using heapq.merge — O(N log P)
            let heapq = PyModule::import(py, "heapq")?;
            let key_fn = builtins.getattr("eval")?.call1(("lambda x: x[0]",))?;
            let kw = PyDict::new(py);
            kw.set_item("key", &key_fn)?;
            kw.set_item("reverse", (!asc).into_pyobject(py)?)?;
            let py_parts: Vec<Bound<'_, PyAny>> = sorted_parts
                .iter()
                .map(|p| Ok(PyList::new(py, p.iter().map(|e| e.bind(py).clone()))?.into_any()))
                .collect::<PyResult<_>>()?;
            let args = PyTuple::new(py, &py_parts)?;
            let merged = heapq.call_method("merge", args, Some(&kw))?;

            let elements: Vec<Py<PyAny>> = merged
                .try_iter()?
                .map(|x| Ok(x?.unbind()))
                .collect::<PyResult<_>>()?;
            return Ok(PyRdd::from_data(
                py,
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        // Local path: driver-side sort by key
        for item in &self.elements {
            let bound = item.bind(py);
            let (_, _) = PyRdd::extract_pair(bound)?;
        }
        let builtins = PyModule::import(py, "builtins")?;
        let key_fn = builtins
            .getattr("eval")?
            .call1(("lambda p: p[0]",))?
            .unbind();
        self.sort_by(py, Some(key_fn), ascending)
    }
}
