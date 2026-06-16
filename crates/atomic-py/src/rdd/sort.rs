use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use super::PyRdd;

#[pymethods]
impl PyRdd {
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
    /// Each element must be a `(key, value)` 2-tuple; sorting is performed on the key.
    #[pyo3(signature = (ascending=None))]
    pub fn sort_by_key(&self, py: Python, ascending: Option<bool>) -> PyResult<PyRdd> {
        for item in &self.elements {
            let pair = item.bind(py).cast::<pyo3::types::PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "sort_by_key requires elements to be 2-tuples",
                ));
            }
        }
        let builtins = PyModule::import(py, "builtins")?;
        let key_fn = builtins
            .getattr("eval")?
            .call1(("lambda p: p[0]",))?
            .unbind();
        self.sort_by(py, Some(key_fn), ascending)
    }
}
