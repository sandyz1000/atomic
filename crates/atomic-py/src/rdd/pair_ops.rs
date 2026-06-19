use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::PyRdd;

#[pymethods]
impl PyRdd {
    /// Group `(key, value)` pairs by key.
    ///
    /// O(N) via a `PyDict` accumulator — previously O(N × K) linear scan.
    pub fn group_by_key(&self, py: Python) -> PyResult<PyRdd> {
        // key → PyList of values
        let dict = PyDict::new(py);

        for item in &self.elements {
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "group_by_key requires elements to be 2-tuples",
                ));
            }
            let key = pair.get_item(0)?;
            let val = pair.get_item(1)?;
            match dict.get_item(&key)? {
                Some(lst) => lst.downcast::<PyList>()?.append(val)?,
                None => {
                    let lst = PyList::new(py, [val])?;
                    dict.set_item(&key, lst)?;
                }
            }
        }

        let elements = dict
            .iter()
            .map(|(key, vals)| {
                Ok(PyTuple::new(py, [&key, &vals])?.unbind().into_any())
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
    ///
    /// O(N) via a `PyDict` accumulator — previously O(N × K) linear scan.
    pub fn reduce_by_key(&self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        let dict = PyDict::new(py);

        for item in &self.elements {
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "reduce_by_key requires elements to be 2-tuples",
                ));
            }
            let key = pair.get_item(0)?;
            let val = pair.get_item(1)?;
            match dict.get_item(&key)? {
                Some(acc) => {
                    let new_acc = f.call1(py, (acc, val))?;
                    dict.set_item(&key, new_acc)?;
                }
                None => {
                    dict.set_item(&key, val)?;
                }
            }
        }

        let elements = dict
            .iter()
            .map(|(key, acc)| {
                Ok(PyTuple::new(py, [&key, &acc])?.unbind().into_any())
            })
            .collect::<PyResult<Vec<_>>>()?;

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
                let pair = item.bind(py).downcast::<PyTuple>()?;
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
                let pair = item.bind(py).downcast::<PyTuple>()?;
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
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                continue;
            }
            if pair.get_item(0)?.eq(key.bind(py))? {
                vals.append(pair.get_item(1)?)?;
            }
        }
        Ok(vals.into_any().unbind())
    }

    /// Inner join two pair RDDs on their keys.
    ///
    /// Builds a hash map from `other` (right side), then probes with `self` (left side).
    /// O(N + M) — previously O(N × M) nested loop.
    #[pyo3(signature = (other))]
    pub fn join(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        // Build right-side map: key → [val, val, ...]
        let right_map: Bound<'_, PyDict> = PyDict::new(py);
        for item in &other.elements {
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "join requires elements to be 2-tuples",
                ));
            }
            let key = pair.get_item(0)?;
            let val = pair.get_item(1)?;
            match right_map.get_item(&key)? {
                Some(lst) => lst.downcast::<PyList>()?.append(val)?,
                None => {
                    right_map.set_item(&key, PyList::new(py, [val])?)?;
                }
            }
        }

        // Probe left side
        let mut elements: Vec<Py<PyAny>> = Vec::new();
        for item in &self.elements {
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "join requires elements to be 2-tuples",
                ));
            }
            let lk = pair.get_item(0)?;
            let lv = pair.get_item(1)?;
            if let Some(right_vals) = right_map.get_item(&lk)? {
                for rv in right_vals.downcast::<PyList>()?.iter() {
                    let inner = PyTuple::new(py, [&lv, &rv])?.unbind().into_any();
                    let outer = PyTuple::new(py, [&lk, inner.bind(py)])?.unbind().into_any();
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
    /// O(N + M) — previously O(N × M) nested loop.
    #[pyo3(signature = (other))]
    pub fn left_outer_join(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        // Build right-side map: key → [val, ...]
        let right_map = PyDict::new(py);
        for item in &other.elements {
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "left_outer_join requires elements to be 2-tuples",
                ));
            }
            let key = pair.get_item(0)?;
            let val = pair.get_item(1)?;
            match right_map.get_item(&key)? {
                Some(lst) => lst.downcast::<PyList>()?.append(val)?,
                None => {
                    right_map.set_item(&key, PyList::new(py, [val])?)?;
                }
            }
        }

        // Probe left side; unmatched keys emit (lv, None)
        let mut elements: Vec<Py<PyAny>> = Vec::new();
        for item in &self.elements {
            let pair = item.bind(py).downcast::<PyTuple>()?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "left_outer_join requires elements to be 2-tuples",
                ));
            }
            let lk = pair.get_item(0)?;
            let lv = pair.get_item(1)?;
            match right_map.get_item(&lk)? {
                Some(right_vals) => {
                    for rv in right_vals.downcast::<PyList>()?.iter() {
                        let inner = PyTuple::new(py, [&lv, &rv])?.unbind().into_any();
                        let outer =
                            PyTuple::new(py, [&lk, inner.bind(py)])?.unbind().into_any();
                        elements.push(outer);
                    }
                }
                None => {
                    let none_val = py.None();
                    let inner =
                        PyTuple::new(py, [&lv, none_val.bind(py)])?.unbind().into_any();
                    let outer =
                        PyTuple::new(py, [&lk, inner.bind(py)])?.unbind().into_any();
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
}
