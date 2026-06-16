use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use super::PyRdd;

#[pymethods]
impl PyRdd {
    /// Group `(key, value)` pairs by key.
    pub fn group_by_key(&self, py: Python) -> PyResult<PyRdd> {
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
        let mut groups: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();

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

    /// Inner join two pair RDDs on their keys.
    ///
    /// Both RDDs must contain `(key, value)` 2-tuples.
    /// Emits `(key, (left_val, right_val))` for every matching key pair.
    #[pyo3(signature = (other))]
    pub fn join(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
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
}
