use std::sync::Arc;

use atomic_data::distributed::TaskAction;
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PyList, PyTuple};

use super::PyRdd;

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
}
