use std::sync::Arc;

use atomic_data::distributed::TaskAction;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::PyRdd;

#[pymethods]
impl PyRdd {
    /// Group `(key, value)` pairs by key.
    ///
    /// In distributed mode dispatches a partition-level partial grouping to workers and
    /// merges the N partial dicts on the driver — avoids shipping all values over the wire.
    pub fn group_by_key(&mut self, py: Python) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            // group_by_key is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let helpers = PyModule::import(py, "atomic_compute._pair_helpers")?;
            let partial_fn = helpers.getattr("make_partial_group_fn")?.call0()?;
            let fn_bytes = Self::pickle_fn(py, &partial_fn.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;

            let staged = self.staged.as_ref().unwrap();
            let (source_partitions, ops) = (staged.source_partitions.clone(), staged.ops.clone());
            self.staged = saved_staged;
            let result_bytes = self
                .context
                .dispatch_pipeline(source_partitions, ops)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Merge N partial (key → [val…]) dicts on the driver
            let json_mod = PyModule::import(py, "json")?;
            let merged: Bound<'_, PyDict> = PyDict::new(py);
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                for item in json_mod.call_method1("loads", (json_str,))?.try_iter()? {
                    let (k, v) = Self::extract_pair(&item?)?;
                    match merged.get_item(&k)? {
                        Some(lst) => {
                            for elem in v.try_iter()? {
                                lst.cast::<PyList>()?.append(elem?)?;
                            }
                        }
                        None => {
                            let lst = PyList::empty(py);
                            for elem in v.try_iter()? {
                                lst.append(elem?)?;
                            }
                            merged.set_item(&k, lst)?;
                        }
                    }
                }
            }
            let elements = merged
                .iter()
                .map(|(k, v)| Ok(PyTuple::new(py, [&k, &v])?.unbind().into_any()))
                .collect::<PyResult<Vec<_>>>()?;
            return Ok(PyRdd::from_data(
                py,
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        // Local path — O(N) PyDict accumulator
        let dict = PyDict::new(py);
        for item in &self.elements {
            let bound = item.bind(py);
            let (key, val) = Self::extract_pair(bound)?;
            match dict.get_item(&key)? {
                Some(lst) => lst.downcast::<PyList>()?.append(val)?,
                None => {
                    dict.set_item(&key, PyList::new(py, [val])?)?;
                }
            }
        }
        let elements = dict
            .iter()
            .map(|(key, vals)| Ok(PyTuple::new(py, [&key, &vals])?.unbind().into_any()))
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
    /// In distributed mode dispatches a partition-level partial reduce to workers and
    /// merges the N partial dicts on the driver. Eliminates the intermediate `collect()`
    /// when chaining after `flat_map`/`map` — the whole pipeline is dispatched at once.
    pub fn reduce_by_key(&mut self, py: Python, f: Py<PyAny>) -> PyResult<PyRdd> {
        if self.context.is_distributed() {
            // reduce_by_key is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let helpers = PyModule::import(py, "atomic_compute._pair_helpers")?;
            let partial_fn = helpers
                .getattr("make_partial_reduce_fn")?
                .call1((f.bind(py),))?;
            let fn_bytes = Self::pickle_fn(py, &partial_fn.unbind())?;
            // push_op (called by stage_python_udf) auto-creates the staged pipeline from
            // self.elements when not already staged — works for both chained and standalone use.
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;

            let staged = self.staged.as_ref().unwrap();
            let (source_partitions, ops) = (staged.source_partitions.clone(), staged.ops.clone());
            self.staged = saved_staged;
            let result_bytes = self
                .context
                .dispatch_pipeline(source_partitions, ops)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Merge N partial dicts on the driver with the same f
            let json_mod = PyModule::import(py, "json")?;
            let merged = PyDict::new(py);
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                for item in json_mod.call_method1("loads", (json_str,))?.try_iter()? {
                    let (k, v) = Self::extract_pair(&item?)?;
                    match merged.get_item(&k)? {
                        Some(acc) => {
                            merged.set_item(&k, f.call1(py, (acc, v))?)?;
                        }
                        None => {
                            merged.set_item(&k, v)?;
                        }
                    }
                }
            }
            let elements = merged
                .iter()
                .map(|(k, v)| Ok(PyTuple::new(py, [&k, &v])?.unbind().into_any()))
                .collect::<PyResult<Vec<_>>>()?;
            return Ok(PyRdd::from_data(
                py,
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        // Local path — O(N) PyDict accumulator
        let dict = PyDict::new(py);
        for item in &self.elements {
            let bound = item.bind(py);
            let (key, val) = Self::extract_pair(bound)?;
            match dict.get_item(&key)? {
                Some(acc) => {
                    dict.set_item(&key, f.call1(py, (acc, val))?)?;
                }
                None => {
                    dict.set_item(&key, val)?;
                }
            }
        }
        let elements = dict
            .iter()
            .map(|(k, v)| Ok(PyTuple::new(py, [&k, &v])?.unbind().into_any()))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Extract the key from each `(key, value)` pair.
    pub fn keys(&self, py: Python) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let bound = item.bind(py);
                let (k, _) = Self::extract_pair(bound)?;
                Ok(k.unbind())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd::from_data(
            py,
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Extract the value from each `(key, value)` pair.
    pub fn values(&self, py: Python) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let bound = item.bind(py);
                let (_, v) = Self::extract_pair(bound)?;
                Ok(v.unbind())
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
            let bound = item.bind(py);
            let (k, v) = Self::extract_pair(bound)?;
            if k.eq(key.bind(py))? {
                vals.append(v)?;
            }
        }
        Ok(vals.into_any().unbind())
    }

    /// Inner join two pair RDDs on their keys.
    ///
    /// In distributed mode the right side (if already in driver memory) is captured in the
    /// join closure and shipped to workers — each worker joins against its left partition
    /// slice without data moving to the driver first. Falls back to driver-side hash join
    /// when the right side is itself a pending staged pipeline.
    #[pyo3(signature = (other))]
    pub fn join(&mut self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        if self.context.is_distributed() && other.staged.is_none() {
            // join is an action: dispatch immediately and leave `self` reusable for
            // further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let right_json = Self::elements_to_json(py, &other.elements)?;
            if right_json.len() > 50_000_000 {
                let warnings = PyModule::import(py, "warnings")?;
                warnings.call_method1(
                    "warn",
                    ("join: right side exceeds 50 MB serialized; consider pre-reducing cardinality",),
                )?;
            }
            let helpers = PyModule::import(py, "atomic_compute._pair_helpers")?;
            let join_fn = helpers
                .getattr("make_join_fn")?
                .call1((right_json.as_str(),))?;
            let fn_bytes = Self::pickle_fn(py, &join_fn.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;

            let staged = self.staged.as_ref().unwrap();
            let (source_partitions, ops) = (staged.source_partitions.clone(), staged.ops.clone());
            self.staged = saved_staged;
            let result_bytes = self
                .context
                .dispatch_pipeline(source_partitions, ops)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let elements = Self::decode_result_bytes(py, result_bytes)?;
            return Ok(PyRdd::from_data(
                py,
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        if self.context.is_distributed() && other.staged.is_some() {
            eprintln!("join: right side is a staged pipeline; collecting both sides to driver");
        }

        // Driver-side hash join (local mode or staged right side)
        let right_map = PyDict::new(py);
        for item in &other.elements {
            let bound = item.bind(py);
            let (k, v) = Self::extract_pair(bound)?;
            match right_map.get_item(&k)? {
                Some(lst) => lst.downcast::<PyList>()?.append(v)?,
                None => {
                    right_map.set_item(&k, PyList::new(py, [v])?)?;
                }
            }
        }
        let mut elements: Vec<Py<PyAny>> = Vec::new();
        for item in &self.elements {
            let bound = item.bind(py);
            let (lk, lv) = Self::extract_pair(bound)?;
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
    /// Unmatched left keys emit `(key, (left_val, None))`. In distributed mode the right
    /// side is captured in the worker closure — same approach as `join`.
    #[pyo3(signature = (other))]
    pub fn left_outer_join(&mut self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        if self.context.is_distributed() && other.staged.is_none() {
            // left_outer_join is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let right_json = Self::elements_to_json(py, &other.elements)?;
            if right_json.len() > 50_000_000 {
                let warnings = PyModule::import(py, "warnings")?;
                warnings.call_method1(
                    "warn",
                    ("left_outer_join: right side exceeds 50 MB serialized; consider pre-reducing cardinality",),
                )?;
            }
            let helpers = PyModule::import(py, "atomic_compute._pair_helpers")?;
            let join_fn = helpers
                .getattr("make_left_outer_join_fn")?
                .call1((right_json.as_str(),))?;
            let fn_bytes = Self::pickle_fn(py, &join_fn.unbind())?;
            self.stage_python_udf(py, fn_bytes, TaskAction::Map)?;

            let staged = self.staged.as_ref().unwrap();
            let (source_partitions, ops) = (staged.source_partitions.clone(), staged.ops.clone());
            self.staged = saved_staged;
            let result_bytes = self
                .context
                .dispatch_pipeline(source_partitions, ops)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let elements = Self::decode_result_bytes(py, result_bytes)?;
            return Ok(PyRdd::from_data(
                py,
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        if self.context.is_distributed() && other.staged.is_some() {
            eprintln!(
                "left_outer_join: right side is a staged pipeline; collecting both sides to driver"
            );
        }

        // Driver-side (local mode or staged right side)
        let right_map = PyDict::new(py);
        for item in &other.elements {
            let bound = item.bind(py);
            let (k, v) = Self::extract_pair(bound)?;
            match right_map.get_item(&k)? {
                Some(lst) => lst.downcast::<PyList>()?.append(v)?,
                None => {
                    right_map.set_item(&k, PyList::new(py, [v])?)?;
                }
            }
        }
        let mut elements: Vec<Py<PyAny>> = Vec::new();
        for item in &self.elements {
            let bound = item.bind(py);
            let (lk, lv) = Self::extract_pair(bound)?;
            match right_map.get_item(&lk)? {
                Some(right_vals) => {
                    for rv in right_vals.downcast::<PyList>()?.iter() {
                        let inner = PyTuple::new(py, [&lv, &rv])?.unbind().into_any();
                        let outer = PyTuple::new(py, [&lk, inner.bind(py)])?.unbind().into_any();
                        elements.push(outer);
                    }
                }
                None => {
                    let none_val = py.None();
                    let inner = PyTuple::new(py, [&lv, none_val.bind(py)])?
                        .unbind()
                        .into_any();
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
}

impl PyRdd {
    /// Extract `(key, val)` from any 2-element sequence — accepts both `PyTuple` (local)
    /// and `PyList` (JSON round-trip from distributed collect).
    pub(crate) fn extract_pair<'py>(
        item: &Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        Ok((item.get_item(0)?, item.get_item(1)?))
    }

    /// JSON-encode a slice of Python objects into a UTF-8 string.
    fn elements_to_json(py: Python, elements: &[Py<PyAny>]) -> PyResult<String> {
        let json_mod = PyModule::import(py, "json")?;
        let lst = PyList::new(py, elements.iter().map(|e| e.bind(py).clone()))?;
        json_mod.call_method1("dumps", (lst,))?.extract::<String>()
    }

    /// Decode per-partition JSON result bytes into a flat `Vec<Py<PyAny>>`.
    fn decode_result_bytes(py: Python, result_bytes: Vec<Vec<u8>>) -> PyResult<Vec<Py<PyAny>>> {
        let json_mod = PyModule::import(py, "json")?;
        let mut elements = Vec::new();
        for bytes in result_bytes {
            let json_str = std::str::from_utf8(&bytes)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            for item in json_mod.call_method1("loads", (json_str,))?.try_iter()? {
                elements.push(item?.unbind());
            }
        }
        Ok(elements)
    }
}
