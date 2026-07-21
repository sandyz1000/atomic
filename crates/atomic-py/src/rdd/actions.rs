use std::sync::Arc;

use atomic_data::distributed::{PythonTaskPayload, Step, StepKind, TaskAction, TaskRuntime};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyIterator, PyList, PyTuple};

use super::PyRdd;

#[pymethods]
impl PyRdd {
    /// Return all elements as a Python list.
    ///
    /// In distributed mode with a staged pipeline, dispatches to workers and
    /// returns the combined results.
    pub fn collect(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            if let Some(ref staged) = self.staged {
                let result_bytes = self
                    .context
                    .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                return Self::collect_distributed(py, result_bytes);
            }
        }
        Ok(
            PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))
                .unwrap()
                .into_any()
                .unbind(),
        )
    }

    /// Return the number of elements.
    pub fn count(&mut self, py: Python) -> PyResult<usize> {
        if self.context.is_distributed() && self.staged.is_some() {
            let list = self.collect(py)?;
            return Ok(list.bind(py).len()?);
        }
        Ok(self.elements.len())
    }

    /// Return the first element.
    pub fn first(&self, py: Python) -> PyResult<Py<PyAny>> {
        self.elements
            .first()
            .map(|e| Ok(e.clone_ref(py)))
            .unwrap_or_else(|| Err(pyo3::exceptions::PyStopIteration::new_err("RDD is empty")))
    }

    /// Return the first `n` elements as a Python list.
    pub fn take(&self, py: Python, n: usize) -> Py<PyAny> {
        PyList::new(py, self.elements.iter().take(n).map(|e| e.bind(py).clone()))
            .unwrap()
            .into_any()
            .unbind()
    }

    /// Aggregate all elements using `f(acc, element) -> acc`.
    ///
    /// In distributed mode this dispatches the reduce to workers (each returns a
    /// per-partition scalar), then combines those scalars on the driver.
    pub fn reduce(&mut self, py: Python, f: Py<PyAny>) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper(
                py,
                "lambda partition, _f=_f: [] if not partition else [__import__('functools').reduce(_f, partition)]",
                &f,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            let payload_struct = PythonTaskPayload {
                fn_bytes,
                zero_bytes: vec![],
            };
            let payload = serde_json::to_vec(&payload_struct)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let op = Step {
                op_id: "atomic::task::python".to_string(),
                kind: StepKind::Task(TaskAction::Reduce),
                runtime: TaskRuntime::Python,
                payload,
            };
            self.push_op(py, op)?;

            let staged = self.staged.as_ref().unwrap();
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let json_mod = PyModule::import(py, "json")?;
            let mut acc: Option<Bound<'_, PyAny>> = None;
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let items: Bound<'_, PyAny> = json_mod.call_method1("loads", (json_str,))?;
                if items.len()? == 0 {
                    continue;
                }
                let scalar = items.get_item(0)?;
                acc = Some(match acc {
                    None => scalar,
                    Some(a) => f.call1(py, (a, scalar))?.bind(py).clone(),
                });
            }
            return acc.map(|a| Ok(a.unbind())).unwrap_or_else(|| {
                Err(pyo3::exceptions::PyValueError::new_err(
                    "reduce on empty RDD",
                ))
            });
        }
        let mut iter = self.elements.iter();
        let first = iter
            .next()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("reduce on empty RDD"))?;
        let mut acc = first.clone_ref(py);
        for item in iter {
            acc = f.call1(py, (acc, item.clone_ref(py)))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial `zero` value using `f(acc, element) -> acc`.
    ///
    /// In distributed mode this dispatches the fold to workers.
    pub fn fold(&mut self, py: Python, zero: Py<PyAny>, f: Py<PyAny>) -> PyResult<Py<PyAny>> {
        if self.context.is_distributed() {
            let wrapper = Self::make_partition_wrapper_with_zero(
                py,
                "lambda partition, _f=_f, _z=_z: [__import__('functools').reduce(_f, partition, _z)]",
                &f,
                &zero,
            )?;
            let fn_bytes = Self::pickle_fn(py, &wrapper.unbind())?;
            let zero_bytes = Self::pickle_fn(py, &zero)?;
            let payload_struct = PythonTaskPayload {
                fn_bytes,
                zero_bytes: vec![],
            };
            let payload = serde_json::to_vec(&payload_struct)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let op = Step {
                op_id: "atomic::task::python".to_string(),
                kind: StepKind::Task(TaskAction::Fold),
                runtime: TaskRuntime::Python,
                payload,
            };
            self.push_op(py, op)?;

            let staged = self.staged.as_ref().unwrap();
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let json_mod = PyModule::import(py, "json")?;
            let pickle =
                PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
            let zero_again = pickle.call_method1("loads", (PyBytes::new(py, &zero_bytes),))?;
            let mut acc: Bound<'_, PyAny> = zero_again;
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let items: Bound<'_, PyAny> = json_mod.call_method1("loads", (json_str,))?;
                let scalar = items.get_item(0)?;
                acc = f.call1(py, (acc, scalar))?.bind(py).clone();
            }
            return Ok(acc.unbind());
        }
        let mut acc = zero;
        for item in &self.elements {
            acc = f.call1(py, (acc, item.clone_ref(py)))?;
        }
        Ok(acc)
    }

    /// Apply `f` to each element for side effects.
    pub fn for_each(&self, py: Python, f: Py<PyAny>) -> PyResult<()> {
        for item in &self.elements {
            f.call1(py, (item.clone_ref(py),))?;
        }
        Ok(())
    }

    /// Apply `f` to each logical partition for side effects.
    pub fn for_each_partition(&self, py: Python, f: Py<PyAny>) -> PyResult<()> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        for (start, end) in super::slice_positions(total, np) {
            let py_list = PyList::new(
                py,
                self.elements[start..end].iter().map(|e| e.bind(py).clone()),
            )?;
            f.call1(py, (py_list,))?;
        }
        Ok(())
    }

    /// Count occurrences of each distinct element. Returns a Python dict keyed by items.
    pub fn count_by_value(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for item in &self.elements {
            let key = item.bind(py);
            let current: i64 = dict
                .get_item(key.clone())?
                .map(|v| v.extract::<i64>().unwrap_or(0))
                .unwrap_or(0);
            dict.set_item(key, current + 1)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Count elements per key in a pair RDD. Returns a Python dict keyed by key objects.
    pub fn count_by_key(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for item in &self.elements {
            let pair = item.bind(py).cast::<PyTuple>()?;
            if pair.len() < 1 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "count_by_key requires elements to be non-empty tuples",
                ));
            }
            let key = pair.get_item(0)?;
            let current: i64 = dict
                .get_item(key.clone())?
                .map(|v| v.extract::<i64>().unwrap_or(0))
                .unwrap_or(0);
            dict.set_item(key, current + 1)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Return True if the RDD has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Return the maximum element. Optional `key` function `f(x) => comparable`.
    #[pyo3(signature = (key=None))]
    pub fn max(&self, py: Python, key: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        if self.elements.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("max on empty RDD"));
        }
        let mut result = self.elements[0].clone_ref(py);
        let mut result_key = match &key {
            Some(f) => Some(f.call1(py, (result.clone_ref(py),))?),
            None => None,
        };
        for item in &self.elements[1..] {
            let item_key = match &key {
                Some(f) => Some(f.call1(py, (item.clone_ref(py),))?),
                None => None,
            };
            let is_greater = match (&item_key, &result_key) {
                (Some(ik), Some(rk)) => ik.bind(py).gt(rk.bind(py))?,
                _ => item.bind(py).gt(result.bind(py))?,
            };
            if is_greater {
                result = item.clone_ref(py);
                result_key = item_key;
            }
        }
        Ok(result)
    }

    /// Return the minimum element. Optional `key` function `f(x) => comparable`.
    #[pyo3(signature = (key=None))]
    pub fn min(&self, py: Python, key: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        if self.elements.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("min on empty RDD"));
        }
        let mut result = self.elements[0].clone_ref(py);
        let mut result_key = match &key {
            Some(f) => Some(f.call1(py, (result.clone_ref(py),))?),
            None => None,
        };
        for item in &self.elements[1..] {
            let item_key = match &key {
                Some(f) => Some(f.call1(py, (item.clone_ref(py),))?),
                None => None,
            };
            let is_less = match (&item_key, &result_key) {
                (Some(ik), Some(rk)) => ik.bind(py).lt(rk.bind(py))?,
                _ => item.bind(py).lt(result.bind(py))?,
            };
            if is_less {
                result = item.clone_ref(py);
                result_key = item_key;
            }
        }
        Ok(result)
    }

    /// Write each element as a line to `path`.
    ///
    /// Accepts a local file path or an S3 URI (`s3://bucket/prefix`).
    pub fn save_as_text_file(&self, py: Python, path: String) -> PyResult<()> {
        if path.starts_with("s3://") {
            return self.write_s3(py, &path);
        }
        use std::io::Write;
        let mut file = std::fs::File::create(&path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        for item in &self.elements {
            let line = item.bind(py).str()?.to_string();
            writeln!(file, "{}", line)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Two-phase aggregation: `seq_fn(acc, elem)` within partitions,
    /// `comb_fn(acc, acc)` across partition results.
    pub fn aggregate(
        &self,
        py: Python,
        zero: Py<PyAny>,
        seq_fn: Py<PyAny>,
        comb_fn: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();

        let mut partition_results = Vec::new();
        for (start, end) in super::slice_positions(total, np) {
            let mut acc = zero.clone_ref(py);
            for item in &self.elements[start..end] {
                acc = seq_fn.call1(py, (acc, item.clone_ref(py)))?;
            }
            partition_results.push(acc);
        }

        let mut result = zero;
        for part_acc in partition_results {
            result = comb_fn.call1(py, (result, part_acc))?;
        }
        Ok(result)
    }

    /// Return a Python list of sublists — one sublist per logical partition.
    pub fn glom(&self, py: Python) -> PyResult<Py<PyAny>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let outer = PyList::empty(py);
        for (start, end) in super::slice_positions(total, np) {
            let inner = PyList::new(
                py,
                self.elements[start..end].iter().map(|e| e.bind(py).clone()),
            )?;
            outer.append(inner)?;
        }
        Ok(outer.into_any().unbind())
    }

    /// No-op in local mode: return a clone of this RDD.
    pub fn cache(&self, py: Python) -> PyRdd {
        PyRdd {
            elements: self.elements.iter().map(|e| e.clone_ref(py)).collect(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: None,
        }
    }

    /// No-op in local mode: return a clone of this RDD.
    pub fn persist(&self, py: Python) -> PyRdd {
        self.cache(py)
    }

    /// No-op in local mode: return a clone of this RDD.
    pub fn unpersist(&self, py: Python) -> PyRdd {
        self.cache(py)
    }

    /// Materialize elements to `{path}/checkpoint.pkl` using Python pickle, then return
    /// a new RDD with the same elements (lineage is conceptually truncated).
    ///
    /// Write is atomic: data goes to `{path}/checkpoint.pkl.tmp` first, then renamed.
    #[pyo3(signature = (path))]
    pub fn checkpoint(&self, py: Python, path: &str) -> PyResult<PyRdd> {
        use std::io::Write;

        std::fs::create_dir_all(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let pickle =
            PyModule::import(py, "cloudpickle").or_else(|_| PyModule::import(py, "pickle"))?;
        let py_list = PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))?;
        let bytes_obj = pickle.call_method1("dumps", (py_list,))?;
        let bytes: Vec<u8> = bytes_obj.extract()?;

        let tmp_path = format!("{}/checkpoint.pkl.tmp", path);
        let final_path = format!("{}/checkpoint.pkl", path);
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        f.write_all(&bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        drop(f);
        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok(PyRdd {
            elements: self.elements.iter().map(|e| e.clone_ref(py)).collect(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: None,
        })
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn __len__(&self) -> usize {
        self.elements.len()
    }

    /// Iterate over elements: `for x in rdd`.
    pub fn __iter__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))?;
        let iter = PyIterator::from_object(list.as_any())?;
        Ok(iter.into_any().unbind())
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Rdd(count={}, num_partitions={}, distributed={})",
            self.elements.len(),
            self.num_partitions,
            self.context.is_distributed(),
        )
    }
}

// A plain (non-`#[pymethods]`) impl block for private helpers — pyo3's proc-macro
// only processes items inside a `#[pymethods]`-annotated block.
impl PyRdd {
    fn write_s3(&self, py: Python, path: &str) -> PyResult<()> {
        use atomic_compute::io::s3::{S3Uri, write_text};
        let s3uri = S3Uri::parse(path).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "save_as_text_file: invalid S3 URI: {path}"
            ))
        })?;
        let content: String = self
            .elements
            .iter()
            .map(|item| {
                let s = item
                    .bind(py)
                    .str()
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                format!("{s}\n")
            })
            .collect();
        let key = format!("{}/part-0", s3uri.key.trim_end_matches('/'));
        write_text(&s3uri.bucket, &key, content)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }
}
