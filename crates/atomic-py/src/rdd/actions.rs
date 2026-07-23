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
        if self.context.is_distributed()
            && let Some(ref staged) = self.staged
        {
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Self::collect_distributed(py, result_bytes);
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
            return list.bind(py).len();
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
                task_name: "atomic::task::python".to_string(),
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
                task_name: "atomic::task::python".to_string(),
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
        let elements = self.materialized_elements(py)?;
        if elements.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("max on empty RDD"));
        }
        let mut result = elements[0].clone_ref(py);
        let mut result_key = match &key {
            Some(f) => Some(f.call1(py, (result.clone_ref(py),))?),
            None => None,
        };
        for item in &elements[1..] {
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
        let elements = self.materialized_elements(py)?;
        if elements.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("min on empty RDD"));
        }
        let mut result = elements[0].clone_ref(py);
        let mut result_key = match &key {
            Some(f) => Some(f.call1(py, (result.clone_ref(py),))?),
            None => None,
        };
        for item in &elements[1..] {
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

    /// Balanced binary-tree reduce. `depth` controls the number of tree merge levels
    /// (default 2). More numerically stable than a left-to-right `reduce`.
    #[pyo3(signature = (f, depth=2))]
    pub fn tree_reduce(&self, py: Python, f: Py<PyAny>, depth: usize) -> PyResult<Py<PyAny>> {
        let mut partials = self.materialized_elements(py)?;
        if partials.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "tree_reduce on empty RDD",
            ));
        }
        let levels = depth.max(1);
        for _ in 0..levels {
            if partials.len() <= 1 {
                break;
            }
            let mut next = Vec::with_capacity(partials.len() / 2 + 1);
            let mut iter = partials.into_iter();
            while let Some(a) = iter.next() {
                match iter.next() {
                    Some(b) => next.push(f.call1(py, (a, b))?),
                    None => next.push(a),
                }
            }
            partials = next;
        }
        partials
            .into_iter()
            .next()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("tree_reduce produced no value"))
    }

    /// Balanced binary-tree aggregate. `seq_fn(acc, elem)` within partitions,
    /// `comb_fn(acc, acc)` in a balanced tree across partitions.
    #[pyo3(signature = (zero, seq_fn, comb_fn, depth=2))]
    pub fn tree_aggregate(
        &self,
        py: Python,
        zero: Py<PyAny>,
        seq_fn: Py<PyAny>,
        comb_fn: Py<PyAny>,
        depth: usize,
    ) -> PyResult<Py<PyAny>> {
        let elements = self.materialized_elements(py)?;
        let np = self.num_partitions.max(1);
        let total = elements.len();
        let mut partials: Vec<Py<PyAny>> = Vec::with_capacity(np);
        for (start, end) in super::slice_positions(total, np) {
            let mut acc = zero.clone_ref(py);
            for item in &elements[start..end] {
                acc = seq_fn.call1(py, (acc, item.clone_ref(py)))?;
            }
            partials.push(acc);
        }
        let levels = depth.max(1);
        for _ in 0..levels {
            if partials.len() <= 1 {
                break;
            }
            let mut next = Vec::with_capacity(partials.len() / 2 + 1);
            let mut iter = partials.into_iter();
            while let Some(a) = iter.next() {
                match iter.next() {
                    Some(b) => next.push(comb_fn.call1(py, (a, b))?),
                    None => next.push(a),
                }
            }
            partials = next;
        }
        Ok(partials.into_iter().next().unwrap_or(zero))
    }

    /// Single-pass summary statistics. Returns a dict with keys:
    /// `count`, `mean`, `sum`, `min`, `max`, `variance`, `stdev`.
    /// Elements must be numeric (int or float). Returns `NaN` for stats on
    /// empty input.
    pub fn stats(&self, py: Python) -> PyResult<Py<PyAny>> {
        let elements = self.materialized_elements(py)?;
        let dict = PyDict::new(py);
        if elements.is_empty() {
            let nan = f64::NAN.into_pyobject(py).unwrap().into_any().unbind();
            for key in &["count", "mean", "sum", "min", "max", "variance", "stdev"] {
                dict.set_item(*key, &nan)?;
            }
            return Ok(dict.into_any().unbind());
        }
        let nums: Vec<f64> = elements
            .iter()
            .map(|e| e.bind(py).extract::<f64>())
            .collect::<PyResult<_>>()?;
        let count = nums.len() as u64;
        let sum: f64 = nums.iter().sum();
        let mean = sum / count as f64;
        let min = nums.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let m2: f64 = nums.iter().map(|x| (x - mean) * (x - mean)).sum();
        let variance = m2 / count as f64;
        let stdev = variance.sqrt();
        dict.set_item("count", count.into_pyobject(py).unwrap())?;
        dict.set_item("mean", mean.into_pyobject(py).unwrap())?;
        dict.set_item("sum", sum.into_pyobject(py).unwrap())?;
        dict.set_item("min", min.into_pyobject(py).unwrap())?;
        dict.set_item("max", max.into_pyobject(py).unwrap())?;
        dict.set_item("variance", variance.into_pyobject(py).unwrap())?;
        dict.set_item("stdev", stdev.into_pyobject(py).unwrap())?;
        Ok(dict.into_any().unbind())
    }

    /// Arithmetic mean of numeric elements. Returns `NaN` if empty.
    pub fn mean(&self, py: Python) -> PyResult<f64> {
        let elements = self.materialized_elements(py)?;
        if elements.is_empty() {
            return Ok(f64::NAN);
        }
        let sum: f64 = elements
            .iter()
            .map(|e| e.bind(py).extract::<f64>())
            .collect::<PyResult<Vec<_>>>()?
            .iter()
            .sum();
        Ok(sum / elements.len() as f64)
    }

    /// Population variance of numeric elements. Returns `NaN` if empty.
    pub fn variance(&self, py: Python) -> PyResult<f64> {
        let elements = self.materialized_elements(py)?;
        if elements.is_empty() {
            return Ok(f64::NAN);
        }
        let nums: Vec<f64> = elements
            .iter()
            .map(|e| e.bind(py).extract::<f64>())
            .collect::<PyResult<_>>()?;
        let m = nums.iter().sum::<f64>() / nums.len() as f64;
        let m2: f64 = nums.iter().map(|v| (v - m) * (v - m)).sum();
        Ok(m2 / nums.len() as f64)
    }

    /// Population standard deviation — `sqrt(variance())`.
    pub fn stdev(&self, py: Python) -> PyResult<f64> {
        Ok(self.variance(py)?.sqrt())
    }

    /// Bucketed counts of elements over ascending `bounds`.
    ///
    /// `bounds` has `n + 1` edges defining `n` buckets; returns a list of `u64`
    /// counts where index `i` counts elements in `[bounds[i], bounds[i+1])`,
    /// with the final bucket right-inclusive.
    pub fn histogram(&self, py: Python, bounds: Vec<f64>) -> PyResult<Vec<u64>> {
        if bounds.len() < 2 {
            return Ok(vec![]);
        }
        let n = bounds.len() - 1;
        let lo = bounds[0];
        let hi = bounds[n];
        let elements = self.materialized_elements(py)?;
        let mut counts = vec![0u64; n];
        for elem in &elements {
            let v: f64 = elem.bind(py).extract()?;
            if v < lo || v > hi {
                continue;
            }
            let idx = bounds
                .partition_point(|&b| b <= v)
                .saturating_sub(1)
                .min(n - 1);
            counts[idx] += 1;
        }
        Ok(counts)
    }

    /// Return a sampled subset of this RDD.
    ///
    /// `with_replacement = True` — Poisson sampling (elements may repeat).
    /// `with_replacement = False` — Bernoulli sampling (each element at most once).
    /// Optional `seed` seeds Python's `random.Random` for reproducibility.
    #[pyo3(signature = (with_replacement, fraction, seed=None))]
    pub fn sample(
        &self,
        py: Python,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> PyResult<PyRdd> {
        let source = self.materialized_elements(py)?;
        let random_mod = PyModule::import(py, "random")?;
        let rng = if let Some(s) = seed {
            let s_obj = s.into_pyobject(py)?;
            random_mod.call_method1("Random", (s_obj,))?
        } else {
            random_mod.call_method0("Random")?
        };
        if with_replacement {
            // Poisson sampling via per-element random draws.
            let mut elements: Vec<Py<PyAny>> = Vec::new();
            for elem in &source {
                let mut remaining = fraction;
                loop {
                    let roll: f64 = rng.call_method0("random")?.extract()?;
                    if roll < remaining {
                        elements.push(elem.clone_ref(py));
                        remaining -= roll;
                    } else {
                        break;
                    }
                }
            }
            Ok(PyRdd::from_data(
                py,
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ))
        } else {
            // Bernoulli: each element included with probability = fraction.
            let mut elements: Vec<Py<PyAny>> = Vec::new();
            for elem in &source {
                let roll: f64 = rng.call_method0("random")?.extract()?;
                if roll < fraction {
                    elements.push(elem.clone_ref(py));
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

    /// Return a fixed-size random sample as a list.
    ///
    /// This is an **action**. `with_replacement = True` draws with replacement;
    /// `False` draws distinct elements. `seed` makes the sample reproducible.
    #[pyo3(signature = (with_replacement, num, seed=None))]
    pub fn take_sample(
        &self,
        py: Python,
        with_replacement: bool,
        num: usize,
        seed: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let source = self.materialized_elements(py)?;
        if source.is_empty() || num == 0 {
            return Ok(PyList::empty(py).into_any().unbind());
        }
        let random_mod = PyModule::import(py, "random")?;
        let rng = if let Some(s) = seed {
            let s_obj = s.into_pyobject(py)?;
            random_mod.call_method1("Random", (s_obj,))?
        } else {
            random_mod.call_method0("Random")?
        };
        if with_replacement {
            let n = source.len();
            let zero = 0i64.into_pyobject(py)?;
            let n_minus = ((n - 1) as i64).into_pyobject(py)?;
            let mut result: Vec<Py<PyAny>> = Vec::with_capacity(num);
            for _ in 0..num {
                let idx: usize = rng
                    .call_method1("randint", (&zero, &n_minus))?
                    .extract::<i64>()? as usize;
                result.push(source[idx].clone_ref(py));
            }
            Ok(PyList::new(py, result.iter().map(|e| e.bind(py).clone()))?
                .into_any()
                .unbind())
        } else {
            let take = num.min(source.len());
            let range_list =
                PyList::new(py, (0..source.len()).map(|i| i.into_pyobject(py).unwrap()))?;
            let take_obj = take.into_pyobject(py)?;
            let indices: Vec<usize> = rng
                .call_method1("sample", (&range_list, &take_obj))?
                .extract()?;
            let result: Vec<Py<PyAny>> = indices.iter().map(|&i| source[i].clone_ref(py)).collect();
            Ok(PyList::new(py, result.iter().map(|e| e.bind(py).clone()))?
                .into_any()
                .unbind())
        }
    }

    /// Approximate distinct count via a Python-set cardinality check.
    ///
    /// Collects all elements to the driver; cheap for small-to-medium datasets where
    /// exact `set`-based counting suffices. For large datasets use the Rust core's
    /// HLL-based `count_approx_distinct` (available on `TypedRdd<T>` via `#[task]`).
    pub fn count_approx_distinct(&self, py: Python) -> PyResult<u64> {
        let elements = self.materialized_elements(py)?;
        let mut seen = std::collections::HashSet::new();
        for elem in &elements {
            let key = elem
                .bind(py)
                .repr()
                .map(|r| r.to_string())
                .unwrap_or_default();
            seen.insert(key);
        }
        Ok(seen.len() as u64)
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
    /// The elements to aggregate over: in distributed mode with a staged pipeline this dispatches
    /// the pipeline to workers and returns the transformed rows, so aggregations see the current
    /// logical elements rather than the stale source. In local mode it clones `self.elements`.
    fn materialized_elements(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        if self.context.is_distributed()
            && let Some(ref staged) = self.staged
        {
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let list = Self::collect_distributed(py, result_bytes)?;
            let mut out = Vec::new();
            for item in list.bind(py).try_iter()? {
                out.push(item?.unbind());
            }
            Ok(out)
        } else {
            Ok(self.elements.iter().map(|e| e.clone_ref(py)).collect())
        }
    }

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
