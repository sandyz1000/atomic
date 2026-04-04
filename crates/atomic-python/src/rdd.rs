use std::collections::HashMap;

use pyo3::ffi::PyObject;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::docker_stub::PyDockerStub;

/// An in-memory distributed dataset (RDD) of Python objects.
///
/// `PyRdd` is the Python-facing equivalent of Rust's `TypedRdd<T>`. It holds
/// elements as `PyObject` values and applies transformations eagerly when each
/// method is called. All local execution happens in the calling thread.
///
/// Partitioning is tracked logically: `num_partitions` is stored and influences
/// `map_via` Docker dispatch (one Docker call per partition), but local
/// operations process all elements as a flat sequence.
///
/// # Example
/// ```python
/// import atomic
/// ctx = atomic.Context()
///
/// # Map, filter, collect
/// result = ctx.parallelize([1, 2, 3, 4]) \
///             .map(lambda x: x * 2) \
///             .filter(lambda x: x > 4) \
///             .collect()
/// # [6, 8]
///
/// # Word count
/// words = ctx.parallelize(["hello world", "hello atomic"]) \
///            .flat_map(lambda line: line.split()) \
///            .map(lambda w: (w, 1)) \
///            .reduce_by_key(lambda a, b: a + b) \
///            .collect()
/// # [("atomic", 1), ("hello", 2), ("world", 1)]
/// ```
#[pyclass(name = "Rdd")]
pub struct PyRdd {
    pub(crate) elements: Vec<PyObject>,
    pub(crate) num_partitions: usize,
}

impl PyRdd {
    pub fn from_data(_py: Python, elements: Vec<PyObject>, num_partitions: usize) -> Self {
        Self { elements, num_partitions }
    }
}

#[pymethods]
impl PyRdd {
    // ── Transformations ──────────────────────────────────────────────────────

    /// Apply `f` to each element, returning a new RDD.
    pub fn map(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| f.call1(py, (item,)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Keep only elements for which `f` returns truthy.
    pub fn filter(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
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
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Apply `f` to each element and flatten the results (f must return an iterable).
    pub fn flat_map(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        let mut elements = Vec::new();
        for item in &self.elements {
            let result = f.call1(py, (item,))?;
            let iter = result.bind(py).iter()?;
            for val in iter {
                elements.push(val?.unbind());
            }
        }
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Apply `f` only to the value in each `(key, value)` pair.
    ///
    /// Requires elements to be 2-tuples. Returns an RDD of `(key, f(value))` pairs.
    pub fn map_values(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let pair = item.downcast_bound::<PyTuple>(py)?;
                if pair.len() != 2 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "map_values requires elements to be 2-tuples",
                    ));
                }
                let key = pair.get_item(0)?.unbind();
                let new_val = f.call1(py, (pair.get_item(1)?,))?;
                Ok(PyTuple::new(py, [key, new_val])?.unbind().into())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Apply `f` to each value in `(key, value)` pairs and flatten the results.
    ///
    /// `f` must return an iterable. Returns an RDD of `(key, item)` pairs.
    pub fn flat_map_values(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        let mut elements = Vec::new();
        for item in &self.elements {
            let pair = item.downcast_bound::<PyTuple>(py)?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "flat_map_values requires elements to be 2-tuples",
                ));
            }
            let key = pair.get_item(0)?.unbind();
            let iter_result = f.call1(py, (pair.get_item(1)?,))?;
            for val in iter_result.bind(py).iter()? {
                let new_pair = PyTuple::new(py, [key.clone_ref(py), val?.unbind()])?.unbind();
                elements.push(new_pair.into());
            }
        }
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Produce `(f(element), element)` pairs, keying each element by `f`.
    pub fn key_by(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        let elements = self
            .elements
            .iter()
            .map(|item| {
                let key = f.call1(py, (item,))?;
                Ok(PyTuple::new(py, [key, item.clone_ref(py)])?.unbind().into())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Group elements by `f(element)`, returning `(key, [elements])` pairs.
    pub fn group_by(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        self.key_by(py, f)?.group_by_key(py)
    }

    /// Group `(key, value)` pairs by key, returning `(key, [values])` pairs.
    ///
    /// Requires elements to be 2-tuples `(key, value)`.
    pub fn group_by_key(&self, py: Python) -> PyResult<PyRdd> {
        let mut groups: HashMap<String, (PyObject, Vec<PyObject>)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for item in &self.elements {
            let pair = item.downcast_bound::<PyTuple>(py)?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "group_by_key requires elements to be 2-tuples",
                ));
            }
            let key_obj = pair.get_item(0)?.unbind();
            let val_obj = pair.get_item(1)?.unbind();
            // Use Python repr as the HashMap key for grouping.
            let key_str = key_obj.bind(py).repr()?.to_string();
            let entry = groups.entry(key_str.clone()).or_insert_with(|| {
                order.push(key_str);
                (key_obj.clone_ref(py), Vec::new())
            });
            entry.1.push(val_obj);
        }

        let elements = order
            .iter()
            .map(|k| {
                let (key, vals) = &groups[k];
                let py_list = PyList::new(py, vals.iter().map(|v| v.bind(py)))?.unbind();
                let items: [&pyo3::Bound<'_, pyo3::PyAny>; 2] =
                    [key.bind(py), py_list.bind(py).as_any()];
                Ok(PyTuple::new(py, items)?.unbind().into())
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Aggregate values with the same key using `f(acc, value) -> acc`.
    ///
    /// Requires elements to be `(key, value)` 2-tuples.
    ///
    /// # Example
    /// ```python
    /// word_counts = words.map(lambda w: (w, 1)).reduce_by_key(lambda a, b: a + b)
    /// ```
    pub fn reduce_by_key(&self, py: Python, f: PyObject) -> PyResult<PyRdd> {
        let mut accum: HashMap<String, (PyObject, PyObject)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for item in &self.elements {
            let pair = item.downcast_bound::<PyTuple>(py)?;
            if pair.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "reduce_by_key requires elements to be 2-tuples",
                ));
            }
            let key_obj = pair.get_item(0)?.unbind();
            let val_obj = pair.get_item(1)?.unbind();
            let key_str = key_obj.bind(py).repr()?.to_string();

            match accum.get_mut(&key_str) {
                Some((_, acc)) => {
                    let new_acc = f.call1(py, (acc.clone_ref(py), val_obj))?;
                    *acc = new_acc;
                }
                None => {
                    order.push(key_str.clone());
                    accum.insert(key_str, (key_obj, val_obj));
                }
            }
        }

        let elements = order
            .iter()
            .map(|k| {
                let (key, val) = &accum[k];
                Ok(PyTuple::new(py, [key.bind(py).clone(), val.bind(py).clone()])?.unbind().into())
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Merge two RDDs with the same element type into one.
    pub fn union(&self, py: Python, other: &PyRdd) -> PyRdd {
        let mut elements: Vec<PyObject> = self.elements.iter().map(|e| e.clone_ref(py)).collect();
        elements.extend(other.elements.iter().map(|e| e.clone_ref(py)));
        let partitions = self.num_partitions + other.num_partitions;
        PyRdd { elements, num_partitions: partitions }
    }

    /// Zip two RDDs element-wise into an RDD of `(a, b)` tuples.
    ///
    /// Both RDDs must have the same number of elements.
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
                Ok(PyTuple::new(py, [a.bind(py).clone(), b.bind(py).clone()])?.unbind().into())
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRdd { elements, num_partitions: self.num_partitions })
    }

    /// Compute the Cartesian product of two RDDs as an RDD of `(a, b)` tuples.
    pub fn cartesian(&self, py: Python, other: &PyRdd) -> PyResult<PyRdd> {
        let mut elements = Vec::new();
        for a in &self.elements {
            for b in &other.elements {
                elements.push(
                    PyTuple::new(py, [a.bind(py).clone(), b.bind(py).clone()])?.unbind().into(),
                );
            }
        }
        let partitions = self.num_partitions * other.num_partitions.max(1);
        Ok(PyRdd { elements, num_partitions: partitions })
    }

    /// Return a new RDD coalesced into `num_partitions` logical partitions.
    ///
    /// For local execution this is a no-op on the data; it only updates the
    /// partition count metadata (used by Docker dispatch).
    pub fn coalesce(&self, py: Python, num_partitions: usize) -> PyRdd {
        PyRdd {
            elements: self.elements.iter().map(|e| e.clone_ref(py)).collect(),
            num_partitions: num_partitions.max(1),
        }
    }

    /// Alias for `coalesce` that always shuffles (same behaviour here since
    /// execution is in-process).
    pub fn repartition(&self, py: Python, num_partitions: usize) -> PyRdd {
        self.coalesce(py, num_partitions)
    }

    // ── Artifact dispatch ────────────────────────────────────────────────────

    /// Send each partition to a pre-built Docker container and collect results.
    ///
    /// The stub references a pre-built Docker image — the same image that a Rust
    /// driver would use. Partition data is JSON-encoded and sent to the container
    /// via stdin; results are JSON-decoded from stdout.
    ///
    /// # Example
    /// ```python
    /// stub = atomic.DockerStub.from_manifest("manifest.toml", "demo.map.v1")
    /// result = rdd.map_via(stub)
    /// ```
    pub fn map_via(&self, py: Python, stub: &PyDockerStub) -> PyResult<PyRdd> {
        stub.execute_partitions(py, &self.elements, self.num_partitions)
    }

    /// Send each partition to Docker and flatten `Vec<T>` results into a single RDD.
    pub fn collect_via(&self, py: Python, stub: &PyDockerStub) -> PyResult<PyRdd> {
        // collect_via flattens — same Docker call, then flatten the list results
        let result_rdd = stub.execute_partitions(py, &self.elements, self.num_partitions)?;
        let mut flat = Vec::new();
        for item in &result_rdd.elements {
            match item.downcast_bound::<PyList>(py) {
                Ok(list) => {
                    for v in list.iter() {
                        flat.push(v.unbind());
                    }
                }
                Err(_) => flat.push(item.clone_ref(py)),
            }
        }
        Ok(PyRdd { elements: flat, num_partitions: self.num_partitions })
    }

    // ── Actions ──────────────────────────────────────────────────────────────

    /// Return all elements as a Python list.
    pub fn collect(&self, py: Python) -> PyObject {
        PyList::new(py, self.elements.iter().map(|e| e.bind(py).clone()))
            .unwrap()
            .unbind()
            .into()
    }

    /// Return the number of elements.
    pub fn count(&self) -> usize {
        self.elements.len()
    }

    /// Return the first element, or raise `StopIteration` if the RDD is empty.
    pub fn first(&self, py: Python) -> PyResult<PyObject> {
        self.elements
            .first()
            .map(|e| Ok(e.clone_ref(py)))
            .unwrap_or_else(|| {
                Err(pyo3::exceptions::PyStopIteration::new_err(
                    "RDD is empty",
                ))
            })
    }

    /// Return the first `n` elements as a Python list.
    pub fn take(&self, py: Python, n: usize) -> PyObject {
        PyList::new(
            py,
            self.elements.iter().take(n).map(|e| e.bind(py).clone()),
        )
        .unwrap()
        .unbind()
        .into()
    }

    /// Aggregate all elements using a binary function `f(acc, element) -> acc`.
    ///
    /// # Example
    /// ```python
    /// total = ctx.parallelize([1, 2, 3]).reduce(lambda a, b: a + b)
    /// # 6
    /// ```
    pub fn reduce(&self, py: Python, f: PyObject) -> PyResult<PyObject> {
        let mut iter = self.elements.iter();
        let first = iter.next().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("reduce on empty RDD")
        })?;
        let mut acc = first.clone_ref(py);
        for item in iter {
            acc = f.call1(py, (acc, item.clone_ref(py)))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial `zero` value using `f(acc, element) -> acc`.
    ///
    /// Unlike `reduce`, works on empty RDDs (returns `zero`).
    ///
    /// # Example
    /// ```python
    /// total = ctx.parallelize([1, 2, 3]).fold(0, lambda a, b: a + b)
    /// # 6
    /// ```
    pub fn fold(&self, py: Python, zero: PyObject, f: PyObject) -> PyResult<PyObject> {
        let mut acc = zero;
        for item in &self.elements {
            acc = f.call1(py, (acc, item.clone_ref(py)))?;
        }
        Ok(acc)
    }

    /// Return the number of logical partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn __len__(&self) -> usize {
        self.elements.len()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Rdd(count={}, num_partitions={})",
            self.elements.len(),
            self.num_partitions
        )
    }
}
