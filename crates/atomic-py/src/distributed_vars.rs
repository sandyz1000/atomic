use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// A read-only value broadcast to all tasks.
///
/// Created by `Context.broadcast(value)`.  The value is serialized with
/// cloudpickle (or stdlib `pickle` as fallback) on the driver and
/// deserialized on demand via `value()`.
///
/// # Example
/// ```python
/// bv = ctx.broadcast({"threshold": 10})
/// rdd.filter(lambda x: x > bv.value()["threshold"]).collect()
/// ```
#[pyclass(name = "BroadcastVar")]
pub struct PyBroadcastVar {
    data: Arc<Vec<u8>>, // cloudpickle-serialized bytes
}

#[pymethods]
impl PyBroadcastVar {
    /// Deserialize and return the broadcast value.
    pub fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let pickle = py.import("pickle")?;
        pickle
            .call_method1("loads", (PyBytes::new(py, &self.data),))
            .map(|b| b.unbind())
    }

    pub fn __repr__(&self) -> String {
        format!("BroadcastVar(size={} bytes)", self.data.len())
    }
}

impl PyBroadcastVar {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }
}

/// A mutable accumulator that can be updated from tasks.
///
/// Created by `Context.accumulator(zero)`.  Supports numeric (`int`, `float`),
/// string concatenation, and list-append semantics.  An optional `merge_fn`
/// callable `(current, delta) -> new_value` overrides the default add logic.
///
/// # Example
/// ```python
/// acc = ctx.accumulator(0)
/// ctx.parallelize([1, 2, 3]).for_each(lambda x: acc.add(x))
/// assert acc.value() == 6
///
/// max_acc = ctx.accumulator(0, merge_fn=lambda a, b: max(a, b))
/// ctx.parallelize([3, 1, 4, 1, 5]).for_each(lambda x: max_acc.add(x))
/// assert max_acc.value() == 5
/// ```
#[pyclass(name = "Accumulator")]
pub struct PyAccumulator {
    inner: Arc<Mutex<serde_json::Value>>,
    initial: serde_json::Value,
    merge_fn: Option<Py<PyAny>>,
}

#[pymethods]
impl PyAccumulator {
    /// Add a delta to the accumulator using the merge function (if set) or default add logic.
    pub fn add(&self, py: Python<'_>, delta: Py<PyAny>) -> PyResult<()> {
        let delta_val = pythonobj_to_json(py, &delta)?;
        let mut guard = self.inner.lock();
        if let Some(f) = &self.merge_fn {
            let current_py = json_to_python(py, guard.clone())?;
            let delta_py = json_to_python(py, delta_val)?;
            let result = f.call1(py, (current_py, delta_py))?;
            *guard = pythonobj_to_json(py, &result)?;
        } else {
            *guard = json_add(guard.clone(), delta_val)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
        }
        Ok(())
    }

    /// Return the current accumulated value.
    pub fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let val = self.inner.lock().clone();
        json_to_python(py, val)
    }

    /// Reset the accumulator to its initial value.
    pub fn reset(&self) {
        *self.inner.lock() = self.initial.clone();
    }

    pub fn __repr__(&self) -> String {
        format!("Accumulator(value={})", self.inner.lock())
    }
}

impl PyAccumulator {
    pub fn new(initial: serde_json::Value, merge_fn: Option<Py<PyAny>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(initial.clone())),
            initial,
            merge_fn,
        }
    }
}

pub(crate) fn pythonobj_to_json(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<serde_json::Value> {
    let bound = obj.bind(py);
    // Bool must be checked before i64 because Python's bool is a subtype of int.
    if let Ok(b) = bound.extract::<bool>() {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Ok(v) = bound.extract::<i64>() {
        return Ok(serde_json::Value::Number(v.into()));
    }
    if let Ok(v) = bound.extract::<f64>() {
        let n = serde_json::Number::from_f64(v)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("non-finite float"))?;
        return Ok(serde_json::Value::Number(n));
    }
    if let Ok(s) = bound.extract::<String>() {
        return Ok(serde_json::Value::String(s));
    }
    // list — append style
    if let Ok(list) = bound.extract::<Vec<Py<PyAny>>>() {
        let arr: Vec<serde_json::Value> = list
            .iter()
            .map(|item| pythonobj_to_json(py, item))
            .collect::<PyResult<_>>()?;
        return Ok(serde_json::Value::Array(arr));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "Accumulator.add: unsupported type (use int, float, str, bool, or list)",
    ))
}

fn json_to_python(py: Python<'_>, val: serde_json::Value) -> PyResult<Py<PyAny>> {
    match val {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(pyo3::exceptions::PyValueError::new_err(
                    "number out of range",
                ))
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for v in arr {
                list.append(json_to_python(py, v)?)?;
            }
            Ok(list.into_any().unbind())
        }
        serde_json::Value::Object(_) => Err(pyo3::exceptions::PyTypeError::new_err(
            "dict not supported in Accumulator",
        )),
    }
}

fn json_add(a: serde_json::Value, b: serde_json::Value) -> Result<serde_json::Value, String> {
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            // Prefer integer; fall back to float.
            if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
                Ok(serde_json::Value::Number((xi + yi).into()))
            } else {
                let xf = x.as_f64().unwrap_or(0.0);
                let yf = y.as_f64().unwrap_or(0.0);
                let n =
                    serde_json::Number::from_f64(xf + yf).ok_or("non-finite float after add")?;
                Ok(serde_json::Value::Number(n))
            }
        }
        (serde_json::Value::Array(mut av), serde_json::Value::Array(bv)) => {
            av.extend(bv);
            Ok(serde_json::Value::Array(av))
        }
        (serde_json::Value::Array(mut av), single) => {
            av.push(single);
            Ok(serde_json::Value::Array(av))
        }
        (serde_json::Value::String(mut s), serde_json::Value::String(t)) => {
            s.push_str(&t);
            Ok(serde_json::Value::String(s))
        }
        _ => Err(
            "Accumulator.add: incompatible types (both must be numeric, list, or string)"
                .to_string(),
        ),
    }
}
