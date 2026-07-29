//! Interpreter for a [`PyDStreamInner`] tree: walks parent chains to materialize one
//! batch, applying each [`PyStreamTransform`] node along the way. Used by both
//! synchronous (`run_one_batch`) and background-thread (`start`) execution in
//! [`PyStreamingContext`](crate::streaming::context::PyStreamingContext).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyIterator, PyList, PyTuple};

use super::dstream::{PyDStreamInner, PyStreamTransform};

pub(crate) fn compute_batch(
    py: Python<'_>,
    inner: &PyDStreamInner,
    state_store: &mut HashMap<String, Vec<u8>>,
) -> PyResult<Vec<Py<PyAny>>> {
    match inner {
        PyDStreamInner::Queue { queue } => {
            let mut q = queue.lock();
            Ok(q.pop_front().unwrap_or_default())
        }
        PyDStreamInner::Socket { .. } => Ok(vec![]),
        PyDStreamInner::File { directory } => {
            let mut lines: Vec<Py<PyAny>> = Vec::new();
            if let Ok(dir) = std::fs::read_dir(directory) {
                for entry in dir.flatten() {
                    if let Ok(content) = std::fs::read_to_string(entry.path()) {
                        for line in content.lines() {
                            lines.push(line.to_string().into_pyobject(py)?.into_any().unbind());
                        }
                    }
                }
            }
            Ok(lines)
        }
        PyDStreamInner::Transform { parent, op } => {
            let parent_elems = compute_batch(py, parent, state_store)?;
            apply_transform(py, parent_elems, op, state_store)
        }
        PyDStreamInner::Windowed {
            parent,
            window_ms,
            buffer,
        } => {
            let batch = compute_batch(py, parent, state_store)?;
            let now = Instant::now();
            let cutoff = Duration::from_millis(*window_ms);
            let mut buf = buffer.lock();
            buf.push_back((now, batch));
            buf.retain(|(ts, _)| now.duration_since(*ts) < cutoff);
            Ok(buf
                .iter()
                .flat_map(|(_, elems)| elems.iter().map(|e| e.clone_ref(py)))
                .collect())
        }
    }
}

/// Build a Python tuple `(a, b)` from two `Py<PyAny>`.
fn py_tuple2(py: Python<'_>, a: &Py<PyAny>, b: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    Ok(PyTuple::new(py, [a.bind(py), b.bind(py)])?
        .into_any()
        .unbind())
}

fn apply_transform(
    py: Python<'_>,
    elements: Vec<Py<PyAny>>,
    op: &PyStreamTransform,
    state_store: &mut HashMap<String, Vec<u8>>,
) -> PyResult<Vec<Py<PyAny>>> {
    match op {
        PyStreamTransform::Map(f) => elements
            .iter()
            .map(|e| f.call1(py, (e.bind(py),)))
            .collect(),

        PyStreamTransform::Filter(f) => elements
            .iter()
            .filter_map(
                |e| match f.call1(py, (e.bind(py),)).and_then(|r| r.is_truthy(py)) {
                    Ok(true) => Some(Ok(e.clone_ref(py))),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect(),

        PyStreamTransform::FlatMap(f) => {
            let mut result = Vec::new();
            for e in &elements {
                let iter_result = f.call1(py, (e.bind(py),))?;
                let iter = PyIterator::from_object(iter_result.bind(py))?;
                for item in iter {
                    result.push(item?.unbind());
                }
            }
            Ok(result)
        }

        PyStreamTransform::ReduceByKey(f) => {
            let mut groups: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::new();
            for e in &elements {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?.unbind();
                let v = pair.get_item(1)?.unbind();
                let mut found = false;
                for (ek, acc) in &mut groups {
                    if ek.bind(py).eq(k.bind(py))? {
                        *acc = f.call1(py, (acc.bind(py), v.bind(py)))?;
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, v));
                }
            }
            groups.iter().map(|(k, v)| py_tuple2(py, k, v)).collect()
        }

        PyStreamTransform::GroupByKey => {
            let mut groups: Vec<(Py<PyAny>, Vec<Py<PyAny>>)> = Vec::new();
            for e in &elements {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?.unbind();
                let v = pair.get_item(1)?.unbind();
                let mut found = false;
                for (ek, vals) in &mut groups {
                    if ek.bind(py).eq(k.bind(py))? {
                        vals.push(v.clone_ref(py));
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, vec![v]));
                }
            }
            groups
                .iter()
                .map(|(k, vals)| {
                    let vlist: Py<PyAny> = PyList::new(py, vals.iter().map(|v| v.bind(py)))?
                        .into_any()
                        .unbind();
                    py_tuple2(py, k, &vlist)
                })
                .collect()
        }

        PyStreamTransform::Join(right_inner) => {
            let mut dummy: HashMap<String, Vec<u8>> = HashMap::new();
            let right_elems = compute_batch(py, right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le.bind(py).cast::<PyTuple>()?;
                let lk = lp.get_item(0)?.unbind();
                let lv = lp.get_item(1)?.unbind();
                for re in &right_elems {
                    let rp = re.bind(py).cast::<PyTuple>()?;
                    let rk = rp.get_item(0)?.unbind();
                    let rv = rp.get_item(1)?.unbind();
                    if lk.bind(py).eq(rk.bind(py))? {
                        let vpair = py_tuple2(py, &lv, &rv)?;
                        result.push(py_tuple2(py, &lk, &vpair)?);
                    }
                }
            }
            Ok(result)
        }

        PyStreamTransform::LeftOuterJoin(right_inner) => {
            let mut dummy: HashMap<String, Vec<u8>> = HashMap::new();
            let right_elems = compute_batch(py, right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le.bind(py).cast::<PyTuple>()?;
                let lk = lp.get_item(0)?.unbind();
                let lv = lp.get_item(1)?.unbind();
                let mut matched = false;
                for re in &right_elems {
                    let rp = re.bind(py).cast::<PyTuple>()?;
                    let rk = rp.get_item(0)?.unbind();
                    let rv = rp.get_item(1)?.unbind();
                    if lk.bind(py).eq(rk.bind(py))? {
                        let vpair = py_tuple2(py, &lv, &rv)?;
                        result.push(py_tuple2(py, &lk, &vpair)?);
                        matched = true;
                    }
                }
                if !matched {
                    let none_val: Py<PyAny> = py.None();
                    let vpair = py_tuple2(py, &lv, &none_val)?;
                    result.push(py_tuple2(py, &lk, &vpair)?);
                }
            }
            Ok(result)
        }

        PyStreamTransform::UpdateStateByKey(f) => {
            let pickle = PyModule::import(py, "pickle")?;

            // Group new values by key (repr as map key).
            let mut new_vals: Vec<(String, Py<PyAny>, Vec<Py<PyAny>>)> = Vec::new();
            for e in &elements {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?;
                let v = pair.get_item(1)?.unbind();
                let k_str = k.repr()?.to_string();
                let mut found = false;
                for (ks, _, vals) in &mut new_vals {
                    if *ks == k_str {
                        vals.push(v.clone_ref(py));
                        found = true;
                        break;
                    }
                }
                if !found {
                    new_vals.push((k_str, k.unbind(), vec![v]));
                }
            }

            // All keys: new keys + state-only keys.
            let mut all_keys: Vec<(String, Py<PyAny>)> = new_vals
                .iter()
                .map(|(ks, k, _)| (ks.clone(), k.clone_ref(py)))
                .collect();
            for k_str in state_store.keys() {
                if !new_vals.iter().any(|(ks, _, _)| ks == k_str) {
                    let py_k: Py<PyAny> = k_str.clone().into_pyobject(py)?.into_any().unbind();
                    all_keys.push((k_str.clone(), py_k));
                }
            }

            let mut result = Vec::new();
            for (key_str, key_obj) in &all_keys {
                let new_values_for_key: &[Py<PyAny>] = new_vals
                    .iter()
                    .find(|(ks, _, _)| ks == key_str)
                    .map(|(_, _, v)| v.as_slice())
                    .unwrap_or(&[]);
                let old_state: Py<PyAny> = if let Some(state_bytes) = state_store.get(key_str) {
                    pickle
                        .call_method1("loads", (PyBytes::new(py, state_bytes),))?
                        .unbind()
                } else {
                    py.None()
                };
                let new_vals_list = PyList::new(py, new_values_for_key.iter().map(|v| v.bind(py)))?;
                let new_state: Py<PyAny> =
                    f.call1(py, (new_vals_list.into_any(), old_state.bind(py)))?;
                if new_state.is_none(py) {
                    state_store.remove(key_str);
                } else {
                    let serialized: Vec<u8> = pickle
                        .call_method1("dumps", (new_state.bind(py),))?
                        .extract()?;
                    state_store.insert(key_str.clone(), serialized);
                    result.push(py_tuple2(py, key_obj, &new_state)?);
                }
            }
            Ok(result)
        }

        PyStreamTransform::MapValues(f) => elements
            .iter()
            .map(|e| {
                let pair = e.bind(py).cast::<PyTuple>()?;
                let k = pair.get_item(0)?.unbind();
                let new_v: Py<PyAny> = f.call1(py, (pair.get_item(1)?,))?;
                py_tuple2(py, &k, &new_v)
            })
            .collect(),
    }
}

pub(crate) fn call_output_fn(
    py: Python<'_>,
    func: &Py<PyAny>,
    elements: Vec<Py<PyAny>>,
) -> PyResult<()> {
    let list = PyList::new(py, elements.iter().map(|e| e.bind(py)))?;
    func.call1(py, (list.into_any(),))?;
    Ok(())
}
