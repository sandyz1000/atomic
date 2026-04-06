//! External UDF execution backends — Python (via PyO3) and JavaScript (via QuickJS).
//!
//! These are invoked by [`crate::native_backend::NativeBackend`] when the pipeline op
//! action is [`TaskAction::PythonUdf`] or [`TaskAction::JavaScriptUdf`].
//!
//! Both backends operate on **JSON-encoded partition data**: the driver encodes each
//! partition as a JSON array of elements, and the worker returns a JSON array of results.
//!
//! ## Supported actions
//!
//! | Action          | Input format          | Output format              |
//! |-----------------|-----------------------|----------------------------|
//! | `map`           | `[elem, ...]`         | `[fn(elem), ...]`          |
//! | `filter`        | `[elem, ...]`         | `[elem, ...]` (kept only)  |
//! | `flat_map`      | `[elem, ...]`         | `[v, ...]` (flattened)     |
//! | `map_values`    | `[[k,v], ...]`        | `[[k, fn(v)], ...]`        |
//! | `flat_map_values` | `[[k,v], ...]`      | `[[k,v'], ...]` (flattened)|
//! | `key_by`        | `[elem, ...]`         | `[[fn(e),e], ...]`         |
//! | `reduce`        | `[elem, ...]`         | `[scalar]` or `[]`         |
//! | `fold`          | `[elem, ...]`         | `[scalar]`                 |
//!
//! # Feature flags
//!
//! - `python-udf` — enables [`execute_python_udf`] (requires PyO3 with `auto-initialize`)
//! - `js-udf` — enables [`execute_js_udf`] (requires rquickjs)
//! - `udf` — enables both (convenience alias for the `atomic-worker` binary)
//!
//! When a feature is NOT enabled the corresponding function returns an error string so
//! that `NativeBackend::execute` can return a `FatalFailure` result rather than panic.

use atomic_data::distributed::{JsUdfPayload, PythonUdfPayload};

// ── Python UDF ────────────────────────────────────────────────────────────────

/// Execute a Python UDF pipeline op.
///
/// Requires the `python-udf` crate feature (embeds a Python interpreter via PyO3).
///
/// - `payload`: serde_json-encoded [`PythonUdfPayload`]
/// - `data`: JSON-encoded `Vec` of input elements
///
/// Returns JSON-encoded `Vec` of output elements, or a single-element JSON array
/// `[scalar]` for `fold`/`reduce` (so the driver can combine partitions).
/// Returns `[]` for `reduce` on an empty partition.
#[cfg(feature = "python-udf")]
pub fn execute_python_udf(payload: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyList};
    #[allow(unused_imports)]
    use pyo3::types::PyAnyMethods;

    let spec: PythonUdfPayload = serde_json::from_slice(payload)
        .map_err(|e| format!("python_udf payload decode: {e}"))?;

    Python::attach(|py| -> Result<Vec<u8>, String> {
        // 1. Unpickle the callable
        let pickle = PyModule::import(py, "pickle").map_err(|e| e.to_string())?;
        let fn_bytes = PyBytes::new(py, &spec.fn_bytes);
        let fn_obj: Bound<'_, PyAny> = pickle
            .call_method1("loads", (fn_bytes,))
            .map_err(|e| format!("pickle.loads(fn): {e}"))?;

        // 2. Decode partition data from JSON → Python list
        let json_str =
            std::str::from_utf8(data).map_err(|e| format!("data utf8: {e}"))?;
        let json_mod = PyModule::import(py, "json").map_err(|e| e.to_string())?;
        let items: Bound<'_, PyAny> = json_mod
            .call_method1("loads", (json_str,))
            .map_err(|e| format!("json.loads: {e}"))?;

        let builtins = PyModule::import(py, "builtins").map_err(|e| e.to_string())?;

        // 3. Apply UDF according to action
        let result: Bound<'_, PyAny> = match spec.action.as_str() {
            "map" => {
                let mapped: Bound<'_, PyAny> = builtins
                    .call_method1("map", (&fn_obj, &items))
                    .map_err(|e| format!("builtins.map: {e}"))?;
                builtins
                    .call_method1("list", (mapped,))
                    .map_err(|e| format!("list(map(...)): {e}"))?
            }
            "filter" => {
                let filtered: Bound<'_, PyAny> = builtins
                    .call_method1("filter", (&fn_obj, &items))
                    .map_err(|e| format!("builtins.filter: {e}"))?;
                builtins
                    .call_method1("list", (filtered,))
                    .map_err(|e| format!("list(filter(...)): {e}"))?
            }
            "flat_map" => {
                // list(chain.from_iterable(map(fn, items)))
                let itertools = PyModule::import(py, "itertools").map_err(|e| e.to_string())?;
                let mapped: Bound<'_, PyAny> = builtins
                    .call_method1("map", (&fn_obj, &items))
                    .map_err(|e| format!("builtins.map (flat_map): {e}"))?;
                let chain_cls: Bound<'_, PyAny> = itertools
                    .getattr("chain")
                    .map_err(|e| e.to_string())?;
                let chained: Bound<'_, PyAny> = chain_cls
                    .call_method1("from_iterable", (mapped,))
                    .map_err(|e| format!("chain.from_iterable: {e}"))?;
                builtins
                    .call_method1("list", (chained,))
                    .map_err(|e| format!("list(flat_map(...)): {e}"))?
            }
            "map_values" => {
                // items is [[k, v], ...] — apply fn to v, return [[k, fn(v)], ...]
                let result_list = PyList::empty(py);
                let pairs: Bound<'_, PyAny> = builtins
                    .call_method1("list", (&items,))
                    .map_err(|e| format!("list(items) for map_values: {e}"))?;
                for pair in pairs.try_iter().map_err(|e| format!("iter pairs: {e}"))? {
                    let pair = pair.map_err(|e| format!("pair iter: {e}"))?;
                    let key = pair.get_item(0).map_err(|e| format!("pair[0]: {e}"))?;
                    let val = pair.get_item(1).map_err(|e| format!("pair[1]: {e}"))?;
                    let new_val: Bound<'_, PyAny> = fn_obj
                        .call1((val,))
                        .map_err(|e| format!("fn(val): {e}"))?;
                    let new_pair = PyList::new(py, [key, new_val])
                        .map_err(|e| format!("new_pair list: {e}"))?;
                    result_list.append(new_pair).map_err(|e| format!("append: {e}"))?;
                }
                result_list.into_any()
            }
            "flat_map_values" => {
                // items is [[k, v], ...] — apply fn to v (returns iterable), flatten, return [[k, v'], ...]
                let result_list = PyList::empty(py);
                let pairs: Bound<'_, PyAny> = builtins
                    .call_method1("list", (&items,))
                    .map_err(|e| format!("list(items) for flat_map_values: {e}"))?;
                for pair in pairs.try_iter().map_err(|e| format!("iter pairs: {e}"))? {
                    let pair = pair.map_err(|e| format!("pair iter: {e}"))?;
                    let key = pair.get_item(0).map_err(|e| format!("pair[0]: {e}"))?;
                    let val = pair.get_item(1).map_err(|e| format!("pair[1]: {e}"))?;
                    let sub_iter: Bound<'_, PyAny> = fn_obj
                        .call1((val,))
                        .map_err(|e| format!("fn(val): {e}"))?;
                    for v in sub_iter.try_iter().map_err(|e| format!("iter sub: {e}"))? {
                        let v = v.map_err(|e| format!("sub iter val: {e}"))?;
                        let new_pair = PyList::new(py, [key.clone(), v])
                            .map_err(|e| format!("new_pair list: {e}"))?;
                        result_list.append(new_pair).map_err(|e| format!("append: {e}"))?;
                    }
                }
                result_list.into_any()
            }
            "key_by" => {
                // items is [elem, ...] — return [[fn(elem), elem], ...]
                let result_list = PyList::empty(py);
                let elems: Bound<'_, PyAny> = builtins
                    .call_method1("list", (&items,))
                    .map_err(|e| format!("list(items) for key_by: {e}"))?;
                for elem in elems.try_iter().map_err(|e| format!("iter elems: {e}"))? {
                    let elem = elem.map_err(|e| format!("elem iter: {e}"))?;
                    let key: Bound<'_, PyAny> = fn_obj
                        .call1((elem.clone(),))
                        .map_err(|e| format!("fn(elem): {e}"))?;
                    let pair = PyList::new(py, [key, elem])
                        .map_err(|e| format!("key_by pair list: {e}"))?;
                    result_list.append(pair).map_err(|e| format!("append: {e}"))?;
                }
                result_list.into_any()
            }
            "reduce" => {
                // Return [functools.reduce(fn, items)] if non-empty, else []
                let elems: Bound<'_, PyAny> = builtins
                    .call_method1("list", (&items,))
                    .map_err(|e| format!("list(items) for reduce: {e}"))?;
                let length: usize = builtins
                    .call_method1("len", (&elems,))
                    .and_then(|l| l.extract::<usize>())
                    .map_err(|e| format!("len(items): {e}"))?;
                if length == 0 {
                    PyList::empty(py).into_any()
                } else {
                    let functools = PyModule::import(py, "functools").map_err(|e| e.to_string())?;
                    let scalar: Bound<'_, PyAny> = functools
                        .call_method1("reduce", (&fn_obj, &elems))
                        .map_err(|e| format!("functools.reduce: {e}"))?;
                    let result_list = PyList::empty(py);
                    result_list.append(&scalar).map_err(|e| format!("reduce append: {e}"))?;
                    result_list.into_any()
                }
            }
            "fold" => {
                // functools.reduce(fn, items, zero) → scalar wrapped in [scalar]
                let zero_bytes = PyBytes::new(py, &spec.zero_bytes);
                let zero: Bound<'_, PyAny> = pickle
                    .call_method1("loads", (zero_bytes,))
                    .map_err(|e| format!("pickle.loads(zero): {e}"))?;
                let functools = PyModule::import(py, "functools").map_err(|e| e.to_string())?;
                let scalar: Bound<'_, PyAny> = functools
                    .call_method1("reduce", (&fn_obj, &items, &zero))
                    .map_err(|e| format!("functools.reduce: {e}"))?;
                // Wrap scalar in a list so JSON encoding is always an array
                let result_list = PyList::empty(py);
                result_list
                    .append(&scalar)
                    .map_err(|e| format!("fold append scalar: {e}"))?;
                result_list.into_any()
            }
            other => return Err(format!("python_udf: unknown action '{other}'")),
        };

        // 4. Re-encode result to JSON
        let result_json: Bound<'_, PyAny> = json_mod
            .call_method1("dumps", (&result,))
            .map_err(|e| format!("json.dumps: {e}"))?;
        let result_str: String = result_json
            .extract()
            .map_err(|e| format!("extract result_json: {e}"))?;
        Ok(result_str.into_bytes())
    })
}

#[cfg(not(feature = "python-udf"))]
pub fn execute_python_udf(_payload: &[u8], _data: &[u8]) -> Result<Vec<u8>, String> {
    Err("python-udf feature not enabled in this build; use the atomic-worker binary".to_string())
}

// ── JavaScript UDF ────────────────────────────────────────────────────────────

/// Execute a JavaScript UDF pipeline op.
///
/// Requires the `js-udf` crate feature (embeds QuickJS via rquickjs).
///
/// - `payload`: serde_json-encoded [`JsUdfPayload`]
/// - `data`: JSON-encoded `Vec` of input elements
///
/// Returns JSON-encoded `Vec` of output elements, or a single-element JSON array
/// `[scalar]` for `fold`/`reduce` (empty array `[]` for `reduce` on empty partition).
#[cfg(feature = "js-udf")]
pub fn execute_js_udf(payload: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    let spec: JsUdfPayload =
        serde_json::from_slice(payload).map_err(|e| format!("js_udf payload decode: {e}"))?;

    let data_str = std::str::from_utf8(data).map_err(|e| format!("data utf8: {e}"))?;

    // Build a self-contained JS expression that evaluates to a JSON string.
    let script = match spec.action.as_str() {
        "map" => format!(
            "JSON.stringify(JSON.parse({data:?}).map({fn_src}))",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "filter" => format!(
            "JSON.stringify(JSON.parse({data:?}).filter({fn_src}))",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "flat_map" => format!(
            "JSON.stringify(JSON.parse({data:?}).flatMap({fn_src}))",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "map_values" => format!(
            // items is [[k,v],...] — apply fn to v
            "JSON.stringify(JSON.parse({data:?}).map((pair) => [pair[0], ({fn_src})(pair[1])]))",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "flat_map_values" => format!(
            // items is [[k,v],...] — apply fn to v, flatten, tag each result with k
            "JSON.stringify(JSON.parse({data:?}).flatMap((pair) => ({fn_src})(pair[1]).map((v) => [pair[0], v])))",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "key_by" => format!(
            // items is [elem,...] — return [[fn(elem), elem],...]
            "JSON.stringify(JSON.parse({data:?}).map((x) => [({fn_src})(x), x]))",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "reduce" => format!(
            // Return [scalar] if non-empty, else []
            "(function(){{ var a = JSON.parse({data:?}); return a.length === 0 ? '[]' : JSON.stringify([a.reduce({fn_src})]); }})()",
            data = data_str,
            fn_src = spec.fn_source,
        ),
        "fold" => format!(
            "JSON.stringify([JSON.parse({data:?}).reduce({fn_src}, {zero})])",
            data = data_str,
            fn_src = spec.fn_source,
            zero = spec.zero_json,
        ),
        other => return Err(format!("js_udf: unknown action '{other}'")),
    };

    let rt = rquickjs::Runtime::new().map_err(|e| format!("QuickJS runtime: {e:?}"))?;
    rt.set_memory_limit(256 * 1024 * 1024);
    let ctx =
        rquickjs::Context::full(&rt).map_err(|e| format!("QuickJS context: {e:?}"))?;

    let result: String = ctx
        .with(|ctx| ctx.eval(script))
        .map_err(|e| format!("JS eval: {e:?}"))?;

    Ok(result.into_bytes())
}

#[cfg(not(feature = "js-udf"))]
pub fn execute_js_udf(_payload: &[u8], _data: &[u8]) -> Result<Vec<u8>, String> {
    Err("js-udf feature not enabled in this build; use the atomic-worker binary".to_string())
}
