//! UDF execution backends — Python (via PyO3) and JavaScript (via QuickJS).
//!
//! Both backends implement the [`UdfExecutor`] trait, which executes a single
//! pipeline op over JSON-encoded partition data. Workers embed both runtimes so
//! they can evaluate UDF payloads sent by drivers:
//!
//! - Python: driver `pickle.dumps(lambda)` → worker eval via embedded CPython
//! - JavaScript: driver `fn.toString()` → worker eval via embedded QuickJS
//!
//! # Feature flags
//!
//! - `python-udf` — enables [`PythonUdfExecutor`] (requires PyO3 with `auto-initialize`)
//! - `js-udf`     — enables [`JsUdfExecutor`] (requires rquickjs)

use atomic_data::distributed::UdfAction;

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Single-op UDF executor.
///
/// Receives the action kind, raw payload bytes (serde_json-encoded spec), and
/// raw data bytes (JSON-encoded `Vec` of partition elements). Returns the
/// transformed partition as JSON bytes, or an error string.
pub trait UdfExecutor: Send + Sync {
    fn execute(&self, action: UdfAction, payload: &[u8], data: &[u8]) -> Result<Vec<u8>, String>;
}

// ── Python UDF ────────────────────────────────────────────────────────────────

/// Executes Python UDF pipeline ops via PyO3 (embedded CPython).
///
/// Requires the `python-udf` crate feature.
pub struct PythonUdfExecutor;

#[cfg(feature = "python-udf")]
impl UdfExecutor for PythonUdfExecutor {
    fn execute(&self, action: UdfAction, payload: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
        use atomic_data::distributed::PythonUdfPayload;
        use pyo3::prelude::*;

        let spec: PythonUdfPayload = Self::decode_payload(payload)?;

        Python::attach(|py| {
            let pickle = Self::import_pickle(py)?;
            let json_mod = Self::import_json(py)?;
            let builtins = Self::import_builtins(py)?;

            let fn_obj = Self::unpickle_fn(py, &pickle, &spec.fn_bytes)?;
            let items = Self::decode_data(py, &json_mod, data)?;
            let result =
                Self::apply_action(py, &builtins, &pickle, action, &fn_obj, &items, &spec)?;
            Self::encode_result(py, &json_mod, result)
        })
    }
}

#[cfg(feature = "python-udf")]
impl PythonUdfExecutor {
    fn decode_payload(
        payload: &[u8],
    ) -> Result<atomic_data::distributed::PythonUdfPayload, String> {
        serde_json::from_slice(payload).map_err(|e| format!("python_udf payload decode: {e}"))
    }

    fn import_pickle<'py>(
        py: pyo3::Python<'py>,
    ) -> Result<pyo3::Bound<'py, pyo3::types::PyModule>, String> {
        pyo3::types::PyModule::import(py, "pickle").map_err(|e| e.to_string())
    }

    fn import_json<'py>(
        py: pyo3::Python<'py>,
    ) -> Result<pyo3::Bound<'py, pyo3::types::PyModule>, String> {
        pyo3::types::PyModule::import(py, "json").map_err(|e| e.to_string())
    }

    fn import_builtins<'py>(
        py: pyo3::Python<'py>,
    ) -> Result<pyo3::Bound<'py, pyo3::types::PyModule>, String> {
        pyo3::types::PyModule::import(py, "builtins").map_err(|e| e.to_string())
    }

    fn unpickle_fn<'py>(
        py: pyo3::Python<'py>,
        pickle: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_bytes: &[u8],
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::PyAnyMethods;
        use pyo3::types::PyBytes;
        let bytes = PyBytes::new(py, fn_bytes);
        pickle
            .call_method1("loads", (bytes,))
            .map_err(|e| format!("pickle.loads(fn): {e}"))
    }

    fn decode_data<'py>(
        py: pyo3::Python<'py>,
        json_mod: &pyo3::Bound<'py, pyo3::types::PyModule>,
        data: &[u8],
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::PyAnyMethods;
        let json_str = std::str::from_utf8(data).map_err(|e| format!("data utf8: {e}"))?;
        json_mod
            .call_method1("loads", (json_str,))
            .map_err(|e| format!("json.loads: {e}"))
    }

    fn encode_result<'py>(
        _py: pyo3::Python<'py>,
        json_mod: &pyo3::Bound<'py, pyo3::types::PyModule>,
        result: pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<Vec<u8>, String> {
        use pyo3::types::PyAnyMethods;
        let result_json = json_mod
            .call_method1("dumps", (&result,))
            .map_err(|e| format!("json.dumps: {e}"))?;
        let result_str: String = result_json
            .extract()
            .map_err(|e| format!("extract result_json: {e}"))?;
        Ok(result_str.into_bytes())
    }

    fn apply_action<'py>(
        py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        pickle: &pyo3::Bound<'py, pyo3::types::PyModule>,
        action: UdfAction,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
        spec: &atomic_data::distributed::PythonUdfPayload,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::{PyAnyMethods, PyList};
        match action {
            UdfAction::Map => Self::apply_map(py, builtins, fn_obj, items),
            UdfAction::Filter => Self::apply_filter(py, builtins, fn_obj, items),
            UdfAction::FlatMap => Self::apply_flat_map(py, builtins, fn_obj, items),
            UdfAction::MapValues => Self::apply_map_values(py, builtins, fn_obj, items),
            UdfAction::FlatMapValues => Self::apply_flat_map_values(py, builtins, fn_obj, items),
            UdfAction::KeyBy => Self::apply_key_by(py, builtins, fn_obj, items),
            UdfAction::Reduce => Self::apply_reduce(py, builtins, fn_obj, items),
            UdfAction::Fold => {
                Self::apply_fold(py, builtins, pickle, fn_obj, items, &spec.zero_bytes)
            }
        }
    }

    fn apply_map<'py>(
        _py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::PyAnyMethods;
        let mapped = builtins
            .call_method1("map", (fn_obj, items))
            .map_err(|e| format!("builtins.map: {e}"))?;
        builtins
            .call_method1("list", (mapped,))
            .map_err(|e| format!("list(map(...)): {e}"))
    }

    fn apply_filter<'py>(
        _py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::PyAnyMethods;
        let filtered = builtins
            .call_method1("filter", (fn_obj, items))
            .map_err(|e| format!("builtins.filter: {e}"))?;
        builtins
            .call_method1("list", (filtered,))
            .map_err(|e| format!("list(filter(...)): {e}"))
    }

    fn apply_flat_map<'py>(
        _py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::PyAnyMethods;
        let itertools =
            pyo3::types::PyModule::import(_py, "itertools").map_err(|e| e.to_string())?;
        let mapped = builtins
            .call_method1("map", (fn_obj, items))
            .map_err(|e| format!("builtins.map (flat_map): {e}"))?;
        let chain_cls = itertools.getattr("chain").map_err(|e| e.to_string())?;
        let chained = chain_cls
            .call_method1("from_iterable", (mapped,))
            .map_err(|e| format!("chain.from_iterable: {e}"))?;
        builtins
            .call_method1("list", (chained,))
            .map_err(|e| format!("list(flat_map(...)): {e}"))
    }

    fn apply_map_values<'py>(
        py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::{PyAnyMethods, PyList, PyListMethods};
        let result_list = PyList::empty(py);
        let pairs = builtins
            .call_method1("list", (items,))
            .map_err(|e| format!("list(items) for map_values: {e}"))?;
        for pair in pairs.try_iter().map_err(|e| format!("iter pairs: {e}"))? {
            let pair = pair.map_err(|e| format!("pair iter: {e}"))?;
            let key = pair.get_item(0).map_err(|e| format!("pair[0]: {e}"))?;
            let val = pair.get_item(1).map_err(|e| format!("pair[1]: {e}"))?;
            let new_val = fn_obj.call1((val,)).map_err(|e| format!("fn(val): {e}"))?;
            let new_pair =
                PyList::new(py, [key, new_val]).map_err(|e| format!("new_pair list: {e}"))?;
            result_list
                .append(new_pair)
                .map_err(|e| format!("append: {e}"))?;
        }
        Ok(result_list.into_any())
    }

    fn apply_flat_map_values<'py>(
        py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::{PyAnyMethods, PyList, PyListMethods};
        let result_list = PyList::empty(py);
        let pairs = builtins
            .call_method1("list", (items,))
            .map_err(|e| format!("list(items) for flat_map_values: {e}"))?;
        for pair in pairs.try_iter().map_err(|e| format!("iter pairs: {e}"))? {
            let pair = pair.map_err(|e| format!("pair iter: {e}"))?;
            let key = pair.get_item(0).map_err(|e| format!("pair[0]: {e}"))?;
            let val = pair.get_item(1).map_err(|e| format!("pair[1]: {e}"))?;
            let sub_iter = fn_obj.call1((val,)).map_err(|e| format!("fn(val): {e}"))?;
            for v in sub_iter.try_iter().map_err(|e| format!("iter sub: {e}"))? {
                let v = v.map_err(|e| format!("sub iter val: {e}"))?;
                let new_pair =
                    PyList::new(py, [key.clone(), v]).map_err(|e| format!("new_pair list: {e}"))?;
                result_list
                    .append(new_pair)
                    .map_err(|e| format!("append: {e}"))?;
            }
        }
        Ok(result_list.into_any())
    }

    fn apply_key_by<'py>(
        py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::{PyAnyMethods, PyList};
        let result_list = PyList::empty(py);
        let elems = builtins
            .call_method1("list", (items,))
            .map_err(|e| format!("list(items) for key_by: {e}"))?;
        for elem in elems.try_iter().map_err(|e| format!("iter elems: {e}"))? {
            use pyo3::types::PyListMethods;

            let elem = elem.map_err(|e| format!("elem iter: {e}"))?;
            let key = fn_obj
                .call1((elem.clone(),))
                .map_err(|e| format!("fn(elem): {e}"))?;
            let pair =
                PyList::new(py, [key, elem]).map_err(|e| format!("key_by pair list: {e}"))?;
            result_list
                .append(pair)
                .map_err(|e| format!("append: {e}"))?;
        }
        Ok(result_list.into_any())
    }

    fn apply_reduce<'py>(
        py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::{PyAnyMethods, PyList, PyListMethods};
        let elems = builtins
            .call_method1("list", (items,))
            .map_err(|e| format!("list(items) for reduce: {e}"))?;
        let length: usize = builtins
            .call_method1("len", (&elems,))
            .and_then(|l| l.extract::<usize>())
            .map_err(|e| format!("len(items): {e}"))?;
        if length == 0 {
            return Ok(PyList::empty(py).into_any());
        }
        let functools =
            pyo3::types::PyModule::import(py, "functools").map_err(|e| e.to_string())?;
        let scalar = functools
            .call_method1("reduce", (fn_obj, &elems))
            .map_err(|e| format!("functools.reduce: {e}"))?;
        let result_list = PyList::empty(py);
        result_list
            .append(&scalar)
            .map_err(|e| format!("reduce append: {e}"))?;
        Ok(result_list.into_any())
    }

    fn apply_fold<'py>(
        py: pyo3::Python<'py>,
        builtins: &pyo3::Bound<'py, pyo3::types::PyModule>,
        pickle: &pyo3::Bound<'py, pyo3::types::PyModule>,
        fn_obj: &pyo3::Bound<'py, pyo3::PyAny>,
        items: &pyo3::Bound<'py, pyo3::PyAny>,
        zero_bytes: &[u8],
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>, String> {
        use pyo3::types::{PyAnyMethods, PyBytes, PyList, PyListMethods};
        let zero_py = PyBytes::new(py, zero_bytes);
        let zero = pickle
            .call_method1("loads", (zero_py,))
            .map_err(|e| format!("pickle.loads(zero): {e}"))?;
        let functools =
            pyo3::types::PyModule::import(py, "functools").map_err(|e| e.to_string())?;
        let scalar = functools
            .call_method1("reduce", (fn_obj, items, &zero))
            .map_err(|e| format!("functools.reduce (fold): {e}"))?;
        let result_list = PyList::empty(py);
        result_list
            .append(&scalar)
            .map_err(|e| format!("fold append scalar: {e}"))?;
        Ok(result_list.into_any())
    }
}

#[cfg(not(feature = "python-udf"))]
impl UdfExecutor for PythonUdfExecutor {
    fn execute(
        &self,
        _action: UdfAction,
        _payload: &[u8],
        _data: &[u8],
    ) -> Result<Vec<u8>, String> {
        Err(
            "python-udf feature not enabled in this build; use the atomic-worker binary"
                .to_string(),
        )
    }
}

// ── JavaScript UDF ────────────────────────────────────────────────────────────

/// Executes JavaScript UDF pipeline ops via an embedded QuickJS runtime.
///
/// Workers receive a [`JsUdfPayload`] containing the function's source string
/// (captured by `fn.toString()` in the napi module). QuickJS evaluates it and
/// returns JSON-encoded results — identical to how Python workers evaluate
/// pickled lambdas via PyO3.
///
/// Requires the `js-udf` crate feature (enabled automatically via `udf`).
///
/// [`JsUdfPayload`]: atomic_data::distributed::JsUdfPayload
pub struct JsUdfExecutor;

#[cfg(feature = "js-udf")]
impl UdfExecutor for JsUdfExecutor {
    fn execute(&self, action: UdfAction, payload: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
        let spec = Self::decode_payload(payload)?;
        let data_str = std::str::from_utf8(data).map_err(|e| format!("data utf8: {e}"))?;
        let script = Self::build_script(action, &spec, data_str);
        let result = Self::eval_script(script)?;
        Ok(result.into_bytes())
    }
}

#[cfg(feature = "js-udf")]
impl JsUdfExecutor {
    fn decode_payload(payload: &[u8]) -> Result<atomic_data::distributed::JsUdfPayload, String> {
        serde_json::from_slice(payload).map_err(|e| format!("js_udf payload decode: {e}"))
    }

    /// Build the QuickJS eval script for the given action.
    ///
    /// All data encoding/decoding stays inside the script — the QuickJS context
    /// handles only strings, so no Rust↔JS type marshalling is needed.
    fn build_script(
        action: UdfAction,
        spec: &atomic_data::distributed::JsUdfPayload,
        data_str: &str,
    ) -> String {
        match action {
            UdfAction::Map => format!(
                "JSON.stringify(JSON.parse({data:?}).map({fn_src}))",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::Filter => format!(
                "JSON.stringify(JSON.parse({data:?}).filter({fn_src}))",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::FlatMap => format!(
                "JSON.stringify(JSON.parse({data:?}).flatMap({fn_src}))",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::MapValues => format!(
                "JSON.stringify(JSON.parse({data:?}).map((pair) => [pair[0], ({fn_src})(pair[1])]))",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::FlatMapValues => format!(
                "JSON.stringify(JSON.parse({data:?}).flatMap((pair) => ({fn_src})(pair[1]).map((v) => [pair[0], v])))",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::KeyBy => format!(
                "JSON.stringify(JSON.parse({data:?}).map((x) => [({fn_src})(x), x]))",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::Reduce => format!(
                "(function(){{ var a = JSON.parse({data:?}); return a.length === 0 ? '[]' : JSON.stringify([a.reduce({fn_src})]); }})()",
                data = data_str,
                fn_src = spec.fn_source,
            ),
            UdfAction::Fold => format!(
                "JSON.stringify([JSON.parse({data:?}).reduce({fn_src}, {zero})])",
                data = data_str,
                fn_src = spec.fn_source,
                zero = spec.zero_json,
            ),
        }
    }

    /// Evaluate a JS script in an isolated QuickJS runtime, returning the result string.
    fn eval_script(script: String) -> Result<String, String> {
        let rt = rquickjs::Runtime::new().map_err(|e| format!("QuickJS runtime: {e:?}"))?;
        rt.set_memory_limit(256 * 1024 * 1024);
        let ctx = rquickjs::Context::full(&rt).map_err(|e| format!("QuickJS context: {e:?}"))?;
        ctx.with(|ctx| ctx.eval(script))
            .map_err(|e| format!("JS eval: {e:?}"))
    }
}

#[cfg(not(feature = "js-udf"))]
impl UdfExecutor for JsUdfExecutor {
    fn execute(
        &self,
        _action: UdfAction,
        _payload: &[u8],
        _data: &[u8],
    ) -> Result<Vec<u8>, String> {
        Err("js-udf feature not enabled; build atomic-worker with the udf feature".to_string())
    }
}

