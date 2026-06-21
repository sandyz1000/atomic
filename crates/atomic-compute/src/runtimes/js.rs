use crate::error::{ComputeError, ComputeResult};
use atomic_data::distributed::JsTaskPayload;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum JsTaskError {
    #[error("payload decode: {0}")]
    PayloadDecode(#[from] serde_json::Error),
    #[error("partition data is not valid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("JS runtime error: {0}")]
    Eval(String),
}

impl From<JsTaskError> for ComputeError {
    fn from(e: JsTaskError) -> Self {
        ComputeError::InvalidPayload(e.to_string())
    }
}
/// Thread-local V8 JavaScript runtime for task execution (deno_core).
///
/// One [`deno_core::JsRuntime`] is kept per tokio blocking thread.  Because V8
/// isolates are `!Send`, thread-local storage is the correct placement and also
/// avoids any synchronisation cost on the hot path.
///
/// Each thread pays the V8 cold-start cost exactly once; subsequent tasks on
/// the same thread reuse the warm runtime at negligible overhead.
///
/// # Function compilation cache
///
/// task functions (e.g. `"x => x * 2"`) are compiled into `globalThis.__f<hash>`
/// on first use and referenced by name on all subsequent partitions.  V8 parses
/// each distinct function expression exactly once per thread rather than once per
/// partition call.  The per-thread cache tracks which functions have been compiled.
///
/// # Context capture
///
/// The caller may pass a `context_json` string (a JSON object).  It is injected
/// as `globalThis.__ctx` before the task script runs, allowing functions written
/// as `(x, ctx) => x > ctx.threshold` to access driver-side values without
/// closure serialization.
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static JS_RT: RefCell<Option<deno_core::JsRuntime>> = RefCell::new(None);
    /// FxHash64(fn_source) → JS globalThis identifier (e.g. "__f0a1b2c3d4e5f678").
    /// Entries persist for the thread's lifetime; valid as long as JS_RT is Some.
    static FN_CACHE: RefCell<HashMap<u64, String>> = RefCell::new(HashMap::new());
}

/// [`OpDispatcher`] for `TaskRuntime::JavaScript` ops.
///
/// Decodes the [`JsTaskPayload`] from `op.payload` and evaluates the partition
/// through the thread-local V8 runtime.
///
/// V8 isolates are `!Send`; the runtime lives in thread-local storage by
/// architectural necessity. Fields are added here when per-instance V8 config
/// (memory limits, module resolution) is needed.
pub(crate) struct JsDispatcher {
    // V8 isolates are !Send; runtime lives in thread-local storage by architectural necessity.
}

impl JsDispatcher {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Access the thread-local V8 runtime, initialising it on first use.
    pub(crate) fn with_runtime<F, R>(f: F) -> R
    where
        F: FnOnce(&mut deno_core::JsRuntime) -> R,
    {
        JS_RT.with(|cell| {
            let mut borrow = cell.borrow_mut();
            if borrow.is_none() {
                *borrow = Some(deno_core::JsRuntime::new(
                    deno_core::RuntimeOptions::default(),
                ));
            }
            f(borrow
                .as_mut()
                .expect("JS runtime was just initialised; cannot be None"))
        })
    }

    /// Compile `fn_source` into `globalThis.__f<hash>` on first call per thread.
    ///
    /// Returns the JS identifier to use in place of the raw function source.
    /// On subsequent calls with the same source, returns the cached identifier
    /// without re-running any script.
    fn ensure_fn_compiled(
        rt: &mut deno_core::JsRuntime,
        fn_source: &str,
    ) -> Result<String, JsTaskError> {
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        fn_source.hash(&mut hasher);
        let key = hasher.finish();

        if let Some(js_name) = FN_CACHE.with(|c| c.borrow().get(&key).cloned()) {
            return Ok(js_name);
        }

        let js_name = format!("__f{key:016x}");
        rt.execute_script(
            "<fn_init>",
            format!("globalThis.{js_name} = ({fn_source});"),
        )
        .map_err(|e| JsTaskError::Eval(e.to_string()))?;

        FN_CACHE.with(|c| c.borrow_mut().insert(key, js_name.clone()));
        Ok(js_name)
    }

    /// Evaluate a task partition operation using the thread-local V8 runtime.
    ///
    /// - `fn_source` — JavaScript function expression (from `fn.toString()` on the driver).
    /// - `context_json` — optional JSON object injected as `globalThis.__ctx`.
    /// - `data_str` — JSON-encoded `Vec` of partition elements (the current partition).
    ///
    /// Returns JSON-encoded result bytes.
    fn eval_partition(
        &self,
        fn_source: &str,
        context_json: Option<&str>,
        data_str: &str,
    ) -> Result<Vec<u8>, JsTaskError> {
        Self::with_runtime(|rt| {
            let ctx_js = match context_json {
                Some(ctx) => format!("globalThis.__ctx = {ctx};"),
                None => "globalThis.__ctx = undefined;".to_string(),
            };
            rt.execute_script("<ctx>", ctx_js)
                .map_err(|e| JsTaskError::Eval(e.to_string()))?;

            let fn_var = Self::ensure_fn_compiled(rt, fn_source)?;

            let script = format!("JSON.stringify(({})({}));", fn_var, data_str);
            let result = rt
                .execute_script("<task>", script)
                .map_err(|e| JsTaskError::Eval(e.to_string()))?;

            // SAFETY: hs (ScopeStorage) is immediately shadowed by the PinnedRef below;
            // it stays on the stack and cannot be moved after this point.
            let context = rt.main_context();
            let mut hs = deno_core::v8::HandleScope::new(rt.v8_isolate());
            let mut hs = {
                let pinned = unsafe { std::pin::Pin::new_unchecked(&mut hs) };
                pinned.init()
            };
            let hs = &mut hs;
            let ctx_local = deno_core::v8::Local::new(&*hs, context);
            let mut cs = deno_core::v8::ContextScope::new(hs, ctx_local);
            let local = deno_core::v8::Local::new(&**cs, result);
            Ok(local.to_rust_string_lossy(&*cs).into_bytes())
        })
    }
}

/// Run a JS tool function against one JSON-text argument and return one JSON-text result.
///
/// `fn_source` is a JS function expression (`fn.toString()`-captured, same convention as
/// ordinary map/filter tasks); it receives the parsed `args_json` value as its single
/// argument. Used by `agent_step` tool dispatch (`atomic-nlq`'s `TOOL_CALL:` handling) for
/// tools resolved from `AgentStepPayload.resolved_tools`.
pub fn run_tool_call(fn_source: &str, args_json: &str) -> Result<String, String> {
    let bytes = JsDispatcher::new()
        .eval_partition(fn_source, None, args_json)
        .map_err(|e| e.to_string())?;
    String::from_utf8(bytes).map_err(|e| format!("JS tool result is not valid UTF-8: {e}"))
}

impl JsDispatcher {
    fn dispatch_impl(
        &self,
        op: &atomic_data::distributed::PipelineOp,
        data: &[u8],
    ) -> Result<Vec<u8>, JsTaskError> {
        let spec: JsTaskPayload = serde_json::from_slice(&op.payload)?;
        let data_str = std::str::from_utf8(data)?;
        self.eval_partition(&spec.fn_source, spec.context_json.as_deref(), data_str)
    }
}

impl crate::runtimes::OpDispatcher for JsDispatcher {
    fn dispatch(
        &self,
        op: &atomic_data::distributed::PipelineOp,
        _partition_id: usize,
        data: &[u8],
    ) -> ComputeResult<Vec<u8>> {
        Ok(self.dispatch_impl(op, data)?)
    }
}

#[cfg(all(test, feature = "js"))]
mod tests {
    use super::*;

    fn dispatcher() -> JsDispatcher {
        JsDispatcher::new()
    }

    #[test]
    fn map_doubles_integers() {
        let d = dispatcher();
        let result = d
            .eval_partition("(partition) => partition.map(x => x * 2)", None, "[1,2,3]")
            .expect("eval ok");
        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![2, 4, 6]);
    }

    #[test]
    fn filter_with_context_json() {
        let d = dispatcher();
        let result = d
            .eval_partition(
                "(partition) => partition.filter(x => x > globalThis.__ctx.threshold)",
                Some(r#"{"threshold": 2}"#),
                "[1,2,3,4,5]",
            )
            .expect("eval ok");
        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![3, 4, 5]);
    }

    #[test]
    fn thread_local_runtime_reused() {
        let d = dispatcher();
        d.eval_partition(
            "(partition) => partition.map(x => { globalThis.__marker = 99; return x; })",
            None,
            "[1]",
        )
        .unwrap();
        let r2 = d
            .eval_partition(
                "(partition) => partition.map(x => globalThis.__marker)",
                None,
                "[0]",
            )
            .unwrap();
        let v2: Vec<i64> = serde_json::from_slice(&r2).unwrap();
        assert_eq!(v2, vec![99]);
    }

    #[test]
    fn fn_compiled_once_per_thread() {
        let d = dispatcher();
        let fn_source = "(p) => p.map(x => x + 1)";
        d.eval_partition(fn_source, None, "[10]").unwrap();
        d.eval_partition(fn_source, None, "[20]").unwrap();

        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        fn_source.hash(&mut hasher);
        let key = hasher.finish();
        let cached = FN_CACHE.with(|c| c.borrow().contains_key(&key));
        assert!(cached, "fn_source should be in FN_CACHE after first call");

        let r = d.eval_partition(fn_source, None, "[99]").unwrap();
        let v: Vec<i64> = serde_json::from_slice(&r).unwrap();
        assert_eq!(v, vec![100]);
    }

    #[test]
    fn scope_fresh_each_call() {
        let d = dispatcher();
        for i in 0..10i64 {
            let data = format!("[{}]", i);
            let r = d
                .eval_partition("(p) => p.map(x => x * x)", None, &data)
                .unwrap();
            let v: Vec<i64> = serde_json::from_slice(&r).unwrap();
            assert_eq!(v, vec![i * i]);
        }
    }

    #[test]
    fn scope_large_result() {
        let d = dispatcher();
        let data = "[1,2,3,4,5,6,7,8,9,10]";
        let r = d
            .eval_partition("(p) => p.map(x => x.toString().repeat(100))", None, data)
            .unwrap();
        let v: Vec<String> = serde_json::from_slice(&r).unwrap();
        assert_eq!(v.len(), 10);
        assert_eq!(v[0].len(), 100);
    }

    #[test]
    fn scope_context_switch() {
        let d = dispatcher();
        let r1 = d
            .eval_partition(
                "(p) => p.map(x => x + globalThis.__ctx.offset)",
                Some(r#"{"offset": 10}"#),
                "[1, 2]",
            )
            .unwrap();
        let r2 = d
            .eval_partition(
                "(p) => p.map(x => x + globalThis.__ctx.offset)",
                Some(r#"{"offset": 20}"#),
                "[1, 2]",
            )
            .unwrap();
        let v1: Vec<i64> = serde_json::from_slice(&r1).unwrap();
        let v2: Vec<i64> = serde_json::from_slice(&r2).unwrap();
        assert_eq!(v1, vec![11, 12]);
        assert_eq!(v2, vec![21, 22]);
    }

    #[test]
    fn scope_clears_context() {
        let d = dispatcher();
        d.eval_partition("(p) => p", Some(r#"{"secret": 42}"#), "[1]")
            .unwrap();
        let r = d
            .eval_partition("(p) => p.map(x => typeof globalThis.__ctx)", None, "[0]")
            .unwrap();
        let v: Vec<String> = serde_json::from_slice(&r).unwrap();
        assert_eq!(v, vec!["undefined"]);
    }
}
