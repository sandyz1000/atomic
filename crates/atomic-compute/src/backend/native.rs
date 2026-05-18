use atomic_data::distributed::{TaskAction, TaskEnvelope, TaskResultEnvelope, TaskRuntime};

use crate::backend::Backend;
use crate::error::{Error, LibResult};
use crate::task_registry::{SHUFFLE_MAP_REGISTRY, TASK_REGISTRY};

/// Native in-process task executor.
///
/// Looks up each `op_id` from the incoming [`TaskEnvelope`]'s pipeline in the
/// compile-time [`TASK_REGISTRY`] and calls the registered dispatch handlers in
/// sequence, threading data through each step.
///
/// Python UDF ops are dispatched to the process-wide [`python_pool::PythonWorkerPool`];
/// each worker subprocess has its own GIL, so partitions execute in true parallel.
///
/// JavaScript UDF ops are dispatched to a thread-local [`js_runtime::JsRuntime`]
/// (V8, initialised from a startup snapshot), one runtime per tokio blocking thread.
///
/// This is only valid when the driver and worker run the same binary — the
/// dispatch table is built at compile time from all `#[task]`-annotated functions
/// linked into the binary.
#[derive(Default)]
pub struct NativeBackend;

impl Backend for NativeBackend {
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope> {
        if task.ops.is_empty() {
            return Err(Error::UnknownOperation("empty pipeline".to_string()));
        }

        let mut data = task.data.clone();

        for op in &task.ops {
            log::info!(
                "[{}] pipeline op '{}' {:?} data_bytes={}",
                worker_id,
                op.op_id,
                op.action,
                data.len(),
            );

            #[allow(unreachable_patterns)]
            let op_result: Result<Vec<u8>, String> = match &op.runtime {
                #[cfg(feature = "python-udf")]
                TaskRuntime::Python => dispatch_python_udf(&op.payload, &data),
                #[cfg(feature = "js-v8")]
                TaskRuntime::JavaScript => dispatch_js_udf(&op.payload, &data),
                _ => match &op.action {
                    TaskAction::ShuffleMap {
                        shuffle_id,
                        num_output_partitions,
                    } => {
                        (|| {
                            let type_id = std::str::from_utf8(&op.payload)
                                .map_err(|e| format!("ShuffleMap: payload not UTF-8: {e}"))?;
                            match SHUFFLE_MAP_REGISTRY.get(type_id) {
                                None => Err(format!(
                                    "no shuffle handler for type_id='{type_id}'; \
                                     add `register_shuffle_map!(K, V)` to your binary"
                                )),
                                Some(handler) => handler(
                                    &data,
                                    *shuffle_id,
                                    task.partition_id,
                                    *num_output_partitions,
                                )
                                .map(|_| data.clone()),
                            }
                        })()
                    }
                    _ => match TASK_REGISTRY.get(op.op_id.as_str()) {
                        None => Err(format!("unknown op: {}", op.op_id)),
                        Some(handler) => handler(&op.action, &op.payload, &data),
                    },
                },
            };

            match op_result {
                Ok(out) => data = out,
                Err(msg) => {
                    return Ok(TaskResultEnvelope::fatal_failure(
                        task.run_id,
                        task.stage_id,
                        task.task_id,
                        task.attempt_id,
                        task.partition_id,
                        worker_id.to_string(),
                        msg,
                    ));
                }
            }
        }

        let shuffle_server_uri = task
            .ops
            .iter()
            .any(|op| matches!(op.action, TaskAction::ShuffleMap { .. }))
            .then(|| atomic_data::env::SHUFFLE_SERVER_URI.get().cloned())
            .flatten();

        Ok(TaskResultEnvelope::ok(
            task.run_id,
            task.stage_id,
            task.task_id,
            task.attempt_id,
            task.partition_id,
            worker_id.to_string(),
            data,
            shuffle_server_uri,
        ))
    }
}

// Provide a direct `execute` method so callers don't need to import the trait.
impl NativeBackend {
    pub fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope> {
        Backend::execute(self, worker_id, task)
    }
}

// ── Python UDF dispatch ───────────────────────────────────────────────────────

#[cfg(feature = "python-udf")]
fn dispatch_python_udf(
    payload: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, String> {
    use atomic_data::distributed::PythonUdfPayload;
    use crate::backend::python_pool::PYTHON_WORKER_POOL;

    let spec: PythonUdfPayload = serde_json::from_slice(payload)
        .map_err(|e| format!("python_udf payload decode: {e}"))?;
    let pool = PYTHON_WORKER_POOL
        .get()
        .ok_or("PythonWorkerPool not initialized — call init_python_worker_pool() at startup")?;
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(pool.execute(&spec, data))
    })
}

#[cfg(not(feature = "python-udf"))]
fn dispatch_python_udf(
    _payload: &[u8],
    _data: &[u8],
) -> Result<Vec<u8>, String> {
    Err("python-udf feature not enabled; rebuild with the python-udf feature".to_string())
}

// ── JavaScript UDF dispatch ───────────────────────────────────────────────────

#[cfg(feature = "js-v8")]
fn dispatch_js_udf(
    payload: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, String> {
    use atomic_data::distributed::JsUdfPayload;
    use crate::backend::js_runtime;

    let spec: JsUdfPayload = serde_json::from_slice(payload)
        .map_err(|e| format!("js_udf payload decode: {e}"))?;
    let data_str = std::str::from_utf8(data)
        .map_err(|e| format!("data utf8: {e}"))?;
    js_runtime::eval_partition(
        &spec.fn_source,
        spec.context_json.as_deref(),
        data_str,
    )
}

#[cfg(not(feature = "js-v8"))]
fn dispatch_js_udf(
    _payload: &[u8],
    _data: &[u8],
) -> Result<Vec<u8>, String> {
    Err("js-v8 feature not enabled; rebuild with the js-v8 feature".to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{PipelineOp, ResultStatus, TaskAction, TaskRuntime};

    fn make_task(op_id: &str, action: TaskAction, runtime: TaskRuntime, data: Vec<u8>) -> TaskEnvelope {
        TaskEnvelope::new(
            1, 2, 3, 0, 0,
            "test-trace".to_string(),
            vec![PipelineOp { op_id: op_id.to_string(), action, runtime, payload: vec![] }],
            data,
        )
    }

    #[test]
    fn native_runtime_unknown_op_returns_fatal_failure() {
        let backend = NativeBackend;
        let task = make_task("no.such.op", TaskAction::Map, TaskRuntime::Native, vec![]);
        let result = backend.execute("test-worker", &task).unwrap();
        assert_eq!(result.status, ResultStatus::FatalFailure);
        assert!(result.error.unwrap().contains("no.such.op"));
    }

    // Note: TaskRuntime::Python and TaskRuntime::JavaScript only exist when
    // the `python` / `javascript` features of atomic-data are enabled (which
    // happens via `python-udf` / `js-v8` in atomic-compute). Attempting to
    // construct a PipelineOp with those variants without the feature is a
    // compile-time error — the feature gate IS the safety mechanism.
}
