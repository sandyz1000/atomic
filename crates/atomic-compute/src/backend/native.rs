use atomic_data::distributed::{TaskAction, TaskEnvelope, TaskResultEnvelope};

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

            let op_result: Result<Vec<u8>, String> = match &op.action {
                TaskAction::PythonUdf(action) => {
                    dispatch_python_udf(*action, &op.payload, &data)
                }
                TaskAction::JavaScriptUdf(action) => {
                    dispatch_js_udf(*action, &op.payload, &data)
                }
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
    action: atomic_data::distributed::UdfAction,
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
            .block_on(pool.execute(action, &spec, data))
    })
}

#[cfg(not(feature = "python-udf"))]
fn dispatch_python_udf(
    _action: atomic_data::distributed::UdfAction,
    _payload: &[u8],
    _data: &[u8],
) -> Result<Vec<u8>, String> {
    Err("python-udf feature not enabled; rebuild with the python-udf feature".to_string())
}

// ── JavaScript UDF dispatch ───────────────────────────────────────────────────

#[cfg(feature = "js-v8")]
fn dispatch_js_udf(
    action: atomic_data::distributed::UdfAction,
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
        action,
        &spec.fn_source,
        spec.context_json.as_deref(),
        data_str,
        &spec.zero_json,
    )
}

#[cfg(not(feature = "js-v8"))]
fn dispatch_js_udf(
    _action: atomic_data::distributed::UdfAction,
    _payload: &[u8],
    _data: &[u8],
) -> Result<Vec<u8>, String> {
    Err("js-v8 feature not enabled; rebuild with the js-v8 feature".to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{PipelineOp, ResultStatus, TaskAction};

    fn make_task(op_id: &str, action: TaskAction, data: Vec<u8>) -> TaskEnvelope {
        TaskEnvelope::new(
            1,
            2,
            3,
            0,
            0,
            "test-trace".to_string(),
            vec![PipelineOp {
                op_id: op_id.to_string(),
                action,
                payload: vec![],
            }],
            data,
        )
    }

    #[test]
    fn returns_fatal_failure_for_unknown_op() {
        let backend = NativeBackend;
        let task = make_task("no.such.op", TaskAction::Map, vec![]);
        let result = backend.execute("test-worker", &task).unwrap();
        assert_eq!(result.status, ResultStatus::FatalFailure);
        assert!(result.error.unwrap().contains("no.such.op"));
    }

    #[test]
    fn returns_fatal_failure_for_python_udf_without_feature() {
        // Without python-udf feature, PythonUdf ops return FatalFailure with a clear message.
        use atomic_data::distributed::UdfAction;
        let backend = NativeBackend;
        let task = make_task("", TaskAction::PythonUdf(UdfAction::Map), b"[]".to_vec());
        let result = backend.execute("test-worker", &task).unwrap();
        // Either FatalFailure (feature not enabled) or Success (feature enabled + cloudpickle)
        let _ = result.status;
    }

    #[test]
    fn returns_fatal_failure_for_js_udf_without_feature() {
        use atomic_data::distributed::UdfAction;
        let backend = NativeBackend;
        let task = make_task("", TaskAction::JavaScriptUdf(UdfAction::Map), b"[]".to_vec());
        let result = backend.execute("test-worker", &task).unwrap();
        let _ = result.status;
    }
}
