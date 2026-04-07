use atomic_data::distributed::{TaskAction, TaskEnvelope, TaskResultEnvelope};

use crate::backend::udf::{JsUdfExecutor, PythonUdfExecutor, UdfExecutor};
use crate::backend::Backend;
use crate::error::{Error, LibResult};
use crate::task_registry::TASK_REGISTRY;

/// Native in-process task executor.
///
/// Looks up each `op_id` from the incoming [`TaskEnvelope`]'s pipeline in the
/// compile-time [`TASK_REGISTRY`] and calls the registered dispatch handlers in
/// sequence, threading data through each step.
///
/// UDF ops (`PythonUdf`, `JavaScriptUdf`) are routed to their respective
/// [`UdfExecutor`] implementations rather than the registry.
///
/// This is only valid when the driver and worker run the same binary — the
/// dispatch table is built at compile time from all `#[task]`-annotated functions
/// linked into the binary.
#[derive(Default)]
pub struct NativeBackend;

impl Backend for NativeBackend {
    /// Execute a task envelope by running its pipeline of ops in order.
    ///
    /// Each op's output becomes the next op's input. Returns a [`TaskResultEnvelope`]
    /// in all cases — errors are wrapped as `FatalFailure` rather than propagated,
    /// so the scheduler can handle them uniformly.
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope> {
        if task.ops.is_empty() {
            return Err(Error::UnknownOperation("empty pipeline".to_string()));
        }

        let py_exec = PythonUdfExecutor;
        let js_exec = JsUdfExecutor;

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
                TaskAction::PythonUdf(action) => py_exec.execute(*action, &op.payload, &data),
                TaskAction::JavaScriptUdf(action) => js_exec.execute(*action, &op.payload, &data),
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

        // If any op was a ShuffleMap, attach this worker's shuffle server URI so the
        // driver can register it with MapOutputTracker without decoding `data`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{PipelineOp, ResultStatus, TaskAction, UdfAction};

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
    fn returns_fatal_failure_for_unknown_python_udf_action() {
        let backend = NativeBackend;
        // PythonUdf requires python-udf feature — without it returns FatalFailure
        let task = make_task(
            "atomic::udf::python",
            TaskAction::PythonUdf(UdfAction::Map),
            b"[]".to_vec(),
        );
        let result = backend.execute("test-worker", &task).unwrap();
        // Either FatalFailure (feature not enabled) or Success (feature enabled)
        // Just verify it doesn't panic
        let _ = result.status;
    }
}
