use std::collections::HashMap;

use atomic_data::accumulator;
use atomic_data::broadcast;
use atomic_data::distributed::{
    PipelineOp, TaskAction, TaskEnvelope, TaskResultEnvelope, TaskRuntime,
};

use crate::backend::{Backend, OpDispatcher};
use crate::error::{Error, Result};
use crate::task_registry::{SHUFFLE_MAP_REGISTRY, TASK_REGISTRY};

// ── NativeDispatcher ─────────────────────────────────────────────────────────

/// Handles `TaskRuntime::Native` ops — both compile-time `#[task]` registry
/// lookups and shuffle-map writes.
pub(crate) struct NativeDispatcher;

impl OpDispatcher for NativeDispatcher {
    fn dispatch(
        &self,
        op: &PipelineOp,
        partition_id: usize,
        data: &[u8],
    ) -> std::result::Result<Vec<u8>, String> {
        match &op.action {
            TaskAction::ShuffleMap {
                shuffle_id,
                num_output_partitions,
            } => {
                let type_id = std::str::from_utf8(&op.payload)
                    .map_err(|e| format!("ShuffleMap: payload not UTF-8: {e}"))?;
                match SHUFFLE_MAP_REGISTRY.get(type_id) {
                    None => Err(format!(
                        "no shuffle handler for type_id='{type_id}'; \
                         add `register_shuffle_map!(K, V)` to your binary"
                    )),
                    Some(handler) => {
                        handler(data, *shuffle_id, partition_id, *num_output_partitions)
                            .map(|_| data.to_vec())
                    }
                }
            }
            _ => match TASK_REGISTRY.get(op.op_id.as_str()) {
                None => {
                    let registered: Vec<&str> = TASK_REGISTRY.keys().copied().collect();
                    Err(format!(
                        "Task '{}' not registered in TASK_REGISTRY. \
                         Ensure this binary was compiled with the crate that defines \
                         #[task] or task_fn! for '{}'. \
                         Registered ops ({} total): [{}]",
                        op.op_id,
                        op.op_id,
                        registered.len(),
                        registered.join(", ")
                    ))
                }
                Some(handler) => handler(&op.action, &op.payload, data),
            },
        }
    }
}

// ── NativeBackend ─────────────────────────────────────────────────────────────

/// Native in-process task executor.
///
/// Dispatches each [`PipelineOp`] to the [`OpDispatcher`] registered for its
/// [`TaskRuntime`] via an O(1) [`HashMap`] lookup.  Adding a new runtime requires
/// only one new `impl OpDispatcher` and one entry in [`NativeBackend::default`] —
/// this function never changes.
///
/// This is only valid when the driver and worker run the same binary; the
/// dispatch table is built at compile time from all `#[task]`-annotated functions
/// linked into the binary.
pub struct NativeBackend {
    dispatchers: HashMap<TaskRuntime, Box<dyn OpDispatcher>>,
}

impl Default for NativeBackend {
    fn default() -> Self {
        let mut d: HashMap<TaskRuntime, Box<dyn OpDispatcher>> = HashMap::new();
        d.insert(TaskRuntime::Native, Box::new(NativeDispatcher));
        #[cfg(feature = "python")]
        d.insert(
            TaskRuntime::Python,
            Box::new(crate::backend::python_executor::PythonDispatcher),
        );
        #[cfg(feature = "js")]
        d.insert(
            TaskRuntime::JavaScript,
            Box::new(crate::backend::js_executor::JsDispatcher),
        );
        NativeBackend { dispatchers: d }
    }
}

impl Backend for NativeBackend {
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> Result<TaskResultEnvelope> {
        if task.ops.is_empty() {
            return Err(Error::UnknownOperation("empty pipeline".to_string()));
        }

        if !task.broadcast_values.is_empty() {
            broadcast::load_broadcast_values(&task.broadcast_values);
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

            let dispatcher = self.dispatchers.get(&op.runtime).ok_or_else(|| {
                Error::UnknownOperation(format!("unknown TaskRuntime: {:?}", op.runtime))
            })?;

            match dispatcher.dispatch(op, task.partition_id, &data) {
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

        if !task.broadcast_values.is_empty() {
            broadcast::clear_broadcast_values();
        }

        let acc_deltas = accumulator::drain_deltas();

        let shuffle_server_uri = task
            .ops
            .iter()
            .any(|op| matches!(op.action, TaskAction::ShuffleMap { .. }))
            .then(|| atomic_data::env::get_shuffle_server_uri())
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
        )
        .with_accumulator_deltas(acc_deltas))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{PipelineOp, ResultStatus, TaskAction, TaskRuntime};

    fn make_task(
        op_id: &str,
        action: TaskAction,
        runtime: TaskRuntime,
        data: Vec<u8>,
    ) -> TaskEnvelope {
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
                runtime,
                payload: vec![],
            }],
            data,
        )
    }

    #[test]
    fn native_runtime_unknown_op_returns_fatal_failure() {
        let backend = NativeBackend::default();
        let task = make_task("no.such.op", TaskAction::Map, TaskRuntime::Native, vec![]);
        let result = backend.execute("test-worker", &task).unwrap();
        assert_eq!(result.status, ResultStatus::FatalFailure);
        assert!(result.error.unwrap().contains("no.such.op"));
    }
}
