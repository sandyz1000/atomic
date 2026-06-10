use std::collections::HashMap;

use atomic_data::accumulator;
use atomic_data::broadcast;
use atomic_data::distributed::{
    PipelineOp, TaskAction, TaskEnvelope, TaskResultEnvelope, TaskRuntime,
};

use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, OpDispatcher};
use crate::task_registry::{SHUFFLE_MAP_REGISTRY, TASK_REGISTRY};

/// Handles `TaskRuntime::Native` ops — both compile-time `#[task]` registry
/// lookups and shuffle-map writes.
pub(crate) struct NativeDispatcher {}

impl NativeDispatcher {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl OpDispatcher for NativeDispatcher {
    fn dispatch(
        &self,
        op: &PipelineOp,
        partition_id: usize,
        data: &[u8],
    ) -> ComputeResult<Vec<u8>> {
        match &op.action {
            TaskAction::ShuffleMap {
                shuffle_id,
                num_output_partitions,
            } => {
                let type_id = std::str::from_utf8(&op.payload)?;
                match SHUFFLE_MAP_REGISTRY.get(type_id) {
                    None => Err(ComputeError::UnknownOperation(format!(
                        "no shuffle handler for type_id='{type_id}'; \
                         add `register_shuffle_map!(K, V)` to your binary"
                    ))),
                    Some(handler) => {
                        handler(data, *shuffle_id, partition_id, *num_output_partitions)?;
                        Ok(data.to_vec())
                    }
                }
            }
            _ => match TASK_REGISTRY.get(op.op_id.as_str()) {
                None => {
                    let registered: Vec<&str> = TASK_REGISTRY.keys().copied().collect();
                    Err(ComputeError::UnknownOperation(format!(
                        "Task '{}' not registered in TASK_REGISTRY. \
                         Ensure this binary was compiled with the crate that defines \
                         #[task] or task_fn! for '{}'. \
                         Registered ops ({} total): [{}]",
                        op.op_id,
                        op.op_id,
                        registered.len(),
                        registered.join(", ")
                    )))
                }
                Some(handler) => Ok(handler(&op.action, &op.payload, data)?),
            },
        }
    }
}

/// Multi-runtime task executor.
///
/// Routes each [`PipelineOp`] to the [`OpDispatcher`] registered for its
/// [`TaskRuntime`] via an O(1) [`HashMap`] lookup.  Adding a new runtime requires
/// only one new `impl OpDispatcher` and one entry in [`ComputeEngine::default`] —
/// `execute()` never changes.
///
/// This is only valid when the driver and worker run the same binary; the
/// dispatch table is built at compile time from all `#[task]`-annotated functions
/// linked into the binary.
pub struct ComputeEngine {
    dispatchers: HashMap<TaskRuntime, Box<dyn OpDispatcher>>,
}

impl Default for ComputeEngine {
    fn default() -> Self {
        let mut dispatchers: HashMap<TaskRuntime, Box<dyn OpDispatcher>> = HashMap::new();
        dispatchers.insert(TaskRuntime::Native, Box::new(NativeDispatcher::new()));
        #[cfg(feature = "python")]
        dispatchers.insert(
            TaskRuntime::Python,
            Box::new(crate::runtimes::py::PythonDispatcher::new(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4),
            )),
        );
        #[cfg(feature = "js")]
        dispatchers.insert(
            TaskRuntime::JavaScript,
            Box::new(crate::runtimes::js::JsDispatcher::new()),
        );
        ComputeEngine { dispatchers }
    }
}

impl Backend for ComputeEngine {
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> ComputeResult<TaskResultEnvelope> {
        if task.ops.is_empty() {
            return Err(ComputeError::UnknownOperation("empty pipeline".to_string()));
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
                ComputeError::UnknownOperation(format!("unknown TaskRuntime: {:?}", op.runtime))
            })?;

            match dispatcher.dispatch(op, task.partition_id, &data) {
                Ok(out) => data = out,
                Err(e) => {
                    return Ok(TaskResultEnvelope::fatal_failure(
                        task.run_id,
                        task.stage_id,
                        task.task_id,
                        task.attempt_id,
                        task.partition_id,
                        worker_id.to_string(),
                        e.to_string(),
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
        let backend = ComputeEngine::default();
        let task = make_task("no.such.op", TaskAction::Map, TaskRuntime::Native, vec![]);
        let result = backend.execute("test-worker", &task).unwrap();
        assert_eq!(result.status, ResultStatus::FatalFailure);
        assert!(result.error.unwrap().contains("no.such.op"));
    }

    #[test]
    fn default_backend_has_native_dispatcher() {
        let backend = ComputeEngine::default();
        let task = make_task("nonexistent", TaskAction::Map, TaskRuntime::Native, vec![]);
        let result = backend.execute("w", &task);
        assert!(result.is_ok(), "Native dispatcher must be registered");
        assert_eq!(result.unwrap().status, ResultStatus::FatalFailure);
    }

    #[test]
    fn empty_pipeline_returns_err() {
        let backend = ComputeEngine::default();
        let task = TaskEnvelope::new(1, 2, 3, 0, 0, "t".into(), vec![], vec![]);
        assert!(
            backend.execute("w", &task).is_err(),
            "empty pipeline must return Err, not Ok"
        );
    }

    #[test]
    fn unregistered_runtime_returns_err() {
        let backend = ComputeEngine::default();
        let task = make_task("no.such.op", TaskAction::Map, TaskRuntime::Native, vec![]);
        assert!(backend.execute("w", &task).is_ok());
    }
}
