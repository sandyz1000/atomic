use std::collections::HashMap;

use atomic_data::accumulator;
use atomic_data::broadcast;
use atomic_data::distributed::{
    PipelineOp, TaskAction, TaskEnvelope, TaskResultEnvelope, TaskRuntime,
};

use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, OpDispatcher};
use crate::task_registry::{SHUFFLE_MAP_REGISTRY, SORT_SHUFFLE_MAP_REGISTRY, TASK_REGISTRY};

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
                // Payload carries the dispatch key + the shipped partitioner spec.
                let payload: crate::shuffle_map::ShuffleMapPayload =
                    bincode::decode_from_slice(&op.payload, bincode::config::standard())
                        .map(|(p, _)| p)
                        .map_err(|e| {
                            ComputeError::InvalidPayload(format!("shuffle-map payload decode: {e}"))
                        })?;
                let type_id = payload.type_id.as_str();
                let spec = &payload.partitioner_spec;
                // Range (sort) shuffles use the sorted handler when one is registered for the type
                // (K: Ord, via register_sort_shuffle_map!); otherwise fall back to the hash handler.
                let handler = spec
                    .is_range()
                    .then(|| SORT_SHUFFLE_MAP_REGISTRY.get(type_id))
                    .flatten()
                    .or_else(|| SHUFFLE_MAP_REGISTRY.get(type_id));
                match handler {
                    None => Err(ComputeError::UnknownOperation(format!(
                        "no shuffle handler for type_id='{type_id}'; \
                         add `register_shuffle_map!(K, V)` to your binary"
                    ))),
                    Some(handler) => {
                        handler(
                            data,
                            *shuffle_id,
                            partition_id,
                            *num_output_partitions,
                            spec,
                        )?;
                        Ok(data.to_vec())
                    }
                }
            }
            TaskAction::Cache { rdd_id } => {
                // Terminal identity op: store this partition's bytes for later reuse.
                atomic_data::cache::worker_partition_cache().put(
                    *rdd_id,
                    partition_id,
                    data.to_vec(),
                );
                Ok(data.to_vec())
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
        // Locality read: load this partition's input from the worker cache when
        // `cache_source` is set, else use the shipped `data`. A miss is a failure the
        // driver recovers from by recomputing the partition (see scheduler 6c).
        let mut data = match task.cache_source {
            Some(rdd_id) => {
                match atomic_data::cache::worker_partition_cache().get(rdd_id, task.partition_id) {
                    Some(bytes) => bytes,
                    None => {
                        return Ok(TaskResultEnvelope::fatal_failure(
                            task.run_id,
                            task.stage_id,
                            task.task_id,
                            task.attempt_id,
                            task.partition_id,
                            worker_id.to_string(),
                            format!("cache miss: rdd {rdd_id} partition {}", task.partition_id),
                        ));
                    }
                }
            }
            None => task.data.clone(),
        };

        if task.ops.is_empty() {
            // A pure cache serve (cache_source + no ops) returns the cached bytes;
            // a genuinely empty pipeline is an error.
            if task.cache_source.is_some() {
                return Ok(TaskResultEnvelope::ok(
                    task.run_id,
                    task.stage_id,
                    task.task_id,
                    task.attempt_id,
                    task.partition_id,
                    worker_id.to_string(),
                    data,
                    None,
                ));
            }
            return Err(ComputeError::UnknownOperation("empty pipeline".to_string()));
        }

        if !task.broadcast_values.is_empty() {
            broadcast::load_broadcast_values(&task.broadcast_values);
        }

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
            .then(atomic_data::env::get_shuffle_server_uri)
            .flatten();

        // Report partitions cached by any `Cache` op so the driver can record locality.
        let cached_partitions: Vec<(usize, usize)> = task
            .ops
            .iter()
            .filter_map(|op| match op.action {
                TaskAction::Cache { rdd_id } => Some((rdd_id, task.partition_id)),
                _ => None,
            })
            .collect();

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
        .with_accumulator_deltas(acc_deltas)
        .with_cached_partitions(cached_partitions))
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

    #[test]
    fn cache_op_stores_reports() {
        let backend = ComputeEngine::default();
        let data = vec![1u8, 2, 3, 4];
        // A Cache op is an identity pass-through that stores the partition bytes and
        // reports (rdd_id, partition_id) for driver-side locality registration.
        let task = make_task(
            "",
            TaskAction::Cache { rdd_id: 9001 },
            TaskRuntime::Native,
            data.clone(),
        );
        let result = backend.execute("w", &task).unwrap();

        assert_eq!(result.status, ResultStatus::Success);
        assert_eq!(result.data, data); // identity pass-through
        assert_eq!(result.cached_partitions, vec![(9001, 0)]);

        let cache = atomic_data::cache::worker_partition_cache();
        assert!(cache.contains(9001, 0));
        assert_eq!(cache.get(9001, 0), Some(data));
    }

    #[test]
    fn cache_source_serves_bytes() {
        let backend = ComputeEngine::default();
        let cached = vec![7u8, 8, 9];
        atomic_data::cache::worker_partition_cache().put(9100, 0, cached.clone());

        // No ops + cache_source = a pure locality serve from the worker cache.
        let task =
            TaskEnvelope::new(1, 2, 3, 0, 0, "t".into(), vec![], vec![]).with_cache_source(9100);
        let result = backend.execute("w", &task).unwrap();
        assert_eq!(result.status, ResultStatus::Success);
        assert_eq!(result.data, cached);
    }

    #[test]
    fn cache_source_miss_fails() {
        let backend = ComputeEngine::default();
        let task =
            TaskEnvelope::new(1, 2, 3, 0, 0, "t".into(), vec![], vec![]).with_cache_source(9199); // never populated
        let result = backend.execute("w", &task).unwrap();
        assert_eq!(result.status, ResultStatus::FatalFailure);
        assert!(result.error.unwrap().contains("cache miss"));
    }
}
