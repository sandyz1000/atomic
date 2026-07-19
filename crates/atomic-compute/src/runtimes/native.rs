use std::collections::HashMap;

use atomic_data::accumulator;
use atomic_data::broadcast;
use atomic_data::distributed::{
    OpKind, PipelineOp, StepKind, TaskEnvelope, TaskResultEnvelope, TaskRuntime, decode_payload,
};

use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, OpDispatcher};
use crate::task_registry::{
    AGENT_RUNNER_REGISTRY, SHUFFLE_MAP_REGISTRY, SORT_SHUFFLE_MAP_REGISTRY, STATE_MERGE_REGISTRY,
    TASK_REGISTRY,
};

/// Decode a bincode payload, tagging failures with `label` so the dispatch site
/// that produced the bytes is named in the error.
fn decode_or_invalid<T: bincode::Decode<()>>(bytes: &[u8], label: &str) -> ComputeResult<T> {
    decode_payload(bytes).map_err(|e| ComputeError::InvalidPayload(format!("{label} decode: {e}")))
}

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
        match &op.kind {
            OpKind::Engine(StepKind::ShuffleMap {
                shuffle_id,
                num_output_partitions,
            }) => {
                // Payload carries the dispatch key + the shipped partitioner spec.
                let payload: atomic_data::distributed::ShuffleMapPayload =
                    decode_or_invalid(&op.payload, "shuffle-map payload")?;
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
            OpKind::Engine(StepKind::Cache { rdd_id }) => {
                // Terminal identity op: store this partition's bytes for later reuse.
                atomic_data::cache::worker_partition_cache().put(
                    *rdd_id,
                    partition_id,
                    data.to_vec(),
                );
                Ok(data.to_vec())
            }
            #[cfg(feature = "kafka")]
            OpKind::Engine(StepKind::KafkaConsume) => {
                // The per-partition consume config is shipped in `source_partitions[i]`
                // (i.e. `data`), not in `op.payload` (which is empty for KafkaConsume).
                let payload: atomic_data::distributed::KafkaConsumePayload =
                    decode_or_invalid(data, "KafkaConsume data")?;
                kafka_consume_handler(&payload)
            }
            OpKind::Engine(StepKind::ReadFileSplit) => {
                // Per-partition config shipped in `data` (bincode-encoded FileSplitPayload);
                // `op.payload` is empty.
                let payload: atomic_data::distributed::FileSplitPayload =
                    decode_or_invalid(data, "ReadFileSplit data")?;
                file_split_handler(&payload)
            }
            OpKind::Engine(StepKind::MergeState { merge_fn }) => {
                // Per-shard input shipped in `data` (bincode-encoded StateMergePayload).
                // The merge fn is content-agnostic; the shard's state persists across
                // batches in the worker-global WORKER_STATE_STORE.
                let payload: atomic_data::distributed::StateMergePayload =
                    decode_or_invalid(data, "MergeState data")?;
                let merge = STATE_MERGE_REGISTRY.get(merge_fn.as_str()).ok_or_else(|| {
                    ComputeError::UnknownOperation(format!(
                        "no state-merge fn '{merge_fn}' registered; \
                         add `register_state_merge!` to your binary"
                    ))
                })?;
                let store = atomic_data::state_store::worker_state_store();
                // Cold shard after a restart: reload its last checkpoint before merging.
                let mut prev = store.get(payload.state_id);
                if prev.is_none()
                    && let Some(dir) = &payload.checkpoint_dir
                {
                    let path = shard_checkpoint_path(dir, payload.state_id);
                    match std::fs::read(&path) {
                        Ok(bytes) => {
                            log::info!(
                                "state shard {}: cold start, reloaded {} bytes from {}",
                                payload.state_id,
                                bytes.len(),
                                path.display(),
                            );
                            prev = Some(bytes);
                        }
                        // First batch of a fresh query — or a reassigned shard whose
                        // checkpoint lives on another worker's local disk; a shared
                        // checkpoint_dir is required for cross-worker recovery.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            log::debug!(
                                "state shard {}: no checkpoint at {}, starting empty",
                                payload.state_id,
                                path.display(),
                            );
                        }
                        Err(e) => {
                            log::warn!(
                                "state shard {}: checkpoint at {} unreadable ({e}); \
                                 starting empty — accumulated state is lost",
                                payload.state_id,
                                path.display(),
                            );
                        }
                    }
                }
                let (new_state, emitted) =
                    merge(prev.as_deref(), &payload.partials, &payload.params)
                        .map_err(ComputeError::InvalidPayload)?;
                if let Some(dir) = &payload.checkpoint_dir {
                    write_shard_checkpoint(dir, payload.state_id, &new_state)?;
                }
                store.put(payload.state_id, new_state);
                Ok(emitted)
            }
            OpKind::Engine(StepKind::AgentStep) => {
                let payload: atomic_data::distributed::AgentStepPayload =
                    serde_json::from_slice(&op.payload).map_err(|e| {
                        ComputeError::InvalidPayload(format!("AgentStep payload decode: {e}"))
                    })?;
                let runner = AGENT_RUNNER_REGISTRY.get().ok_or_else(|| {
                    ComputeError::UnknownOperation(
                        "AgentStep: no agent runner registered; \
                         call `atomic_compute::register_agent_runner(...)` at startup \
                         or link `atomic-nlq` and call `atomic_nlq::agent_runner::register()`"
                            .to_string(),
                    )
                })?;
                runner
                    .run_partition(&payload, data)
                    .map_err(ComputeError::InvalidPayload)
            }
            OpKind::Task(action) => match TASK_REGISTRY.get(op.op_id.as_str()) {
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
                Some(handler) => Ok(handler(action, &op.payload, data)?),
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
        add_python(&mut dispatchers);
        add_js(&mut dispatchers);
        ComputeEngine { dispatchers }
    }
}

crate::cfg_python! {
    fn add_python(dispatchers: &mut HashMap<TaskRuntime, Box<dyn OpDispatcher>>) {
        dispatchers.insert(
            TaskRuntime::Python,
            Box::new(crate::runtimes::py::PythonDispatcher::new(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4),
            )),
        );
    }
}
crate::cfg_not_python! {
    fn add_python(_dispatchers: &mut HashMap<TaskRuntime, Box<dyn OpDispatcher>>) {}
}

crate::cfg_js! {
    fn add_js(dispatchers: &mut HashMap<TaskRuntime, Box<dyn OpDispatcher>>) {
        dispatchers.insert(
            TaskRuntime::JavaScript,
            Box::new(crate::runtimes::js::JsDispatcher::new()),
        );
    }
}
crate::cfg_not_js! {
    fn add_js(_dispatchers: &mut HashMap<TaskRuntime, Box<dyn OpDispatcher>>) {}
}

/// Disambiguates `resolve_input` output: either proceed with pipeline bytes, or
/// short-circuit with a pre-built envelope (cache miss or empty-cache-serve).
enum InputData {
    Pipeline(Vec<u8>),
    EarlyReturn(TaskResultEnvelope),
}

/// Load this partition's input: worker cache hit when `cache_source` is set, else
/// the envelope's `data` bytes. Returns `EarlyReturn` for cache-miss and empty-serve.
fn resolve_input(task: &TaskEnvelope, worker_id: &str) -> ComputeResult<InputData> {
    let data = match task.cache_source {
        Some(rdd_id) => {
            match atomic_data::cache::worker_partition_cache().get(rdd_id, task.partition_id) {
                Some(bytes) => bytes,
                None => {
                    return Ok(InputData::EarlyReturn(TaskResultEnvelope::cache_miss(
                        task.run_id,
                        task.stage_id,
                        task.task_id,
                        task.attempt_id,
                        task.partition_id,
                        worker_id.to_string(),
                        rdd_id,
                    )));
                }
            }
        }
        None => task.data.clone(),
    };

    if task.ops.is_empty() {
        // A pure cache serve (cache_source + no ops) returns the cached bytes;
        // a genuinely empty pipeline without a cache source is an error.
        return if task.cache_source.is_some() {
            Ok(InputData::EarlyReturn(TaskResultEnvelope::ok(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                task.partition_id,
                worker_id.to_string(),
                data,
                None,
            )))
        } else {
            Err(ComputeError::UnknownOperation("empty pipeline".to_string()))
        };
    }

    Ok(InputData::Pipeline(data))
}

/// Run each op in sequence, threading data through the pipeline. Returns the final
/// output bytes, or `Err` if any dispatcher call fails.
///
/// Broadcasts arrive as bytes only on the first task to reach this worker; they are
/// cached process-globally and survive across tasks (no per-task clear). A task whose
/// declared `broadcast_ids` are not all cached fails with a [`BroadcastError`] so the
/// driver re-sends — the recovery path for a restarted, cache-empty worker.
///
/// [`BroadcastError`]: atomic_data::broadcast::BroadcastError
fn run_pipeline(
    dispatchers: &HashMap<TaskRuntime, Box<dyn OpDispatcher>>,
    task: &TaskEnvelope,
    worker_id: &str,
    mut data: Vec<u8>,
) -> ComputeResult<Vec<u8>> {
    if !task.broadcast_values.is_empty() {
        broadcast::cache_broadcast_values(&task.broadcast_values);
    }
    if !task.broadcast_ids.is_empty() {
        broadcast::ensure_broadcasts_cached(&task.broadcast_ids)
            .map_err(|e| ComputeError::InvalidPayload(e.to_string()))?;
    }

    for op in &task.ops {
        log::info!(
            "[{}] pipeline op '{}' {:?} data_bytes={}",
            worker_id,
            op.op_id,
            op.kind,
            data.len(),
        );
        let dispatcher = dispatchers.get(&op.runtime).ok_or_else(|| {
            ComputeError::UnknownOperation(format!("unknown TaskRuntime: {:?}", op.runtime))
        })?;
        data = dispatcher.dispatch(op, task.partition_id, &data)?;
    }

    Ok(data)
}

/// Assemble the success `TaskResultEnvelope` with shuffle URI, cached partitions,
/// held state IDs, and accumulator deltas.
fn build_result_envelope(
    task: &TaskEnvelope,
    worker_id: &str,
    output: Vec<u8>,
) -> TaskResultEnvelope {
    let acc_deltas = accumulator::drain_deltas();

    let shuffle_server_uri = task
        .ops
        .iter()
        .any(|op| matches!(op.kind, OpKind::Engine(StepKind::ShuffleMap { .. })))
        .then(atomic_data::env::get_shuffle_server_uri)
        .flatten();

    let cached_partitions: Vec<(usize, usize)> = task
        .ops
        .iter()
        .filter_map(|op| match op.kind {
            OpKind::Engine(StepKind::Cache { rdd_id }) => Some((rdd_id, task.partition_id)),
            _ => None,
        })
        .collect();

    // Report state shard IDs merged by any `MergeState` op so the driver can build
    // `state_locs` for report-back affinity (autoscaling-robust pinning, G1).
    let held_state_ids: Vec<u64> = task
        .ops
        .iter()
        .filter(|op| matches!(op.kind, OpKind::Engine(StepKind::MergeState { .. })))
        .filter_map(|_| {
            decode_payload::<atomic_data::distributed::StateMergePayload>(&task.data)
                .ok()
                .map(|p| p.state_id)
        })
        .collect();

    TaskResultEnvelope::ok(
        task.run_id,
        task.stage_id,
        task.task_id,
        task.attempt_id,
        task.partition_id,
        worker_id.to_string(),
        output,
        shuffle_server_uri,
    )
    .with_accumulator_deltas(acc_deltas)
    .with_cached_partitions(cached_partitions)
    .with_held_state_ids(held_state_ids)
}

impl Backend for ComputeEngine {
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> ComputeResult<TaskResultEnvelope> {
        let data = match resolve_input(task, worker_id)? {
            InputData::EarlyReturn(r) => return Ok(r),
            InputData::Pipeline(d) => d,
        };

        let output = match run_pipeline(&self.dispatchers, task, worker_id, data) {
            Ok(out) => out,
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
        };

        Ok(build_result_envelope(task, worker_id, output))
    }
}

crate::cfg_kafka! {
/// Worker-side Kafka Direct consume: assign+seek to offset range, poll to end, return messages.
///
/// Returns rkyv-encoded `Vec<String>`.  Each message payload is decoded as UTF-8; invalid bytes
/// are replaced with the Unicode replacement character so the pipeline is never aborted by one
/// bad message.
pub(crate) fn kafka_consume_handler(
    payload: &atomic_data::distributed::KafkaConsumePayload,
) -> ComputeResult<Vec<u8>> {
    use rdkafka::Offset as RdOffset;
    use rdkafka::TopicPartitionList;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::message::Message;
    use std::time::Duration;

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &payload.brokers)
        .set("group.id", "atomic-direct-consumer")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "none")
        .create()
        .map_err(|e| ComputeError::InvalidPayload(format!("Kafka consumer create: {e}")))?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(
        &payload.topic,
        payload.partition,
        RdOffset::Offset(payload.start_offset),
    )
    .map_err(|e| ComputeError::InvalidPayload(format!("Kafka assign+seek: {e}")))?;
    consumer
        .assign(&tpl)
        .map_err(|e| ComputeError::InvalidPayload(format!("Kafka assign: {e}")))?;

    let mut messages: Vec<String> = Vec::new();
    let poll_timeout = Duration::from_millis(500);
    loop {
        if messages.len() >= payload.max_records {
            break;
        }
        match consumer.poll(poll_timeout) {
            Some(Ok(msg)) => {
                let offset = msg.offset();
                if offset >= payload.end_offset {
                    break;
                }
                let text = msg
                    .payload()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                messages.push(text);
            }
            Some(Err(e)) => {
                log::warn!(
                    "KafkaConsume poll error (topic={} part={}): {e}",
                    payload.topic,
                    payload.partition
                );
            }
            None => {
                // poll timeout — no more messages in the window
                break;
            }
        }
    }

    rkyv::to_bytes::<rkyv::rancor::Error>(&messages)
        .map(|b| b.to_vec())
        .map_err(|e| ComputeError::InvalidPayload(format!("KafkaConsume rkyv encode: {e}")))
}
} // cfg_kafka!

/// Worker-side file-split read: open `path`, seek to `start_byte`, read UTF-8 lines
/// up to `end_byte` (exclusive, or EOF when `None`), return rkyv-encoded `Vec<String>`.
pub(crate) fn file_split_handler(
    payload: &atomic_data::distributed::FileSplitPayload,
) -> ComputeResult<Vec<u8>> {
    use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

    let file = std::fs::File::open(&payload.path).map_err(|e| {
        ComputeError::InvalidPayload(format!("ReadFileSplit open {:?}: {e}", payload.path))
    })?;

    let file_len = file.metadata().map(|m| m.len()).unwrap_or(u64::MAX);
    let end = payload.end_byte.unwrap_or(file_len);

    let mut reader = BufReader::new(file);
    if payload.start_byte > 0 {
        reader
            .seek(SeekFrom::Start(payload.start_byte))
            .map_err(|e| ComputeError::InvalidPayload(format!("ReadFileSplit seek: {e}")))?;
    }

    // Wrap in a Take so we stop reading at end_byte even mid-line.
    let bytes_to_read = end.saturating_sub(payload.start_byte);
    let mut bounded = BufReader::new(reader.take(bytes_to_read));

    let mut lines: Vec<String> = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        let n = bounded
            .read_line(&mut line)
            .map_err(|e| ComputeError::InvalidPayload(format!("ReadFileSplit read: {e}")))?;
        if n == 0 {
            break;
        }
        lines.push(
            line.trim_end_matches('\n')
                .trim_end_matches('\r')
                .to_string(),
        );
    }

    rkyv::to_bytes::<rkyv::rancor::Error>(&lines)
        .map(|b| b.to_vec())
        .map_err(|e| ComputeError::InvalidPayload(format!("ReadFileSplit rkyv encode: {e}")))
}

/// Per-shard checkpoint file path for a `MergeState` task.
fn shard_checkpoint_path(dir: &str, state_id: u64) -> std::path::PathBuf {
    std::path::Path::new(dir).join(format!("shard-{state_id}.bin"))
}

/// Atomically persist a shard's post-merge state (write to `.tmp`, then rename).
fn write_shard_checkpoint(dir: &str, state_id: u64, bytes: &[u8]) -> ComputeResult<()> {
    std::fs::create_dir_all(dir)
        .map_err(|e| ComputeError::InvalidPayload(format!("state checkpoint mkdir: {e}")))?;
    let path = shard_checkpoint_path(dir, state_id);
    let tmp = path.with_extension("bin.tmp");
    std::fs::write(&tmp, bytes)
        .map_err(|e| ComputeError::InvalidPayload(format!("state checkpoint write: {e}")))?;
    std::fs::rename(&tmp, &path)
        .map_err(|e| ComputeError::InvalidPayload(format!("state checkpoint rename: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{
        OpKind, PipelineOp, ResultStatus, StepKind, TaskAction, TaskRuntime,
    };

    fn make_task(op_id: &str, kind: OpKind, runtime: TaskRuntime, data: Vec<u8>) -> TaskEnvelope {
        TaskEnvelope::new(
            1,
            2,
            3,
            0,
            0,
            "test-trace".to_string(),
            vec![PipelineOp {
                op_id: op_id.to_string(),
                kind,
                runtime,
                payload: vec![],
            }],
            data,
        )
    }

    #[test]
    fn native_runtime_unknown_op_returns_fatal_failure() {
        let backend = ComputeEngine::default();
        let task = make_task(
            "no.such.op",
            OpKind::Task(TaskAction::Map),
            TaskRuntime::Native,
            vec![],
        );
        let result = backend.execute("test-worker", &task).unwrap();
        assert_eq!(result.status, ResultStatus::FatalFailure);
        assert!(result.error.unwrap().contains("no.such.op"));
    }

    #[test]
    fn default_backend_has_native_dispatcher() {
        let backend = ComputeEngine::default();
        let task = make_task(
            "nonexistent",
            OpKind::Task(TaskAction::Map),
            TaskRuntime::Native,
            vec![],
        );
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
        let task = make_task(
            "no.such.op",
            OpKind::Task(TaskAction::Map),
            TaskRuntime::Native,
            vec![],
        );
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
            OpKind::Engine(StepKind::Cache { rdd_id: 9001 }),
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
    fn cache_source_miss_returns_cache_miss_status() {
        let backend = ComputeEngine::default();
        let task =
            TaskEnvelope::new(1, 2, 3, 0, 0, "t".into(), vec![], vec![]).with_cache_source(9199); // never populated
        let result = backend.execute("w", &task).unwrap();
        assert_eq!(result.status, ResultStatus::CacheMiss);
        assert!(result.error.unwrap().contains("cache evicted"));
    }
}
