use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::WIRE_SCHEMA_V1;

/// Which execution runtime handles a [`PipelineOp`] on the worker.
///
/// `Native` (the default) looks up `op_id` in the compile-time `TASK_REGISTRY`.
/// `Python` and `JavaScript` dispatch the serialized partition-level function to
/// the respective runtime; the worker calls `fn(partition)` without inspecting
/// `action`.
///
/// Runtime variants are feature-gated so pure-Rust deployments compile without
/// pulling in Python or V8 symbols:
/// - feature `python`     → enables [`TaskRuntime::Python`]
/// - feature `javascript` → enables [`TaskRuntime::JavaScript`]
///
/// Discriminants are pinned explicitly. Driver and worker binaries routinely compile
/// this crate with *different* feature sets (e.g. `atomic-py` enables only `python`,
/// `atomic-js` enables only `javascript`, while `atomic-worker` enables both) — without
/// pinned values, the implicit "position among compiled-in variants" discriminant for
/// `JavaScript` would shift between those builds (1 vs 2), corrupting the wire protocol
/// silently. Pinning makes the discriminant a property of the variant, not the build.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
    Default,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskRuntime {
    /// Compile-time `TASK_REGISTRY` lookup via `op_id` (default for `#[task]` functions).
    #[default]
    Native = 0,
    /// Pickled partition-level Python callable executed in the subprocess worker pool.
    #[cfg(feature = "python")]
    Python = 1,
    /// Partition-level JavaScript function evaluated in the embedded V8 (deno_core) runtime.
    #[cfg(feature = "javascript")]
    JavaScript = 2,
}

/// One step in a multi-op pipeline sent to a worker.
///
/// A [`TaskEnvelope`] carries a sequence of these; the worker threads partition
/// data through them in order, feeding each step's output as the next step's input.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct PipelineOp {
    /// Registered op_id, e.g. `"task_double::double"`. Looked up in the worker's
    /// compile-time dispatch table. Empty string for Python/JS UDF ops.
    pub op_id: String,
    /// Which operation this step performs. Authoritative for Native runtime;
    /// informational (for observability) for Python and JavaScript runtimes.
    pub action: TaskAction,
    /// Which runtime executes this op. Defaults to [`TaskRuntime::Native`].
    pub runtime: TaskRuntime,
    /// rkyv-encoded config: fold zero value for Fold/Aggregate; serde_json-encoded
    /// [`PythonUdfPayload`] / [`JsUdfPayload`] for Python/JS ops; empty for Map/Filter.
    pub payload: Vec<u8>,
}

/// The action the worker should apply over the partition data using the named task function.
///
/// The driver sets this based on which RDD operation triggered the task submission.
/// The worker dispatch handler reads this to decide how to iterate over the partition.
///
/// `KafkaConsume` is `#[cfg(feature = "kafka")]`-gated and declared **last**, intentionally.
/// Variants with struct fields can't carry explicit discriminants in Rust (unlike
/// [`TaskRuntime`]'s pinned values), so the only way to keep every other variant's implicit
/// discriminant stable across builds with/without the `kafka` feature is to put the
/// cfg-gated variant after all of them — inserting or removing it then never shifts anyone
/// else's position.
#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskAction {
    /// Apply task function element-wise: `output = task_fn(element)` for each element.
    Map,
    /// Keep elements where task function returns true.
    Filter,
    /// Apply task function element-wise, each call returns an iterator of output elements.
    FlatMap,
    /// Fold partition to a single value. `payload` carries the rkyv-encoded zero/identity value.
    Fold,
    /// Reduce partition to a single value using task function as the combiner.
    Reduce,
    /// Aggregate partition. `payload` carries the rkyv-encoded zero/identity value.
    Aggregate,
    /// Collect all elements from partition (identity pass-through).
    Collect,
    /// Shuffle map phase: repartition elements by key into `num_output_partitions` buckets.
    ShuffleMap {
        shuffle_id: usize,
        num_output_partitions: usize,
    },
    /// Terminal identity op that caches this partition's bytes on the worker under
    /// `(rdd_id, partition_id)`, so a later job can be scheduled here and served from
    /// cache instead of recomputing. Emitted by a distributed `.cache()` / `persist()`.
    Cache { rdd_id: usize },
    /// File-split source op. The worker opens `path`, optionally seeks to `start_byte`,
    /// reads lines up to `end_byte` (or EOF), and returns them as rkyv-encoded `Vec<String>`.
    /// All config is in the task `data` (bincode-encoded `FileSplitPayload`); `op.payload`
    /// is empty.
    ReadFileSplit,
    /// Distributed stateful-streaming merge. The worker reads its shard's serialized
    /// state from `WORKER_STATE_STORE[state_id]`, applies the registered `merge_fn`
    /// (looked up in `STATE_MERGE_REGISTRY`) to this batch's partials, stores the new
    /// state, and returns the emitted cells. All per-shard config (state_id, params,
    /// partials) is in the task `data` (bincode-encoded `StateMergePayload`).
    MergeState {
        /// Registered state-merge function name (e.g. `"atomic_structured::windowed_v1"`).
        merge_fn: String,
    },
    /// Framework-native distributed sub-agent loop. The worker looks up the
    /// registered `AgentRunner` (installed by `atomic-nlq`) and runs a multi-round
    /// LLM plan→execute→evaluate loop over the partition's string inputs.
    /// Config is in `PipelineOp.payload` (JSON-encoded `AgentStepPayload`);
    /// input bytes are rkyv-encoded `Vec<String>` (or JSON fallback for Python/JS).
    /// Returns rkyv-encoded `Vec<AgentFindings>`.
    AgentStep,
    /// Kafka Direct source op (requires `kafka` feature). The worker `assign`+`seek`s to
    /// the given offset range and polls until `end_offset`, returning the messages as
    /// `rkyv`-encoded `Vec<String>`. `data` in the TaskEnvelope is ignored; all config
    /// is in `PipelineOp.payload` (bincode-encoded `KafkaConsumePayload`).
    #[cfg(feature = "kafka")]
    KafkaConsume,
}

/// Config for a [`TaskAction::AgentStep`] op.
///
/// Serialized as JSON into `PipelineOp.payload` so it can be decoded by the
/// worker's `NativeDispatcher` without a shared rkyv schema.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentStepPayload {
    /// LLM model identifier, e.g. `"gpt-4o"` or `"claude-opus-4-8"`.
    pub model: String,
    /// System prompt describing the agent's task.
    pub system_prompt: String,
    /// Maximum number of LLM rounds per input.
    pub max_rounds: u32,
    /// Tool IDs the agent may reference (builtin or user Python/JS tools).
    pub tool_refs: Vec<String>,
    /// Provider string: `"openai"` (default) or `"anthropic"`.
    pub provider: String,
    /// Optional JSON schema for output validation (best-effort check).
    pub output_schema: Option<String>,
    /// Optional token budget across all inputs in this partition.
    pub max_tokens_total: Option<u64>,
}

/// Structured result returned by a sub-agent for one input string.
///
/// One `AgentFindings` is produced per input element in the partition.
/// `Vec<AgentFindings>` is rkyv-encoded as the partition output of an
/// [`TaskAction::AgentStep`] op.
#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
pub struct AgentFindings {
    /// Index of the input string within this partition (0-based).
    pub input_id: usize,
    /// Final answer or extracted content produced by the agent.
    pub answer: String,
    /// Number of LLM rounds completed.
    pub rounds: usize,
    /// Confidence estimate in `[0.0, 1.0]` (set by the runner; not validated).
    pub confidence: f32,
    /// `true` if the token budget was exhausted before all inputs were processed.
    pub budget_exceeded: bool,
}

/// Metadata carried in `PipelineOp.payload` for a Python UDF step.
///
/// Serialized as JSON so both Python (via `json` stdlib) and Rust (`serde_json`) can
/// produce and consume it without a shared binary format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonUdfPayload {
    /// `cloudpickle`/`pickle`-serialized partition-level Python callable.
    pub fn_bytes: Vec<u8>,
    /// Reserved for future use (currently unused). Empty for all operations.
    pub zero_bytes: Vec<u8>,
}

/// Metadata carried in `PipelineOp.payload` for a JavaScript UDF step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsUdfPayload {
    /// JavaScript partition-level function source.
    pub fn_source: String,
    /// Reserved for future use (currently unused). Empty for all operations.
    pub zero_json: String,
    /// Optional JSON-encoded object of driver-side values to expose as `globalThis.__ctx`.
    /// `None` means no context — all existing payloads without this field deserialize to `None`.
    #[serde(default)]
    pub context_json: Option<String>,
}

/// Config for one Kafka Direct consume op shipped inside `PipelineOp.payload`.
///
/// The worker creates a one-shot consumer, assigns to `(topic, partition)`,
/// seeks to `start_offset`, polls until it reaches `end_offset` or `max_records`
/// (whichever comes first), then closes the consumer and returns the messages.
#[cfg(feature = "kafka")]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct KafkaConsumePayload {
    pub brokers: String,
    pub topic: String,
    /// Kafka partition index (0-based).
    pub partition: i32,
    /// Inclusive start offset (`assign`+`seek` here).
    pub start_offset: i64,
    /// Exclusive end offset — stop before consuming this offset.
    pub end_offset: i64,
    /// Upper bound on messages consumed regardless of offset gap.
    pub max_records: usize,
}

/// Config for one file-split read op shipped as `data` in a task envelope.
///
/// The worker opens `path`, seeks to `start_byte`, reads UTF-8 lines up to
/// `end_byte` (exclusive, or EOF when `None`), and returns them rkyv-encoded
/// as `Vec<String>`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct FileSplitPayload {
    pub path: String,
    /// Byte offset to seek to before reading (0 = beginning of file).
    pub start_byte: u64,
    /// Exclusive end byte; `None` means read to EOF.
    pub end_byte: Option<u64>,
}

/// Per-shard input for a [`TaskAction::MergeState`] task (bincode-encoded into the
/// task `data`). Content-agnostic: `params` and `partials` are opaque to the data
/// layer and interpreted only by the registered state-merge function.
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct StateMergePayload {
    /// Identifies the shard whose persistent state this task merges into.
    pub state_id: u64,
    /// Merge/emit configuration (e.g. watermark, output mode, window size).
    pub params: Vec<u8>,
    /// This batch's partial state for the shard.
    pub partials: Vec<u8>,
    /// When `Some`, the worker checkpoints the shard's post-merge state to
    /// `{dir}/shard-{state_id}.bin` and, on a cold shard (no in-memory state),
    /// reloads from there first — giving per-shard recovery after a restart.
    pub checkpoint_dir: Option<String>,
}

/// Result status codes for task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ResultStatus {
    Success,
    RetryableFailure,
    FatalFailure,
    /// The worker holds a `cache_source` request for an RDD partition that was
    /// evicted from its `WORKER_PARTITION_CACHE` (LRU pressure, not worker death).
    /// The driver recovers by re-dispatching the full recompute pipeline for this
    /// partition and removing the stale `cache_locs` entry.
    CacheMiss,
}

/// The wire envelope sent from driver to worker for every distributed task.
///
/// Contains everything the worker needs to execute one partition of work:
/// - an ordered pipeline of operations (`ops`) — each carries an `op_id`, `action`, and `payload`
/// - the partition elements (`data`, rkyv-encoded `Vec<T>`)
///
/// Workers execute `ops` in order, threading partition data through each step.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct TaskEnvelope {
    pub version: u16,
    pub run_id: usize,
    pub stage_id: usize,
    pub task_id: usize,
    pub attempt_id: usize,
    pub partition_id: usize,
    /// Correlates scheduler logs and worker logs.
    pub trace_id: String,
    /// Ordered pipeline of operations to apply to the partition.
    pub ops: Vec<PipelineOp>,
    /// Serialized partition elements (rkyv-encoded `Vec<T>`).
    pub data: Vec<u8>,
    /// Broadcast variable payloads: `(broadcast_id, rkyv-encoded value)` pairs.
    pub broadcast_values: Vec<(usize, Vec<u8>)>,
    /// When `Some(rdd_id)`, the worker loads this partition's input from its local
    /// cache instead of `data` — a locality-scheduled read of a previously-cached partition.
    pub cache_source: Option<usize>,
}

impl TaskEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        trace_id: String,
        ops: Vec<PipelineOp>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            trace_id,
            ops,
            data,
            broadcast_values: vec![],
            cache_source: None,
        }
    }

    /// Mark this task to load its input from the worker's partition cache under `rdd_id`.
    pub fn with_cache_source(mut self, rdd_id: usize) -> Self {
        self.cache_source = Some(rdd_id);
        self
    }

    /// Attach broadcast variable payloads to this envelope.
    pub fn with_broadcasts(mut self, values: Vec<(usize, Vec<u8>)>) -> Self {
        self.broadcast_values = values;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct TaskResultEnvelope {
    pub version: u16,
    pub run_id: usize,
    pub stage_id: usize,
    pub task_id: usize,
    pub attempt_id: usize,
    pub partition_id: usize,
    pub status: ResultStatus,
    pub data: Vec<u8>,
    pub error: Option<String>,
    pub worker_id: String,
    /// Set by the worker when the task contained a `ShuffleMap` action op.
    pub shuffle_server_uri: Option<String>,
    /// Accumulator deltas collected during task execution: `(accumulator_id, rkyv-encoded delta)`.
    pub accumulator_deltas: Vec<(usize, Vec<u8>)>,
    /// Partitions this task cached on its worker: `(rdd_id, partition_id)` pairs.
    pub cached_partitions: Vec<(usize, usize)>,
    /// State shards merged on this worker: the `state_id` values from any `MergeState` ops.
    /// The driver uses these to update `state_locs` for autoscaling-robust shard affinity.
    pub held_state_ids: Vec<u64>,
}

impl TaskResultEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn ok(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        worker_id: String,
        data: Vec<u8>,
        shuffle_server_uri: Option<String>,
    ) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            status: ResultStatus::Success,
            data,
            error: None,
            worker_id,
            shuffle_server_uri,
            accumulator_deltas: vec![],
            cached_partitions: vec![],
            held_state_ids: vec![],
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn retryable_failure(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        worker_id: String,
        error: String,
        data: Vec<u8>,
        shuffle_server_uri: Option<String>,
    ) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            status: ResultStatus::RetryableFailure,
            data,
            error: Some(error),
            worker_id,
            shuffle_server_uri,
            accumulator_deltas: vec![],
            cached_partitions: vec![],
            held_state_ids: vec![],
        }
    }

    pub fn fatal_failure(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        worker_id: String,
        error: String,
    ) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            status: ResultStatus::FatalFailure,
            data: Vec::new(),
            error: Some(error),
            worker_id,
            shuffle_server_uri: None,
            accumulator_deltas: vec![],
            cached_partitions: vec![],
            held_state_ids: vec![],
        }
    }

    /// The worker's `WORKER_PARTITION_CACHE` entry for this partition was evicted
    /// (LRU pressure). The driver should re-dispatch a full recompute task.
    pub fn cache_miss(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        worker_id: String,
        rdd_id: usize,
    ) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            status: ResultStatus::CacheMiss,
            data: Vec::new(),
            error: Some(format!(
                "cache evicted: rdd {rdd_id} partition {partition_id}"
            )),
            worker_id,
            shuffle_server_uri: None,
            accumulator_deltas: vec![],
            cached_partitions: vec![],
            held_state_ids: vec![],
        }
    }

    /// Attach accumulator deltas to a successful result (called by NativeBackend).
    pub fn with_accumulator_deltas(mut self, deltas: Vec<(usize, Vec<u8>)>) -> Self {
        self.accumulator_deltas = deltas;
        self
    }

    /// Attach the partitions this task cached on its worker (called by NativeBackend).
    pub fn with_cached_partitions(mut self, cached: Vec<(usize, usize)>) -> Self {
        self.cached_partitions = cached;
        self
    }

    /// Attach the state shard IDs merged on this worker (called by NativeBackend for
    /// `MergeState` ops). The driver uses these to build `state_locs` for affinity routing.
    pub fn with_held_state_ids(mut self, ids: Vec<u64>) -> Self {
        self.held_state_ids = ids;
        self
    }
}
