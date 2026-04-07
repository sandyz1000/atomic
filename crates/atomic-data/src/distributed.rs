use crate::error::{BaseError, BaseResult};
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, de::pooling::Pool,
    rancor::Error, ser::allocator::ArenaHandle, util::AlignedVec,
};
use serde::{Deserialize, Serialize};

pub type RkyvWireSerializer<'a> =
    rkyv::api::high::HighSerializer<AlignedVec, ArenaHandle<'a>, Error>;
pub type RkyvWireValidator<'a> = rkyv::api::high::HighValidator<'a, Error>;
pub type RkyvWireStrategy = rkyv::rancor::Strategy<Pool, Error>;

/// Semantic version for wire contracts used by distributed task transport.
pub const WIRE_SCHEMA_V1: u16 = 1;
pub const TRANSPORT_FRAME_MAGIC: [u8; 4] = *b"ATOM";
pub const TRANSPORT_FRAME_VERSION_V1: u8 = 1;
pub const TRANSPORT_HEADER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFrameKind {
    TaskEnvelope = 3,
    TaskResultEnvelope = 4,
    WorkerCapabilities = 5,
}

impl TryFrom<u8> for TransportFrameKind {
    type Error = BaseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::TaskEnvelope),
            4 => Ok(Self::TaskResultEnvelope),
            5 => Ok(Self::WorkerCapabilities),
            _ => Err(BaseError::Other(format!(
                "unknown transport frame kind: {}",
                value
            ))),
        }
    }
}

pub fn encode_transport_frame(kind: TransportFrameKind, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(TRANSPORT_HEADER_LEN + payload.len());
    frame.extend_from_slice(&TRANSPORT_FRAME_MAGIC);
    frame.push(TRANSPORT_FRAME_VERSION_V1);
    frame.push(kind as u8);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn parse_transport_header(
    header: &[u8; TRANSPORT_HEADER_LEN],
) -> BaseResult<(TransportFrameKind, usize)> {
    if header[..4] != TRANSPORT_FRAME_MAGIC {
        return Err(BaseError::Other(
            "invalid transport frame magic".to_string(),
        ));
    }

    if header[4] != TRANSPORT_FRAME_VERSION_V1 {
        return Err(BaseError::Other(format!(
            "unsupported transport frame version: {}",
            header[4]
        )));
    }

    let kind = TransportFrameKind::try_from(header[5])?;
    let payload_len = u32::from_be_bytes([header[6], header[7], header[8], header[9]]) as usize;
    Ok((kind, payload_len))
}

/// Encodes a value into the distributed wire format.
pub trait WireEncode {
    fn encode_wire(&self) -> BaseResult<Vec<u8>>;
}

/// Decodes a value from the distributed wire format.
pub trait WireDecode: Sized {
    fn decode_wire(bytes: &[u8]) -> BaseResult<Self>;
}

impl<T> WireEncode for T
where
    T: for<'a> RkyvSerialize<RkyvWireSerializer<'a>>,
{
    fn encode_wire(&self) -> BaseResult<Vec<u8>> {
        rkyv::to_bytes::<Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(|err| BaseError::Other(err.to_string()))
    }
}

impl<T> WireDecode for T
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
        + RkyvDeserialize<T, RkyvWireStrategy>,
{
    fn decode_wire(bytes: &[u8]) -> BaseResult<Self> {
        rkyv::from_bytes::<T, Error>(bytes).map_err(|err| BaseError::Other(err.to_string()))
    }
}

/// One step in a multi-op pipeline sent to a worker.
///
/// A [`TaskEnvelope`] carries a sequence of these; the worker threads partition
/// data through them in order, feeding each step's output as the next step's input.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct PipelineOp {
    /// Registered op_id, e.g. `"task_double::double"`. Looked up in the worker's
    /// compile-time dispatch table.
    pub op_id: String,
    /// Which action to perform with this function.
    pub action: TaskAction,
    /// rkyv-encoded config: fold zero value for Fold/Aggregate, empty for Map/Filter.
    pub payload: Vec<u8>,
}

/// The action the worker should apply over the partition data using the named task function.
///
/// The driver sets this based on which RDD operation triggered the task submission.
/// The worker dispatch handler reads this to decide how to iterate over the partition.
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
    ShuffleMap { shuffle_id: usize, num_output_partitions: usize },
    /// Execute a Python UDF with the given operation.
    /// `payload` is a serde_json-encoded [`PythonUdfPayload`].
    /// Partition `data` is a JSON-encoded `Vec` of elements.
    PythonUdf(UdfAction),
    /// Execute a JavaScript UDF with the given operation.
    /// `payload` is a serde_json-encoded [`JsUdfPayload`].
    /// Partition `data` is a JSON-encoded `Vec` of elements.
    JavaScriptUdf(UdfAction),
}

/// Which operation a UDF pipeline step should perform.
///
/// Embedded directly in [`TaskAction::PythonUdf`] and [`TaskAction::JavaScriptUdf`] so
/// the UDF runtime does not need to store a separate action string in the payload.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum UdfAction {
    Map,
    Filter,
    FlatMap,
    MapValues,
    FlatMapValues,
    KeyBy,
    Reduce,
    Fold,
}

/// Metadata carried in `PipelineOp.payload` for a Python UDF step.
///
/// Serialized as JSON so both Python (via `json` stdlib) and Rust (`serde_json`) can
/// produce and consume it without a shared binary format.
/// The operation to perform is carried in [`TaskAction::PythonUdf`], not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonUdfPayload {
    /// `cloudpickle`/`pickle`-serialized Python callable.
    pub fn_bytes: Vec<u8>,
    /// `pickle`-serialized fold zero value (empty for non-fold operations).
    pub zero_bytes: Vec<u8>,
}

/// Metadata carried in `PipelineOp.payload` for a JavaScript UDF step.
/// The operation to perform is carried in [`TaskAction::JavaScriptUdf`], not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsUdfPayload {
    /// JavaScript function source obtained via `fn.toString()`.
    pub fn_source: String,
    /// JSON-encoded zero value for fold (empty string for non-fold).
    pub zero_json: String,
}

/// Result status codes for task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ResultStatus {
    Success,
    RetryableFailure,
    FatalFailure,
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
    /// Workers execute these in sequence, feeding each result as the next step's input.
    pub ops: Vec<PipelineOp>,
    /// Serialized partition elements (rkyv-encoded `Vec<T>`).
    pub data: Vec<u8>,
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
        }
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
    /// Carries the worker's `ShuffleManager` base URI so the driver can register
    /// it with `MapOutputTracker` without decoding `data`.
    pub shuffle_server_uri: Option<String>,
}

impl TaskResultEnvelope {
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
        }
    }

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
        }
    }
}

/// Worker capabilities reported to the driver on handshake.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct WorkerCapabilities {
    pub version: u16,
    pub worker_id: String,
    pub max_tasks: u16,
}

impl WorkerCapabilities {
    pub fn new(worker_id: String, max_tasks: u16) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            worker_id,
            max_tasks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_envelope_round_trips_with_rkyv() {
        let ops = vec![PipelineOp {
            op_id: "mycrate::double".to_string(),
            action: TaskAction::Map,
            payload: vec![],
        }];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 4, "trace-1".to_string(), ops, vec![1, 2, 3]);
        let bytes = envelope.encode_wire().expect("serialize envelope");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize envelope");
        assert_eq!(decoded.ops.len(), 1);
        assert_eq!(decoded.ops[0].op_id, "mycrate::double");
        assert_eq!(decoded.ops[0].action, TaskAction::Map);
        assert_eq!(decoded.data, vec![1, 2, 3]);
    }

    #[test]
    fn task_result_envelope_ok_round_trips() {
        let result = TaskResultEnvelope::ok(1, 2, 3, 0, 0, "worker-1".to_string(), vec![4, 5, 6], None);
        let bytes = result.encode_wire().expect("serialize result");
        let decoded = TaskResultEnvelope::decode_wire(&bytes).expect("deserialize result");
        assert_eq!(decoded.status, ResultStatus::Success);
        assert_eq!(decoded.data, vec![4, 5, 6]);
        assert_eq!(decoded.worker_id, "worker-1");
    }

    #[test]
    fn task_action_fold_round_trips() {
        let ops = vec![PipelineOp {
            op_id: "mycrate::sum".to_string(),
            action: TaskAction::Fold,
            payload: 0_i32.to_le_bytes().to_vec(),
        }];
        let envelope =
            TaskEnvelope::new(1, 2, 3, 0, 4, "trace-2".to_string(), ops, vec![1, 2, 3]);
        let bytes = envelope.encode_wire().expect("serialize");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize");
        assert_eq!(decoded.ops[0].action, TaskAction::Fold);
        assert_eq!(decoded.ops[0].payload, 0_i32.to_le_bytes().to_vec());
    }

    #[test]
    fn shuffle_map_action_carries_ids() {
        let ops = vec![PipelineOp {
            op_id: "sys.shuffle_map".to_string(),
            action: TaskAction::ShuffleMap { shuffle_id: 7, num_output_partitions: 4 },
            payload: vec![],
        }];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 4, "trace-3".to_string(), ops, vec![]);
        let bytes = envelope.encode_wire().expect("serialize");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize");
        assert!(matches!(
            decoded.ops[0].action,
            TaskAction::ShuffleMap { shuffle_id: 7, num_output_partitions: 4 }
        ));
    }

    #[test]
    fn multi_op_pipeline_round_trips() {
        let ops = vec![
            PipelineOp {
                op_id: "myapp::double".to_string(),
                action: TaskAction::Map,
                payload: vec![],
            },
            PipelineOp {
                op_id: "myapp::is_positive".to_string(),
                action: TaskAction::Filter,
                payload: vec![],
            },
            PipelineOp {
                op_id: "myapp::add".to_string(),
                action: TaskAction::Fold,
                payload: 0_i32.to_le_bytes().to_vec(),
            },
        ];
        let envelope = TaskEnvelope::new(1, 2, 3, 0, 0, "trace-multi".to_string(), ops, vec![]);
        let bytes = envelope.encode_wire().expect("serialize");
        let decoded = TaskEnvelope::decode_wire(&bytes).expect("deserialize");
        assert_eq!(decoded.ops.len(), 3);
        assert_eq!(decoded.ops[0].op_id, "myapp::double");
        assert_eq!(decoded.ops[1].action, TaskAction::Filter);
        assert_eq!(decoded.ops[2].op_id, "myapp::add");
    }

    #[test]
    fn worker_capabilities_round_trips() {
        let caps = WorkerCapabilities::new("worker-42".to_string(), 8);
        let bytes = caps.encode_wire().expect("serialize caps");
        let decoded = WorkerCapabilities::decode_wire(&bytes).expect("deserialize caps");
        assert_eq!(decoded.worker_id, "worker-42");
        assert_eq!(decoded.max_tasks, 8);
    }

    #[test]
    fn legacy_frame_ids_are_rejected() {
        assert!(TransportFrameKind::try_from(1).is_err());
        assert!(TransportFrameKind::try_from(2).is_err());
    }

    #[test]
    fn rkyv_codec_round_trips_partition_values() {
        let encoded = vec![1_u32, 2, 3].encode_wire().expect("serialize values");
        let decoded = Vec::<u32>::decode_wire(&encoded).expect("deserialize values");
        assert_eq!(decoded, vec![1, 2, 3]);
    }
}
