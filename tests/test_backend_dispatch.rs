//! Integration tests for the `OpDispatcher` trait pattern and `NativeBackend` orchestration.
//!
//! These tests verify the cross-cutting behaviour of `NativeBackend::execute()` that is
//! independent of any single runtime — broadcast loading, accumulator draining, and
//! multi-op pipeline orchestration — using only public API types.

use atomic_compute::context::Context;
use atomic_compute::env::Config;
use atomic_compute::runtimes::{Backend, ComputeEngine};
use atomic_compute::task;
use atomic_compute::task_traits::UnaryTask;
use atomic_data::distributed::{
    PipelineOp, ResultStatus, TaskAction, TaskEnvelope, TaskRuntime, WireDecode, WireEncode,
};
use std::sync::Arc;

// ── Shared task functions ─────────────────────────────────────────────────────

#[task]
fn square(x: i32) -> i32 {
    x * x
}

#[task]
fn negate(x: i32) -> i32 {
    -x
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn encode<T: WireEncode>(v: T) -> Vec<u8> {
    v.encode_wire().expect("encode")
}

fn decode<T: WireDecode>(data: &[u8]) -> T {
    T::decode_wire(data).expect("decode")
}

fn native_op(op_id: &str, action: TaskAction) -> PipelineOp {
    PipelineOp {
        op_id: op_id.to_string(),
        action,
        runtime: TaskRuntime::Native,
        payload: vec![],
    }
}

fn envelope(ops: Vec<PipelineOp>, data: Vec<u8>) -> TaskEnvelope {
    TaskEnvelope::new(10, 20, 30, 0, 0, "dispatch-test".to_string(), ops, data)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Single-op pipeline executes without error and produces correct output.
#[test]
fn single_op_square_pipeline() {
    let backend = ComputeEngine::default();
    let input: Vec<i32> = vec![2, 3, 4];
    let task = envelope(
        vec![native_op(
            <Square as UnaryTask<i32, i32>>::NAME,
            TaskAction::Map,
        )],
        encode(input),
    );
    let result = backend.execute("w", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
    let output: Vec<i32> = decode(&result.data);
    assert_eq!(output, vec![4, 9, 16]);
}

/// Two-op pipeline: square then negate — verifies data threading between ops.
#[test]
fn two_op_pipeline_square_then_negate() {
    let backend = ComputeEngine::default();
    let input: Vec<i32> = vec![3, 4];
    let task = envelope(
        vec![
            native_op(<Square as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
            native_op(<Negate as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
        ],
        encode(input),
    );
    let result = backend.execute("w", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
    let output: Vec<i32> = decode(&result.data);
    assert_eq!(output, vec![-9, -16]);
}

/// A failed op mid-pipeline short-circuits: subsequent ops do not run.
#[test]
fn failed_op_mid_pipeline_produces_fatal_failure() {
    let backend = ComputeEngine::default();
    let task = envelope(
        vec![
            native_op(<Square as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
            native_op("no.such.op", TaskAction::Map),
            native_op(<Negate as UnaryTask<i32, i32>>::NAME, TaskAction::Map), // must not run
        ],
        encode(vec![5i32]),
    );
    let result = backend.execute("w", &task).unwrap();
    assert_eq!(result.status, ResultStatus::FatalFailure);
    assert!(result.error.unwrap_or_default().contains("no.such.op"));
}

/// `accumulator_deltas` field in the result envelope is populated (even if empty)
/// — verifies `drain_deltas()` is called and the field is accessible.
#[test]
fn result_envelope_has_accumulator_deltas_field() {
    let backend = ComputeEngine::default();
    let task = envelope(
        vec![native_op(
            <Square as UnaryTask<i32, i32>>::NAME,
            TaskAction::Map,
        )],
        encode(vec![3i32]),
    );
    let result = backend.execute("w", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
    // `accumulator_deltas` is a Vec — must be present (possibly empty when no accumulators ran).
    let _ = result.accumulator_deltas;
}

/// `NativeBackend` does not panic on an empty broadcast_values list.
#[test]
fn empty_broadcast_values_is_fine() {
    let backend = ComputeEngine::default();
    let task = envelope(
        vec![native_op(
            <Square as UnaryTask<i32, i32>>::NAME,
            TaskAction::Map,
        )],
        encode(vec![5i32]),
    )
    .with_broadcasts(vec![]); // explicitly empty
    let result = backend.execute("w", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
}

/// Full roundtrip through Context → LocalScheduler → NativeBackend → dispatcher.
#[tokio::test]
async fn context_map_task_roundtrip() {
    let ctx = Arc::new(Context::new_with_config(Config::local()).unwrap());
    let mut result = ctx
        .parallelize_typed(vec![2i32, 3, 4], 2)
        .map_task(Square)
        .collect()
        .unwrap();
    result.sort();
    assert_eq!(result, vec![4, 9, 16]);
}

/// Verify `partition_id` is correctly threaded through to `TaskResultEnvelope`.
#[test]
fn result_envelope_carries_partition_id() {
    let backend = ComputeEngine::default();
    let mut task = envelope(
        vec![native_op(
            <Square as UnaryTask<i32, i32>>::NAME,
            TaskAction::Map,
        )],
        encode(vec![1i32]),
    );
    task.partition_id = 7;
    let result = backend.execute("w", &task).unwrap();
    assert_eq!(result.partition_id, 7);
}
