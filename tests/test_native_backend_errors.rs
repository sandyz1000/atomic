//! `NativeBackend::execute()` edge cases — empty pipeline and pipeline
//! data-threading correctness.
//!
//! The existing inline test in `native_executor.rs` covers the unknown-op-id path.
//! These tests add coverage for:
//!
//!   • Empty `ops` vector: should return `Err` (or `FatalFailure`), not panic.
//!     Currently handled at the top of `NativeBackend::execute` in `native_executor.rs` —
//!     returning `Err(Error::UnknownOperation("empty pipeline"))`.
//!
//!   • Multi-op pipeline data threading: each op must receive the OUTPUT of the
//!     previous op as its input. Verifies the `for op in &task.ops` loop in
//!     `native_executor.rs`.
//!
//!   • FatalFailure is propagated cleanly — the scheduler must never see a panic.
//!
//! Note: these tests call `NativeBackend` and `TaskEnvelope` via the public API
//! (`atomic_compute::backend::NativeBackend`, `atomic_data::distributed::*`).

use atomic_compute::runtimes::{Backend, ComputeEngine};
use atomic_compute::context::Context;
use atomic_compute::env::Config;
use atomic_compute::task;
use atomic_compute::task_traits::{BinaryTask, UnaryTask};
use atomic_data::distributed::{
    PipelineOp, ResultStatus, TaskAction, TaskEnvelope, TaskRuntime, WireDecode, WireEncode,
};
use std::sync::Arc;

// ── Helper tasks ──────────────────────────────────────────────────────────────

#[task]
fn add_ten(x: i32) -> i32 {
    x + 10
}

#[task]
fn double_val(x: i32) -> i32 {
    x * 2
}

#[task]
fn keep_positive(x: i32) -> Option<i32> {
    if x > 0 { Some(x) } else { None }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn encode<T: WireEncode>(v: T) -> Vec<u8> {
    v.encode_wire().expect("encode failed")
}

fn decode<T: WireDecode>(data: &[u8]) -> T {
    T::decode_wire(data).expect("decode failed")
}

fn make_envelope_with_ops(ops: Vec<PipelineOp>, data: Vec<u8>) -> TaskEnvelope {
    TaskEnvelope::new(1, 1, 1, 0, 0, "test".to_string(), ops, data)
}

fn single_op(op_id: &str, action: TaskAction) -> PipelineOp {
    PipelineOp {
        op_id: op_id.to_string(),
        action,
        runtime: TaskRuntime::Native,
        payload: vec![],
    }
}

// ── Empty pipeline ────────────────────────────────────────────────────────────

/// Empty `ops` must return `Err`, not panic.
///
/// `NativeBackend::execute` guards against this at the top of the function —
/// returning `Err(Error::UnknownOperation("empty pipeline"))`.
///
/// This test documents and pins that behaviour. If it breaks, someone removed
/// the guard — a regression.
#[test]
fn test_empty_pipeline() {
    let backend = ComputeEngine::default();
    let task = make_envelope_with_ops(vec![], vec![]);
    let result = backend.execute("worker-0", &task);
    assert!(
        result.is_err(),
        "empty pipeline must return Err, not Ok with empty result"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("empty") || err_msg.contains("pipeline"),
        "expected an 'empty pipeline' error message, got: {err_msg}"
    );
}

// ── Unknown op_id → FatalFailure ──────────────────────────────────────────────

/// An unknown op_id must produce `FatalFailure`, not panic, and include the
/// op_id in the error message for easy debugging.
///
/// This complements the inline test in `native_executor.rs` with an external call.
#[test]
fn test_unknown_op_fatal() {
    let backend = ComputeEngine::default();
    let task = make_envelope_with_ops(
        vec![single_op("no.such.handler.xyz", TaskAction::Map)],
        encode(vec![1i32, 2, 3]),
    );
    let envelope = backend.execute("worker-0", &task).unwrap();
    assert_eq!(envelope.status, ResultStatus::FatalFailure);
    let err = envelope.error.as_deref().unwrap_or("");
    assert!(
        err.contains("no.such.handler.xyz"),
        "error message should name the missing op_id, got: {err}"
    );
}

// ── Multi-op pipeline data threading ─────────────────────────────────────────

/// A two-op pipeline must feed each op the output of the previous op.
///
/// Pipeline: vec![1, 2, 3]
///   op1: add_ten   → [11, 12, 13]
///   op2: double_val → [22, 24, 26]
///
/// Validates the data-threading loop in `native_executor.rs`.
#[test]
fn test_2op_thread() {
    let backend = ComputeEngine::default();
    let input: Vec<i32> = vec![1, 2, 3];
    let ops = vec![
        single_op(<AddTen as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
        single_op(<DoubleVal as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
    ];
    let task = make_envelope_with_ops(ops, encode(input));
    let result = backend.execute("worker-0", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
    let output: Vec<i32> = decode(&result.data);
    assert_eq!(output, vec![22, 24, 26]);
}

/// A three-op pipeline to stress data threading further.
///
/// Pipeline: vec![5, -3, 8]
///   op1: add_ten   → [15, 7, 18]
///   op2: add_ten   → [25, 17, 28]
///   op3: double_val → [50, 34, 56]
#[test]
fn test_3op_thread() {
    let backend = ComputeEngine::default();
    let input: Vec<i32> = vec![5, -3, 8];
    let ops = vec![
        single_op(<AddTen as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
        single_op(<AddTen as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
        single_op(<DoubleVal as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
    ];
    let task = make_envelope_with_ops(ops, encode(input));
    let result = backend.execute("worker-0", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
    let output: Vec<i32> = decode(&result.data);
    assert_eq!(output, vec![50, 34, 56]);
}

/// A failing op mid-pipeline must produce `FatalFailure` and NOT execute
/// subsequent ops (the pipeline stops at the first error).
#[test]
fn test_mid_op_shortcircuit() {
    let backend = ComputeEngine::default();
    let ops = vec![
        single_op(<AddTen as UnaryTask<i32, i32>>::NAME, TaskAction::Map),
        single_op("DOES_NOT_EXIST", TaskAction::Map), // fails here
        single_op(<DoubleVal as UnaryTask<i32, i32>>::NAME, TaskAction::Map), // must NOT run
    ];
    let task = make_envelope_with_ops(ops, encode(vec![1i32]));
    let result = backend.execute("worker-0", &task).unwrap();
    assert_eq!(result.status, ResultStatus::FatalFailure);
}

// ── Fold / Reduce actions ─────────────────────────────────────────────────────

/// A fold op uses `TaskAction::Fold` rather than `Map`.
/// This test verifies that builtin `sum::i32` correctly sums a partition.
#[test]
fn test_fold_op_sums_partition() {
    let backend = ComputeEngine::default();
    let zero: i32 = 0;
    let items: Vec<i32> = vec![1, 2, 3, 4, 5];
    let op = PipelineOp {
        op_id: "atomic::builtin::sum::i32".to_string(),
        action: TaskAction::Fold,
        runtime: TaskRuntime::Native,
        payload: encode(zero),
    };
    let task = make_envelope_with_ops(vec![op], encode(items));
    let result = backend.execute("worker-0", &task).unwrap();
    assert_eq!(result.status, ResultStatus::Success);
    let sum: i32 = decode(&result.data);
    assert_eq!(sum, 15);
}

// ── Context-level pipeline dispatch ──────────────────────────────────────────

/// Full Context pipeline smoke test: map_task + collect in local mode.
/// Exercises the entire dispatch path (Context → LocalScheduler → NativeBackend).
#[tokio::test]
async fn test_context_map_collect() {
    let ctx = Arc::new(Context::new_with_config(Config::local()).unwrap());
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3, 4], 2)
        .map_task(AddTen)
        .collect()
        .unwrap();
    let mut sorted = result;
    sorted.sort();
    assert_eq!(sorted, vec![11, 12, 13, 14]);
}

/// Two chained `map_task` calls exercise multi-op dispatch from Context.
#[tokio::test]
async fn test_context_chained_maps() {
    let ctx = Arc::new(Context::new_with_config(Config::local()).unwrap());
    let result = ctx
        .parallelize_typed(vec![1i32, 2, 3], 1)
        .map_task(AddTen)
        .map_task(DoubleVal)
        .collect()
        .unwrap();
    assert_eq!(result, vec![22, 24, 26]);
}
