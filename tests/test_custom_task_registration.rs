//! Usage example + test: defining and registering a *custom* task the same way
//! `atomic-compute`'s `builtin_tasks` (`max`, `mean`, `top_k`, ...) do, using the three
//! hand-registration macros (`register_binary_task!`, `register_aggregate_task!`,
//! `register_partition_task!`) directly rather than `#[task]`.
//!
//! `#[task]` covers the common `fn(T) -> U` / `fn(T, T) -> T` shapes. These three macros
//! exist for the shapes it can't express — an accumulator type distinct from the element
//! type ([`AggregateTask`]), or a whole-partition transform with no element-level combine
//! ([`PartitionTask`]) — see `atomic_compute::builtin_tasks` module docs for the full
//! shape-to-trait-to-macro table. This test dispatches each through the same
//! `TASK_REGISTRY` path a worker uses, proving end-to-end that a type defined outside
//! `atomic-compute` (this crate) registers and dispatches identically to a built-in.

use atomic_compute::register_binary_task;
use atomic_compute::registry::TASK_REGISTRY;
use atomic_compute::task_traits::{AggregateTask, BinaryTask, PartitionTask};
use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
use atomic_data::error::DataResult;

fn encode<T: WireEncode>(v: T) -> Vec<u8> {
    v.encode_wire().expect("encode")
}

fn decode<T: WireDecode>(data: &[u8]) -> T {
    T::decode_wire(data).expect("decode")
}

#[derive(Clone, Copy, Default)]
struct GcdTask;

impl BinaryTask<u64> for GcdTask {
    const NAME: &'static str = "test_custom_task_registration::gcd";
    fn call(&self, a: u64, b: u64) -> u64 {
        let (mut a, mut b) = (a, b);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    }
}

register_binary_task!(GcdTask, u64);

#[test]
fn test_binary_task_dispatch() {
    let entry = *TASK_REGISTRY
        .get(GcdTask::NAME)
        .expect("GcdTask not found in TASK_REGISTRY — register_binary_task! did not register it");

    let data = encode(vec![54u64, 24, 18]);
    let out = entry
        .call(&TaskAction::Fold, &[], &data)
        .expect("dispatch failed");
    let result: u64 = decode(&out);

    assert_eq!(result, 6); // gcd(gcd(54, 24), 18) == 6
}

#[derive(Clone, Copy, Default)]
struct SumCountTask;

impl AggregateTask<(f64, u64), f64> for SumCountTask {
    const NAME: &'static str = "test_custom_task_registration::sum_count";
    fn seq(&self, (sum, n): (f64, u64), x: f64) -> (f64, u64) {
        (sum + x, n + 1)
    }
    fn comb(&self, a: (f64, u64), b: (f64, u64)) -> (f64, u64) {
        (a.0 + b.0, a.1 + b.1)
    }
}

atomic_compute::register_aggregate_task!(SumCountTask, (f64, u64), f64);

#[test]
fn test_aggregate_task_dispatch() {
    let entry = *TASK_REGISTRY
        .get(<SumCountTask as AggregateTask<(f64, u64), f64>>::NAME)
        .expect(
            "SumCountTask not found in TASK_REGISTRY — register_aggregate_task! did not register it",
        );

    let zero = encode((0.0f64, 0u64));
    let data = encode(vec![1.0f64, 2.0, 3.0, 4.0]);
    let out = entry
        .call(&TaskAction::Aggregate, &zero, &data)
        .expect("dispatch failed");
    let (sum, n): (f64, u64) = decode(&out);

    assert_eq!((sum, n), (10.0, 4));
}

#[derive(Default)]
struct ReverseTask;

impl PartitionTask<i32> for ReverseTask {
    const NAME: &'static str = "test_custom_task_registration::reverse";
    fn transform(&self, mut items: Vec<i32>, _payload: &[u8]) -> DataResult<Vec<i32>> {
        items.reverse();
        Ok(items)
    }
}

atomic_compute::register_partition_task!(ReverseTask, i32);

#[test]
fn test_partition_task_dispatch() {
    let entry = *TASK_REGISTRY
        .get(<ReverseTask as PartitionTask<i32>>::NAME)
        .expect(
            "ReverseTask not found in TASK_REGISTRY — register_partition_task! did not register it",
        );

    let data = encode(vec![1i32, 2, 3, 4]);
    let out = entry
        .call(&TaskAction::Collect, &[], &data)
        .expect("dispatch failed");
    let result: Vec<i32> = decode(&out);

    assert_eq!(result, vec![4, 3, 2, 1]);
}
