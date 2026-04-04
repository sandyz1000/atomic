pub mod cartesian;
pub mod co_grouped;
pub mod coalesced;
pub mod flatmapper;
pub mod map_partitions;
pub mod mapper;
pub mod pair;
pub mod parallel_collection;
pub mod partitionwise_sampled;
pub mod rdd_val;
pub mod shuffled;
pub mod typed;
pub mod union_rdd;
pub mod wasm;
pub mod zip;

pub use crate::context::Context;
pub use atomic_data::task_context::TaskContext;
pub use atomic_utils::bpq::BoundedPriorityQueue;

pub use atomic_data::{
    data::Data,
    error::BaseError,
    rdd::{Rdd, RddBase},
};

pub(crate) fn rdd_wasm_bytes<T: Data>(
    rdd: &dyn Rdd<Item = T>,
    partition: usize,
    error_message: impl FnOnce() -> String,
) -> Option<Result<Vec<u8>, BaseError>> {
    let split = rdd.splits().get(partition)?.clone();
    let iter = match rdd.iterator(split) {
        Ok(iter) => iter,
        Err(err) => return Some(Err(err)),
    };

    let bytes = iter
        .map(|item| item.as_any().downcast_ref::<u8>().copied())
        .collect::<Option<Vec<u8>>>();

    Some(bytes.ok_or_else(|| BaseError::Other(error_message())))
}

pub(crate) fn inherited_wasm_bytes<T: Data>(
    prev: &Arc<dyn Rdd<Item = T>>,
    partition: usize,
    error_message: impl FnOnce() -> String,
) -> Option<Result<Vec<u8>, BaseError>> {
    if let Some(bytes) = prev.get_rdd_base().wasm_bytes(partition) {
        return Some(bytes);
    }

    rdd_wasm_bytes(prev.as_ref(), partition, error_message)
}

// Re-export commonly used types
pub use parallel_collection::ParallelCollection;
pub use typed::TypedRdd;
pub use union_rdd::UnionRdd;
pub use wasm::{WasmBytesRddExt, WasmRddExt};
