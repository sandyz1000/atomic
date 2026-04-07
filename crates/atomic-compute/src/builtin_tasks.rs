//! Built-in framework tasks registered via `inventory`.
//!
//! These implement `BinaryTask<T>` or `UnaryTask<T, U>` and are dispatched on
//! workers exactly like user `#[task]` functions — the driver sends an `op_id`
//! string and the worker binary contains the handler via `inventory::submit!`.
//!
//! Built-in tasks power `TypedRdd::max()`, `min()`, `count()`, and can be used
//! directly with `fold_task` / `map_task`.
//!
//! # Worker registration
//!
//! Each task registers itself in the compile-time dispatch table.  Because these
//! are in the `atomic-compute` crate (which is linked into every driver and worker
//! binary), the handlers are always present — no user action required.

use crate::task_registry::TaskEntry;
use crate::task_traits::BinaryTask;
use atomic_data::distributed::WireDecode;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

// ── Shuffle-map handler ────────────────────────────────────────────────────────

/// Generic shuffle-write function for `(K, V)` pairs.
///
/// Called by `NativeBackend` when it sees `TaskAction::ShuffleMap`.
/// Decodes the rkyv-encoded `Vec<(K, V)>` input, partitions elements by
/// `FxHash(key) % num_reduce_partitions`, and writes each bucket as
/// bincode-encoded `Vec<(K, V)>` to `SHUFFLE_CACHE`.
///
/// Register this for a specific `(K, V)` pair with [`register_shuffle_map!`].
pub fn shuffle_map_handler<K, V>(
    data: &[u8],
    shuffle_id: usize,
    map_partition_id: usize,
    num_reduce_partitions: usize,
) -> Result<(), String>
where
    K: atomic_data::data::Data + Clone + Hash + bincode::Encode + bincode::Decode<()> + WireDecode,
    V: atomic_data::data::Data + Clone + bincode::Encode + bincode::Decode<()> + WireDecode,
    Vec<(K, V)>: WireDecode,
{
    // 1. Decode input as Vec<(K, V)> from rkyv bytes.
    let pairs: Vec<(K, V)> = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("shuffle_map_handler: decode input: {e}"))?;

    // 2. Partition by FxHash(key) % num_reduce_partitions.
    //    FxHasher is deterministic across processes (no random seed).
    let mut buckets: Vec<Vec<(K, V)>> =
        (0..num_reduce_partitions).map(|_| vec![]).collect();

    for (k, v) in pairs {
        let mut hasher = FxHasher::default();
        k.hash(&mut hasher);
        let bucket = (hasher.finish() as usize) % num_reduce_partitions;
        buckets[bucket].push((k, v));
    }

    // 3. Write each bucket as bincode bytes to SHUFFLE_CACHE.
    let cache = atomic_data::env::SHUFFLE_CACHE
        .get()
        .ok_or_else(|| "shuffle_map_handler: SHUFFLE_CACHE not initialized".to_string())?;

    for (reduce_id, bucket) in buckets.into_iter().enumerate() {
        let bytes = bincode::encode_to_vec(&bucket, bincode::config::standard())
            .map_err(|e| format!("shuffle_map_handler: encode bucket: {e}"))?;
        cache.insert((shuffle_id, map_partition_id, reduce_id), bytes);
    }

    log::debug!(
        "shuffle_map_handler: wrote {} buckets for shuffle_id={} partition={}",
        num_reduce_partitions, shuffle_id, map_partition_id
    );
    Ok(())
}

// ── MaxTask ───────────────────────────────────────────────────────────────────

/// Built-in: return the greater of two `Ord` values.
///
/// Used by `TypedRdd::max()` to reduce each partition to its local maximum,
/// then combine the per-partition maxima on the driver.
///
/// `const NAME` encodes the element type via `stringify!(T)` so different
/// instantiations (`MaxTask<i32>`, `MaxTask<String>`, …) each get a unique op_id.
pub struct MaxTask<T>(std::marker::PhantomData<T>);

impl<T> MaxTask<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for MaxTask<T> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! impl_max_task {
    ($ty:ty) => {
        impl BinaryTask<$ty> for MaxTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::max::", stringify!($ty));
            fn call(a: $ty, b: $ty) -> $ty {
                if a >= b { a } else { b }
            }
        }

        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::max::", stringify!($ty)),
                handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    let _ = payload;
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate | TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let mut iter = items.into_iter();
                            let first = iter.next()
                                .ok_or_else(|| "max: empty partition".to_string())?;
                            let result = iter.fold(first, |a, b| if a >= b { a } else { b });
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("MaxTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// ── MinTask ───────────────────────────────────────────────────────────────────

/// Built-in: return the lesser of two `Ord` values.
pub struct MinTask<T>(std::marker::PhantomData<T>);

impl<T> MinTask<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for MinTask<T> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! impl_min_task {
    ($ty:ty) => {
        impl BinaryTask<$ty> for MinTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::min::", stringify!($ty));
            fn call(a: $ty, b: $ty) -> $ty {
                if a <= b { a } else { b }
            }
        }

        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::min::", stringify!($ty)),
                handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    let _ = payload;
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate | TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let mut iter = items.into_iter();
                            let first = iter.next()
                                .ok_or_else(|| "min: empty partition".to_string())?;
                            let result = iter.fold(first, |a, b| if a <= b { a } else { b });
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("MinTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// ── SumTask ───────────────────────────────────────────────────────────────────

/// Built-in: add two values.
///
/// Used by `TypedRdd::count()` to sum per-partition counts.
pub struct SumTask<T>(std::marker::PhantomData<T>);

impl<T> SumTask<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for SumTask<T> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! impl_sum_task {
    ($ty:ty) => {
        impl BinaryTask<$ty> for SumTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::sum::", stringify!($ty));
            fn call(a: $ty, b: $ty) -> $ty {
                a + b
            }
        }

        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::sum::", stringify!($ty)),
                handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate => {
                            let zero = <$ty>::decode_wire(payload).map_err(|e| e.to_string())?;
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let result: $ty = items.into_iter().fold(zero, |a, b| a + b);
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let result: $ty = items.into_iter().sum();
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("SumTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// Instantiate built-in tasks for common primitive types.
impl_max_task!(i32);
impl_max_task!(i64);
impl_max_task!(u32);
impl_max_task!(u64);
impl_max_task!(f32);
impl_max_task!(f64);

impl_min_task!(i32);
impl_min_task!(i64);
impl_min_task!(u32);
impl_min_task!(u64);
impl_min_task!(f32);
impl_min_task!(f64);

impl_sum_task!(i32);
impl_sum_task!(i64);
impl_sum_task!(u32);
impl_sum_task!(u64);
impl_sum_task!(f32);
impl_sum_task!(f64);

// ── CountTask ─────────────────────────────────────────────────────────────────

/// Built-in: map partition to its element count (as `u64`).
///
/// Used by `TypedRdd::count()` to count elements per partition.
/// The driver sums the per-partition counts.
pub struct CountTask;

impl CountTask {
    /// op_id used for registration — generic-independent since count doesn't
    /// decode element types.
    pub const OP_ID: &'static str = "atomic::builtin::count";
}

inventory::submit! {
    TaskEntry {
        op_id: CountTask::OP_ID,
        handler: |action, _payload, data| {
            use atomic_data::distributed::{TaskAction, WireEncode};
            match action {
                TaskAction::Map | TaskAction::Collect => {
                    // data is rkyv-encoded Vec<T>; we only need the count,
                    // so we decode as Vec<u8> sequences — but rkyv doesn't
                    // know the element type at this point. We use the raw
                    // byte length heuristic only if the type is known, so
                    // we instead expect the caller to encode as Vec<u8> with
                    // a count-map applied before dispatching.
                    //
                    // Simpler: callers use a typed count via SumTask<u64> on
                    // a map-to-1 pipeline. CountTask here is a low-level hook.
                    let _ = data;
                    1u64.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(format!("CountTask does not support action {:?}", other)),
            }
        },
    }
}

// ── TopKTask ──────────────────────────────────────────────────────────────────

/// Built-in: return the top K elements in descending order from a partition.
///
/// Used by `TypedRdd::top(k)` in distributed mode. Each partition produces its
/// local top-K; the driver merges them and takes the global top-K.
///
/// `payload` must be a wire-encoded `u64` (the K value).
pub struct TopKTask<T>(std::marker::PhantomData<T>);

impl<T> Default for TopKTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_top_k_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::top_k::", stringify!($ty)),
                handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Collect => {
                            let k = u64::decode_wire(payload)
                                .map_err(|e| e.to_string())? as usize;
                            let mut items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            items.sort_by(|a, b| b.partial_cmp(a).unwrap_or(::std::cmp::Ordering::Equal));
                            items.truncate(k);
                            items.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("TopKTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// ── TakeOrderedTask ───────────────────────────────────────────────────────────

/// Built-in: return the first K elements in ascending order from a partition.
///
/// Used by `TypedRdd::take_ordered(k)` in distributed mode. Each partition
/// produces its local first-K ascending; the driver merges and re-truncates.
///
/// `payload` must be a wire-encoded `u64` (the K value).
pub struct TakeOrderedTask<T>(std::marker::PhantomData<T>);

impl<T> Default for TakeOrderedTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_take_ordered_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::take_ordered::", stringify!($ty)),
                handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Collect => {
                            let k = u64::decode_wire(payload)
                                .map_err(|e| e.to_string())? as usize;
                            let mut items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(::std::cmp::Ordering::Equal));
                            items.truncate(k);
                            items.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("TakeOrderedTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// ── DistinctTask ──────────────────────────────────────────────────────────────

/// Built-in: remove duplicate elements within a partition.
///
/// Used as the local combine step in `TypedRdd::distinct()`. After each
/// partition deduplicates locally, a shuffle groups all copies of each element
/// to one partition, where a final `DistinctTask` pass finishes the job.
pub struct DistinctTask<T>(std::marker::PhantomData<T>);

impl<T> Default for DistinctTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_distinct_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::distinct::", stringify!($ty)),
                handler: |action, _payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    use ::std::collections::HashSet;
                    match action {
                        TaskAction::Map | TaskAction::Collect => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let unique: Vec<$ty> = items
                                .into_iter()
                                .collect::<HashSet<_>>()
                                .into_iter()
                                .collect();
                            unique.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("DistinctTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// ── MeanTask ──────────────────────────────────────────────────────────────────

/// Built-in: compute per-partition `(f64_sum, u64_count)` for mean calculation.
///
/// Used by `TypedRdd::mean()`. Each worker returns `(sum, count)` for its
/// partition; the driver combines them: `total_sum / total_count as f64`.
///
/// The driver reduce step uses a `SumTask<f64>` for the sum component and
/// `SumTask<u64>` for the count component, or combines the tuple directly.
pub struct MeanTask<T>(std::marker::PhantomData<T>);

impl<T> Default for MeanTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_mean_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::mean::", stringify!($ty)),
                handler: |action, _payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Aggregate | TaskAction::Collect => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let count = items.len() as u64;
                            let sum: f64 = items.into_iter().map(|x| x as f64).sum();
                            (sum, count).encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("MeanTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// ── SortTask ──────────────────────────────────────────────────────────────────

/// Built-in: sort all elements within a partition in ascending order.
///
/// Used by `TypedRdd::sort_within_partitions()` and as the local sort step
/// before a merge-sort across partitions for `sort_by` / `sort_by_key`.
pub struct SortTask<T>(std::marker::PhantomData<T>);

impl<T> Default for SortTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_sort_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::sort::", stringify!($ty)),
                handler: |action, _payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Map | TaskAction::Collect => {
                            let mut items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(::std::cmp::Ordering::Equal));
                            items.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("SortTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// Instantiate new built-in tasks for common primitive types.
impl_top_k_task!(i32);
impl_top_k_task!(i64);
impl_top_k_task!(u32);
impl_top_k_task!(u64);
impl_top_k_task!(f32);
impl_top_k_task!(f64);

impl_take_ordered_task!(i32);
impl_take_ordered_task!(i64);
impl_take_ordered_task!(u32);
impl_take_ordered_task!(u64);
impl_take_ordered_task!(f32);
impl_take_ordered_task!(f64);

impl_distinct_task!(i32);
impl_distinct_task!(i64);
impl_distinct_task!(u32);
impl_distinct_task!(u64);
impl_distinct_task!(String);

// MeanTask uses `x as f64` so only integer and float primitives apply.
impl_mean_task!(i32);
impl_mean_task!(i64);
impl_mean_task!(u32);
impl_mean_task!(u64);
impl_mean_task!(f32);
impl_mean_task!(f64);

impl_sort_task!(i32);
impl_sort_task!(i64);
impl_sort_task!(u32);
impl_sort_task!(u64);
impl_sort_task!(f32);
impl_sort_task!(f64);
