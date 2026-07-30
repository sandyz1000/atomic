//! Shuffle-map dispatch: registration plus the handler logic it points at, owned
//! together in one file (the handler logic used to live in a separate
//! `shuffle_map.rs`, three files away from the registration that references it —
//! see the `registry` module doc for why they were merged).
//!
//! Shuffle-map handlers are `TaskEntry`s in the same [`crate::registry::TASK_REGISTRY`]
//! every `#[task]`/`task_fn!`/`register_*_task!` handler dispatches through — one registry,
//! one entry shape, one ABI (`TaskHandlerFn`) for every unit of distributed work dispatched
//! by name. There is no shuffle-specific registry or ABI to keep in sync with it.
//!
//! # Registering a new `(K, V)` pair
//!
//! ```rust,ignore
//! // In main.rs, before any shuffle operations (`reduce_by_key`/`group_by_key`):
//! atomic_compute::register_shuffle_map!(String, u32);
//!
//! // For Ord keys that may be range-partitioned (e.g. via `sort_by_key`), register
//! // the sorted variant instead — it also registers the base hash handler:
//! atomic_compute::register_sort_shuffle_map!(i64, f64);
//! ```
//!
//! `NativeDispatcher` (`runtimes/native.rs`) looks up the shuffle's `(K, V)` dispatch key
//! (the `"K::V"` string) in `TASK_REGISTRY` — trying the `"K::V::sorted"` key first when the
//! shuffle was marked sorted, via [`resolve_shuffle_handler`] — when it sees
//! `StepKind::Engine(EngineAction::ShuffleMap)`. The driver looks up [`SHUFFLE_KEY_REGISTRY`]
//! by `TypeId` to embed the right dispatch key in the op; that lookup is unrelated to the
//! handler-dispatch unification above (it's a driver-side `TypeId`→string reflection table,
//! not a handler ABI).
//!
//! # Partitioning strategy vs. wire ABI
//!
//! [`HashShuffleWriter`] and [`SortShuffleWriter`] implement [`ShuffleWriter`]
//! (`task_traits`) — the same "trait + one method" shape as `BinaryTask`/`PartitionTask` —
//! and own *only* the bucketing decision. [`shuffle_map_handler`]/[`sort_shuffle_map_handler`]
//! are the `TaskHandlerFn`s wrapping them, registered under the `"K::V"`/`"K::V::sorted"` keys.
//! [`write_buckets`] is the shared, non-generic encode-and-cache tail both wrappers call into.

use std::any::TypeId;
use std::hash::Hasher;

use atomic_data::distributed::{TaskAction, WireDecode};
use atomic_data::partitioner::PartitionerSchema;
use once_cell::sync::Lazy;
use rustc_hash::FxHasher;
use std::collections::HashMap;

use crate::error::{ComputeError, ComputeResult};
use crate::registry::{TASK_REGISTRY, TaskEntry};
use crate::task_traits::{OrdShuffleKey, ShuffleKey, ShuffleValue, ShuffleWriter};

/// Per-invocation shuffle-write coordinates, decoded from the `payload` a registered
/// shuffle-map `TaskEntry` receives. `map_partition_id` travels here rather than as a
/// separate typed argument because `TaskHandlerFn`'s signature (shared by every registered
/// handler) is `fn(&TaskAction, payload, data)` — no dedicated slot for it — so
/// `NativeDispatcher` packs it in fresh at each call, from the `partition_id` it already has
/// in scope from its own `dispatch(&self, op, partition_id, data)` parameter.
#[derive(bincode::Encode, bincode::Decode)]
pub struct ShuffleWriteCtx {
    pub shuffle_id: usize,
    pub map_partition_id: usize,
    pub num_reduce_partitions: usize,
    pub partitioner_spec: PartitionerSchema,
}

/// Resolve the shuffle-write `TaskEntry` for `type_id` from `TASK_REGISTRY`, applying the
/// sorted-vs-hash fallback policy in the one place that owns it. If `is_range` (the shuffle
/// was marked as a range/sort shuffle), try the `"K::V::sorted"` key first; otherwise, or if
/// no sorted handler was registered for this type, fall back to the plain `"K::V"` key.
pub fn resolve_shuffle_handler(type_id: &str, is_range: bool) -> Option<&'static TaskEntry> {
    if is_range && let Some(&entry) = TASK_REGISTRY.get(sorted_key(type_id).as_str()) {
        return Some(entry);
    }
    TASK_REGISTRY.get(type_id).copied()
}

fn sorted_key(type_id: &str) -> String {
    format!("{type_id}::sorted")
}

/// Maps a concrete `(K, V)` `TypeId` to its stable shuffle dispatch key.
///
/// Used by the driver (in `rdd/typed.rs`) to look up the key when building a
/// `ShuffleMap` pipeline op. The key is the same `stringify!`-based string used as the
/// `TASK_REGISTRY` key for that pair's shuffle-map `TaskEntry`, ensuring driver and worker
/// use an identical lookup.
///
/// Submitted by `register_shuffle_map!(K, V)` alongside the shuffle-map `TaskEntry`. Pure
/// data (no behavior to wrap in a method), so unlike the handler entry, the registry stores
/// the plain key string directly rather than the whole entry.
pub struct ShuffleKeyEntry {
    /// Returns `TypeId::of::<(K, V)>()` — used as the lookup key by the driver.
    pub type_id: fn() -> TypeId,
    /// The stable dispatch key (e.g. `"String::u32"`).
    pub key: &'static str,
}

inventory::collect!(ShuffleKeyEntry);

/// Global map from `TypeId::of::<(K, V)>()` to the shuffle dispatch key string.
///
/// The driver calls `SHUFFLE_KEY_REGISTRY.get(&TypeId::of::<(K, V)>())` to obtain
/// the key to embed in a `ShuffleMap` pipeline op payload.
pub static SHUFFLE_KEY_REGISTRY: Lazy<HashMap<TypeId, &'static str>> = Lazy::new(|| {
    inventory::iter::<ShuffleKeyEntry>
        .into_iter()
        .map(|entry| ((entry.type_id)(), entry.key))
        .collect()
});

/// Allocate `num_reduce_partitions` empty buckets and distribute `pairs` into them by
/// `index_of(&k)`. Shared by both writer strategies below — they differ only in how the
/// bucket index is computed, and, for [`SortShuffleWriter`], an extra post-bucketing sort
/// pass that stays in its own `partition` body since [`HashShuffleWriter`] has no equivalent.
fn bucket_pairs<K, V>(
    pairs: Vec<(K, V)>,
    num_reduce_partitions: usize,
    mut index_of: impl FnMut(&K) -> usize,
) -> Vec<Vec<(K, V)>> {
    let mut buckets: Vec<Vec<(K, V)>> = (0..num_reduce_partitions).map(|_| vec![]).collect();
    for (k, v) in pairs {
        let bucket = index_of(&k);
        buckets[bucket].push((k, v));
    }
    buckets
}

/// Hash-partitioned shuffle-write strategy — the default for any `(K, V)` pair.
///
/// Uses a registered named partitioner rebuilt from `spec` if the shuffle carried one (from
/// `partition_by_named`); otherwise partitions by `FxHash(K)` (deterministic across
/// processes, no seed).
#[derive(Default)]
pub struct HashShuffleWriter;

impl<K, V> ShuffleWriter<K, V> for HashShuffleWriter
where
    K: ShuffleKey,
{
    fn partition(
        &self,
        pairs: Vec<(K, V)>,
        num_reduce_partitions: usize,
        spec: &PartitionerSchema,
    ) -> Vec<Vec<(K, V)>> {
        let custom = spec
            .custom_name()
            .and_then(|name| super::partitioner::lookup_partitioner(name, num_reduce_partitions));

        bucket_pairs(pairs, num_reduce_partitions, |k| match &custom {
            Some(p) => p
                .get_partition(k as &dyn std::any::Any)
                .min(num_reduce_partitions.saturating_sub(1)),
            None => {
                let mut hasher = FxHasher::default();
                k.hash(&mut hasher);
                (hasher.finish() as usize) % num_reduce_partitions
            }
        })
    }
}

/// Sorted shuffle-write strategy for `K: Ord`.
///
/// Partitions with the RDD's real partitioner (range bounds reconstructed from `spec` for
/// `sort_by_key`; Hash/Custom specs degrade to hash partitioning) and sorts each bucket, so
/// the driver-side reduce can k-way merge globally-ordered runs instead of full-sorting.
#[derive(Default)]
pub struct SortShuffleWriter;

impl<K, V> ShuffleWriter<K, V> for SortShuffleWriter
where
    K: OrdShuffleKey,
{
    fn partition(
        &self,
        pairs: Vec<(K, V)>,
        num_reduce_partitions: usize,
        spec: &PartitionerSchema,
    ) -> Vec<Vec<(K, V)>> {
        let partitioner = spec.into_partitioner::<K>();
        let descending = matches!(
            spec,
            PartitionerSchema::Range {
                ascending: false,
                ..
            }
        );

        let mut buckets = bucket_pairs(pairs, num_reduce_partitions, |k| {
            partitioner
                .get_partition(k as &dyn std::any::Any)
                .min(num_reduce_partitions.saturating_sub(1))
        });

        for bucket in &mut buckets {
            if descending {
                bucket.sort_by(|a, b| b.0.cmp(&a.0));
            } else {
                bucket.sort_by(|a, b| a.0.cmp(&b.0));
            }
        }
        buckets
    }
}

/// Encode each bucket and write it to `SHUFFLE_CACHE` — the shared tail both writer
/// strategies funnel through, so the consolidated-vs-per-bucket layout decision (and the
/// completion log line) exists in exactly one place instead of being duplicated per strategy.
fn write_buckets<K, V>(ctx: &ShuffleWriteCtx, buckets: Vec<Vec<(K, V)>>) -> ComputeResult<()>
where
    K: bincode::Encode,
    V: bincode::Encode,
{
    let cache = atomic_data::env::get_shuffle_cache()
        .ok_or_else(|| ComputeError::Other("shuffle cache not initialized".to_string()))?;

    // Encode each reduce-partition bucket once (per-partition framing is preserved in both layouts).
    let encoded: Vec<Vec<u8>> = buckets
        .into_iter()
        .map(|bucket| {
            bincode::encode_to_vec(&bucket, bincode::config::standard())
                .map_err(|e| ComputeError::InvalidPayload(format!("shuffle bucket encode: {e}")))
        })
        .collect::<ComputeResult<_>>()?;

    if ctx.num_reduce_partitions >= atomic_data::env::sort_shuffle_threshold() {
        // Consolidated (sort-shuffle) layout: one DATA blob + one INDEX for this map task.
        atomic_data::shuffle::cache::write_consolidated(
            cache.as_ref(),
            ctx.shuffle_id,
            ctx.map_partition_id,
            &encoded,
        )?;
    } else {
        // Legacy per-bucket layout: one entry per reduce partition.
        for (reduce_id, bytes) in encoded.into_iter().enumerate() {
            cache.insert((ctx.shuffle_id, ctx.map_partition_id, reduce_id), bytes);
        }
    }

    log::debug!(
        "write_buckets: wrote {} buckets for shuffle_id={} partition={}",
        ctx.num_reduce_partitions,
        ctx.shuffle_id,
        ctx.map_partition_id
    );
    Ok(())
}

/// `TaskHandlerFn` wrapper for `(K, V)`: decode `payload` into a [`ShuffleWriteCtx`] and
/// `data` into the wire pairs, delegate bucketing to [`HashShuffleWriter`], then write via
/// [`write_buckets`]. Registered by `register_shuffle_map!(K, V)` under the `"K::V"` key.
pub fn shuffle_map_handler<K, V>(
    _action: &TaskAction,
    payload: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, String>
where
    K: ShuffleKey,
    V: ShuffleValue,
    Vec<(K, V)>: WireDecode,
{
    let ctx: ShuffleWriteCtx = decode_ctx(payload)?;
    let pairs: Vec<(K, V)> = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("shuffle_map_handler: decode input: {e}"))?;
    let buckets =
        HashShuffleWriter.partition(pairs, ctx.num_reduce_partitions, &ctx.partitioner_spec);
    write_buckets(&ctx, buckets).map_err(|e| e.to_string())?;
    Ok(data.to_vec())
}

/// `TaskHandlerFn` wrapper for `(K, V)`, `K: Ord`: same as [`shuffle_map_handler`] but
/// delegates bucketing+sorting to [`SortShuffleWriter`]. Registered by
/// `register_sort_shuffle_map!(K, V)` under the `"K::V::sorted"` key.
pub fn sort_shuffle_map_handler<K, V>(
    _action: &TaskAction,
    payload: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, String>
where
    K: OrdShuffleKey,
    V: ShuffleValue,
    Vec<(K, V)>: WireDecode,
{
    let ctx: ShuffleWriteCtx = decode_ctx(payload)?;
    let pairs: Vec<(K, V)> = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("sort_shuffle_map_handler: decode input: {e}"))?;
    let buckets =
        SortShuffleWriter.partition(pairs, ctx.num_reduce_partitions, &ctx.partitioner_spec);
    write_buckets(&ctx, buckets).map_err(|e| e.to_string())?;
    Ok(data.to_vec())
}

/// Decode the `ShuffleWriteCtx` `NativeDispatcher` packs into `payload` at each call —
/// shared by both handlers above.
fn decode_ctx(payload: &[u8]) -> Result<ShuffleWriteCtx, String> {
    bincode::decode_from_slice(payload, bincode::config::standard())
        .map(|(ctx, _)| ctx)
        .map_err(|e| format!("shuffle write ctx decode: {e}"))
}
