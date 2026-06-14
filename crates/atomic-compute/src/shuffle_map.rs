use atomic_data::distributed::WireDecode;
use atomic_data::partitioner::PartitionerSchema;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// Wire payload for a `ShuffleMap` op: the registry dispatch key (`"K::V"`) plus the serializable
/// partitioner spec, so the worker can partition shuffle output using the RDD's real partitioner
/// (e.g. range for `sort_by_key`) instead of plain hash.
#[derive(bincode::Encode, bincode::Decode)]
pub struct ShuffleMapPayload {
    pub type_id: String,
    pub partitioner_spec: PartitionerSchema,
}

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
    // Hash partitioner handler ignores the spec (always FxHash); the sorted handler consumes it.
    _spec: &PartitionerSchema,
) -> Result<(), String>
where
    K: atomic_data::data::Data + Clone + Hash + bincode::Encode + bincode::Decode<()> + WireDecode,
    V: atomic_data::data::Data + Clone + bincode::Encode + bincode::Decode<()> + WireDecode,
    Vec<(K, V)>: WireDecode,
{
    let pairs: Vec<(K, V)> = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("shuffle_map_handler: decode input: {e}"))?;

    // FxHasher is deterministic across processes (no random seed).
    let mut buckets: Vec<Vec<(K, V)>> = (0..num_reduce_partitions).map(|_| vec![]).collect();

    for (k, v) in pairs {
        let mut hasher = FxHasher::default();
        k.hash(&mut hasher);
        let bucket = (hasher.finish() as usize) % num_reduce_partitions;
        buckets[bucket].push((k, v));
    }

    let cache = atomic_data::env::get_shuffle_cache()
        .ok_or_else(|| "shuffle_map_handler: SHUFFLE_CACHE not initialized".to_string())?;

    // Encode each reduce-partition bucket once (per-partition framing is preserved in both layouts).
    let encoded: Vec<Vec<u8>> = buckets
        .into_iter()
        .map(|bucket| {
            bincode::encode_to_vec(&bucket, bincode::config::standard())
                .map_err(|e| format!("shuffle_map_handler: encode bucket: {e}"))
        })
        .collect::<Result<_, _>>()?;

    if num_reduce_partitions >= atomic_data::env::sort_shuffle_threshold() {
        // Consolidated (sort-shuffle) layout: one DATA blob + one INDEX for this map task.
        atomic_data::shuffle::cache::write_consolidated(
            cache.as_ref(),
            shuffle_id,
            map_partition_id,
            &encoded,
        )?;
    } else {
        // Legacy per-bucket layout: one entry per reduce partition.
        for (reduce_id, bytes) in encoded.into_iter().enumerate() {
            cache.insert((shuffle_id, map_partition_id, reduce_id), bytes);
        }
    }

    log::debug!(
        "shuffle_map_handler: wrote {} buckets for shuffle_id={} partition={}",
        num_reduce_partitions,
        shuffle_id,
        map_partition_id
    );
    Ok(())
}

/// Sorted shuffle-write handler for `K: Ord`. Unlike [`shuffle_map_handler`], it partitions using
/// the **real** partitioner reconstructed from the shipped `spec` (range bounds for `sort_by_key`),
/// then sorts each reduce-partition bucket by key — so the driver-side reduce can k-way merge
/// globally-ordered sorted runs. Registered via [`register_sort_shuffle_map!`].
pub fn sort_shuffle_map_handler<K, V>(
    data: &[u8],
    shuffle_id: usize,
    map_partition_id: usize,
    num_reduce_partitions: usize,
    spec: &PartitionerSchema,
) -> Result<(), String>
where
    K: atomic_data::data::Data
        + Clone
        + Hash
        + Ord
        + Eq
        + bincode::Encode
        + bincode::Decode<()>
        + WireDecode,
    V: atomic_data::data::Data + Clone + bincode::Encode + bincode::Decode<()> + WireDecode,
    Vec<(K, V)>: WireDecode,
{
    let pairs: Vec<(K, V)> = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("sort_shuffle_map_handler: decode input: {e}"))?;

    // Reconstruct the RDD's real partitioner from the shipped spec (range bounds → correct global
    // ranges). Hash/Custom specs degrade to hash partitioning.
    let partitioner = spec.into_partitioner::<K>();
    let descending = matches!(
        spec,
        PartitionerSchema::Range {
            ascending: false,
            ..
        }
    );

    let mut buckets: Vec<Vec<(K, V)>> = (0..num_reduce_partitions).map(|_| vec![]).collect();
    for (k, v) in pairs {
        let bucket = partitioner
            .get_partition(&k as &dyn std::any::Any)
            .min(num_reduce_partitions.saturating_sub(1));
        buckets[bucket].push((k, v));
    }

    let cache = atomic_data::env::get_shuffle_cache()
        .ok_or_else(|| "sort_shuffle_map_handler: SHUFFLE_CACHE not initialized".to_string())?;

    // Sort each bucket by key (matching the range direction) → sorted runs for the reduce merge.
    let encoded: Vec<Vec<u8>> = buckets
        .into_iter()
        .map(|mut bucket| {
            if descending {
                bucket.sort_by(|a, b| b.0.cmp(&a.0));
            } else {
                bucket.sort_by(|a, b| a.0.cmp(&b.0));
            }
            bincode::encode_to_vec(&bucket, bincode::config::standard())
                .map_err(|e| format!("sort_shuffle_map_handler: encode bucket: {e}"))
        })
        .collect::<Result<_, _>>()?;

    if num_reduce_partitions >= atomic_data::env::sort_shuffle_threshold() {
        atomic_data::shuffle::cache::write_consolidated(
            cache.as_ref(),
            shuffle_id,
            map_partition_id,
            &encoded,
        )?;
    } else {
        for (reduce_id, bytes) in encoded.into_iter().enumerate() {
            cache.insert((shuffle_id, map_partition_id, reduce_id), bytes);
        }
    }
    Ok(())
}
