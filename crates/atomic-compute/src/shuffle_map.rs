
use crate::task_registry::TaskEntry;
use crate::task_traits::BinaryTask;
use atomic_data::distributed::WireDecode;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

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

    for (reduce_id, bucket) in buckets.into_iter().enumerate() {
        let bytes = bincode::encode_to_vec(&bucket, bincode::config::standard())
            .map_err(|e| format!("shuffle_map_handler: encode bucket: {e}"))?;
        cache.insert((shuffle_id, map_partition_id, reduce_id), bytes);
    }

    log::debug!(
        "shuffle_map_handler: wrote {} buckets for shuffle_id={} partition={}",
        num_reduce_partitions,
        shuffle_id,
        map_partition_id
    );
    Ok(())
}



