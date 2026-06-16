use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{BaseError, BaseResult};

/// Reserved `reduce_id` sentinel for the consolidated DATA blob of the sort-shuffle layout.
/// A map task writing the consolidated layout stores its single data blob under
/// `(shuffle_id, map_id, SHUFFLE_DATA_KEY)`. Never used as a real reduce-partition id.
pub const SHUFFLE_DATA_KEY: usize = usize::MAX;

/// Reserved `reduce_id` sentinel for the consolidated offset INDEX (`bincode(Vec<u64>)`,
/// length `R + 1`). Partition `r`'s slice of the DATA blob is `[index[r], index[r + 1])`.
pub const SHUFFLE_INDEX_KEY: usize = usize::MAX - 1;

/// Trait for shuffle cache operations
///
/// This abstracts the storage mechanism for shuffle data,
/// allowing different implementations (in-memory, distributed, etc.)
pub trait ShuffleCache: Send + Sync + Debug {
    fn insert(&self, key: (usize, usize, usize), value: Vec<u8>);
    fn get(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>>;
    fn remove(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>>;
    fn clear(&self);

    /// Return `value[start..start + len]` for `key`, or `None` if absent / out of range.
    ///
    /// The default loads the whole value and slices it. Spillable implementations override
    /// this to do a ranged read from disk, so a reduce fetch of one partition's slice never
    /// loads the entire consolidated DATA blob (the large-shuffle win).
    fn get_slice(&self, key: &(usize, usize, usize), start: usize, len: usize) -> Option<Vec<u8>> {
        let v = self.get(key)?;
        v.get(start..start.checked_add(len)?).map(<[u8]>::to_vec)
    }

    /// Return the total byte count stored for a specific reduce partition across
    /// all map tasks.  Used by the adaptive coalescing logic.
    ///
    /// Handles both layouts per map task: the consolidated (sort-shuffle) layout reads the
    /// partition's size from the offset INDEX; the legacy per-bucket layout sums the size of
    /// `(shuffle_id, map_id, reduce_id)`.
    fn bytes_for_reduce_partition(
        &self,
        shuffle_id: usize,
        num_map_partitions: usize,
        reduce_id: usize,
    ) -> u64 {
        (0..num_map_partitions)
            .map(|map_id| {
                if let Some(idx_bytes) = self.get(&(shuffle_id, map_id, SHUFFLE_INDEX_KEY)) {
                    // Consolidated layout: size is the span between adjacent offsets.
                    match bincode::decode_from_slice::<Vec<u64>, _>(
                        &idx_bytes,
                        bincode::config::standard(),
                    ) {
                        Ok((index, _)) if reduce_id + 1 < index.len() => {
                            index[reduce_id + 1] - index[reduce_id]
                        }
                        _ => 0,
                    }
                } else {
                    // Legacy per-bucket layout.
                    self.get(&(shuffle_id, map_id, reduce_id))
                        .map(|v| v.len() as u64)
                        .unwrap_or(0)
                }
            })
            .sum()
    }
}

/// Write `R` per-partition encoded buckets as the consolidated sort-shuffle layout:
/// one DATA blob (`buckets` concatenated) under `SHUFFLE_DATA_KEY` plus one offset INDEX
/// (`Vec<u64>` of length `R + 1`) under `SHUFFLE_INDEX_KEY` — two entries per map task
/// instead of `R`.
///
/// `encoded_buckets[r]` must be the bincode-encoded `Vec<(K, V)>` (or `Vec<(K, C)>`) for
/// reduce partition `r`. Because the per-partition framing is preserved inside the blob,
/// the slice `[index[r], index[r + 1])` is byte-identical to the legacy per-bucket entry,
/// so the reduce-side fetch/decode path is unchanged.
pub fn write_consolidated(
    cache: &dyn ShuffleCache,
    shuffle_id: usize,
    map_id: usize,
    encoded_buckets: &[Vec<u8>],
) -> BaseResult<()> {
    let mut data = Vec::new();
    let mut index: Vec<u64> = Vec::with_capacity(encoded_buckets.len() + 1);
    index.push(0);
    for bucket in encoded_buckets {
        data.extend_from_slice(bucket);
        index.push(data.len() as u64);
    }
    let index_bytes = bincode::encode_to_vec(&index, bincode::config::standard())
        .map_err(|e| BaseError::Other(format!("write_consolidated: encode index: {e}")))?;
    cache.insert((shuffle_id, map_id, SHUFFLE_DATA_KEY), data);
    cache.insert((shuffle_id, map_id, SHUFFLE_INDEX_KEY), index_bytes);
    Ok(())
}

/// Read one reduce partition's bytes for `(shuffle_id, map_id)`: if a consolidated INDEX
/// exists, return the partition's slice from the DATA blob; otherwise fall back to the
/// legacy per-bucket entry. Used by the shuffle HTTP server so the reduce client is unaware
/// of the storage layout.
pub fn get_consolidated_or_bucket(
    cache: &dyn ShuffleCache,
    shuffle_id: usize,
    map_id: usize,
    reduce_id: usize,
) -> Option<Vec<u8>> {
    if let Some(idx_bytes) = cache.get(&(shuffle_id, map_id, SHUFFLE_INDEX_KEY)) {
        let (index, _): (Vec<u64>, _) =
            bincode::decode_from_slice(&idx_bytes, bincode::config::standard()).ok()?;
        if reduce_id + 1 >= index.len() {
            return None;
        }
        let start = index[reduce_id] as usize;
        let end = index[reduce_id + 1] as usize;
        return cache.get_slice(&(shuffle_id, map_id, SHUFFLE_DATA_KEY), start, end - start);
    }
    cache.get(&(shuffle_id, map_id, reduce_id))
}

/// In-memory shuffle cache backed by a DashMap.
///
/// Key is `(shuffle_id, map_id, reduce_id)` matching the shuffle wire protocol.
#[derive(Debug, Default)]
pub struct DashMapShuffleCache {
    inner: dashmap::DashMap<(usize, usize, usize), Vec<u8>>,
}

impl ShuffleCache for DashMapShuffleCache {
    fn insert(&self, key: (usize, usize, usize), value: Vec<u8>) {
        self.inner.insert(key, value);
    }

    fn get(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>> {
        self.inner.get(key).map(|r| r.value().clone())
    }

    fn remove(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>> {
        self.inner.remove(key).map(|(_, v)| v)
    }

    fn clear(&self) {
        self.inner.clear();
    }
}

/// Shuffle cache that spills buckets to disk when the in-memory byte total would exceed
/// `threshold_bytes`.  Buckets that fit within the threshold stay in memory; those that
/// would push the total over the limit are written atomically to `spill_dir` instead.
///
/// On `get()`, memory is checked first; if not found the corresponding spill file is read.
/// This is transparent to callers — they never need to know whether a bucket is in memory
/// or on disk.
///
/// # Atomic writes
/// Each spill file is first written to `<key>.bin.tmp` and then renamed to `<key>.bin`,
/// which is atomic on POSIX systems and avoids partial reads.
#[derive(Debug)]
pub struct SpillableShuffleCache {
    memory: dashmap::DashMap<(usize, usize, usize), Vec<u8>>,
    spill_dir: PathBuf,
    threshold_bytes: usize,
    current_bytes: std::sync::Arc<AtomicUsize>,
}

impl SpillableShuffleCache {
    pub fn new(spill_dir: PathBuf, threshold_bytes: usize) -> Self {
        std::fs::create_dir_all(&spill_dir).ok();
        SpillableShuffleCache {
            memory: dashmap::DashMap::new(),
            spill_dir,
            threshold_bytes,
            current_bytes: std::sync::Arc::new(AtomicUsize::new(0)),
        }
    }

    fn spill_path(&self, key: (usize, usize, usize)) -> PathBuf {
        self.spill_dir
            .join(format!("spill-{}-{}-{}.bin", key.0, key.1, key.2))
    }

    fn write_spill(&self, key: (usize, usize, usize), value: &[u8]) {
        let final_path = self.spill_path(key);
        let tmp_path = final_path.with_extension("bin.tmp");
        if std::fs::write(&tmp_path, value).is_ok() {
            std::fs::rename(&tmp_path, &final_path).ok();
        }
    }
}

impl ShuffleCache for SpillableShuffleCache {
    fn insert(&self, key: (usize, usize, usize), value: Vec<u8>) {
        let new_size = value.len();
        let current = self.current_bytes.load(Ordering::Relaxed);

        if current + new_size > self.threshold_bytes {
            // Bucket would push in-memory usage over threshold — spill to disk.
            log::debug!(
                "SpillableShuffleCache: spilling bucket {:?} ({} bytes) to disk (memory={}/{} bytes)",
                key,
                new_size,
                current,
                self.threshold_bytes
            );
            self.write_spill(key, &value);
        } else {
            self.current_bytes.fetch_add(new_size, Ordering::Relaxed);
            self.memory.insert(key, value);
        }
    }

    fn get(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>> {
        // Memory-first: avoids disk I/O when possible.
        if let Some(v) = self.memory.get(key) {
            return Some(v.value().clone());
        }
        // Fall through to disk.
        std::fs::read(self.spill_path(*key)).ok()
    }

    fn get_slice(&self, key: &(usize, usize, usize), start: usize, len: usize) -> Option<Vec<u8>> {
        // Memory: slice directly.
        if let Some(v) = self.memory.get(key) {
            return v
                .value()
                .get(start..start.checked_add(len)?)
                .map(<[u8]>::to_vec);
        }
        // Disk: ranged read — seek to `start` and read exactly `len` bytes so a reduce fetch of
        // one partition's slice never loads the whole spilled consolidated blob.
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(self.spill_path(*key)).ok()?;
        f.seek(SeekFrom::Start(start as u64)).ok()?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf).ok()?;
        Some(buf)
    }

    fn remove(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>> {
        if let Some((_, v)) = self.memory.remove(key) {
            self.current_bytes.fetch_sub(v.len(), Ordering::Relaxed);
            return Some(v);
        }
        let path = self.spill_path(*key);
        let data = std::fs::read(&path).ok();
        if data.is_some() {
            std::fs::remove_file(&path).ok();
        }
        data
    }

    fn clear(&self) {
        self.memory.clear();
        self.current_bytes.store(0, Ordering::Relaxed);
        // Remove all spill files in the spill directory.
        if let Ok(entries) = std::fs::read_dir(&self.spill_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.extension().is_some_and(|e| e == "bin") {
                    std::fs::remove_file(p).ok();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_cache_memory_path() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SpillableShuffleCache::new(dir.path().to_path_buf(), 1024);
        let key = (0usize, 0usize, 0usize);
        cache.insert(key, vec![1u8; 100]);
        assert_eq!(cache.get(&key).unwrap(), vec![1u8; 100]);
        assert!(
            !dir.path().join("spill-0-0-0.bin").exists(),
            "small item should stay in memory"
        );
    }

    #[test]
    fn spill_cache_disk_path() {
        let dir = tempfile::tempdir().unwrap();
        // threshold = 50 bytes, item is 100 bytes — goes to disk immediately
        let cache = SpillableShuffleCache::new(dir.path().to_path_buf(), 50);
        let key = (1usize, 2usize, 3usize);
        cache.insert(key, vec![9u8; 100]);
        assert!(
            dir.path().join("spill-1-2-3.bin").exists(),
            "large item should spill to disk"
        );
        assert_eq!(cache.get(&key).unwrap(), vec![9u8; 100]);
    }

    #[test]
    fn spill_cache_mixed_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SpillableShuffleCache::new(dir.path().to_path_buf(), 200);
        // First item: 150 bytes — stays in memory (150 < 200)
        cache.insert((0, 0, 0), vec![1u8; 150]);
        // Second item: 100 bytes — 150+100=250 > 200 → spills to disk
        cache.insert((0, 0, 1), vec![2u8; 100]);
        assert_eq!(cache.get(&(0, 0, 0)).unwrap(), vec![1u8; 150]);
        assert_eq!(cache.get(&(0, 0, 1)).unwrap(), vec![2u8; 100]);
        // Remove disk-backed item
        let removed = cache.remove(&(0, 0, 1)).unwrap();
        assert_eq!(removed, vec![2u8; 100]);
        assert!(!dir.path().join("spill-0-0-1.bin").exists());
        // clear removes everything
        cache.clear();
        assert!(cache.get(&(0, 0, 0)).is_none());
    }

    fn enc(pairs: &[(i32, i32)]) -> Vec<u8> {
        bincode::encode_to_vec(pairs.to_vec(), bincode::config::standard()).unwrap()
    }

    #[test]
    fn consolidated_roundtrip_and_slice() {
        let cache = DashMapShuffleCache::default();
        let b0 = enc(&[(1, 10)]);
        let b1 = enc(&[]); // empty reduce partition still framed
        let b2 = enc(&[(2, 20), (3, 30)]);
        write_consolidated(&cache, 0, 0, &[b0.clone(), b1.clone(), b2.clone()]).unwrap();

        // Two entries per map task, not R; no per-bucket entries.
        assert!(cache.get(&(0, 0, 0)).is_none());
        assert!(cache.get(&(0, 0, SHUFFLE_DATA_KEY)).is_some());
        assert!(cache.get(&(0, 0, SHUFFLE_INDEX_KEY)).is_some());

        // Each partition's slice is byte-identical to its original encoded bucket,
        // and decodes back to the original pairs.
        for (r, (bytes, expect)) in [
            (b0, vec![(1, 10)]),
            (b1, vec![]),
            (b2, vec![(2, 20), (3, 30)]),
        ]
        .into_iter()
        .enumerate()
        {
            let slice = get_consolidated_or_bucket(&cache, 0, 0, r).unwrap();
            assert_eq!(slice, bytes, "slice bytes match original bucket for r={r}");
            let (decoded, _): (Vec<(i32, i32)>, _) =
                bincode::decode_from_slice(&slice, bincode::config::standard()).unwrap();
            assert_eq!(decoded, expect, "decoded pairs match for r={r}");
        }

        // Coalescing sizing reads the index span, matching each bucket's encoded length.
        assert_eq!(
            cache.bytes_for_reduce_partition(0, 1, 0),
            enc(&[(1, 10)]).len() as u64
        );
        assert_eq!(
            cache.bytes_for_reduce_partition(0, 1, 2),
            enc(&[(2, 20), (3, 30)]).len() as u64
        );
    }

    #[test]
    fn legacy_bucket_still_served() {
        let cache = DashMapShuffleCache::default();
        cache.insert((1, 0, 0), vec![1, 2, 3]);
        // No index → falls back to the per-bucket entry.
        assert_eq!(
            get_consolidated_or_bucket(&cache, 1, 0, 0),
            Some(vec![1, 2, 3])
        );
        assert_eq!(cache.bytes_for_reduce_partition(1, 1, 0), 3);
    }

    #[test]
    fn get_slice_default_bounds() {
        let cache = DashMapShuffleCache::default();
        cache.insert((0, 0, SHUFFLE_DATA_KEY), vec![10, 11, 12, 13, 14]);
        assert_eq!(
            cache.get_slice(&(0, 0, SHUFFLE_DATA_KEY), 1, 3),
            Some(vec![11, 12, 13])
        );
        assert_eq!(cache.get_slice(&(0, 0, SHUFFLE_DATA_KEY), 3, 10), None); // out of range
    }

    #[test]
    fn spill_get_slice_ranged_disk_read() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SpillableShuffleCache::new(dir.path().to_path_buf(), 0); // 0 → always spill
        cache.insert((0, 0, SHUFFLE_DATA_KEY), (0..20u8).collect());
        assert!(
            dir.path()
                .join(format!("spill-0-0-{SHUFFLE_DATA_KEY}.bin"))
                .exists()
        );
        // Ranged read straight from disk (no whole-blob load).
        assert_eq!(
            cache.get_slice(&(0, 0, SHUFFLE_DATA_KEY), 5, 4),
            Some(vec![5, 6, 7, 8])
        );
        // Range past EOF → None.
        assert_eq!(cache.get_slice(&(0, 0, SHUFFLE_DATA_KEY), 18, 5), None);
    }

    #[test]
    fn consolidated_served_from_spill() {
        let dir = tempfile::tempdir().unwrap();
        let cache = SpillableShuffleCache::new(dir.path().to_path_buf(), 0); // force DATA+INDEX to disk
        let b0 = enc(&[(1, 10)]);
        let b1 = enc(&[(2, 20), (3, 30)]);
        write_consolidated(&cache, 0, 0, &[b0.clone(), b1.clone()]).unwrap();
        // Index read from disk, DATA sliced from disk — byte-identical to the original buckets.
        assert_eq!(get_consolidated_or_bucket(&cache, 0, 0, 0), Some(b0));
        assert_eq!(get_consolidated_or_bucket(&cache, 0, 0, 1), Some(b1));
    }
}
