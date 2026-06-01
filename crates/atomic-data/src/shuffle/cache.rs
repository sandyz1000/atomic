use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Trait for shuffle cache operations
///
/// This abstracts the storage mechanism for shuffle data,
/// allowing different implementations (in-memory, distributed, etc.)
pub trait ShuffleCache: Send + Sync + Debug {
    /// Insert shuffle data for a given (shuffle_id, map_id, reduce_id) tuple
    fn insert(&self, key: (usize, usize, usize), value: Vec<u8>);

    /// Get shuffle data for a given key
    fn get(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>>;

    /// Remove shuffle data for a given key
    fn remove(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>>;

    /// Clear all shuffle data
    fn clear(&self);

    /// Return the total byte count stored for a specific reduce partition across
    /// all map tasks.  Used by the adaptive coalescing logic.
    ///
    /// Sums the size of buckets `(shuffle_id, 0..num_map_partitions, reduce_id)`.
    fn bytes_for_reduce_partition(
        &self,
        shuffle_id: usize,
        num_map_partitions: usize,
        reduce_id: usize,
    ) -> u64 {
        (0..num_map_partitions)
            .map(|map_id| {
                self.get(&(shuffle_id, map_id, reduce_id))
                    .map(|v| v.len() as u64)
                    .unwrap_or(0)
            })
            .sum()
    }
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

// ── SpillableShuffleCache ─────────────────────────────────────────────────────

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
                key, new_size, current, self.threshold_bytes
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
                if p.extension().map_or(false, |e| e == "bin") {
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
        assert!(!dir.path().join("spill-0-0-0.bin").exists(), "small item should stay in memory");
    }

    #[test]
    fn spill_cache_disk_path() {
        let dir = tempfile::tempdir().unwrap();
        // threshold = 50 bytes, item is 100 bytes — goes to disk immediately
        let cache = SpillableShuffleCache::new(dir.path().to_path_buf(), 50);
        let key = (1usize, 2usize, 3usize);
        cache.insert(key, vec![9u8; 100]);
        assert!(dir.path().join("spill-1-2-3.bin").exists(), "large item should spill to disk");
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
}
