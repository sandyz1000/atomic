pub mod error;

pub use error::{CacheError, Result};

use std::any::Any;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use dashmap::DashMap;
use lru::LruCache;

// StorageLevel

/// Hints for how an RDD's partitions should be persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageLevel {
    #[default]
    MemoryOnly,
    /// Memory-first with disk spill on LRU eviction.
    ///
    /// When the cache is full and this partition is evicted, it is written to
    /// `{work_dir}/rdd-cache/{rdd_id}/{partition}.bin`.  On the next cache miss
    /// the disk file is read back into memory.  Requires the RDD to be persisted
    /// via `TypedRdd::persist_with_disk(StorageLevel::MemoryAndDisk)`.
    MemoryAndDisk,
    /// Reserved — treated as `MemoryOnly` (serialized form, same memory path).
    MemoryOnlySer,
    /// Reserved — treated as `MemoryOnly` until disk-only path is implemented.
    DiskOnly,
}

// Disk helpers — used by PartitionStore::register_spill_path and by cached.rs

pub fn disk_write_partition<T>(path: &std::path::Path, items: &[T]) -> std::io::Result<()>
where
    T: bincode::Encode,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("bin.tmp");
    let bytes = bincode::encode_to_vec(items, bincode::config::standard())
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)
}

pub fn disk_read_partition<T>(path: &std::path::Path) -> std::io::Result<Vec<T>>
where
    T: bincode::Decode<()>,
{
    let bytes = std::fs::read(path)?;
    bincode::decode_from_slice::<Vec<T>, _>(&bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

// PartitionStore — typed in-memory cache for CachedRdd

/// Default maximum number of partitions the global `PARTITION_CACHE` will hold
/// before evicting least-recently-used entries.
pub const DEFAULT_PARTITION_CACHE_CAP: usize = 1024;

// Type-erased spill operations for MemoryAndDisk partitions.
//
// `write` is called by `put()` when an entry is LRU-evicted; it receives the
// evicted value as `&dyn Any` and serialises it to disk.
// `read` is called by `get()` on a memory miss; it deserialises the partition
// from disk and returns it as `Box<dyn Any + Send + Sync>`.

/// Serialises an LRU-evicted partition (passed as `&dyn Any`) to disk.
type SpillWriteFn = Box<dyn Fn(&dyn Any) -> std::io::Result<()> + Send + Sync>;
/// Reads a spilled partition back from disk on a memory miss.
type SpillReadFn = Box<dyn Fn() -> std::io::Result<Box<dyn Any + Send + Sync>> + Send + Sync>;

struct SpillEntry {
    write: SpillWriteFn,
    read: SpillReadFn,
}

/// A type-erased, LRU-bounded store for cached RDD partitions.
///
/// Keys are `(rdd_id, partition_index)`.  Values are `Arc<Vec<T>>` boxed as
/// `dyn Any + Send + Sync` so that any `T: 'static + Send + Sync` can be
/// stored without serialization.  When `cap` is reached the LRU entry is
/// evicted automatically.
///
/// For `MemoryAndDisk` partitions, a spill entry may be registered via
/// `register_spill_path`.  When such a partition is LRU-evicted, it is
/// written to disk; on a subsequent miss the disk copy is read back.
pub struct PartitionStore {
    map: Mutex<LruCache<(usize, usize), Box<dyn Any + Send + Sync>>>,
    // Registered per partition for MemoryAndDisk — lives until unpersist().
    spill: DashMap<(usize, usize), SpillEntry>,
}

impl PartitionStore {
    /// Create a store with the default LRU cap.
    pub fn new() -> Self {
        Self::with_cap(DEFAULT_PARTITION_CACHE_CAP)
    }

    /// Create a store with a custom LRU cap.
    pub fn with_cap(cap: usize) -> Self {
        let cap = NonZeroUsize::new(cap).unwrap_or(NonZeroUsize::new(1).unwrap());
        PartitionStore {
            map: Mutex::new(LruCache::new(cap)),
            spill: DashMap::new(),
        }
    }

    /// Register per-partition disk-spill handlers for a `MemoryAndDisk` RDD.
    ///
    /// After registration:
    /// - `put()` will write the partition to `path` when it is LRU-evicted.
    /// - `get()` will read from `path` on a memory miss (without recomputing).
    pub fn register_spill_path<T>(&self, rdd_id: usize, partition: usize, path: PathBuf)
    where
        T: bincode::Encode + bincode::Decode<()> + Any + Send + Sync + 'static,
    {
        let write_path = path.clone();
        let read_path = path;
        self.spill.insert(
            (rdd_id, partition),
            SpillEntry {
                write: Box::new(move |any_ref: &dyn Any| {
                    if let Some(arc) = any_ref.downcast_ref::<Arc<Vec<T>>>() {
                        disk_write_partition::<T>(&write_path, arc)
                    } else {
                        Ok(()) // type mismatch — skip silently
                    }
                }),
                read: Box::new(move || {
                    disk_read_partition::<T>(&read_path)
                        .map(|v| Box::new(Arc::new(v)) as Box<dyn Any + Send + Sync>)
                }),
            },
        );
    }

    /// Retrieve a cached partition, returning `None` on a miss.
    ///
    /// On a memory miss, checks whether the partition was spilled to disk (via
    /// `register_spill_path`) and restores it to memory if so.  Logs a warning
    /// when the key exists in memory but the stored type doesn't match `T`.
    pub fn get<T: Any + Send + Sync + 'static>(
        &self,
        rdd_id: usize,
        partition: usize,
    ) -> Option<Arc<Vec<T>>> {
        let key = (rdd_id, partition);

        // Phase 1: memory hit (hold mutex briefly — no disk I/O inside lock)
        {
            let mut map = self.map.lock().unwrap();
            if let Some(entry) = map.get(&key) {
                let result = entry.downcast_ref::<Arc<Vec<T>>>().cloned();
                if result.is_none() {
                    log::warn!(
                        "PartitionStore type mismatch: rdd_id={} partition={} — \
                         stored type does not match requested type, treating as cache miss",
                        rdd_id,
                        partition
                    );
                }
                return result;
            }
        }

        // Phase 2: disk check (lock NOT held during I/O)
        let spill_entry = self.spill.get(&key)?;
        let any_box = (spill_entry.read)().ok()?;
        drop(spill_entry); // release DashMap shard lock before re-acquiring map lock

        let arc: Arc<Vec<T>> = *any_box.downcast().ok()?;

        // Re-insert restored partition into memory
        let evicted = {
            let mut map = self.map.lock().unwrap();
            map.push(key, Box::new(arc.clone()))
        };
        // If the re-insert itself evicted another entry, spill it too
        if let Some((evicted_key, evicted_val)) = evicted
            && evicted_key != key
            && let Some(ev_entry) = self.spill.get(&evicted_key)
            && let Err(e) = (ev_entry.write)(evicted_val.as_ref())
        {
            log::warn!("cache spill failed for evicted {evicted_key:?}: {e}");
        }

        Some(arc)
    }

    /// Insert a computed partition, evicting the LRU entry if at capacity.
    ///
    /// If the evicted entry has a registered spill handler (i.e. it was
    /// persisted with `MemoryAndDisk`), the handler is called to write the
    /// entry to disk before it is dropped from memory.
    pub fn put<T: Any + Send + Sync + 'static>(
        &self,
        rdd_id: usize,
        partition: usize,
        data: Arc<Vec<T>>,
    ) {
        let key = (rdd_id, partition);
        let evicted = self.map.lock().unwrap().push(key, Box::new(data));
        // True LRU eviction (not a same-key replacement) with a registered spill handler.
        if let Some((evicted_key, evicted_val)) = evicted
            && evicted_key != key
            && let Some(entry) = self.spill.get(&evicted_key)
            && let Err(e) = (entry.write)(evicted_val.as_ref())
        {
            log::warn!("cache spill failed for evicted {evicted_key:?}: {e}");
        }
    }

    /// Remove all cached partitions for an RDD (unpersist).
    pub fn remove_rdd(&self, rdd_id: usize, num_partitions: usize) {
        {
            let mut map = self.map.lock().unwrap();
            for p in 0..num_partitions {
                map.pop(&(rdd_id, p));
            }
        }
        for p in 0..num_partitions {
            self.spill.remove(&(rdd_id, p));
        }
    }

    /// Returns `true` if the given `(rdd_id, partition)` pair is currently in the cache.
    pub fn contains(&self, rdd_id: usize, partition: usize) -> bool {
        self.map.lock().unwrap().contains(&(rdd_id, partition))
    }

    /// Current number of cached partitions.
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Returns `true` if no partitions are currently cached in memory.
    pub fn is_empty(&self) -> bool {
        self.map.lock().unwrap().is_empty()
    }
}

impl Default for PartitionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Global in-memory partition cache initialised by `init_partition_cache()`.
pub static PARTITION_CACHE: OnceLock<PartitionStore> = OnceLock::new();

/// Must be called once during `Context` startup (before any RDD is cached).
pub fn init_partition_cache() {
    PARTITION_CACHE.get_or_init(PartitionStore::new);
}

/// LRU-bounded **byte** cache of cached RDD partitions held on a worker.
///
/// Unlike [`PartitionStore`] (which stores typed `Arc<Vec<T>>` for the local-mode
/// cache), workers operate on rkyv-encoded bytes and don't know `T`, so distributed
/// caching stores the raw partition bytes keyed by `(rdd_id, partition)`. Written by
/// a `TaskAction::Cache` op and read back when a later task's `cache_source` matches.
pub struct WorkerPartitionCache {
    inner: Mutex<LruCache<(usize, usize), Vec<u8>>>,
}

impl WorkerPartitionCache {
    pub fn new() -> Self {
        Self::with_cap(DEFAULT_PARTITION_CACHE_CAP)
    }

    pub fn with_cap(cap: usize) -> Self {
        let cap = NonZeroUsize::new(cap).unwrap_or(NonZeroUsize::MIN);
        WorkerPartitionCache {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Store a partition's bytes, evicting the LRU entry if at capacity.
    pub fn put(&self, rdd_id: usize, partition: usize, bytes: Vec<u8>) {
        self.inner.lock().unwrap().put((rdd_id, partition), bytes);
    }

    /// Fetch a partition's bytes (promotes it in the LRU), or `None` on a miss.
    pub fn get(&self, rdd_id: usize, partition: usize) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .get(&(rdd_id, partition))
            .cloned()
    }

    pub fn contains(&self, rdd_id: usize, partition: usize) -> bool {
        self.inner.lock().unwrap().contains(&(rdd_id, partition))
    }

    /// Drop every cached partition for an RDD (used by `unpersist`).
    pub fn remove_rdd(&self, rdd_id: usize) {
        let mut guard = self.inner.lock().unwrap();
        let keys: Vec<(usize, usize)> = guard
            .iter()
            .map(|(k, _)| *k)
            .filter(|(r, _)| *r == rdd_id)
            .collect();
        for k in keys {
            guard.pop(&k);
        }
    }

    /// Number of cached partitions (for tests/metrics).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for WorkerPartitionCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Global worker-side byte cache for distributed RDD caching.
pub static WORKER_PARTITION_CACHE: OnceLock<WorkerPartitionCache> = OnceLock::new();

/// Accessor that lazily initialises the worker byte cache on first use.
pub fn worker_partition_cache() -> &'static WorkerPartitionCache {
    WORKER_PARTITION_CACHE.get_or_init(WorkerPartitionCache::new)
}

#[cfg(test)]
mod worker_cache_tests {
    use super::WorkerPartitionCache;

    #[test]
    fn put_get_contains() {
        let cache = WorkerPartitionCache::new();
        assert!(!cache.contains(1, 0));
        cache.put(1, 0, vec![10, 20]);
        cache.put(1, 1, vec![30]);
        assert!(cache.contains(1, 0));
        assert_eq!(cache.get(1, 0), Some(vec![10, 20]));
        assert_eq!(cache.get(1, 1), Some(vec![30]));
        assert_eq!(cache.get(1, 2), None);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn remove_rdd_drops_all() {
        let cache = WorkerPartitionCache::new();
        cache.put(1, 0, vec![1]);
        cache.put(1, 1, vec![2]);
        cache.put(2, 0, vec![3]);
        cache.remove_rdd(1);
        assert!(!cache.contains(1, 0));
        assert!(!cache.contains(1, 1));
        assert!(cache.contains(2, 0)); // other RDDs untouched
    }

    #[test]
    fn evicts_at_capacity() {
        let cache = WorkerPartitionCache::with_cap(2);
        cache.put(1, 0, vec![1]);
        cache.put(1, 1, vec![2]);
        cache.put(1, 2, vec![3]); // evicts LRU (1,0)
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(1, 0));
        assert!(cache.contains(1, 2));
    }
}
