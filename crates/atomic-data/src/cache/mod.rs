pub mod error;
pub mod tracker;

pub use error::{CacheError, Result};

use std::any::Any;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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
struct SpillEntry {
    write: Box<dyn Fn(&dyn Any) -> std::io::Result<()> + Send + Sync>,
    read: Box<dyn Fn() -> std::io::Result<Box<dyn Any + Send + Sync>> + Send + Sync>,
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
    pub fn register_spill_path<T>(
        &self,
        rdd_id: usize,
        partition: usize,
        path: PathBuf,
    ) where
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
        if let Some((evicted_key, evicted_val)) = evicted {
            if evicted_key != key {
                if let Some(ev_entry) = self.spill.get(&evicted_key) {
                    let _ = (ev_entry.write)(evicted_val.as_ref());
                }
            }
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
        if let Some((evicted_key, evicted_val)) = evicted {
            if evicted_key != key {
                // True LRU eviction (not a same-key replacement)
                if let Some(entry) = self.spill.get(&evicted_key) {
                    let _ = (entry.write)(evicted_val.as_ref());
                }
            }
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
}

/// Global in-memory partition cache initialised by `init_partition_cache()`.
pub static PARTITION_CACHE: OnceLock<PartitionStore> = OnceLock::new();

/// Must be called once during `Context` startup (before any RDD is cached).
pub fn init_partition_cache() {
    PARTITION_CACHE.get_or_init(PartitionStore::new);
}

#[derive(Debug)]
pub enum CachePutResponse {
    CachePutSuccess(usize),
    CachePutFailure,
}

type CacheMap = Arc<DashMap<((usize, usize), usize), (Vec<u8>, usize)>>;

// Despite the name, it is currently unbounded cache. Once done with LRU iterator, have to make this bounded.
// Since we are storing everything as serialized objects, size estimation is as simple as getting the length of byte vector
#[derive(Debug, Clone)]
pub struct BoundedMemoryCache {
    max_mbytes: usize,
    next_key_space_id: Arc<AtomicUsize>,
    current_bytes: usize,
    map: CacheMap,
}

// TODO: remove all hardcoded values
impl BoundedMemoryCache {
    pub fn new() -> Self {
        BoundedMemoryCache {
            max_mbytes: 2000, // in MB
            next_key_space_id: Arc::new(AtomicUsize::new(0)),
            current_bytes: 0,
            map: Arc::new(DashMap::new()),
        }
    }

    fn new_key_space_id(&self) -> usize {
        self.next_key_space_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_key_space(&self) -> KeySpace {
        KeySpace::new(self, self.new_key_space_id())
    }

    fn get(&self, dataset_id: (usize, usize), partition: usize) -> Option<Vec<u8>> {
        self.map
            .get(&(dataset_id, partition))
            .map(|entry| entry.0.clone())
    }

    fn put(
        &self,
        dataset_id: (usize, usize),
        partition: usize,
        value: Vec<u8>,
    ) -> CachePutResponse {
        let key = (dataset_id, partition);
        // TODO: logging
        let size = value.len() * 8 + 2 * 8; //this number of MB
        if size as f64 / (1000.0 * 1000.0) > self.max_mbytes as f64 {
            CachePutResponse::CachePutFailure
        } else {
            // TODO: ensure free space needs to be done and this needs to be modified
            self.map.insert(key, (value, size));
            CachePutResponse::CachePutSuccess(size)
        }
    }

    fn ensure_free_space(&self, _dataset_id: u64, _space: u64) -> bool {
        // TODO: logging
        todo!()
    }

    fn report_entry_dropped(_data_set_id: usize, _partition: usize, _entry: (Vec<u8>, usize)) {
        // TODO: loggging
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct KeySpace<'a> {
    pub cache: &'a BoundedMemoryCache,
    pub key_space_id: usize,
}

impl<'a> KeySpace<'a> {
    fn new(cache: &'a BoundedMemoryCache, key_space_id: usize) -> Self {
        KeySpace {
            cache,
            key_space_id,
        }
    }

    pub fn get(&self, dataset_id: usize, partition: usize) -> Option<Vec<u8>> {
        self.cache.get((self.key_space_id, dataset_id), partition)
    }
    pub fn put(&self, dataset_id: usize, partition: usize, value: Vec<u8>) -> CachePutResponse {
        self.cache
            .put((self.key_space_id, dataset_id), partition, value)
    }
    pub fn get_capacity(&self) -> usize {
        self.cache.max_mbytes
    }
}
