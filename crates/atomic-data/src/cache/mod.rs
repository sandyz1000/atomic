pub mod error;
pub mod tracker;

pub use error::{CacheError, Result};

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;

// ─────────────────────────────────────────────────────────────────────────────
// StorageLevel
// ─────────────────────────────────────────────────────────────────────────────

/// Hints for how an RDD's partitions should be persisted.
///
/// Only `MemoryOnly` is currently implemented.  The remaining variants are
/// reserved for API completeness and are treated as `MemoryOnly` at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageLevel {
    #[default]
    MemoryOnly,
    /// Reserved — treated as `MemoryOnly` until disk-spill is implemented.
    MemoryAndDisk,
    /// Reserved — treated as `MemoryOnly` (serialized form, same memory path).
    MemoryOnlySer,
    /// Reserved — treated as `MemoryOnly` until disk-only path is implemented.
    DiskOnly,
}

// ─────────────────────────────────────────────────────────────────────────────
// PartitionStore — typed in-memory cache for CachedRdd
// ─────────────────────────────────────────────────────────────────────────────

/// A lock-free, type-erased store for cached RDD partitions.
///
/// Keys are `(rdd_id, partition_index)`.  Values are `Arc<Vec<T>>` boxed as
/// `dyn Any + Send + Sync` so that any `T: 'static + Send + Sync` can be
/// stored without serialization.
pub struct PartitionStore {
    map: DashMap<(usize, usize), Box<dyn Any + Send + Sync>>,
}

impl PartitionStore {
    pub fn new() -> Self {
        PartitionStore {
            map: DashMap::new(),
        }
    }

    /// Retrieve a cached partition, returning `None` on a miss or a type mismatch.
    pub fn get<T: Any + Send + Sync>(
        &self,
        rdd_id: usize,
        partition: usize,
    ) -> Option<Arc<Vec<T>>> {
        self.map
            .get(&(rdd_id, partition))
            .and_then(|entry| entry.downcast_ref::<Arc<Vec<T>>>().cloned())
    }

    /// Insert a computed partition into the store.
    pub fn put<T: Any + Send + Sync + 'static>(
        &self,
        rdd_id: usize,
        partition: usize,
        data: Arc<Vec<T>>,
    ) {
        self.map.insert((rdd_id, partition), Box::new(data));
    }

    /// Remove all cached partitions for an RDD (unpersist).
    pub fn remove_rdd(&self, rdd_id: usize, num_partitions: usize) {
        for p in 0..num_partitions {
            self.map.remove(&(rdd_id, p));
        }
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
