use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_data::cache::{PARTITION_CACHE, StorageLevel};

/// Global counter for `CachedRdd` IDs.  These are stored in the global
/// `PARTITION_CACHE` which outlives any individual `Context`, so IDs must be
/// unique across all contexts (not just within one context's `next_rdd_id`).
static NEXT_CACHED_ID: AtomicUsize = AtomicUsize::new(0x7000_0000);

fn next_cached_id() -> usize {
    NEXT_CACHED_ID.fetch_add(1, Ordering::Relaxed)
}
use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::split::Split;

use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};

// ─────────────────────────────────────────────────────────────────────────────
// CachedRdd<T>
// ─────────────────────────────────────────────────────────────────────────────

/// An RDD wrapper that memoises each partition's output in the global
/// [`PARTITION_CACHE`] (memory) and optionally spills to disk.
///
/// | `StorageLevel`    | Behaviour |
/// |---|---|
/// | `MemoryOnly`      | Store in `PARTITION_CACHE`; evict via LRU when full |
/// | `MemoryAndDisk`   | Store in `PARTITION_CACHE`; on LRU eviction fall back to a disk file |
/// | `DiskOnly`        | Skip `PARTITION_CACHE`; always read/write from a disk file |
/// | `MemoryOnlySer`   | Treated as `MemoryOnly` (serialized-memory path deferred) |
///
/// Disk partitions are stored at:
/// `{RDD_CACHE_SPILL_DIR}/{rdd_id}/{partition_index}.bin` (bincode-encoded `Vec<T>`).
pub struct CachedRdd<T: Data + Clone> {
    /// The wrapped RDD whose partitions will be memoised.
    inner: Arc<dyn Rdd<Item = T>>,
    /// Metadata (id, dependencies).
    vals: Arc<RddVals>,
    /// Requested storage level.
    storage_level: StorageLevel,
}

impl<T: Data + Clone> CachedRdd<T> {
    /// Wrap `inner` in a caching layer with the given storage level.
    pub fn new_with_level(inner: Arc<dyn Rdd<Item = T>>, level: StorageLevel) -> Self {
        let id = next_cached_id();
        let rdd_base = inner.get_rdd_base();
        let mut vals = RddVals::new(id);
        vals.should_cache = true;
        vals.dependencies.push(Dependency::OneToOne { rdd_base });
        CachedRdd { inner, vals: Arc::new(vals), storage_level: level }
    }

    /// Wrap `inner` with `MemoryOnly` (the default).
    pub fn new(inner: Arc<dyn Rdd<Item = T>>) -> Self {
        Self::new_with_level(inner, StorageLevel::MemoryOnly)
    }

    pub fn rdd_id(&self) -> usize {
        self.vals.id
    }

    pub fn storage_level(&self) -> StorageLevel {
        self.storage_level
    }

    /// Path for the disk-spill file for a given partition.
    pub fn spill_path(&self, partition: usize) -> Option<PathBuf> {
        atomic_data::env::get_rdd_cache_spill_dir().map(|base| {
            base.join(format!("{}", self.rdd_id()))
                .join(format!("{}.bin", partition))
        })
    }
}

impl<T: Data + Clone> Clone for CachedRdd<T> {
    fn clone(&self) -> Self {
        CachedRdd {
            inner: self.inner.clone(),
            vals: self.vals.clone(),
            storage_level: self.storage_level,
        }
    }
}

// ── RddBase ───────────────────────────────────────────────────────────────────

impl<T: Data + Clone> RddBase for CachedRdd<T> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_name(&self) -> String {
        "cache".to_owned()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.inner.preferred_locations(split)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.inner.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.inner.number_of_splits()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

// ── Rdd (memory-only path) ────────────────────────────────────────────────────

impl<T: Data + Clone + 'static> Rdd for CachedRdd<T> {
    type Item = T;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = T>>, BaseError> {
        let idx = split.get_index();
        let rdd_id = self.rdd_id();

        match self.storage_level {
            StorageLevel::MemoryOnly | StorageLevel::MemoryOnlySer => {
                // ── Memory-only path ──────────────────────────────────────────
                if let Some(store) = PARTITION_CACHE.get() {
                    if let Some(cached) = store.get::<T>(rdd_id, idx) {
                        return Ok(Box::new(ArcVecIter { data: cached, pos: 0 }));
                    }
                }
                let items: Vec<T> = self.inner.iterator(split)?.collect();
                let arc = Arc::new(items);
                if let Some(store) = PARTITION_CACHE.get() {
                    store.put::<T>(rdd_id, idx, arc.clone());
                }
                Ok(Box::new(ArcVecIter { data: arc, pos: 0 }))
            }

            StorageLevel::MemoryAndDisk => {
                // ── Memory-first; disk spill is handled by `persist_with_disk()`
                // on TypedRdd<T: bincode::Encode + Decode<()>>.  The generic
                // `Rdd::compute` path cannot call bincode without adding `Decode`
                // bounds, so it falls back to MemoryOnly semantics here and
                // recomputes on LRU eviction. For true disk spill use
                // `TypedRdd::persist_with_disk(StorageLevel::MemoryAndDisk)`.
                if let Some(store) = PARTITION_CACHE.get() {
                    if let Some(cached) = store.get::<T>(rdd_id, idx) {
                        return Ok(Box::new(ArcVecIter { data: cached, pos: 0 }));
                    }
                }
                let items: Vec<T> = self.inner.iterator(split)?.collect();
                let arc = Arc::new(items);
                if let Some(store) = PARTITION_CACHE.get() {
                    store.put::<T>(rdd_id, idx, arc.clone());
                }
                Ok(Box::new(ArcVecIter { data: arc, pos: 0 }))
            }

            StorageLevel::DiskOnly => {
                // ── Disk path requires bincode bounds; without them, recompute each time.
                // For true disk-only persistence use `persist_with_disk(DiskOnly)`.
                let items: Vec<T> = self.inner.iterator(split)?.collect();
                Ok(Box::new(items.into_iter()))
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Disk helpers — only for T: bincode::Encode + Decode
// ─────────────────────────────────────────────────────────────────────────────

/// Write a partition to disk atomically (.tmp → rename).
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
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Read a partition from disk.
pub fn disk_read_partition<T>(path: &std::path::Path) -> std::io::Result<Vec<T>>
where
    T: bincode::Decode<()>,
{
    let bytes = std::fs::read(path)?;
    bincode::decode_from_slice::<Vec<T>, _>(&bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

// ─────────────────────────────────────────────────────────────────────────────
// ArcVecIter — iterator over Arc<Vec<T>> that clones each item
// ─────────────────────────────────────────────────────────────────────────────

struct ArcVecIter<T> {
    data: Arc<Vec<T>>,
    pos: usize,
}

impl<T: Clone> Iterator for ArcVecIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.pos < self.data.len() {
            let item = self.data[self.pos].clone();
            self.pos += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.data.len() - self.pos;
        (remaining, Some(remaining))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::context::Context;
    use crate::env::Config;

    #[tokio::test]
    async fn cache_returns_same_results_across_actions() {
        let sc = Context::new_with_config(Config::local()).unwrap();
        let data = vec![1i32, 2, 3, 4, 5, 6];
        let rdd = sc.parallelize_typed(data.clone(), 2).cache();
        let r1 = rdd.collect().unwrap();
        let r2 = rdd.collect().unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1.len(), data.len());
    }

    #[tokio::test]
    async fn cache_preserves_values() {
        let sc = Context::new_with_config(Config::local()).unwrap();
        let data = vec![10i32, 20, 30];
        let rdd = sc.parallelize_typed(data.clone(), 1).cache();
        let mut result = rdd.collect().unwrap();
        result.sort();
        assert_eq!(result, data);
    }
}
