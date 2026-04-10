use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_data::cache::PARTITION_CACHE;

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
/// [`PARTITION_CACHE`].
///
/// The first time a partition is computed the result is collected into a
/// `Vec<T>` and stored as `Arc<Vec<T>>` in the cache.  Subsequent requests for
/// the same partition return the cached vector without re-executing the parent
/// DAG.
///
/// # Type bounds
///
/// `T` must be `Data + Clone`.  No serialization is required — the values are
/// kept alive as typed Rust objects in the same process.
pub struct CachedRdd<T: Data + Clone> {
    /// The wrapped RDD whose partitions will be memoised.
    inner: Arc<dyn Rdd<Item = T>>,
    /// Metadata (id, dependencies).
    vals: Arc<RddVals>,
}

impl<T: Data + Clone> CachedRdd<T> {
    /// Wrap `inner` in a caching layer.  A globally unique ID is assigned
    /// automatically so cache keys never collide across `Context` instances.
    pub fn new(inner: Arc<dyn Rdd<Item = T>>) -> Self {
        let id = next_cached_id();
        let rdd_base = inner.get_rdd_base();
        let mut vals = RddVals::new(id);
        vals.should_cache = true;
        vals.dependencies.push(Dependency::OneToOne { rdd_base });
        CachedRdd {
            inner,
            vals: Arc::new(vals),
        }
    }

    fn rdd_id(&self) -> usize {
        self.vals.id
    }
}

impl<T: Data + Clone> Clone for CachedRdd<T> {
    fn clone(&self) -> Self {
        CachedRdd {
            inner: self.inner.clone(),
            vals: self.vals.clone(),
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
        // Delegate to the parent; locality-aware scheduling (querying the
        // PartitionStore for which host already holds the partition) is deferred
        // until CacheTracker is integrated.
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

// ── Rdd ───────────────────────────────────────────────────────────────────────

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

        // ── Cache hit ─────────────────────────────────────────────────────────
        if let Some(store) = PARTITION_CACHE.get() {
            if let Some(cached) = store.get::<T>(rdd_id, idx) {
                // Return an iterator that clones each element out of the Arc<Vec<T>>.
                return Ok(Box::new(ArcVecIter {
                    data: cached,
                    pos: 0,
                }));
            }
        }

        // ── Cache miss — compute, store, return ───────────────────────────────
        let items: Vec<T> = self.inner.iterator(split)?.collect();
        let arc = Arc::new(items);

        if let Some(store) = PARTITION_CACHE.get() {
            store.put::<T>(rdd_id, idx, arc.clone());
        }

        Ok(Box::new(ArcVecIter { data: arc, pos: 0 }))
    }
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

    #[tokio::test]
    async fn cache_works_with_multiple_partitions() {
        let sc = Context::new_with_config(Config::local()).unwrap();
        let data: Vec<i32> = (1..=12).collect();
        let rdd = sc.parallelize_typed(data.clone(), 4).cache();
        let mut r1 = rdd.collect().unwrap();
        let mut r2 = rdd.collect().unwrap();
        r1.sort();
        r2.sort();
        assert_eq!(r1, r2);
        assert_eq!(r1.len(), data.len());
    }
}
