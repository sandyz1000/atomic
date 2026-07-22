//! Best-effort caching wrapper for a DStream.
//!
//! Wrapping a stream in [`CachedDStream`] persists each batch's RDD via the engine's
//! [`CachedRdd`](atomic_compute::rdd::cached::CachedRdd), so repeated actions on the same batch
//! reuse the computed partitions. It is a hint — the engine decides eviction by storage level.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::rdd::cached::CachedRdd;
use atomic_data::cache::StorageLevel;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;

use crate::dstream::{DStream, DStreamBase};

/// A DStream whose per-batch RDDs are wrapped in a caching layer at the given storage level.
pub struct CachedDStream<T: Data + Clone> {
    parent: Arc<dyn DStream<T>>,
    level: StorageLevel,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = T>>>>,
}

impl<T: Data + Clone> CachedDStream<T> {
    /// Wrap `parent`, caching each batch's RDD at `level`.
    pub fn new(parent: Arc<dyn DStream<T>>, level: StorageLevel) -> Self {
        CachedDStream {
            parent,
            level,
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T: Data + Clone> DStreamBase for CachedDStream<T> {
    fn slide_duration(&self) -> Duration {
        self.parent.slide_duration()
    }
    fn id(&self) -> usize {
        self.parent.id()
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<T: Data + Clone> DStream<T> for CachedDStream<T> {
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        let inner = self.parent.get_or_compute(valid_time_ms)?;
        Some(Arc::new(CachedRdd::new_with_level(inner, self.level)))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) {
                return Some(rdd.clone());
            }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}
