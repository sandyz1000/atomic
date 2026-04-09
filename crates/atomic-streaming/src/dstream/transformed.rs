/// TransformedDStream — applies an arbitrary RDD→RDD function over multiple parent DStreams.
///
/// TODO Phase 4: implement `compute`.
use crate::dstream::{DStream, DStreamBase};
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct TransformedDStream<T, U>
where
    T: Data + Clone,
    U: Data + Clone,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    transform_func: Arc<dyn Fn(Arc<dyn Rdd<Item = T>>, u64) -> Arc<dyn Rdd<Item = U>> + Send + Sync>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = U>>>>,
}

impl<T, U> TransformedDStream<T, U>
where
    T: Data + Clone,
    U: Data + Clone,
{
    pub fn new<F>(stream_id: usize, parent: Arc<dyn DStream<T>>, func: F) -> Self
    where
        F: Fn(Arc<dyn Rdd<Item = T>>, u64) -> Arc<dyn Rdd<Item = U>> + Send + Sync + 'static,
    {
        TransformedDStream {
            stream_id,
            parent,
            transform_func: Arc::new(func),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T, U> DStreamBase for TransformedDStream<T, U>
where
    T: Data + Clone,
    U: Data + Clone,
{
    fn slide_duration(&self) -> Duration {
        self.parent.slide_duration()
    }
    fn id(&self) -> usize {
        self.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<T, U> DStream<U> for TransformedDStream<T, U>
where
    T: Data + Clone,
    U: Data + Clone,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = U>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        Some((self.transform_func)(parent_rdd, valid_time_ms))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = U>>> {
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
