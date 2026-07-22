/// TransformedDStream — applies an arbitrary RDD→RDD function over multiple parent DStreams.
///
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
    transform_func: TransformFn<T, U>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = U>>>>,
}

/// User transform applied per batch: `(batch_rdd, time_ms) -> new_rdd`.
type TransformFn<T, U> =
    Arc<dyn Fn(Arc<dyn Rdd<Item = T>>, u64) -> Arc<dyn Rdd<Item = U>> + Send + Sync>;

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

/// User transform over two parent batches: `(rdd1, rdd2, time_ms) -> new_rdd`.
type TransformWithFn<T, U, V> = Arc<
    dyn Fn(Arc<dyn Rdd<Item = T>>, Arc<dyn Rdd<Item = U>>, u64) -> Arc<dyn Rdd<Item = V>>
        + Send
        + Sync,
>;

/// TransformedWithDStream — applies an arbitrary two-parent RDD→RDD function per batch.
///
/// The transform receives both parents' RDDs for the same batch time; the batch is skipped
/// when either parent has no RDD for that time.
pub struct TransformedWithDStream<T, U, V>
where
    T: Data + Clone,
    U: Data + Clone,
    V: Data + Clone,
{
    stream_id: usize,
    parent1: Arc<dyn DStream<T>>,
    parent2: Arc<dyn DStream<U>>,
    transform_func: TransformWithFn<T, U, V>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = V>>>>,
}

impl<T, U, V> TransformedWithDStream<T, U, V>
where
    T: Data + Clone,
    U: Data + Clone,
    V: Data + Clone,
{
    pub fn new<F>(
        stream_id: usize,
        parent1: Arc<dyn DStream<T>>,
        parent2: Arc<dyn DStream<U>>,
        func: F,
    ) -> Self
    where
        F: Fn(Arc<dyn Rdd<Item = T>>, Arc<dyn Rdd<Item = U>>, u64) -> Arc<dyn Rdd<Item = V>>
            + Send
            + Sync
            + 'static,
    {
        TransformedWithDStream {
            stream_id,
            parent1,
            parent2,
            transform_func: Arc::new(func),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T, U, V> DStreamBase for TransformedWithDStream<T, U, V>
where
    T: Data + Clone,
    U: Data + Clone,
    V: Data + Clone,
{
    fn slide_duration(&self) -> Duration {
        self.parent1.slide_duration()
    }
    fn id(&self) -> usize {
        self.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![
            self.parent1.clone() as Arc<dyn DStreamBase>,
            self.parent2.clone() as Arc<dyn DStreamBase>,
        ]
    }
}

impl<T, U, V> DStream<V> for TransformedWithDStream<T, U, V>
where
    T: Data + Clone,
    U: Data + Clone,
    V: Data + Clone,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = V>>> {
        let r1 = self.parent1.get_or_compute(valid_time_ms)?;
        let r2 = self.parent2.get_or_compute(valid_time_ms)?;
        Some((self.transform_func)(r1, r2, valid_time_ms))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = V>>> {
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
