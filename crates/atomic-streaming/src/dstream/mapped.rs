use crate::context::StreamingContext;
use crate::dstream::{DStream, DStreamBase, OutputOperation, StreamingJob};
use atomic_compute::rdd::flatmapper::FlatMapperRdd;
use atomic_compute::rdd::mapper::MapperRdd;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Global counter for unique RDD IDs created by streaming transforms.
static NEXT_RDD_ID: AtomicUsize = AtomicUsize::new(0x5000_0000);

fn next_rdd_id() -> usize {
    NEXT_RDD_ID.fetch_add(1, Ordering::Relaxed)
}

// ─────────────────────────────────────────────────────────────────────────────
// MappedDStream
// ─────────────────────────────────────────────────────────────────────────────

pub struct MappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> U + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    map_func: Arc<F>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = U>>>>,
}

impl<T, U, F> MappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> U + Send + Sync + 'static,
{
    pub fn new(stream_id: usize, parent: Arc<dyn DStream<T>>, func: F) -> Self {
        MappedDStream {
            stream_id,
            parent,
            map_func: Arc::new(func),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T, U, F> DStreamBase for MappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> U + Send + Sync + 'static,
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

impl<T, U, F> DStream<U> for MappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> U + Send + Sync + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = U>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let f = self.map_func.clone();
        let mapped = MapperRdd::new(next_rdd_id(), parent_rdd, move |x| f(x));
        Some(Arc::new(mapped))
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

// ─────────────────────────────────────────────────────────────────────────────
// FlatMappedDStream
// ─────────────────────────────────────────────────────────────────────────────

pub struct FlatMappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> Vec<U> + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    flat_map_func: Arc<F>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = U>>>>,
}

impl<T, U, F> FlatMappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> Vec<U> + Send + Sync + 'static,
{
    pub fn new(stream_id: usize, parent: Arc<dyn DStream<T>>, func: F) -> Self {
        FlatMappedDStream {
            stream_id,
            parent,
            flat_map_func: Arc::new(func),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T, U, F> DStreamBase for FlatMappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> Vec<U> + Send + Sync + 'static,
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

impl<T, U, F> DStream<U> for FlatMappedDStream<T, U, F>
where
    T: Data + Clone,
    U: Data + Clone,
    F: Fn(T) -> Vec<U> + Send + Sync + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = U>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let f = self.flat_map_func.clone();
        // FlatMapperRdd requires Fn(T) -> Box<dyn Iterator<Item=U>>; adapt from Vec<U>.
        let flat_mapped = FlatMapperRdd::new(
            next_rdd_id(),
            parent_rdd,
            move |x| -> Box<dyn Iterator<Item = U>> { Box::new(f(x).into_iter()) },
        );
        Some(Arc::new(flat_mapped))
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

// ─────────────────────────────────────────────────────────────────────────────
// FilteredDStream
// ─────────────────────────────────────────────────────────────────────────────

pub struct FilteredDStream<T, F>
where
    T: Data + Clone,
    F: Fn(&T) -> bool + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    filter_func: Arc<F>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = T>>>>,
}

impl<T, F> FilteredDStream<T, F>
where
    T: Data + Clone,
    F: Fn(&T) -> bool + Send + Sync + 'static,
{
    pub fn new(stream_id: usize, parent: Arc<dyn DStream<T>>, func: F) -> Self {
        FilteredDStream {
            stream_id,
            parent,
            filter_func: Arc::new(func),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T, F> DStreamBase for FilteredDStream<T, F>
where
    T: Data + Clone,
    F: Fn(&T) -> bool + Send + Sync + 'static,
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

impl<T, F> DStream<T> for FilteredDStream<T, F>
where
    T: Data + Clone,
    F: Fn(&T) -> bool + Send + Sync + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let pred = self.filter_func.clone();
        // No FilterRdd in atomic-compute; use FlatMapperRdd as a filter:
        // elements that pass the predicate emit once, others emit nothing.
        let filtered = FlatMapperRdd::new(
            next_rdd_id(),
            parent_rdd,
            move |x| -> Box<dyn Iterator<Item = T>> {
                if pred(&x) {
                    Box::new(std::iter::once(x))
                } else {
                    Box::new(std::iter::empty())
                }
            },
        );
        Some(Arc::new(filtered))
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

// ─────────────────────────────────────────────────────────────────────────────
// ForEachDStream — the primary output operation
// ─────────────────────────────────────────────────────────────────────────────

pub struct ForEachDStream<T, F>
where
    T: Data + Clone,
    F: Fn(Arc<dyn Rdd<Item = T>>, u64) + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    foreach_func: Arc<F>,
    ssc: Arc<StreamingContext>,
}

impl<T, F> ForEachDStream<T, F>
where
    T: Data + Clone,
    F: Fn(Arc<dyn Rdd<Item = T>>, u64) + Send + Sync + 'static,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<T>>,
        func: F,
        ssc: Arc<StreamingContext>,
    ) -> Self {
        ForEachDStream {
            stream_id,
            parent,
            foreach_func: Arc::new(func),
            ssc,
        }
    }
}

impl<T, F> OutputOperation for ForEachDStream<T, F>
where
    T: Data + Clone,
    F: Fn(Arc<dyn Rdd<Item = T>>, u64) + Send + Sync + 'static,
{
    fn generate_job(&self, time_ms: u64) -> Option<StreamingJob> {
        let rdd = self.parent.get_or_compute(time_ms)?;
        let func = self.foreach_func.clone();
        let t = time_ms;
        Some(StreamingJob::new(time_ms, 0, move || {
            func(rdd, t);
            Ok(())
        }))
    }
}
