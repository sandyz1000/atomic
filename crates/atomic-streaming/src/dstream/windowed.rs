use crate::dstream::{DStream, DStreamBase};
use atomic_compute::rdd::union_rdd::UnionRdd;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_RDD_ID: AtomicUsize = AtomicUsize::new(0x6000_0000);

fn next_rdd_id() -> usize {
    NEXT_RDD_ID.fetch_add(1, Ordering::Relaxed)
}

// ─────────────────────────────────────────────────────────────────────────────
// WindowedDStream
// ─────────────────────────────────────────────────────────────────────────────

/// A DStream that produces an RDD covering a sliding window of batches.
pub struct WindowedDStream<T: Data + Clone> {
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    window_duration: Duration,
    slide_duration: Duration,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = T>>>>,
}

impl<T: Data + Clone> WindowedDStream<T> {
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<T>>,
        window_duration: Duration,
        slide_duration: Duration,
    ) -> Self {
        let parent_slide = parent.slide_duration();
        assert!(
            window_duration.as_millis() % parent_slide.as_millis() == 0,
            "window_duration must be a multiple of parent slide_duration"
        );
        assert!(
            slide_duration.as_millis() % parent_slide.as_millis() == 0,
            "slide_duration must be a multiple of parent slide_duration"
        );
        WindowedDStream {
            stream_id,
            parent,
            window_duration,
            slide_duration,
            generated: Mutex::new(HashMap::new()),
        }
    }

    pub fn window_duration(&self) -> Duration {
        self.window_duration
    }
}

impl<T: Data + Clone> DStreamBase for WindowedDStream<T> {
    fn slide_duration(&self) -> Duration {
        self.slide_duration
    }
    fn id(&self) -> usize {
        self.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<T: Data + Clone> DStream<T> for WindowedDStream<T> {
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        let window_ms = self.window_duration.as_millis() as u64;
        let parent_slide_ms = self.parent.slide_duration().as_millis() as u64;

        // Collect parent RDDs covering [valid_time_ms - window_ms + parent_slide_ms,
        // valid_time_ms], stepping backward by parent_slide_ms.
        let num_steps = (window_ms / parent_slide_ms).max(1);
        let rdds: Vec<Arc<dyn Rdd<Item = T>>> = (0..num_steps)
            .filter_map(|i| {
                let t = valid_time_ms.saturating_sub(i * parent_slide_ms);
                self.parent.get_or_compute(t)
            })
            .collect();

        match rdds.len() {
            0 => None,
            1 => Some(rdds.into_iter().next().unwrap()),
            _ => UnionRdd::new(next_rdd_id(), &rdds)
                .ok()
                .map(|u| Arc::new(u) as Arc<dyn Rdd<Item = T>>),
        }
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
// ReducedWindowedDStream — TODO Phase 4
// ─────────────────────────────────────────────────────────────────────────────

/// A DStream that incrementally reduces over a sliding window using reduce/inverse-reduce.
pub struct ReducedWindowedDStream<T, F, Finv>
where
    T: Data + Clone,
    F: Fn(T, T) -> T + Send + Sync + 'static,
    Finv: Fn(T, T) -> T + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    reduce_func: Arc<F>,
    inv_reduce_func: Arc<Finv>,
    window_duration: Duration,
    slide_duration: Duration,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = T>>>>,
}

impl<T, F, Finv> ReducedWindowedDStream<T, F, Finv>
where
    T: Data + Clone,
    F: Fn(T, T) -> T + Send + Sync + 'static,
    Finv: Fn(T, T) -> T + Send + Sync + 'static,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<T>>,
        reduce_func: F,
        inv_reduce_func: Finv,
        window_duration: Duration,
        slide_duration: Duration,
    ) -> Self {
        ReducedWindowedDStream {
            stream_id,
            parent,
            reduce_func: Arc::new(reduce_func),
            inv_reduce_func: Arc::new(inv_reduce_func),
            window_duration,
            slide_duration,
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T, F, Finv> DStreamBase for ReducedWindowedDStream<T, F, Finv>
where
    T: Data + Clone,
    F: Fn(T, T) -> T + Send + Sync + 'static,
    Finv: Fn(T, T) -> T + Send + Sync + 'static,
{
    fn slide_duration(&self) -> Duration {
        self.slide_duration
    }
    fn id(&self) -> usize {
        self.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<T, F, Finv> DStream<T> for ReducedWindowedDStream<T, F, Finv>
where
    T: Data + Clone,
    F: Fn(T, T) -> T + Send + Sync + 'static,
    Finv: Fn(T, T) -> T + Send + Sync + 'static,
{
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        // TODO Phase 4: incremental windowed reduce using inv_reduce_func
        unimplemented!("ReducedWindowedDStream::compute — implement in Phase 4")
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
