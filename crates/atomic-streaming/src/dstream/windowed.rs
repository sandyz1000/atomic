use crate::context::StreamingContext;
use crate::dstream::{DStream, DStreamBase};
use atomic_compute::rdd::parallel_collection::ParallelCollection;
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

// WindowedDStream

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
            window_duration
                .as_millis()
                .is_multiple_of(parent_slide.as_millis()),
            "window_duration must be a multiple of parent slide_duration"
        );
        assert!(
            slide_duration
                .as_millis()
                .is_multiple_of(parent_slide.as_millis()),
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

/// A DStream that reduces all elements across a sliding window.
///
/// `compute(t)` collects every batch RDD in `[t - window, t]`, reduces all elements
/// per batch using `reduce_func`, then reduces the per-batch results together.
/// When `inv_reduce_func` is `Some`, the incremental path is used: the previous
/// window's reduced result has expired batches removed (via inverse) and new batches
/// added — O(1) per slide instead of O(window/slide).
///
/// For simplicity the driver-side collect approach is used here; a shuffle-based
/// distributed reduce can be substituted later.
pub struct ReducedWindowedDStream<T, F, Finv>
where
    T: Data + Clone,
    F: Fn(T, T) -> T + Send + Sync + 'static,
    Finv: Fn(T, T) -> T + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<T>>,
    ssc: Arc<StreamingContext>,
    reduce_func: Arc<F>,
    /// Inverse of `reduce_func`, used to subtract expired batches on the incremental path.
    inv_reduce_func: Arc<Finv>,
    window_duration: Duration,
    slide_duration: Duration,
    /// The last computed `(batch_time, reduced_value)`, seeding the next slide's incremental
    /// update. `None` before the first computation or after an empty window.
    window_state: Mutex<Option<(u64, T)>>,
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
        ssc: Arc<StreamingContext>,
        reduce_func: F,
        inv_reduce_func: Finv,
        window_duration: Duration,
        slide_duration: Duration,
    ) -> Self {
        ReducedWindowedDStream {
            stream_id,
            parent,
            ssc,
            reduce_func: Arc::new(reduce_func),
            inv_reduce_func: Arc::new(inv_reduce_func),
            window_duration,
            slide_duration,
            window_state: Mutex::new(None),
            generated: Mutex::new(HashMap::new()),
        }
    }

    /// Reduce one parent batch at `t` to a single value, or `None` when that batch is empty.
    fn batch_reduced(&self, t: u64) -> Option<T> {
        let rdd = self.parent.get_or_compute(t)?;
        let items = self
            .ssc
            .sc
            .run_job(rdd, |iter| iter.collect::<Vec<T>>())
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .collect::<Vec<T>>();
        let mut iter = items.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, x| (self.reduce_func)(acc, x)))
    }

    /// Fold `values` together with `reduce_func`, returning `None` for an empty slice.
    fn reduce_all(&self, values: Vec<T>) -> Option<T> {
        let mut iter = values.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, x| (self.reduce_func)(acc, x)))
    }

    /// The full (non-incremental) window reduction: reduce every batch whose time lies in
    /// `(valid_time - window, valid_time]` and combine them.
    fn full_window(&self, valid_time_ms: u64, parent_slide_ms: u64, num_steps: u64) -> Option<T> {
        let mut reduced: Vec<T> = Vec::new();
        for i in 0..num_steps {
            let t = valid_time_ms.saturating_sub(i * parent_slide_ms);
            if let Some(v) = self.batch_reduced(t) {
                reduced.push(v);
            }
        }
        self.reduce_all(reduced)
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
    T: Data + Clone + std::fmt::Debug,
    F: Fn(T, T) -> T + Send + Sync + 'static,
    Finv: Fn(T, T) -> T + Send + Sync + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        let window_ms = self.window_duration.as_millis() as u64;
        let parent_slide_ms = self.parent.slide_duration().as_millis() as u64;
        let num_steps = (window_ms / parent_slide_ms).max(1);
        let ctx = self.ssc.sc.clone();

        let mut state = self.window_state.lock();

        // Incremental path: reuse the previous window's value, adding the batches that entered
        // and subtracting (via the inverse) the batches that expired. Applies only when the
        // previous state is exactly one parent-slide behind, so entered/left are single batches.
        let reduced = match &*state {
            Some((last_t, last_val)) if valid_time_ms == last_t + parent_slide_ms => {
                let entered = self.batch_reduced(valid_time_ms);
                // The batch that just fell out of the trailing edge of the window.
                let expired_t = valid_time_ms.saturating_sub(window_ms);
                let expired = if valid_time_ms >= window_ms {
                    self.batch_reduced(expired_t)
                } else {
                    None
                };

                let mut acc = last_val.clone();
                if let Some(e) = entered {
                    acc = (self.reduce_func)(acc, e);
                }
                if let Some(x) = expired {
                    acc = (self.inv_reduce_func)(acc, x);
                }
                Some(acc)
            }
            // Full recompute on the first call or after an irregular slide.
            _ => self.full_window(valid_time_ms, parent_slide_ms, num_steps),
        };

        match reduced {
            Some(v) => {
                *state = Some((valid_time_ms, v.clone()));
                Some(Arc::new(ParallelCollection::new(
                    ctx.new_rdd_id(),
                    std::iter::once(v),
                    1,
                )))
            }
            None => {
                *state = None;
                Some(Arc::new(ParallelCollection::<T>::new(
                    ctx.new_rdd_id(),
                    std::iter::empty::<T>(),
                    1,
                )))
            }
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
