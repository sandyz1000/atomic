/// Pair DStream operations (reduce_by_key, group_by_key, join, etc.)
///
/// All methods are stubbed — TODO Phase 4.
use crate::dstream::{DStream, DStreamBase};
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// StateSpec and StateSpecImpl (stubs for mapWithState)
// ─────────────────────────────────────────────────────────────────────────────

pub trait StateSpec<K, V, S, M>: Sized + Send + Sync + 'static {
    fn initial_state(self, rdd: Arc<dyn Rdd<Item = (K, S)>>) -> Self;
    fn num_partitions(self, n: usize) -> Self;
    fn timeout(self, idle: Duration) -> Self;
}

#[derive(Clone)]
pub struct StateSpecImpl<K, V, S, M> {
    initial_state_rdd: Option<Arc<dyn Rdd<Item = (K, S)>>>,
    num_partitions: Option<usize>,
    timeout: Option<Duration>,
    _marker: std::marker::PhantomData<(K, V, S, M)>,
}

impl<K, V, S, M> StateSpecImpl<K, V, S, M> {
    pub fn new() -> Self {
        StateSpecImpl {
            initial_state_rdd: None,
            num_partitions: None,
            timeout: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, V, S, M> Default for StateSpecImpl<K, V, S, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S, M> StateSpec<K, V, S, M> for StateSpecImpl<K, V, S, M>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    M: Data + Clone,
{
    fn initial_state(mut self, rdd: Arc<dyn Rdd<Item = (K, S)>>) -> Self {
        self.initial_state_rdd = Some(rdd);
        self
    }
    fn num_partitions(mut self, n: usize) -> Self {
        self.num_partitions = Some(n);
        self
    }
    fn timeout(mut self, idle: Duration) -> Self {
        self.timeout = Some(idle);
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PairDStreamFunctions — extension methods for DStream<(K, V)>
// ─────────────────────────────────────────────────────────────────────────────

/// Extension methods for DStreams of `(K, V)` pairs.
pub struct PairDStreamFunctions<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    pub stream: Arc<dyn DStream<(K, V)>>,
}

impl<K, V> PairDStreamFunctions<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    pub fn new(stream: Arc<dyn DStream<(K, V)>>) -> Self {
        PairDStreamFunctions { stream }
    }

    /// Reduce each key's values within each batch using `func`.
    ///
    /// TODO Phase 4: implement using atomic_compute pair RDD operations.
    pub fn reduce_by_key<F>(&self, _func: F) -> Arc<ReduceByKeyDStream<K, V, F>>
    where
        F: Fn(V, V) -> V + Send + Sync + Clone + 'static,
    {
        unimplemented!("PairDStreamFunctions::reduce_by_key — implement in Phase 4")
    }

    /// Group values with the same key in each batch.
    ///
    /// TODO Phase 4.
    pub fn group_by_key(&self) -> ! {
        unimplemented!("PairDStreamFunctions::group_by_key — implement in Phase 4")
    }

    /// Join two pair DStreams on matching keys in each batch.
    ///
    /// TODO Phase 4.
    pub fn join<W: Data + Clone>(&self, _other: Arc<dyn DStream<(K, W)>>) -> ! {
        unimplemented!("PairDStreamFunctions::join — implement in Phase 4")
    }

    /// Left-outer join two pair DStreams.
    ///
    /// TODO Phase 4.
    pub fn left_outer_join<W: Data + Clone>(&self, _other: Arc<dyn DStream<(K, W)>>) -> ! {
        unimplemented!("PairDStreamFunctions::left_outer_join — implement in Phase 4")
    }

    /// Windowed reduceByKey.
    ///
    /// TODO Phase 4.
    pub fn reduce_by_key_and_window<F>(
        &self,
        _func: F,
        _window: Duration,
        _slide: Duration,
    ) -> !
    where
        F: Fn(V, V) -> V + Send + Sync + 'static,
    {
        unimplemented!("PairDStreamFunctions::reduce_by_key_and_window — implement in Phase 4")
    }

    /// Update the running state for each key.
    ///
    /// TODO Phase 4.
    pub fn update_state_by_key<S, F>(&self, _func: F) -> !
    where
        S: Data + Clone,
        F: Fn(&[V], Option<S>) -> Option<S> + Send + Sync + 'static,
    {
        unimplemented!("PairDStreamFunctions::update_state_by_key — implement in Phase 4")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReduceByKeyDStream (stub)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    F: Fn(V, V) -> V + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<(K, V)>>,
    reduce_func: Arc<F>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, V)>>>>,
}

impl<K, V, F> DStreamBase for ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    F: Fn(V, V) -> V + Send + Sync + 'static,
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

impl<K, V, F> DStream<(K, V)> for ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    F: Fn(V, V) -> V + Send + Sync + 'static,
{
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, V)>>> {
        // TODO Phase 4: implement using atomic_compute's reduce_by_key
        unimplemented!("ReduceByKeyDStream::compute — implement in Phase 4")
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, V)>>> {
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
