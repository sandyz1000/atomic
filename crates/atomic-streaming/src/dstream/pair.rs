/// Pair DStream operations (reduce_by_key, group_by_key, etc.)
use crate::context::StreamingContext;
use crate::dstream::{DStream, DStreamBase};
use atomic_compute::rdd::TypedRdd;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

// StateSpec and StateSpecImpl (stubs for mapWithState)

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
    fn default() -> Self { Self::new() }
}

impl<K, V, S, M> StateSpec<K, V, S, M> for StateSpecImpl<K, V, S, M>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    M: Data + Clone,
{
    fn initial_state(mut self, rdd: Arc<dyn Rdd<Item = (K, S)>>) -> Self {
        self.initial_state_rdd = Some(rdd); self
    }
    fn num_partitions(mut self, n: usize) -> Self {
        self.num_partitions = Some(n); self
    }
    fn timeout(mut self, idle: Duration) -> Self {
        self.timeout = Some(idle); self
    }
}

// PairDStreamFunctions

pub struct PairDStreamFunctions<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    pub stream: Arc<dyn DStream<(K, V)>>,
    pub ssc: Arc<StreamingContext>,
}

impl<K, V> PairDStreamFunctions<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    pub fn new(stream: Arc<dyn DStream<(K, V)>>, ssc: Arc<StreamingContext>) -> Self {
        PairDStreamFunctions { stream, ssc }
    }

    /// Reduce each key's values within each batch using `func`.
    pub fn reduce_by_key<F>(
        &self,
        func: F,
        num_partitions: usize,
    ) -> Arc<ReduceByKeyDStream<K, V, F>>
    where
        F: Fn(V, V) -> V + Send + Sync + Clone + 'static,
    {
        let id = self.ssc.sc.new_rdd_id();
        Arc::new(ReduceByKeyDStream::new(
            id, self.stream.clone(), self.ssc.clone(), func, num_partitions,
        ))
    }

    /// Group values with the same key in each batch.
    pub fn group_by_key(&self, num_partitions: usize) -> Arc<GroupByKeyDStream<K, V>> {
        let id = self.ssc.sc.new_rdd_id();
        Arc::new(GroupByKeyDStream::new(id, self.stream.clone(), self.ssc.clone(), num_partitions))
    }

    /// Join two pair DStreams on matching keys in each batch.
    ///
    /// Collects both batch RDDs to the driver and performs a hash join (same as `TypedRdd::join`).
    pub fn join<W: Data + Clone>(
        &self,
        other: Arc<dyn DStream<(K, W)>>,
    ) -> Arc<JoinDStream<K, V, W>>
    where
        K: std::hash::Hash + Eq,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, W)>: Data + Clone,
    {
        let id = self.ssc.sc.new_rdd_id();
        Arc::new(JoinDStream::new(id, self.stream.clone(), other, self.ssc.clone()))
    }

    /// Left-outer join two pair DStreams.
    pub fn left_outer_join<W: Data + Clone>(
        &self,
        other: Arc<dyn DStream<(K, W)>>,
    ) -> Arc<LeftOuterJoinDStream<K, V, W>>
    where
        K: std::hash::Hash + Eq,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, W)>: Data + Clone,
    {
        let id = self.ssc.sc.new_rdd_id();
        Arc::new(LeftOuterJoinDStream::new(id, self.stream.clone(), other, self.ssc.clone()))
    }

    /// Update running state for each key across batches.
    ///
    /// For each key, `func(&new_values, current_state)` is called once per batch.
    /// Returning `Some(s)` keeps the key alive with new state; returning `None`
    /// evicts the key from the state store.
    ///
    /// The returned `StateDStream` emits the full `(K, S)` state RDD each batch.
    pub fn update_state_by_key<S, F>(
        &self,
        func: F,
    ) -> Arc<StateDStream<K, V, S, F>>
    where
        S: Data + Clone + std::fmt::Debug + 'static,
        F: Fn(&[V], Option<S>) -> Option<S> + Send + Sync + Clone + 'static,
        K: std::fmt::Debug + 'static,
        V: std::fmt::Debug + 'static,
    {
        let id = self.ssc.sc.new_rdd_id();
        Arc::new(StateDStream::new(id, self.stream.clone(), self.ssc.clone(), func))
    }
}

// ReduceByKeyDStream

pub struct ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    F: Fn(V, V) -> V + Send + Sync + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<(K, V)>>,
    ssc: Arc<StreamingContext>,
    reduce_func: Arc<F>,
    num_partitions: usize,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, V)>>>>,
}

impl<K, V, F> ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    F: Fn(V, V) -> V + Send + Sync + 'static,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<(K, V)>>,
        ssc: Arc<StreamingContext>,
        func: F,
        num_partitions: usize,
    ) -> Self {
        ReduceByKeyDStream {
            stream_id, parent, ssc,
            reduce_func: Arc::new(func),
            num_partitions,
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V, F> DStreamBase for ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    F: Fn(V, V) -> V + Send + Sync + 'static,
{
    fn slide_duration(&self) -> Duration { self.parent.slide_duration() }
    fn id(&self) -> usize { self.stream_id }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<K, V, F> DStream<(K, V)> for ReduceByKeyDStream<K, V, F>
where
    K: Data + Clone + Hash + Eq + std::fmt::Debug + Send + Sync + 'static,
    V: Data + Clone + std::fmt::Debug + Send + Sync + 'static,
    F: Fn(V, V) -> V + Send + Sync + Clone + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, V)>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();
        let f = self.reduce_func.clone();

        let pairs = ctx.run_job(parent_rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();

        let mut agg: std::collections::HashMap<K, V> = std::collections::HashMap::new();
        for partition in pairs {
            for (k, v) in partition {
                agg.entry(k)
                    .and_modify(|c| { let new_c = f(c.clone(), v.clone()); *c = new_c; })
                    .or_insert(v);
            }
        }
        let result: Vec<(K, V)> = agg.into_iter().collect();
        let id = ctx.new_rdd_id();
        Some(Arc::new(
            atomic_compute::rdd::parallel_collection::ParallelCollection::new(id, result, 1)
        ))
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

// GroupByKeyDStream

pub struct GroupByKeyDStream<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    stream_id: usize,
    parent: Arc<dyn DStream<(K, V)>>,
    ssc: Arc<StreamingContext>,
    num_partitions: usize,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, Vec<V>)>>>>,
}

impl<K, V> GroupByKeyDStream<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<(K, V)>>,
        ssc: Arc<StreamingContext>,
        num_partitions: usize,
    ) -> Self {
        GroupByKeyDStream { stream_id, parent, ssc, num_partitions, generated: Mutex::new(HashMap::new()) }
    }
}

impl<K, V> DStreamBase for GroupByKeyDStream<K, V>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
{
    fn slide_duration(&self) -> Duration { self.parent.slide_duration() }
    fn id(&self) -> usize { self.stream_id }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<K, V> DStream<(K, Vec<V>)> for GroupByKeyDStream<K, V>
where
    K: Data + Clone + Hash + Eq + std::fmt::Debug + Send + Sync + 'static,
    V: Data + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, Vec<V>)>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();

        let pairs = ctx.run_job(parent_rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();

        let mut agg: std::collections::HashMap<K, Vec<V>> = std::collections::HashMap::new();
        for partition in pairs {
            for (k, v) in partition {
                agg.entry(k).or_default().push(v);
            }
        }
        let result: Vec<(K, Vec<V>)> = agg.into_iter().collect();
        let id = ctx.new_rdd_id();
        Some(Arc::new(
            atomic_compute::rdd::parallel_collection::ParallelCollection::new(id, result, 1)
        ))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, Vec<V>)>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) { return Some(rdd.clone()); }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

// JoinDStream / LeftOuterJoinDStream (driver-side hash join per batch)

pub struct JoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    W: Data + Clone,
{
    stream_id: usize,
    left: Arc<dyn DStream<(K, V)>>,
    right: Arc<dyn DStream<(K, W)>>,
    ssc: Arc<StreamingContext>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, (V, W))>>>>,
}

impl<K, V, W> JoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    W: Data + Clone,
{
    pub fn new(
        stream_id: usize,
        left: Arc<dyn DStream<(K, V)>>,
        right: Arc<dyn DStream<(K, W)>>,
        ssc: Arc<StreamingContext>,
    ) -> Self {
        JoinDStream { stream_id, left, right, ssc, generated: Mutex::new(HashMap::new()) }
    }
}

impl<K, V, W> DStreamBase for JoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    W: Data + Clone,
{
    fn slide_duration(&self) -> Duration { self.left.slide_duration() }
    fn id(&self) -> usize { self.stream_id }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![
            self.left.clone() as Arc<dyn DStreamBase>,
            self.right.clone() as Arc<dyn DStreamBase>,
        ]
    }
}

impl<K, V, W> DStream<(K, (V, W))> for JoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq + std::fmt::Debug + 'static,
    V: Data + Clone + std::fmt::Debug + 'static,
    W: Data + Clone + std::fmt::Debug + 'static,
    Vec<(K, V)>: Data + Clone,
    Vec<(K, W)>: Data + Clone,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, (V, W))>>> {
        let left_rdd = self.left.get_or_compute(valid_time_ms)?;
        let right_rdd = self.right.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();
        let left_typed: TypedRdd<(K, V)> = TypedRdd::new(left_rdd, ctx.clone());
        let right_typed: TypedRdd<(K, W)> = TypedRdd::new(right_rdd, ctx);
        Some(left_typed.join_local(right_typed).into_rdd())
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, (V, W))>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) { return Some(rdd.clone()); }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

pub struct LeftOuterJoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    W: Data + Clone,
{
    stream_id: usize,
    left: Arc<dyn DStream<(K, V)>>,
    right: Arc<dyn DStream<(K, W)>>,
    ssc: Arc<StreamingContext>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, (V, Option<W>))>>>>,
}

impl<K, V, W> LeftOuterJoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    W: Data + Clone,
{
    pub fn new(
        stream_id: usize,
        left: Arc<dyn DStream<(K, V)>>,
        right: Arc<dyn DStream<(K, W)>>,
        ssc: Arc<StreamingContext>,
    ) -> Self {
        LeftOuterJoinDStream { stream_id, left, right, ssc, generated: Mutex::new(HashMap::new()) }
    }
}

impl<K, V, W> DStreamBase for LeftOuterJoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    W: Data + Clone,
{
    fn slide_duration(&self) -> Duration { self.left.slide_duration() }
    fn id(&self) -> usize { self.stream_id }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![
            self.left.clone() as Arc<dyn DStreamBase>,
            self.right.clone() as Arc<dyn DStreamBase>,
        ]
    }
}

impl<K, V, W> DStream<(K, (V, Option<W>))> for LeftOuterJoinDStream<K, V, W>
where
    K: Data + Clone + Hash + Eq + std::fmt::Debug + 'static,
    V: Data + Clone + std::fmt::Debug + 'static,
    W: Data + Clone + std::fmt::Debug + 'static,
    Vec<(K, V)>: Data + Clone,
    Vec<(K, W)>: Data + Clone,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, (V, Option<W>))>>> {
        let left_rdd = self.left.get_or_compute(valid_time_ms)?;
        let right_rdd = self.right.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();
        let left_typed: TypedRdd<(K, V)> = TypedRdd::new(left_rdd, ctx.clone());
        let right_typed: TypedRdd<(K, W)> = TypedRdd::new(right_rdd, ctx);
        Some(left_typed.left_outer_join_local(right_typed).into_rdd())
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, (V, Option<W>))>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) { return Some(rdd.clone()); }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

// StateDStream — updateStateByKey

/// Stateful streaming: maintains a `(K, S)` state RDD across batches.
///
/// Each batch merges new `(K, V)` values into the existing state by calling
/// `update_fn(&new_values_for_k, current_state_for_k)`. Returning `None` evicts
/// the key from the state store.
pub struct StateDStream<K, V, S, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    F: Fn(&[V], Option<S>) -> Option<S> + Send + Sync + Clone + 'static,
{
    stream_id: usize,
    parent: Arc<dyn DStream<(K, V)>>,
    ssc: Arc<StreamingContext>,
    update_fn: Arc<F>,
    /// Current state RDD (grows/shrinks as keys are added/evicted).
    state_rdd: Mutex<Option<Arc<dyn Rdd<Item = (K, S)>>>>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, S)>>>>,
}

impl<K, V, S, F> StateDStream<K, V, S, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    F: Fn(&[V], Option<S>) -> Option<S> + Send + Sync + Clone + 'static,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<(K, V)>>,
        ssc: Arc<StreamingContext>,
        func: F,
    ) -> Self {
        StateDStream {
            stream_id, parent, ssc,
            update_fn: Arc::new(func),
            state_rdd: Mutex::new(None),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V, S, F> DStreamBase for StateDStream<K, V, S, F>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    F: Fn(&[V], Option<S>) -> Option<S> + Send + Sync + Clone + 'static,
{
    fn slide_duration(&self) -> Duration { self.parent.slide_duration() }
    fn id(&self) -> usize { self.stream_id }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![self.parent.clone() as Arc<dyn DStreamBase>]
    }
}

impl<K, V, S, F> DStream<(K, S)> for StateDStream<K, V, S, F>
where
    K: Data + Clone + Hash + Eq + std::fmt::Debug + 'static,
    V: Data + Clone + std::fmt::Debug + 'static,
    S: Data + Clone + std::fmt::Debug + 'static,
    F: Fn(&[V], Option<S>) -> Option<S> + Send + Sync + Clone + 'static,
    Vec<(K, V)>: Data + Clone,
    Vec<(K, S)>: Data + Clone,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, S)>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();
        let func = self.update_fn.clone();

        // Collect new batch values grouped by key.
        let new_pairs = ctx.run_job(parent_rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();
        let mut new_by_key: std::collections::HashMap<K, Vec<V>> = std::collections::HashMap::new();
        for partition in new_pairs {
            for (k, v) in partition {
                new_by_key.entry(k).or_default().push(v);
            }
        }

        // Merge with current state.
        let mut current_state: std::collections::HashMap<K, S> = {
            let state_guard = self.state_rdd.lock();
            if let Some(ref rdd) = *state_guard {
                ctx.run_job(rdd.clone(), |iter| iter.collect::<Vec<(K, S)>>())
                    .unwrap_or_default()
                    .into_iter()
                    .flatten()
                    .collect()
            } else {
                std::collections::HashMap::new()
            }
        };

        // Apply update function to all keys that appear in either new data or existing state.
        let mut all_keys: std::collections::HashSet<K> = new_by_key.keys().cloned().collect();
        all_keys.extend(current_state.keys().cloned());

        let mut new_state: std::collections::HashMap<K, S> = std::collections::HashMap::new();
        for k in all_keys {
            let new_vals = new_by_key.get(&k).map(Vec::as_slice).unwrap_or(&[]);
            let cur = current_state.remove(&k);
            if let Some(s) = func(new_vals, cur) {
                new_state.insert(k, s);
            }
        }

        let state_vec: Vec<(K, S)> = new_state.into_iter().collect();
        let id = ctx.new_rdd_id();
        let new_rdd: Arc<dyn Rdd<Item = (K, S)>> = Arc::new(
            atomic_compute::rdd::parallel_collection::ParallelCollection::new(
                id, state_vec.clone(), 1,
            )
        );

        // Persist the new state for the next batch.
        *self.state_rdd.lock() = Some(new_rdd.clone());

        Some(new_rdd)
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, S)>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) { return Some(rdd.clone()); }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}
