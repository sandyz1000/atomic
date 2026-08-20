//! Distributed `mapWithState` — per-key state sharded across the cluster via the
//! `MergeState` mechanism, mirroring `atomic_structured::distributed_state` (duplicated
//! rather than shared: this crate does not depend on atomic-structured, and the two
//! streaming layers are architecturally separate — see the crate docs).
//!
//! Atomic never ships raw closures to workers (`#[task]` is the only way to register
//! distributed work), so `mapWithState`'s per-key mapping logic can't be an arbitrary
//! `Fn` closure the way it would be in a driver-only implementation. It must be a
//! compile-time registered [`MapWithStateTask`], dispatched by name on the worker that
//! holds the key's shard — register one with [`register_map_with_state`].

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_compute::rdd::ParallelCollection;
use atomic_data::data::Data;
use atomic_data::distributed::{
    EngineAction, StateMergePayload, Step, StepKind, TaskRuntime, WireEncode, WireSerde,
    decode_payload,
};
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;

use crate::context::StreamingContext;
use crate::dstream::{DStream, DStreamBase};

// StateSpecImpl

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

impl<K, V, S, M> StateSpecImpl<K, V, S, M>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    M: Data + Clone,
{
    /// Seed initial per-key state, consumed on the first batch that runs.
    pub fn initial_state(mut self, rdd: Arc<dyn Rdd<Item = (K, S)>>) -> Self {
        self.initial_state_rdd = Some(rdd);
        self
    }

    /// Number of shards to distribute per-key state across. Default 1.
    pub fn num_partitions(mut self, n: usize) -> Self {
        self.num_partitions = Some(n);
        self
    }

    /// Evict a key once it has gone this long without new values.
    pub fn timeout(mut self, idle: Duration) -> Self {
        self.timeout = Some(idle);
        self
    }
}

// MapWithStateTask — the registered, per-key state-transition function

/// Per-key state-transition logic for `mapWithState`, registered at compile time and
/// dispatched by name on the worker holding the key's shard.
///
/// `call(&key, &new_values, current_state)` returns `(output_for_this_batch, new_state)`;
/// `None` for the new state evicts the key.
pub trait MapWithStateTask<K, V, S, M>: Send + Sync + Default + 'static {
    const NAME: &'static str;
    fn call(&self, key: &K, values: &[V], state: Option<S>) -> (Option<M>, Option<S>);
}

/// Register a [`MapWithStateTask`] so `PairDStreamFunctions::map_with_state` can dispatch
/// it to distributed workers. Call once, anywhere linked into the binary — same
/// `inventory::submit!` mechanism as `#[task]`.
#[macro_export]
macro_rules! register_map_with_state {
    ($task:ty, $k:ty, $v:ty, $s:ty, $m:ty) => {
        const _: () = {
            fn __map_with_state_merge(
                prev: ::std::option::Option<&[u8]>,
                partials: &[u8],
                _params: &[u8],
            ) -> ::std::result::Result<
                (::std::vec::Vec<u8>, ::std::vec::Vec<u8>),
                ::std::string::String,
            > {
                $crate::dstream::map_with_state::merge_impl::<$task, $k, $v, $s, $m>(prev, partials)
            }
            ::atomic_compute::register_state_merge!(
                <$task as $crate::dstream::map_with_state::MapWithStateTask<$k, $v, $s, $m>>::NAME,
                __map_with_state_merge
            );
        };
    };
}

/// One shard's routed input for a batch: `(valid_time_ms, timeout_ms, new values per key,
/// initial-state rows this shard owns)`. The last field is non-empty only on the batch
/// that consumes `StateSpecImpl::initial_state`.
type ShardBatch<K, V, S> = (u64, Option<u64>, Vec<(K, Vec<V>)>, Vec<(K, S)>);

/// Seed RDD held until the first batch consumes it via `Mutex::take`.
type SeedRdd<K, S> = Mutex<Option<Arc<dyn Rdd<Item = (K, S)>>>>;

/// The registered merge function's body, generic over the user's task type. Runs
/// worker-side: decode this shard's persisted state and this batch's routed input, apply
/// [`MapWithStateTask::call`] per key, persist the new state, return the emitted outputs.
pub fn merge_impl<T, K, V, S, M>(
    prev: Option<&[u8]>,
    partials: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), String>
where
    T: MapWithStateTask<K, V, S, M>,
    K: WireSerde + Clone + Eq + Hash,
    V: WireSerde + Clone,
    S: WireSerde + Clone,
    M: WireSerde,
{
    let mut state: HashMap<K, (S, u64)> = match prev {
        Some(bytes) => {
            let rows: Vec<(K, S, u64)> = decode_payload(bytes).map_err(|e| e.to_string())?;
            rows.into_iter().map(|(k, s, t)| (k, (s, t))).collect()
        }
        None => HashMap::new(),
    };

    let (valid_time_ms, timeout_ms, new_by_key, seed): ShardBatch<K, V, S> =
        decode_payload(partials).map_err(|e| e.to_string())?;

    // Initial state seeds keys not already carried from a prior batch; it never
    // overwrites live state (only relevant the one batch it's shipped on anyway).
    for (k, s) in seed {
        state.entry(k).or_insert((s, valid_time_ms));
    }

    let task = T::default();
    let mut outputs: Vec<M> = Vec::new();
    for (k, vals) in new_by_key {
        let cur = state.remove(&k).map(|(s, _)| s);
        let (out, new_s) = task.call(&k, &vals, cur);
        if let Some(m) = out {
            outputs.push(m);
        }
        if let Some(s) = new_s {
            state.insert(k, (s, valid_time_ms));
        }
    }

    if let Some(idle_ms) = timeout_ms {
        state.retain(|_, (_, last)| valid_time_ms.saturating_sub(*last) <= idle_ms);
    }

    let state_rows: Vec<(K, S, u64)> = state.into_iter().map(|(k, (s, t))| (k, s, t)).collect();
    let new_state_bytes = state_rows.encode_wire().map_err(|e| e.to_string())?;
    let emitted_bytes = outputs.encode_wire().map_err(|e| e.to_string())?;
    Ok((new_state_bytes, emitted_bytes))
}

/// Stable shard assignment for a key (FNV-1a over its rkyv encoding — deterministic
/// across driver and workers, unlike `Hash`). Duplicated from
/// `atomic_structured::distributed_state::shard_of` (crate-local, see module docs).
fn shard_of<T: WireEncode>(key: &T, num_shards: u32) -> u32 {
    let bytes = key.encode_wire().unwrap_or_default();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in &bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % num_shards as u64) as u32
}

/// Dispatch one `MergeState` task per shard and collect the emitted outputs in shard
/// order. Duplicated from `atomic_structured::distributed_state::dispatch_merge_state`
/// (see module docs); the same `dispatch_pipeline` path runs it in-process in local mode
/// and on workers when distributed.
fn dispatch_merge_state<K, V, S, M>(
    sc: &Context,
    merge_fn: &str,
    state_id_base: u64,
    checkpoint_dir: &Option<String>,
    buckets: Vec<ShardBatch<K, V, S>>,
) -> Result<Vec<M>, String>
where
    K: WireSerde,
    V: WireSerde,
    S: WireSerde,
    M: WireSerde,
{
    let mut source_partitions: Vec<Vec<u8>> = Vec::with_capacity(buckets.len());
    for (shard, bucket) in buckets.into_iter().enumerate() {
        let partials = bucket.encode_wire().map_err(|e| e.to_string())?;
        let payload = StateMergePayload {
            state_id: state_id_base + shard as u64,
            params: Vec::new(),
            partials,
            checkpoint_dir: checkpoint_dir.clone(),
        };
        source_partitions.push(payload.encode_wire().map_err(|e| e.to_string())?);
    }

    let steps = vec![Step {
        task_name: String::new(),
        kind: StepKind::Engine(EngineAction::MergeState {
            merge_fn: merge_fn.to_string(),
        }),
        runtime: TaskRuntime::Native,
        payload: vec![],
    }];

    let placeholder_rdd: Arc<dyn Rdd<Item = ()>> =
        Arc::new(ParallelCollection::new(sc.new_rdd_id(), Vec::new(), 1));
    let results = sc
        .dispatch_pipeline(placeholder_rdd, source_partitions, steps)
        .map_err(|e| format!("mapWithState merge ({merge_fn}): {e}"))?;

    let mut emitted: Vec<M> = Vec::new();
    for bytes in results {
        let items: Vec<M> = decode_payload(&bytes).map_err(|e| e.to_string())?;
        emitted.extend(items);
    }
    Ok(emitted)
}

// MapWithStateDStream

/// Stateful streaming that emits only the mapped output records per batch (not the full
/// state), with optional idle-key timeout and initial-state seeding. Per-key state is
/// sharded across `spec.num_partitions()` shards (default 1) and persists worker-side in
/// `WORKER_STATE_STORE`, merged each batch by the registered `T`.
pub struct MapWithStateDStream<K, V, S, M, T>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    M: Data + Clone,
    T: MapWithStateTask<K, V, S, M>,
{
    stream_id: usize,
    parent: Arc<dyn DStream<(K, V)>>,
    ssc: Arc<StreamingContext>,
    num_shards: u32,
    state_id_base: u64,
    checkpoint_dir: Option<String>,
    timeout_ms: Option<u64>,
    /// Taken (once) on the first batch that runs; empty on every batch after.
    initial_state_rdd: SeedRdd<K, S>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = M>>>>,
    _task: std::marker::PhantomData<T>,
}

impl<K, V, S, M, T> MapWithStateDStream<K, V, S, M, T>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    M: Data + Clone,
    T: MapWithStateTask<K, V, S, M>,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<(K, V)>>,
        ssc: Arc<StreamingContext>,
        spec: StateSpecImpl<K, V, S, M>,
    ) -> Self {
        // Snapshot the checkpoint dir at construction time (before the streaming context
        // starts); each mapWithState DStream gets its own subdirectory so co-located
        // stateful streams don't collide on shard-checkpoint filenames.
        let checkpoint_dir = ssc.checkpoint_dir.lock().as_ref().map(|dir| {
            dir.join(format!("map_with_state_{stream_id}"))
                .to_string_lossy()
                .into_owned()
        });
        MapWithStateDStream {
            stream_id,
            parent,
            ssc,
            num_shards: spec.num_partitions.unwrap_or(1).max(1) as u32,
            state_id_base: (stream_id as u64) << 32,
            checkpoint_dir,
            timeout_ms: spec.timeout.map(|d| d.as_millis() as u64),
            initial_state_rdd: Mutex::new(spec.initial_state_rdd),
            generated: Mutex::new(HashMap::new()),
            _task: std::marker::PhantomData,
        }
    }
}

impl<K, V, S, M, T> DStreamBase for MapWithStateDStream<K, V, S, M, T>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    S: Data + Clone,
    M: Data + Clone,
    T: MapWithStateTask<K, V, S, M>,
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

impl<K, V, S, M, T> DStream<M> for MapWithStateDStream<K, V, S, M, T>
where
    K: Data + Clone + Hash + Eq + WireSerde + 'static,
    V: Data + Clone + WireSerde + 'static,
    S: Data + Clone + WireSerde + 'static,
    M: Data + Clone + WireSerde + 'static,
    T: MapWithStateTask<K, V, S, M>,
    Vec<(K, V)>: Data + Clone,
    Vec<(K, S)>: Data + Clone,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = M>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();

        let mut new_by_key: HashMap<K, Vec<V>> = HashMap::new();
        for partition in ctx
            .run_job(parent_rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default()
        {
            for (k, v) in partition {
                new_by_key.entry(k).or_default().push(v);
            }
        }

        // `take()` fires the seed exactly once, on whichever batch happens to run first.
        let seed_rows: Vec<(K, S)> = self
            .initial_state_rdd
            .lock()
            .take()
            .map(|rdd| {
                ctx.run_job(rdd, |iter| iter.collect::<Vec<(K, S)>>())
                    .unwrap_or_default()
                    .into_iter()
                    .flatten()
                    .collect()
            })
            .unwrap_or_default();

        let mut by_shard: Vec<ShardBatch<K, V, S>> = (0..self.num_shards)
            .map(|_| (valid_time_ms, self.timeout_ms, Vec::new(), Vec::new()))
            .collect();
        for (k, vals) in new_by_key {
            let shard = shard_of(&k, self.num_shards) as usize;
            by_shard[shard].2.push((k, vals));
        }
        for (k, s) in seed_rows {
            let shard = shard_of(&k, self.num_shards) as usize;
            by_shard[shard].3.push((k, s));
        }

        let outputs = match dispatch_merge_state::<K, V, S, M>(
            &ctx,
            T::NAME,
            self.state_id_base,
            &self.checkpoint_dir,
            by_shard,
        ) {
            Ok(outputs) => outputs,
            Err(e) => {
                log::error!("mapWithState '{}': {e}", T::NAME);
                return None;
            }
        };

        let id = ctx.new_rdd_id();
        Some(Arc::new(ParallelCollection::new(id, outputs, 1)))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = M>>> {
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
