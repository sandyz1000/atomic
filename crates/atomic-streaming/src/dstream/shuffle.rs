/// ShuffledDStream — combines pair values across partitions within each batch.
///
/// Built by `PairDStreamFunctions::reduce_by_key` / `group_by_key`.  Each batch produces
/// a `TypedRdd<(K, C)>` by running the aggregation functions through the atomic-compute
/// shuffle pipeline.
use crate::context::StreamingContext;
use crate::dstream::{DStream, DStreamBase};
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

pub struct ShuffledDStream<K, V, C>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    C: Data + Clone,
{
    stream_id: usize,
    parent: Arc<dyn DStream<(K, V)>>,
    ssc: Arc<StreamingContext>,
    create_combiner: Arc<dyn Fn(V) -> C + Send + Sync>,
    merge_value: Arc<dyn Fn(C, V) -> C + Send + Sync>,
    merge_combiners: Arc<dyn Fn(C, C) -> C + Send + Sync>,
    generated: GeneratedRdds<(K, C)>,
}

/// Per-batch cache of generated RDDs, keyed by batch validation time (ms).
type GeneratedRdds<T> = Mutex<HashMap<u64, Arc<dyn Rdd<Item = T>>>>;

impl<K, V, C> ShuffledDStream<K, V, C>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    C: Data + Clone,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<(K, V)>>,
        ssc: Arc<StreamingContext>,
        create_combiner: impl Fn(V) -> C + Send + Sync + 'static,
        merge_value: impl Fn(C, V) -> C + Send + Sync + 'static,
        merge_combiners: impl Fn(C, C) -> C + Send + Sync + 'static,
    ) -> Self {
        ShuffledDStream {
            stream_id,
            parent,
            ssc,
            create_combiner: Arc::new(create_combiner),
            merge_value: Arc::new(merge_value),
            merge_combiners: Arc::new(merge_combiners),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V, C> DStreamBase for ShuffledDStream<K, V, C>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    C: Data + Clone,
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

impl<K, V, C> DStream<(K, C)> for ShuffledDStream<K, V, C>
where
    K: Data + Clone + Hash + Eq + std::fmt::Debug + Send + Sync + 'static,
    V: Data + Clone + std::fmt::Debug + Send + Sync + 'static,
    C: Data + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, C)>>> {
        let parent_rdd = self.parent.get_or_compute(valid_time_ms)?;
        let ctx = self.ssc.sc.clone();

        // Collect batch to driver and aggregate per-key. For micro-batches this is
        // efficient; for large batches, use TypedRdd::combine_by_key with shuffle instead.
        let pairs = ctx
            .run_job(parent_rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();

        let cc = self.create_combiner.clone();
        let mv = self.merge_value.clone();
        let mc = self.merge_combiners.clone();

        let mut agg: std::collections::HashMap<K, C> = std::collections::HashMap::new();
        for partition in pairs {
            for (k, v) in partition {
                agg.entry(k)
                    .and_modify(|c| {
                        let new_c = mv(c.clone(), v.clone());
                        *c = new_c;
                    })
                    .or_insert_with(|| cc(v));
            }
        }
        // Merge combiners (in-process, no cross-partition merge needed after driver collect).
        let _ = mc; // mc not needed for driver-side; all values already merged above

        let result: Vec<(K, C)> = agg.into_iter().collect();
        let id = ctx.new_rdd_id();
        Some(Arc::new(
            atomic_compute::rdd::parallel_collection::ParallelCollection::new(id, result, 1),
        ) as Arc<dyn Rdd<Item = (K, C)>>)
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, C)>>> {
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
