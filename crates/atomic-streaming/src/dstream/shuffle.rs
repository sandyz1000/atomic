/// ShuffledDStream — combines values across partitions within a batch.
///
/// TODO Phase 4: implement using atomic_compute shuffle/combine operations.
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
    create_combiner: Arc<dyn Fn(V) -> C + Send + Sync>,
    merge_value: Arc<dyn Fn(C, V) -> C + Send + Sync>,
    merge_combiners: Arc<dyn Fn(C, C) -> C + Send + Sync>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = (K, C)>>>>,
}

impl<K, V, C> ShuffledDStream<K, V, C>
where
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    C: Data + Clone,
{
    pub fn new(
        stream_id: usize,
        parent: Arc<dyn DStream<(K, V)>>,
        create_combiner: impl Fn(V) -> C + Send + Sync + 'static,
        merge_value: impl Fn(C, V) -> C + Send + Sync + 'static,
        merge_combiners: impl Fn(C, C) -> C + Send + Sync + 'static,
    ) -> Self {
        ShuffledDStream {
            stream_id,
            parent,
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
    K: Data + Clone + Hash + Eq,
    V: Data + Clone,
    C: Data + Clone,
{
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = (K, C)>>> {
        // TODO Phase 4: use atomic_compute combine_by_key
        unimplemented!("ShuffledDStream::compute — implement in Phase 4")
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
