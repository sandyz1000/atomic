use std::marker::PhantomData;
use std::sync::Arc;
use crate::data::{Data};

/// Aggregator for shuffle tasks.
/// Functions are wrapped in Arc to eliminate Clone requirement on C type.
/// Functions take owned values for create_combiner, but references for merging
/// to avoid unnecessary cloning.
pub struct Aggregator<K: Data, V: Data, C: Data> {
    pub create_combiner: Arc<dyn Fn(V) -> C + Send + Sync>,
    pub merge_value: Arc<dyn Fn(&mut C, V) + Send + Sync>,
    pub merge_combiners: Arc<dyn Fn(&mut C, C) + Send + Sync>,
    _marker: PhantomData<K>,
}

impl<K: Data, V: Data, C: Data> Aggregator<K, V, C> {
    pub fn new(
        create_combiner: Arc<dyn Fn(V) -> C + Send + Sync>,
        merge_value: Arc<dyn Fn(&mut C, V) + Send + Sync>,
        merge_combiners: Arc<dyn Fn(&mut C, C) + Send + Sync>,
    ) -> Self {
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
        }
    }
}

impl<K: Data, V: Data + Clone> Default for Aggregator<K, V, Vec<V>> {
    fn default() -> Self {
        let create_combiner = Arc::new(|v: V| vec![v]);
        let merge_value = Arc::new(|buf: &mut Vec<V>, v: V| {
            buf.push(v);
        });
        let merge_combiners = Arc::new(|b1: &mut Vec<V>, mut b2: Vec<V>| {
            b1.append(&mut b2);
        });

        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
        }
    }
}
