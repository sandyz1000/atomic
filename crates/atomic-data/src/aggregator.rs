use crate::data::Data;
use std::marker::PhantomData;
use std::sync::Arc;

/// Starts a combiner `C` from the first value `V` of a key.
pub type CreateCombinerFn<V, C> = Arc<dyn Fn(V) -> C + Send + Sync>;
/// Merges a new value `V` into an existing combiner `C` in place.
pub type MergeValueFn<V, C> = Arc<dyn Fn(&mut C, V) + Send + Sync>;
/// Merges another combiner `C` into an existing combiner `C` in place.
pub type MergeCombinersFn<C> = Arc<dyn Fn(&mut C, C) + Send + Sync>;

/// Aggregator for shuffle tasks.
/// Functions are wrapped in Arc to eliminate Clone requirement on C type.
/// Functions take owned values for create_combiner, but references for merging
/// to avoid unnecessary cloning.
pub struct Aggregator<K: Data, V: Data, C: Data> {
    pub create_combiner: CreateCombinerFn<V, C>,
    pub merge_value: MergeValueFn<V, C>,
    pub merge_combiners: MergeCombinersFn<C>,
    _marker: PhantomData<K>,
}

impl<K: Data, V: Data, C: Data> Aggregator<K, V, C> {
    pub fn new(
        create_combiner: CreateCombinerFn<V, C>,
        merge_value: MergeValueFn<V, C>,
        merge_combiners: MergeCombinersFn<C>,
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
