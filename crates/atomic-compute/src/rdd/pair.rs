use crate::rdd::rdd_val::RddVals;
use crate::rdd::*;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::fn_traits::{RddFlatMapFn, RddFn};
use atomic_data::split::Split;
use std::marker::PhantomData;
use std::sync::Arc;

// DEPRECATED: PairRdd trait is superseded by TypedRdd<(K,V)> methods
// TypedRdd provides the same functionality with proper context handling
// This trait is commented out as it relied on self.get_context() which is removed in Option 5
/*
// Trait containing pair rdd methods. No need of implicit conversion like in Spark version.
pub trait PairRdd<
    K: Data + Eq + Hash + Clone + Encode + bincode::Decode<()>,
    V: Data + Clone + Encode + bincode::Decode<()>,
>: Rdd<Item = (K, V)> + Send + Sync
{
    fn combine_by_key<C: Data + Clone + Encode + bincode::Decode<()>>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, C)>> {
        let context = self.get_context();
        let fetcher = context.shuffle_fetcher.clone();
        Arc::new(ShuffledRdd::new_with_context(
            context,
            self.get_rdd(),
            Arc::new(aggregator),
            partitioner,
            fetcher,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> Arc<dyn Rdd<Item = (K, Vec<V>)>> {
        self.group_by_key_using_partitioner(Partitioner::hash::<K>(num_splits))
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, Vec<V>)>> {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }

    fn reduce_by_key<F>(&self, Fn: F, num_splits: usize) -> Arc<dyn Rdd<Item = (K, V)>>
    where
        F: Fn((V, V)) -> V + Send + Sync + Clone + 'static,
    {
        self.reduce_by_key_using_partitioner(Fn, Partitioner::hash::<K>(num_splits))
    }

    fn reduce_by_key_using_partitioner<F>(
        &self,
        Fn: F,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, V)>>
    where
        F: Fn((V, V)) -> V + Send + Sync + Clone + 'static,
    {
        let create_combiner = Arc::new(|v: V| v) as Arc<dyn Fn(V) -> V + Send + Sync>;
        let f_clone = Fn.clone();
        let merge_value = Arc::new(move |buf: &mut V, v: V| {
            *buf = (f_clone)(((*buf).clone(), v));
        }) as Arc<dyn Fn(&mut V, V) + Send + Sync>;
        let merge_combiners = Arc::new(move |b1: &mut V, b2: V| {
            *b1 = (Fn)(((*b1).clone(), b2));
        }) as Arc<dyn Fn(&mut V, V) + Send + Sync>;
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    fn map_values<U: Data + Clone, F: RddFn<V, U> + Clone>(
        &self,
        f: F,
    ) -> Arc<dyn Rdd<Item = (K, U)>>
    where
        F: RddFn<V, U> + Clone,
    {
        Arc::new(MappedValuesRdd::new_with_context(
            self.get_context(),
            self.get_rdd(),
            f,
        ))
    }

    fn flat_map_values<U, F>(&self, f: F) -> Arc<dyn Rdd<Item = (K, U)>>
    where
        U: Data + Clone,
        F: RddFlatMapFn<V, U> + Clone,
    {
        Arc::new(FlatMappedValuesRdd::new_with_context(
            self.get_context(),
            self.get_rdd(),
            f,
        ))
    }

    fn join<W: Data + Clone>(
        &self,
        other: Arc<dyn Rdd<Item = (K, W)>>,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = (K, (V, W))>> {
        let cogroup_rdd = self.cogroup(other, Partitioner::hash::<K>(num_splits));
        let f = |(k, (vs, ws)): (K, (Vec<V>, Vec<W>))| {
            let combine = vs.into_iter().flat_map(move |v| {
                ws.clone()
                    .into_iter()
                    .map(move |w| (k.clone(), (v.clone(), w)))
            });
            Box::new(combine) as Box<dyn Iterator<Item = (K, (V, W))>>
        };
        Arc::new(FlatMapperRdd::new_with_context(
            self.get_context(),
            cogroup_rdd,
            f,
        ))
    }

    fn cogroup<W: Data + Clone>(
        &self,
        other: Arc<dyn Rdd<Item = (K, W)>>,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, (Vec<V>, Vec<W>))>> {
        let rdds: Vec<Arc<dyn RddBase>> = vec![
            Arc::from(self.get_rdd_base()),
            Arc::from(other.get_rdd_base()),
        ];
        let cg_rdd = CoGroupedRdd::<K>::new(self.get_context(), rdds, partitioner);
        let f = |item: (K, Vec<Vec<Arc<dyn Data>>>)| {
            let (k, vecs) = item;
            let mut vs: Vec<V> = Vec::new();
            let mut ws: Vec<W> = Vec::new();

            for (idx, vec) in vecs.into_iter().enumerate() {
                if idx >= 2 {
                    break;
                }
                if idx == 0 {
                    for arc_data in vec {
                        // CoGroupedRdd uses Arc<dyn Data>, so downcast Arc
                        if let Ok(v) = Arc::downcast::<V>(arc_data) {
                            vs.push(Arc::try_unwrap(v).unwrap_or_else(|arc| (*arc).clone()));
                        }
                    }
                } else if idx == 1 {
                    for arc_data in vec {
                        if let Ok(w) = Arc::downcast::<W>(arc_data) {
                            ws.push(Arc::try_unwrap(w).unwrap_or_else(|arc| (*arc).clone()));
                        }
                    }
                }
            }
            (k, (vs, ws))
        };
        Arc::new(MapperRdd::new_with_context(
            self.get_context(),
            Arc::new(cg_rdd),
            f,
        ))
    }

    fn partition_by_key(&self, partitioner: Partitioner) -> Arc<dyn Rdd<Item = V>> {
        // Guarantee the number of partitions by introducing a shuffle phase
        let context = self.get_context();
        let fetcher = context.shuffle_fetcher.clone();
        let shuffle_steep = ShuffledRdd::new_with_context(
            context.clone(),
            self.get_rdd(),
            Arc::new(Aggregator::<K, V, _>::default()),
            partitioner,
            fetcher,
        );
        // Flatten the results of the combined partitions
        let f = |grouped: (K, Vec<V>)| {
            let (_key, values) = grouped;
            Box::new(values.into_iter()) as Box<dyn Iterator<Item = V>>
        };
        Arc::new(FlatMapperRdd::new_with_context(
            self.get_context(),
            Arc::new(shuffle_steep),
            f,
        ))
    }
}
*/ // End of deprecated PairRdd trait

// Implementing the PairRdd trait for all types which implements Rdd
// impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for Arc<T> where T: Rdd<Item = (K, V)> {}

pub struct MappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: RddFn<V, U>,
{
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    _marker: PhantomData<(K, V, U)>,
}

impl<K: Data, V: Data, U: Data, F> Clone for MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data, F> MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    pub(crate) fn new(id: usize, prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        MappedValuesRdd {
            prev,
            vals,
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F> RddBase for MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }
    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside cogroup_iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>),
        ))
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F: 'static> Rdd
    for MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
    }
}

pub struct FlatMappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: RddFlatMapFn<V, U>,
{
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    _marker: PhantomData<(K, V, U)>,
}

impl<K: Data, V: Data, U: Data, F> Clone for FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data, F> FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    pub(crate) fn new(id: usize, prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        FlatMappedValuesRdd {
            prev,
            vals,
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F> RddBase
    for FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }
    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>),
        ))
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F> Rdd for FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    type Item = (K, U);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev
                .iterator(split)?
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        ))
    }
}
