use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::{Context, RddContext};
use crate::error::Result;
use crate::rdd::co_grouped::CoGroupedRdd;
use crate::rdd::rdd_val::RddVals;
use crate::rdd::shuffled::ShuffledRdd;
use crate::rdd::*;
use ember_data::aggregator::Aggregator;
use ember_data::dependency::Dependency;
use ember_data::partitioner::Partitioner;
use ember_data::split::Split;

// Trait containing pair rdd methods. No need of implicit conversion like in Spark version.
pub trait PairRdd<K: Data + Eq + Hash, V: Data>: Rdd<Item = (K, V)> + Send + Sync {
    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, C)>> {
        Arc::new(ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(aggregator),
            partitioner,
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
        F: Fn((V, V)) -> V,
    {
        self.reduce_by_key_using_partitioner(Fn, Partitioner::hash::<K>(num_splits))
    }

    fn reduce_by_key_using_partitioner<F>(
        &self,
        Fn: F,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, V)>>
    where
        F: Fn((V, V)) -> V,
    {
        let create_combiner = Box::new(|v: V| v);
        let f_clone = Fn.clone();
        let merge_value = Box::new(move |(buf, v)| (f_clone)((buf, v)));
        let merge_combiners = Box::new(move |(b1, b2)| (Fn)((b1, b2)));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    fn map_values<U: Data, F: Fn(V) -> U + Clone>(&self, f: F) -> Arc<dyn Rdd<Item = (K, U)>>
    where
        F: Fn(V) -> U + Clone,
    {
        Arc::new(MappedValuesRdd::new(self.get_rdd(), f))
    }

    fn flat_map_values<U: Data, F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone>(
        &self,
        f: F,
    ) -> Arc<dyn Rdd<Item = (K, U)>>
    where
        F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
    {
        Arc::new(FlatMappedValuesRdd::new(self.get_rdd(), f))
    }

    fn join<W: Data>(
        &self,
        other: Arc<dyn Rdd<Item = (K, W)>>,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = (K, (V, W))>> {
        let f = |v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        };
        self.cogroup(other, Partitioner::hash::<K>(num_splits))
            .flat_map_values(Box::new(f))
    }

    fn cogroup<W: Data>(
        &self,
        other: Arc<dyn Rdd<Item = (K, W)>>,
        partitioner: Partitioner,
    ) -> Arc<dyn Rdd<Item = (K, (Vec<V>, Vec<W>))>> {
        let rdds: Vec<Arc<dyn RddBase>> = vec![
            Arc::from(self.get_rdd_base()),
            Arc::from(other.get_rdd_base()),
        ];
        let cg_rdd = CoGroupedRdd::<K>::new(rdds, partitioner);
        let f = |v: Vec<Vec<Box<dyn Data>>>| -> (Vec<V>, Vec<W>) {
            let mut count = 0;
            let mut vs: Vec<V> = Vec::new();
            let mut ws: Vec<W> = Vec::new();
            for v in v.into_iter() {
                if count >= 2 {
                    break;
                }
                if count == 0 {
                    for i in v {
                        vs.push(*(i.into_any().downcast::<V>().unwrap()))
                    }
                } else if count == 1 {
                    for i in v {
                        ws.push(*(i.into_any().downcast::<W>().unwrap()))
                    }
                }
                count += 1;
            }
            (vs, ws)
        };
        cg_rdd.map_values(Box::new(f))
    }

    fn partition_by_key(&self, partitioner: Partitioner) -> Arc<dyn Rdd<Item = V>> {
        // Guarantee the number of partitions by introducing a shuffle phase
        let shuffle_steep = ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(Aggregator::<K, V, _>::default()),
            partitioner,
        );
        // Flatten the results of the combined partitions
        let flattener = |grouped: (K, Vec<V>)| {
            let (_key, values) = grouped;
            let iter: Box<dyn Iterator<Item = _>> = Box::new(values.into_iter());
            iter
        };
        shuffle_steep.flat_map(flattener)
    }
}

// Implementing the PairRdd trait for all types which implements Rdd
impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for T where T: Rdd<Item = (K, V)> {}
impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for Arc<T> where T: Rdd<Item = (K, V)> {}

pub struct MappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: Fn(V) -> U + Clone,
{
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for MappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> U + Clone,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> MappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> U + Clone,
{
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MappedValuesRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K, V, U, F> RddContext for MappedValuesRdd<K, V, U, F>
where
    K: Data + Clone,
    V: Data + Clone,
    U: Data + Clone,
    F: Fn(V) -> U,
{
    fn get_context(&self) -> Arc<Context> {
        self.vals.get_context()
    }
}

impl<K: Data, V: Data, U: Data, F> RddBase for MappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> U,
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside cogroup_iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v)))),
        ))
    }
}

impl<K: Data + Clone, V: Data, U: Data + Clone, F> Rdd for MappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> U,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
    }
}

pub struct FlatMappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for FlatMappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> FlatMappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        FlatMappedValuesRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_v: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<K: Data, V: Data, U: Data, F> RddBase for FlatMappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>>,
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|(k, v)| Box::new((k, v))),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v)))),
        ))
    }
}

impl<K: Data + Clone, V: Data, U: Data + Clone, F> Rdd for FlatMappedValuesRdd<K, V, U, F>
where
    F: Fn(V) -> Box<dyn Iterator<Item = U>>,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev
                .iterator(split)?
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        ))
    }
}
