// use serde_closure::{traits::Fn, Fn};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc as SerArc;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::rdd::co_grouped_rdd::CoGroupedRdd;
use crate::rdd::shuffled_rdd::ShuffledRdd;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use core::ops::Fn as SerFunc;
use crate::split::Split;
// use erased_serde::Serialize;
use serde_derive::{Deserialize, Serialize};

// Trait containing pair rdd methods. No need of implicit conversion like in version.
// #[typetag::serde(tag = "type")]
// pub trait PairRdd<K: Data + Eq + Hash, V: Data>: Rdd<Item = (K, V)> + Send + Sync + Sized + 'static  {

pub trait PairRdd<'de, K: Data + Eq + Hash, V: Data>: 
    Rdd<Item = (K, V)> + Send + Sync + Sized + 'static + serde::Serialize + serde::de::Deserialize<'de> {

    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
    ) -> std::sync::Arc<dyn Rdd<Item = (K, C)>> {
        std::sync::Arc::new(ShuffledRdd::new(
            self.get_rdd(),
            std::sync::Arc::new(aggregator),
            partitioner,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn Rdd<Item = (K, Vec<V>)>> {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, Vec<V>)>> {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }

    /// TODO: Remove SerArc from all method return type
    /// ReVisit and check if SerArc<dyn Rdd<Item = (K, V)>> is the right return type
    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> SerArc<dyn Rdd<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
    }

    fn reduce_by_key_using_partitioner<F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V
    {
        let create_combiner = Box::new(serde_closure::Fn!(|v: V| v));
        let f_clone = func.clone();
        let merge_value = Box::new(serde_closure::Fn!(move |(buf, v)| { (f_clone)((buf, v)) }));
        let merge_combiners = Box::new(serde_closure::Fn!(move |(b1, b2)| { (func)((b1, b2)) }));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    fn map_values<U: Data, F: SerFunc(V) -> U + Clone>(
        &self,
        f: F,
    ) -> SerArc<dyn Rdd<Item = (K, U)>>
    where
        F: SerFunc(V) -> U + Clone,
        Self: Sized,
    {
        SerArc::new(MappedValuesRdd::new(self.get_rdd(), f))
    }

    fn flat_map_values<U: Data, F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone>(
        &self,
        f: F,
    ) -> SerArc<dyn Rdd<Item = (K, U)>>
    where
        F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
        Self: Sized,
    {
        SerArc::new(FlatMappedValuesRdd::new(self.get_rdd(), f))
    }

    fn join<W: Data>(
        &self,
        other: SerArc<dyn Rdd<Item = (K, W)>>,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = (K, (V, W))>> {
        let f = serde_closure::Fn!(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });
        self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Box::new(f))
    }

    fn cogroup<W: Data>(
        &self,
        other: SerArc<dyn Rdd<Item = (K, W)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, (Vec<V>, Vec<W>))>> {
        let rdds: Vec<SerArc<dyn RddBase>> = vec![
            SerArc::from(self.get_rdd_base()),
            SerArc::from(other.get_rdd_base()),
        ];
        let cg_rdd: CoGroupedRdd<K> = CoGroupedRdd::<K>::new(rdds, partitioner);
        let f = serde_closure::Fn!(|v: Vec<Vec<Box<dyn AnyData>>>| -> (Vec<V>, Vec<W>) {
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
        });
        cg_rdd.map_values(Box::new(f))
    }

    fn partition_by_key(&self, partitioner: Box<dyn Partitioner>) -> SerArc<dyn Rdd<Item = V>> {
        // Guarantee the number of partitions by introducing a shuffle phase
        let shuffle_steep = ShuffledRdd::new(
            self.get_rdd(),
            SerArc::new(Aggregator::<K, V, _>::default()),
            partitioner,
        );
        // Flatten the results of the combined partitions
        let flattener = serde_closure::Fn!(|grouped: (K, Vec<V>)| {
            let (_key, values) = grouped;
            let iter: Box<dyn Iterator<Item = _>> = Box::new(values.into_iter());
            iter
        });
        shuffle_steep.flat_map(flattener)
    }
}

// Implementing the PairRdd trait for all types which implements Rdd
impl<'de, K: Data + Eq + Hash, V: Data, T> PairRdd<'de, K, V> for T where T: Rdd<Item = (K, V)> {

}

// impl<'de, K: Data + Eq + Hash, V: Data, T> PairRdd<'de, K, V> for SerArc<T> where T: Rdd<Item = (K, V)> {
// }

// #[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<K: Data, V: Data, U: Data, F> {
    prev: SerArc<dyn Rdd<Item = (K, V)>>,
    vals: SerArc<RddVals>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for MappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> U + Clone,
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
    F: SerFunc(V) -> U + Clone,
{
    fn new(prev: SerArc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(SerArc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = SerArc::new(vals);
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

impl<K: Data, V: Data, U: Data, F> RddBase for MappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> U,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_context(&self) -> SerArc<Context> {
        self.vals.context.upgrade().unwrap()
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside cogroup_iterator_any mapvaluesrdd");
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K: Data, V: Data, U: Data, F> Rdd for MappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> U,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> SerArc<dyn RddBase> {
        SerArc::new(self.clone()) as SerArc<dyn RddBase>
    }
    fn get_rdd(&self) -> SerArc<dyn Rdd<Item = Self::Item>> {
        SerArc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
    }
}

// #[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<K: Data, V: Data, U: Data, F> {
    prev: SerArc<dyn Rdd<Item = (K, V)>>,
    vals: SerArc<RddVals>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F> Clone for FlatMappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
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
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn new(prev: SerArc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(SerArc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = SerArc::new(vals);
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
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    
    fn get_context(&self) -> SerArc<Context> {
        self.vals.context.upgrade().unwrap()
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }
    
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K: Data, V: Data, U: Data, F> Rdd for FlatMappedValuesRdd<K, V, U, F>
where
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> SerArc<dyn RddBase> {
        SerArc::new(self.clone()) as SerArc<dyn RddBase>
    }
    fn get_rdd(&self) -> SerArc<dyn Rdd<Item = Self::Item>> {
        SerArc::new(self.clone())
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
