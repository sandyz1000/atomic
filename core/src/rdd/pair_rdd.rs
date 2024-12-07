// use serde_closure::{traits::Fn, Fn};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait,
};
use crate::error::Result;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::rdd::co_grouped_rdd::CoGroupedRdd;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::rdd::shuffled_rdd::ShuffledRdd;
use crate::ser_data::{AnyData, Data, SerFunc};
use crate::split::Split;

pub trait PairRdd<'de, K: Data + Eq + Hash, V: Data>:
    Rdd<Item = (K, V)> + Send + Sync + Sized + 'static + serde::Serialize + serde::de::Deserialize<'de>
{
    fn combine_by_key<C: Data, P: Partitioner, F1, F2, F3>(
        &self,
        aggregator: Aggregator<K, V, C, F1, F2, F3>,
        partitioner: Box<P>,
    ) -> Arc<impl Rdd<Item = (K, C)>>
    where
        F1: SerFunc<V, Output = C>,
        F2: SerFunc<(C, V), Output = C>,
        F3: SerFunc<(C, C), Output = C>,
    {
        Arc::new(ShuffledRdd::new(
            self.get_rdd(),
            std::sync::Arc::new(aggregator),
            partitioner,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> Arc<impl Rdd<Item = (K, Vec<V>)>> {
        self.group_by_key_using_partitioner(Box::new(HashPartitioner::<K>::new(num_splits)))
    }

    fn group_by_key_using_partitioner<P: Partitioner, C, F1, F2, F3>(
        &self,
        partitioner: Box<P>,
    ) -> Arc<impl Rdd<Item = (K, Vec<V>)>>
    where
        F1: SerFunc<V, Output = C>,
        F2: SerFunc<(C, V), Output = C>,
        F3: SerFunc<(C, C), Output = C>,
    {
        self.combine_by_key(Aggregator::<K, V, C, F1, F2, F3>::default(), partitioner)
    }

    /// TODO: Remove SerArc from all method return type
    /// ReVisit and check if SerArc<dyn Rdd<Item = (K, V)>> is the right return type
    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> Arc<impl Rdd<Item = (K, V)>>
    where
        F: SerFunc<(V, V), Output = V>,
    {
        self.reduce_by_key_using_partitioner(func, Box::new(HashPartitioner::<K>::new(num_splits)))
    }

    fn reduce_by_key_using_partitioner<F, P>(
        &self,
        func: F,
        partitioner: Box<P>,
    ) -> Arc<impl Rdd<Item = (K, V)>>
    where
        P: Partitioner,
        F: SerFunc<(V, V), Output = V>,
    {
        let create_combiner = Box::new(serde_closure::Fn!(|v: V| v));
        let f_clone = func.clone();
        let merge_value = Box::new(serde_closure::Fn!(move |(buf, v)| { (f_clone)((buf, v)) }));
        let merge_combiners = Box::new(serde_closure::Fn!(move |(b1, b2)| { (func)((b1, b2)) }));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    fn map_values<U: Data, F: SerFunc<V, Output = U>>(&self, f: F) -> Arc<impl Rdd<Item = (K, U)>>
    where
        F: SerFunc<V, Output = U>,
        Self: Sized,
    {
        Arc::new(MappedValuesRdd::new(self.get_rdd(), f))
    }

    fn flat_map_values<U, F>(&self, f: F) -> Arc<impl Rdd<Item = (K, U)>>
    where
        U: Data,
        F: SerFunc<V, Output = Box<dyn Iterator<Item = U>>>,
        Self: Sized,
    {
        Arc::new(FlatMappedValuesRdd::new(self.get_rdd(), f))
    }

    fn join<W: Data, RDD: Rdd<Item = (K, W)>>(
        &self,
        other: Arc<RDD>,
        num_splits: usize,
    ) -> Arc<impl Rdd<Item = (K, (V, W))>> {
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

    fn cogroup<W: Data, RDD: Rdd<Item = (K, W)>, P: Partitioner>(
        &self,
        other: Arc<RDD>,
        partitioner: Box<P>,
    ) -> Arc<impl Rdd<Item = (K, (Vec<V>, Vec<W>))>> {
        let rdds = vec![
            Arc::from(self.get_rdd_base()),
            Arc::from(other.get_rdd_base()),
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

    fn partition_by_key(&self, partitioner: Box<dyn Partitioner>) -> Arc<impl Rdd<Item = V>> {
        // Guarantee the number of partitions by introducing a shuffle phase
        let shuffle_steep = ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(Aggregator::<K, V, _>::default()),
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
// impl<'de, K: Data + Eq + Hash, V: Data, T> PairRdd<'de, K, V> for T where T: Rdd<Item = (K, V)> {

// }

// impl<'de, K: Data + Eq + Hash, V: Data, T> PairRdd<'de, K, V> for SerArc<T> where T: Rdd<Item = (K, V)> {
// }

#[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<K: Data, V: Data, U: Data, F, RDD, ND, SD> {
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K: Data, V: Data, U: Data, F, RDD, ND, SD> Clone for MappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    F: SerFunc<V, Output = U>,
    RDD: Rdd<Item = (K, V)>,
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

impl<K: Data, V: Data, U: Data, F, RDD, ND, SD> MappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    F: SerFunc<V, Output = U>,
    RDD: Rdd<Item = (K, V)>,
{
    fn new(prev: Arc<RDD>, f: F) -> Self {
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

impl<K, V, U, F, RDD, Nd, Sd> RddBase for MappedValuesRdd<K, V, U, F, RDD, Nd, Sd>
where
    F: SerFunc<V, Output = U>,
    U: Data,
    K: Data,
    V: Data,
    RDD: Rdd<Item = (K, V)>,
    Nd: NarrowDependencyTrait,
    Sd: ShuffleDependencyTrait,
{
    type Split = RDD::Split;
    type Partitioner = RDD::Partitioner;
    type ShuffleDeps = Sd;
    type NarrowDeps = Nd;

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    fn get_dependencies(&self) -> Vec<Dependency<Self::NarrowDeps, Self::ShuffleDeps>> {
        self.vals.dependencies.clone()
    }
    fn splits(&self) -> Vec<Box<Self::Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }
    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?.map(|(k, v)| Box::new((k, v))),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside cogroup_iterator_any mapvaluesrdd");
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v)))
        })))
    }
    
}

impl<K, V, U, F, RDD, ND, SD> Rdd for MappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    F: SerFunc<V, Output = U>,
    U: Data,
    K: Data,
    V: Data,
    RDD: Rdd<Item = (K, V)>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static,
{
    type Item = (K, U);
    type RddBase = RDD::RddBase;
    type Split = RDD::Split;

    fn get_rdd_base(&self) -> Arc<Self::RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<Self::Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
    }
}

#[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<K: Data, V: Data, U: Data, F, RDD, ND, SD> {
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    f: F,
    _marker_t: PhantomData<K>, // phantom data is necessary because of type parameter T
    _marker_v: PhantomData<V>,
    _marker_u: PhantomData<U>,
}

impl<K, V, U, F, RDD, ND, SD> Clone for FlatMappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    RDD: Rdd<Item = (K, V)>,
    F: SerFunc<V, Output = Box<dyn Iterator<Item = U>>>,
    U: Data,
    K: Data,
    V: Data,
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

impl<K, V, U, F, RDD, ND, SD> FlatMappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    F: SerFunc<V, Output = Box<dyn Iterator<Item = U>>>,
    U: Data,
    K: Data,
    V: Data,
    RDD: Rdd<Item = (K, V)>,
{
    fn new(prev: Arc<RDD>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
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

impl<K, V, U, F, RDD, ND, SD> RddBase for FlatMappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    F: SerFunc<V, Output = Box<dyn Iterator<Item = U>>>,
    U: Data,
    K: Data,
    V: Data,
    RDD: Rdd<Item = (K, V)>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static,
{
    type Split = RDD::Split;
    type Partitioner = RDD::Partitioner;
    type ShuffleDeps = RDD::ShuffleDeps;
    type NarrowDeps = RDD::NarrowDeps;

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency<Self::NarrowDeps, Self::ShuffleDeps>> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<Self::Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>))
        })))
    }
}

impl<K, V, U, F, RDD, ND, SD> Rdd for FlatMappedValuesRdd<K, V, U, F, RDD, ND, SD>
where
    F: SerFunc<V, Output = Box<dyn Iterator<Item = U>>>,
    U: Data,
    K: Data,
    V: Data,
    RDD: Rdd<Item = (K, V)>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static,
{
    type RddBase = RDD::RddBase;
    type Split = RDD::Split;
    type Item = (K, U);

    fn get_rdd_base(&self) -> Arc<Self::RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<Self::Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev
                .iterator(split)?
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        ))
    }
}
