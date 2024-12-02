use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::rdd::*;
use crate::ser_data::{AnyData, Data};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub enum CoGroupSplitDep<RDD, S> {
    NarrowCoGroupSplitDep { rdd: Arc<RDD>, split: Box<S> },
    ShuffleCoGroupSplitDep { shuffle_id: usize },
}

#[derive(Clone, Serialize, Deserialize)]
struct CoGroupSplit<RDD, S> {
    index: usize,
    deps: Vec<CoGroupSplitDep<RDD, S>>,
}

impl<RDD, S> CoGroupSplit<RDD, S>
where
    RDD: RddBase + Clone,
    S: Split,
{
    fn new(index: usize, deps: Vec<CoGroupSplitDep<RDD, S>>) -> Self {
        CoGroupSplit { index, deps }
    }
}

impl<RDD, S> Hasher for CoGroupSplit<RDD, S>
where
    RDD: RddBase + Clone,
    S: Split + dyn_clone::DynClone,
{
    fn finish(&self) -> u64 {
        self.index as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for i in bytes {
            self.write_u8(*i);
        }
    }
}

impl<RDD, S> Split for CoGroupSplit<RDD, S>
where
    RDD: RddBase + Clone,
    S: Split + dyn_clone::DynClone + Clone,
{
    fn get_index(&self) -> usize {
        self.index
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CoGroupedRdd<K: Data, D: AnyData, RDD, PR, ND, SD> {
    pub(crate) vals: Arc<RddVals<ND, SD>>,
    pub(crate) rdds: Vec<Arc<RDD>>,
    pub(crate) part: Box<PR>,
    _marker: PhantomData<K>,
    _marker_d: PhantomData<D>
}


impl<K, D, RDD, PR, ND, SD> CoGroupedRdd<K, D, RDD, PR, ND, SD>
where
    K: Data + Eq + Hash,
    D: AnyData,
    RDD: RddBase,
    PR: Partitioner,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    pub fn new(rdds: Vec<Arc<RDD>>, part: Box<PR>) -> Self {
        let context = rdds[0].get_context();
        let mut vals = RddVals::new(context.clone());
        let create_combiner = Box::new(serde_closure::Fn!(|v: Box<dyn AnyData>| vec![v]));
        
        fn merge_value<D: AnyData>(
            mut buf: Vec<Box<D>>,
            v: Box<D>,
        ) -> Vec<Box<impl AnyData>> {
            buf.push(v);
            buf
        }
        
        let merge_value = Box::new(serde_closure::Fn!(|(buf, v)| merge_value(buf, v)));
        fn merge_combiners<D: AnyData>(
            mut b1: Vec<Box<D>>,
            mut b2: Vec<Box<D>>,
        ) -> Vec<Box<impl AnyData>> {
            b1.append(&mut b2);
            b1
        }
        let merge_combiners = Box::new(serde_closure::Fn!(|(b1, b2)| merge_combiners(b1, b2)));
        let aggr = Arc::new(Aggregator::new(
            create_combiner,
            merge_value,
            merge_combiners,
        ));
        let mut deps = Vec::new();
        for (_index, rdd) in rdds.iter().enumerate() {
            let part = part.clone();
            if rdd
                .partitioner()
                .map_or(false, |p| p.equals(&part as &dyn Any))
            {
                let rdd_base = rdd.clone().into();
                deps.push(Dependency::NarrowDependency(Arc::new(
                    OneToOneDependency::new(rdd_base),
                )))
            } else {
                let rdd_base = rdd.clone().into();
                log::debug!("creating aggregator inside cogrouprdd");
                deps.push(Dependency::ShuffleDependency(Arc::new(
                    ShuffleDependency::new(
                        context.new_shuffle_id(),
                        true,
                        rdd_base,
                        aggr.clone(),
                        part,
                    ),
                )))
            }
        }
        vals.dependencies = deps;
        let vals = Arc::new(vals);
        CoGroupedRdd {
            vals,
            rdds,
            part,
            _marker: PhantomData,
            _marker_d: PhantomData,
        }
    }
}

impl<K, D, RDD, PR, ND, SD> RddBase for CoGroupedRdd<K, D, RDD, PR, ND, SD>
where
    K: Data + Eq + Hash,
    D: AnyData,
    RDD: RddBase,
    PR: Partitioner,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency<ND, SD>> {
        self.vals.dependencies.clone()
    }

    fn splits<S: Split + ?Sized>(&self) -> Vec<Box<S>> {
        let mut splits = Vec::new();
        for i in 0..self.part.get_num_of_partitions() {
            splits.push(Box::new(CoGroupSplit::new(
                i,
                self.rdds
                    .iter()
                    .enumerate()
                    .map(|(i, r)| match &self.get_dependencies()[i] {
                        Dependency::ShuffleDependency(s) => {
                            CoGroupSplitDep::ShuffleCoGroupSplitDep {
                                shuffle_id: s.get_shuffle_id(),
                            }
                        }
                        _ => CoGroupSplitDep::NarrowCoGroupSplitDep {
                            rdd: r.clone().into(),
                            split: r.splits()[i].clone(),
                        },
                    })
                    .collect(),
            )))
        }
        splits
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner<P: Partitioner + ?Sized>(&self) -> Option<Box<P>> {
        let part = self.part.clone();
        Some(part)
    }

    fn iterator_any<S: Split + ?Sized, AD: AnyData + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        split: Box<S>,
    ) -> Result<Box<dyn Iterator<Item = Box<AD>>>> {
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v)))),
        ))
    }
}


impl<K, D, RDD, PR, ND, SD> Rdd for CoGroupedRdd<K, D, RDD, PR, ND, SD> 
where
    K: Data + Eq + Hash,
    D: AnyData,
    RDD: RddBase,
    PR: Partitioner,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static,
{
    type Item = (K, Vec<Vec<Box<D>>>);
    
    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<RDD> {
        Arc::new(self.clone())
    }

    #[allow(clippy::type_complexity)]
    fn compute<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit<RDD, S>>() {
            let mut agg: HashMap<K, Vec<Vec<Box<D>>>> = HashMap::new();
            for (dep_num, dep) in split.clone().deps.into_iter().enumerate() {
                match dep {
                    CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                        log::debug!("inside iterator CoGroupedRdd narrow dep");
                        for i in rdd.iterator_any(split)? {
                            log::debug!(
                                "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                                i
                            );
                            let b = i
                                .into_any()
                                .downcast::<(Box<D>, Box<D>)>()
                                .unwrap();
                            let (k, v) = *b;
                            let k = *(k.into_any().downcast::<K>().unwrap());
                            agg.entry(k)
                                .or_insert_with(|| vec![Vec::new(); self.rdds.len()])[dep_num]
                                .push(v)
                        }
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                        log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                        let num_rdds = self.rdds.len();
                        let fut = ShuffleFetcher::fetch::<K, Vec<Box<K>>>(
                            shuffle_id,
                            split.get_index(),
                        );
                        for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                            let temp = agg.entry(k).or_insert_with(|| vec![Vec::new(); num_rdds]);
                            for v in c {
                                temp[dep_num].push(v);
                            }
                        }
                    }
                }
            }
            Ok(Box::new(agg.into_iter()))
        } else {
            panic!("Got split object from different concrete type other than CoGroupSplit")
        }
    }
}
