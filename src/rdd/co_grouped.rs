use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::Result;
use crate::rdd::rdd_val::RddVals;
use crate::rdd::*;
use ember_data::aggregator::Aggregator;
use ember_data::dependency::{Dependency, ShuffleDependency};
use ember_data::partitioner::Partitioner;
use ember_data::split::{CoGroupSplit, CoGroupSplitDep, Split};

// Note: CoGroupedRdd uses Arc<dyn Data> instead of Box<dyn Data> for values
// because Arc is Clone-able while Box is not. This allows the Item type to
// satisfy the Data trait bound which requires Clone.

#[derive(Clone)]
pub struct CoGroupedRdd<K: Data> {
    pub(crate) vals: Arc<RddVals>,
    pub(crate) rdds: Vec<Arc<dyn RddBase>>,

    pub(crate) part: Partitioner,
    _marker: PhantomData<K>,
}

impl<K: Data + Eq + Hash> CoGroupedRdd<K> {
    pub fn new(rdds: Vec<Arc<dyn RddBase>>, part: Partitioner) -> Self {
        let context = rdds[0].get_context();
        let mut vals = RddVals::new(context.clone());

        // Use Arc<dyn Data> instead of Box<dyn Data> to support Clone
        let create_combiner = Box::new(|v: Arc<dyn Data>| vec![v]);
        fn merge_value(mut buf: Vec<Arc<dyn Data>>, v: Arc<dyn Data>) -> Vec<Arc<dyn Data>> {
            buf.push(v);
            buf
        }
        let merge_value = Box::new(|(buf, v)| merge_value(buf, v));
        fn merge_combiners(
            mut b1: Vec<Arc<dyn Data>>,
            mut b2: Vec<Arc<dyn Data>>,
        ) -> Vec<Arc<dyn Data>> {
            b1.append(&mut b2);
            b1
        }
        let merge_combiners = Box::new(|(b1, b2)| merge_combiners(b1, b2));
        let aggr = Arc::new(Aggregator::<K, Arc<dyn Data>, Vec<Arc<dyn Data>>>::new(
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
                let rdd_base = rdd.clone();
                deps.push(Dependency::OneToOne { rdd_base })
            } else {
                let rdd_base = rdd.clone();
                log::debug!("creating aggregator inside cogrouprdd");
                deps.push(Dependency::Shuffle(
                    ShuffleDependency::new(
                        context.new_shuffle_id(),
                        true,
                        rdd_base,
                        aggr.clone(),
                        part.clone(),
                    )
                    .into(),
                ))
            }
        }
        vals.dependencies = deps;
        let vals = Arc::new(vals);
        CoGroupedRdd {
            vals,
            rdds,
            part,
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash> RddBase for CoGroupedRdd<K> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
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
            )) as Box<dyn Split>)
        }
        splits
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Partitioner> {
        Some(self.part.clone())
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any CoGroupedRdd");
        Ok(Box::new(
            self.compute(split)?.map(|(k, v)| Box::new((k, v))),
        ))
    }
}

impl<K: Data + Eq + Hash + Clone> Rdd for CoGroupedRdd<K> {
    type Item = (K, Vec<Vec<Arc<dyn Data>>>);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    #[allow(clippy::type_complexity)]
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut agg: HashMap<K, Vec<Vec<Arc<dyn Data>>>> = HashMap::new();
            for (dep_num, dep) in split.clone().deps.into_iter().enumerate() {
                match dep {
                    CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                        log::debug!("inside iterator CoGroupedRdd narrow dep");
                        for i in rdd.iterator_any(split)? {
                            log::debug!(
                                "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                                i
                            );
                            // Downcast Box<dyn Data> to tuple of (K, V) where both are Box<dyn Data>
                            let b = i
                                .into_any()
                                .downcast::<(Box<dyn Data>, Box<dyn Data>)>()
                                .map_err(|_| {
                                    crate::error::Error::DowncastFailure("tuple".to_string())
                                })?;
                            let (k, v) = *b;
                            // Downcast the key to K
                            let k = *(k.into_any().downcast::<K>().map_err(|_| {
                                crate::error::Error::DowncastFailure("key".to_string())
                            })?);
                            // Convert Box<dyn Data> to Arc<dyn Data> for the value
                            let v_arc: Arc<dyn Data> = v.into();
                            agg.entry(k)
                                .or_insert_with(|| vec![Vec::new(); self.rdds.len()])[dep_num]
                                .push(v_arc)
                        }
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                        log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                        let num_rdds = self.rdds.len();
                        // TODO: Fix ShuffleFetcher API - needs tracker instance
                        // For now, use the same pattern as shuffle.rs which also needs fixing
                        // let fetcher = &crate::env::Env::get().shuffle_fetcher;
                        // let fut = fetcher.fetch::<K, Vec<Arc<dyn Data>>>(
                        //     shuffle_id,
                        //     split.get_index(),
                        // );

                        // Temporary workaround - return empty iterator
                        // This needs to be fixed when ShuffleFetcher API is properly integrated
                        log::warn!("ShuffleFetcher not yet integrated - returning empty results");
                        let empty_iter: Vec<(K, Vec<Arc<dyn Data>>)> = Vec::new();
                        for (k, c) in empty_iter.into_iter() {
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
