use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::rdd::rdd_val::RddVals;
use crate::rdd::*;
use atomic_data::aggregator::Aggregator;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::partitioner::Partitioner;
use atomic_data::split::{CoGroupSplit, CoGroupSplitDep, Split};
use bincode::Encode;

// Note: CoGroupedRdd uses Arc<dyn Data> instead of Box<dyn Data> for values
// because Arc is Clone-able while Box is not. This allows the Item type to
// satisfy the Data trait bound which requires Clone.

#[derive(Clone)]
pub struct CoGroupedRdd<K: Data> {
    pub vals: Arc<RddVals>,
    pub rdds: Vec<Arc<dyn RddBase>>,

    pub part: Partitioner,
    _marker: PhantomData<K>,
}

impl<K: Data + Eq + Hash + Encode + Clone> CoGroupedRdd<K> {
    pub fn new(
        id: usize,
        _shuffle_id: usize,
        rdds: Vec<Arc<dyn RddBase>>,
        part: Partitioner,
    ) -> Self {
        let mut vals = RddVals::new(id);

        // Use Arc<dyn Data> instead of Box<dyn Data> to support Clone
        let create_combiner = Arc::new(|v: Arc<dyn Data>| vec![v])
            as Arc<dyn Fn(Arc<dyn Data>) -> Vec<Arc<dyn Data>> + Send + Sync>;
        let merge_value = Arc::new(|buf: &mut Vec<Arc<dyn Data>>, v: Arc<dyn Data>| {
            buf.push(v);
        })
            as Arc<dyn Fn(&mut Vec<Arc<dyn Data>>, Arc<dyn Data>) + Send + Sync>;
        let merge_combiners = Arc::new(|b1: &mut Vec<Arc<dyn Data>>, b2: Vec<Arc<dyn Data>>| {
            b1.extend(b2);
        })
            as Arc<dyn Fn(&mut Vec<Arc<dyn Data>>, Vec<Arc<dyn Data>>) + Send + Sync>;
        let _aggr = Arc::new(Aggregator::<K, Arc<dyn Data>, Vec<Arc<dyn Data>>>::new(
            create_combiner,
            merge_value,
            merge_combiners,
        ));
        let mut deps = Vec::new();
        for rdd in rdds.iter() {
            let part = part.clone();
            if rdd.partitioner().is_some_and(|p| p.equals(&part)) {
                let rdd_base = rdd.clone();
                deps.push(Dependency::OneToOne { rdd_base })
            } else {
                // Note: Cannot create shuffle dependency with type-erased Arc<dyn Data>
                // because it doesn't implement Encode. Would need typed ShuffledRdd instead.
                // For now, using OneToOne dependency (requires pre-shuffled data).
                log::warn!(
                    "CoGroupedRdd: Partitioner mismatch but cannot create shuffle with type-erased data - using OneToOne. \
                    Data should be pre-shuffled or use typed operations."
                );
                let rdd_base = rdd.clone();
                deps.push(Dependency::OneToOne { rdd_base });
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

impl<K: Eq + Hash + Data + Clone> RddBase for CoGroupedRdd<K> {
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
                        Dependency::Shuffle(s) => CoGroupSplitDep::ShuffleCoGroupSplitDep {
                            shuffle_id: s.get_shuffle_id(),
                        },
                        _ => CoGroupSplitDep::NarrowCoGroupSplitDep {
                            rdd: r.clone(),
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any CoGroupedRdd");
        Ok(Box::new(
            self.compute(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }
}

impl<K> Rdd for CoGroupedRdd<K>
where
    K: Data + Eq + Hash + Clone,
{
    type Item = (K, Vec<Vec<Arc<dyn Data>>>);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    #[allow(clippy::type_complexity)]
    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        if let Some(split) = split.as_any().downcast_ref::<CoGroupSplit>() {
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
                            // TODO: This downcasting logic is complex and error-prone
                            // The issue is that we receive Box<dyn Data> which could be a tuple (K, V)
                            // But we can't easily extract K and convert V to Arc<dyn Data>
                            // This would require either:
                            // 1. Making Data object-safe for cloning (add clone_box method)
                            // 2. Redesigning CoGroupedRdd to not use type erasure
                            // 3. Using unsafe code to transmute Box to Arc (not recommended)

                            // For now, log a warning and skip this item
                            log::warn!(
                                "CoGroupedRdd: Skipping narrow dependency item - downcasting from Box<dyn Data> to concrete types not yet implemented"
                            );
                            // TODO: Implement proper downcasting when Data trait is enhanced
                            continue;
                        }
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id: _ } => {
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

    fn iterator(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        self.compute(split)
    }
}
