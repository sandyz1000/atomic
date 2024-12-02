use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;

use itertools::{Itertools, MinMaxResult};
use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, RangeDependency, ShuffleDependencyTrait};
use crate::error::{Error, Result};
use crate::partitioner::Partitioner;
use crate::rdd::union_rdd::UnionVariants::{NonUniquePartitioner, PartitionerAware};
use crate::rdd::rdd::*;
use crate::ser_data::{AnyData, Data};
use crate::split::Split;

#[derive(Clone, Serialize, Deserialize)]
struct UnionSplit<T: 'static, RDD> {
    /// index of the partition
    idx: usize,
    /// the parent RDD this partition refers to
    rdd: Arc<RDD>,
    /// index of the parent RDD this partition refers to
    parent_rdd_index: usize,
    /// index of the partition within the parent RDD this partition refers to
    parent_rdd_split_index: usize,

    _marker: PhantomData<T>
}

impl<T: Data, RDD: Rdd<Item = T>> UnionSplit<T, RDD> {
    fn parent_partition<S: Split + ?Sized>(&self) -> Box<S> {
        self.rdd.splits()[self.parent_rdd_split_index].clone()
    }
}

impl<T: Data, RDD: Rdd<Item = T> + Clone> Split for UnionSplit<T, RDD> {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionerAwareUnionSplit {
    idx: usize,
}

impl PartitionerAwareUnionSplit {
    fn parents<'a, T: Data, RDD: Rdd<Item = T>, S: Split + ?Sized>(
        &'a self,
        rdds: &'a [Arc<RDD>],
    ) -> impl Iterator<Item = Box<S>> + 'a {
        rdds.iter().map(move |rdd| rdd.splits()[self.idx].clone())
    }
}

impl Split for PartitionerAwareUnionSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct UnionRdd<T: 'static, RDD, SD, ND, PR>(UnionVariants<T, RDD, SD, ND, PR>);

impl<T, RDD, SD, ND, PR> UnionRdd<T, RDD, SD, ND, PR>
where
    T: Data,
    RDD: Rdd<Item = T>,
    SD: ShuffleDependencyTrait,
    ND: NarrowDependencyTrait,
    PR: Partitioner
{
    pub(crate) fn new(rdds: &[Arc<RDD>]) -> Result<Self> {
        Ok(UnionRdd(UnionVariants::new(rdds)?))
    }
}

#[derive(Serialize, Deserialize)]
pub enum UnionVariants<T: 'static, RDD, SD, ND, PR> {
    NonUniquePartitioner {
        rdds: Vec<Arc<RDD>>,
        vals: Arc<RddVals<ND, SD>>,
        T: PhantomData<T>
    },
    /// An RDD that can take multiple RDDs partitioned by the same partitioner and
    /// unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
    /// will be unified to a single RDD with p partitions and the same partitioner.
    PartitionerAware {
        rdds: Vec<Arc<RDD>>,
        vals: Arc<RddVals<ND, SD>>,
        part: Box<PR>,
    },
}

impl<T: Data, RDD, SD, ND, PR> Clone for UnionVariants<T, RDD, SD, ND, PR> 
where 
    RDD: Rdd<Item = T>,
    SD: ShuffleDependencyTrait,
    ND: NarrowDependencyTrait,
    PR: Partitioner
{
    fn clone(&self) -> Self {
        match self {
            NonUniquePartitioner { rdds, vals, .. } => NonUniquePartitioner {
                rdds: rdds.clone(),
                vals: vals.clone(),
                T: PhantomData,
            },
            PartitionerAware {
                rdds, vals, part, ..
            } => PartitionerAware {
                rdds: rdds.clone(),
                vals: vals.clone(),
                part: part.clone(),
            },
        }
    }
}

impl<T: Data, RDD, SD, ND, PR> UnionVariants<T, RDD, SD, ND, PR> {
    fn new(rdds: &[Arc<RDD>]) -> Result<Self> {
        let context = rdds[0].get_context();
        let mut vals = RddVals::new(context);

        let mut pos = 0;
        let final_rdds: Vec<_> = rdds.iter().map(|rdd| rdd.clone().into()).collect();

        if !UnionVariants::has_unique_partitioner(rdds) {
            let deps = rdds
                .iter()
                .map(|rdd| {
                    let rdd_base = rdd.get_rdd_base();
                    let num_parts = rdd_base.number_of_splits();
                    let dep = Dependency::NarrowDependency(Arc::new(RangeDependency::new(
                        rdd_base, 0, pos, num_parts,
                    )));
                    pos += num_parts;
                    dep
                })
                .collect();
            vals.dependencies = deps;
            let vals = Arc::new(vals);
            log::debug!("inside unique partitioner constructor");
            Ok(NonUniquePartitioner {
                rdds: final_rdds,
                vals,
                T: PhantomData,
            })
        } else {
            let part = rdds[0].partitioner().ok_or(Error::LackingPartitioner)?;
            log::debug!("inside partition aware constructor");
            let deps = rdds
                .iter()
                .map(|x| {
                    Dependency::NarrowDependency(
                        Arc::new(OneToOneDependency::new(x.get_rdd_base())),
                    )
                })
                .collect();
            vals.dependencies = deps;
            let vals = Arc::new(vals);
            Ok(PartitionerAware {
                rdds: final_rdds,
                vals,
                part,
            })
        }
    }

    fn has_unique_partitioner(rdds: &[Arc<RDD>]) -> bool {
        rdds.iter()
            .map(|p| p.partitioner())
            .try_fold(None, |prev: Option<Box<PR>>, p| {
                if let Some(partitioner) = p {
                    if let Some(prev_partitioner) = prev {
                        if prev_partitioner.equals((&*partitioner).as_any()) {
                            // only continue in case both partitioners are the same
                            Ok(Some(partitioner))
                        } else {
                            Err(())
                        }
                    } else {
                        // first element
                        Ok(Some(partitioner))
                    }
                } else {
                    Err(())
                }
            })
            .is_ok()
    }

    fn current_pref_locs<'a, S: Split + ?Sized>(
        &'a self,
        rdd: Arc<RDD>,
        split: &S,
        context: Arc<Context>,
    ) -> impl Iterator<Item = std::net::Ipv4Addr> + 'a {
        context
            .get_preferred_locs(rdd, split.get_index())
            .into_iter()
    }
}

impl<T: Data, RDD, SD, ND, PR> RddBase for UnionRdd<T, RDD, SD, ND, PR> {
    fn get_rdd_id(&self) -> usize {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.id,
            PartitionerAware { vals, .. } => vals.id,
        }
    }

    fn get_op_name(&self) -> String {
        "union".to_owned()
    }

    fn get_context(&self) -> Arc<Context> {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.context.upgrade().unwrap(),
            PartitionerAware { vals, .. } => vals.context.upgrade().unwrap(),
        }
    }

    fn get_dependencies(&self) -> Vec<Dependency<ND, SD>> {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.dependencies.clone(),
            PartitionerAware { vals, .. } => vals.dependencies.clone(),
        }
    }

    fn preferred_locations<S: Split + ?Sized>(&self, split: Box<S>) -> Vec<Ipv4Addr> {
        match &self.0 {
            NonUniquePartitioner { .. } => Vec::new(),
            PartitionerAware { rdds, .. } => {
                log::debug!(
                    "finding preferred location for PartitionerAwareUnionRdd, partition {}",
                    split.get_index()
                );

                let split = &*split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::DowncastFailure("UnionSplit")))
                    .unwrap();

                let locations =
                    rdds.iter()
                        .zip(split.parents(rdds.as_slice()))
                        .map(|(rdd, part)| {
                            let parent_locations = self.0.current_pref_locs(
                                rdd.get_rdd_base(),
                                &*part,
                                self.get_context(),
                            );
                            log::debug!("location of {} partition {} = {}", 1, 2, 3);
                            parent_locations
                        });

                // find the location that maximum number of parent partitions prefer
                let location = match locations.flatten().minmax_by_key(|loc| *loc) {
                    MinMaxResult::MinMax(_, max) => Some(max),
                    MinMaxResult::OneElement(e) => Some(e),
                    MinMaxResult::NoElements => None,
                };

                log::debug!(
                    "selected location for PartitionerAwareRdd, partition {} = {:?}",
                    split.get_index(),
                    location
                );

                location.into_iter().collect()
            }
        }
    }

    fn splits<S: Split + ?Sized>(&self) -> Vec<Box<S>> {
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => rdds
                .iter()
                .enumerate()
                .flat_map(|(rdd_idx, rdd)| {
                    rdd.splits()
                        .into_iter()
                        .enumerate()
                        .map(move |(split_idx, _split)| (rdd_idx, rdd, split_idx))
                })
                .enumerate()
                .map(|(idx, (rdd_idx, rdd, s_idx))| {
                    Box::new(UnionSplit {
                        idx,
                        rdd: rdd.clone(),
                        parent_rdd_index: rdd_idx,
                        parent_rdd_split_index: s_idx,
                        _marker: PhantomData,
                    })
                })
                .collect(),
            PartitionerAware { part, .. } => {
                let num_partitions = part.get_num_of_partitions();
                (0..num_partitions)
                    .map(|idx| Box::new(PartitionerAwareUnionSplit { idx }))
                    .collect()
            }
        }
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any union_rdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        match &self.0 {
            NonUniquePartitioner { .. } => None,
            PartitionerAware { part, .. } => Some(part.clone()),
        }
    }
}

impl<T: Data, RDD, SD, ND, PR> Rdd for UnionRdd<T, RDD, SD, ND, PR> 
where
    RDD: Rdd<Item = T>,
    SD: ShuffleDependencyTrait + 'static,
    ND: NarrowDependencyTrait + 'static,
    PR: Partitioner
{
    type Item = T;

    fn get_rdd_base<RDB: RddBase>(&self) -> Arc<RDB> {
        Arc::new(UnionRdd(self.0.clone()))
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = T>> {
        Arc::new(UnionRdd(self.0.clone()))
    }

    fn compute<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<dyn Iterator<Item = T>>> {
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => {
                let part = &*split
                    .downcast::<UnionSplit<T>>()
                    .or(Err(Error::DowncastFailure("UnionSplit")))?;
                let parent = &rdds[part.parent_rdd_index];
                parent.iterator(part.parent_partition())
            }
            PartitionerAware { rdds, .. } => {
                let split = split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::DowncastFailure("PartitionerAwareUnionSplit")))?;
                let iter: Result<Vec<_>> = rdds
                    .iter()
                    .zip(split.parents(&rdds))
                    .map(|(rdd, p)| rdd.iterator(p.clone()))
                    .collect();
                Ok(Box::new(iter?.into_iter().flatten()))
            }
        }
    }
}
