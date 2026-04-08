use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::error::Error;
use crate::rdd::rdd_val::RddVals;
use crate::rdd::union_rdd::UnionVariants::{NonUniquePartitioner, PartitionerAware};
use crate::rdd::*;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::partitioner::Partitioner;
use atomic_data::split::{PartitionerAwareUnionSplit, Split, UnionSplit};

pub struct UnionRdd<T: 'static>(UnionVariants<T>);

impl<T> UnionRdd<T>
where
    T: Data,
{
    pub fn new(id: usize, rdds: &[Arc<dyn Rdd<Item = T>>]) -> Result<Self, Error> {
        Ok(UnionRdd(UnionVariants::new(id, rdds)?))
    }
}

pub enum UnionVariants<T: 'static> {
    NonUniquePartitioner {
        rdds: Vec<Arc<dyn Rdd<Item = T>>>,
        vals: Arc<RddVals>,
    },
    /// An RDD that can take multiple RDDs partitioned by the same partitioner and
    /// unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
    /// will be unified to a single RDD with p partitions and the same partitioner.
    PartitionerAware {
        rdds: Vec<Arc<dyn Rdd<Item = T>>>,
        vals: Arc<RddVals>,
        part: Partitioner,
    },
}

impl<T: Data> Clone for UnionVariants<T> {
    fn clone(&self) -> Self {
        match self {
            NonUniquePartitioner { rdds, vals, .. } => NonUniquePartitioner {
                rdds: rdds.clone(),
                vals: vals.clone(),
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

impl<T: Data> UnionVariants<T> {
    fn new(id: usize, rdds: &[Arc<dyn Rdd<Item = T>>]) -> Result<Self, Error> {
        let mut vals = RddVals::new(id);

        let mut pos = 0;
        let final_rdds: Vec<_> = rdds.iter().map(|rdd| rdd.clone()).collect();

        if !UnionVariants::has_unique_partitioner(rdds) {
            let deps = rdds
                .iter()
                .map(|rdd| {
                    let rdd_base = rdd.get_rdd_base();
                    let num_parts = rdd_base.number_of_splits();
                    let dep = Dependency::new_range(rdd_base, 0, pos, num_parts);
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
            })
        } else {
            let part = rdds[0].partitioner().ok_or(Error::LackingPartitioner)?;
            log::debug!("inside partition aware constructor");
            let deps = rdds
                .iter()
                .map(|x| Dependency::new_one_to_one(x.get_rdd_base()))
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

    fn has_unique_partitioner(rdds: &[Arc<dyn Rdd<Item = T>>]) -> bool {
        rdds.iter()
            .map(|p| p.partitioner())
            .try_fold(None, |prev: Option<Partitioner>, p| {
                if let Some(partitioner) = p {
                    if let Some(prev_partitioner) = prev {
                        if prev_partitioner.equals(&partitioner) {
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

    // TODO: Preferred locations functionality needs to be reimplemented at Context/Scheduler layer
    // fn current_pref_locs<'a>(
    //     &'a self,
    //     rdd: Arc<dyn RddBase>,
    //     split: &dyn Split,
    //     context: Arc<Context>,
    // ) -> impl Iterator<Item = std::net::Ipv4Addr> + 'a {
    //     context
    //         .get_preferred_locs(rdd, split.get_index())
    //         .into_iter()
    // }
}

impl<T: Data + Clone> RddBase for UnionRdd<T> {
    fn get_rdd_id(&self) -> usize {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.id,
            PartitionerAware { vals, .. } => vals.id,
        }
    }

    fn get_op_name(&self) -> String {
        "union".to_owned()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.dependencies.clone(),
            PartitionerAware { vals, .. } => vals.dependencies.clone(),
        }
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        match &self.0 {
            NonUniquePartitioner { .. } => Vec::new(),
            PartitionerAware { rdds: _, .. } => {
                log::debug!(
                    "finding preferred location for PartitionerAwareUnionRdd, partition {}",
                    split.get_index()
                );

                let Some(_split) = split.as_any().downcast_ref::<PartitionerAwareUnionSplit>()
                else {
                    log::warn!("Failed to downcast split to PartitionerAwareUnionSplit");
                    return Vec::new();
                };

                // TODO: Preferred locations need to be computed through Context/Scheduler
                // layer rather than at RDD level after decoupling context from RDDs
                log::debug!(
                    "Preferred locations for PartitionerAwareUnionRdd temporarily disabled"
                );
                Vec::new()
            }
        }
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
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
                    }) as Box<dyn Split>
                })
                .collect(),
            PartitionerAware { part, .. } => {
                let num_partitions = part.get_num_of_partitions();
                (0..num_partitions)
                    .map(|idx| Box::new(PartitionerAwareUnionSplit { idx }) as Box<dyn Split>)
                    .collect()
            }
        }
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any union_rdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }

    fn partitioner(&self) -> Option<Partitioner> {
        match &self.0 {
            NonUniquePartitioner { .. } => None,
            PartitionerAware { part, .. } => Some(part.clone()),
        }
    }
}

impl<T: Data + Clone> Rdd for UnionRdd<T> {
    type Item = T;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(UnionRdd(self.0.clone())) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = T>> {
        Arc::new(UnionRdd(self.0.clone())) as Arc<dyn Rdd<Item = T>>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = T>>, BaseError> {
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => {
                let part = split
                    .as_any()
                    .downcast_ref::<UnionSplit<T>>()
                    .ok_or(Error::DowncastFailure("UnionSplit"))?;
                let parent = &rdds[part.parent_rdd_index];
                parent.iterator(part.parent_partition())
            }
            PartitionerAware { rdds, .. } => {
                let split = split
                    .as_any()
                    .downcast_ref::<PartitionerAwareUnionSplit>()
                    .ok_or(Error::DowncastFailure("PartitionerAwareUnionSplit"))?;
                let iter: Result<Vec<_>, BaseError> = rdds
                    .iter()
                    .zip(split.parents(rdds))
                    .map(|(rdd, p)| rdd.iterator(p.clone()))
                    .collect();
                Ok(Box::new(iter?.into_iter().flatten()))
            }
        }
    }
}
