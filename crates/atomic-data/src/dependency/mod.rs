//! RDD dependency edges. The narrow/shuffle lineage model (`DependencyType`,
//! `Dependency`) lives here; the shuffle-dependency subsystem lives in [`shuffle`]
//! and is re-exported so callers keep using `dependency::ShuffleDependency` etc.

use std::sync::Arc;

use crate::rdd::RddBase;
use crate::split::CoalescedRddSplit;

mod shuffle;
pub use shuffle::*;

/// Type of dependency between RDDs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyType {
    /// Narrow dependency where each partition depends on a known subset of parent partitions
    Narrow,
    /// Shuffle dependency where data needs to be redistributed across partitions
    Shuffle,
}

/// Dependency between RDDs
#[derive(Clone)]
pub enum Dependency {
    /// One-to-one narrow dependency where each partition depends on exactly one parent partition
    OneToOne {
        rdd_base: Arc<dyn RddBase>,
    },
    /// Range narrow dependency between ranges of partitions in parent and child RDDs
    Range {
        rdd_base: Arc<dyn RddBase>,
        /// the start of the range in the parent RDD
        in_start: usize,
        /// the start of the range in the child RDD
        out_start: usize,
        /// the length of the range
        length: usize,
    },

    CoalescedSplitDep {
        /// The RDD that this coalesced split depends on.
        /// This is a reference to the base RDD in the dependency graph.
        rdd: Arc<dyn RddBase>,
        /// The previous RDD in the transformation chain.
        /// Used to track the lineage of transformations leading to this coalesced split.
        prev: Arc<dyn RddBase>,
    },
    Shuffle(Arc<ShuffleDependency>),
}

impl Dependency {
    pub fn new_one_to_one(rdd_base: Arc<dyn RddBase>) -> Self {
        Dependency::OneToOne { rdd_base }
    }

    pub fn new_range(
        rdd_base: Arc<dyn RddBase>,
        in_start: usize,
        out_start: usize,
        length: usize,
    ) -> Self {
        Dependency::Range {
            rdd_base,
            in_start,
            out_start,
            length,
        }
    }

    pub fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        match self {
            Dependency::OneToOne { .. } => vec![partition_id],
            Dependency::Range {
                in_start,
                out_start,
                length,
                ..
            } => {
                if partition_id >= *out_start && partition_id < out_start + length {
                    vec![(partition_id - out_start) + in_start]
                } else {
                    Vec::new()
                }
            }
            Dependency::Shuffle(_) => Vec::new(),
            Dependency::CoalescedSplitDep { rdd, .. } => rdd
                .splits()
                .into_iter()
                .enumerate()
                .find(|(i, _)| i == &partition_id)
                .and_then(|(_, p)| CoalescedRddSplit::downcasting(p).ok())
                .map(|split| split.parent_indices)
                .unwrap_or_default(),
        }
    }

    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        match self {
            Dependency::OneToOne { rdd_base } => rdd_base.clone(),
            Dependency::Range { rdd_base, .. } => rdd_base.clone(),
            Dependency::Shuffle(dep) => dep.get_rdd_base(),
            Dependency::CoalescedSplitDep { rdd: _, prev } => prev.clone(),
        }
    }

    pub fn is_shuffle(&self) -> bool {
        matches!(self, Dependency::Shuffle(_))
    }

    pub fn dependency_type(&self) -> DependencyType {
        match self {
            Dependency::OneToOne { .. }
            | Dependency::Range { .. }
            | Dependency::CoalescedSplitDep { .. } => DependencyType::Narrow,
            Dependency::Shuffle(_) => DependencyType::Shuffle,
        }
    }

    pub fn get_shuffle_id(&self) -> Option<usize> {
        match self {
            Dependency::Shuffle(dep) => Some(dep.get_shuffle_id()),
            _ => None,
        }
    }

    pub fn get_shuffle_dep(&self) -> Option<&ShuffleDependency> {
        match self {
            Dependency::Shuffle(dep) => Some(dep),
            _ => None,
        }
    }
}

/// Every reduce-side [`ShuffleDependency`] reachable from `rdd` through narrow
/// (`OneToOne`/`Range`/`CoalescedSplitDep`) dependencies, stopping at each shuffle boundary —
/// a shuffle's own input is the map side, resolved separately, so we never recurse past one.
///
/// An un-staged reduce needs this to find shuffles sitting one or more narrow hops upstream
/// (e.g. `reduce_by_key().values()`); a direct `get_dependencies` scan sees only depth-0 deps
/// and misses them, leaving the shuffle-map undispatched and the reduce-side fetch to hang.
/// Mirrors the narrow-vs-shuffle traversal `StagePlanner::visit_missing_parent` already uses.
pub fn reduce_side_shuffles(rdd: &Arc<dyn RddBase>) -> Vec<Arc<ShuffleDependency>> {
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    collect_reduce_side_shuffles(rdd, &mut out, &mut visited);
    out
}

fn collect_reduce_side_shuffles(
    rdd: &Arc<dyn RddBase>,
    out: &mut Vec<Arc<ShuffleDependency>>,
    visited: &mut std::collections::HashSet<usize>,
) {
    if !visited.insert(rdd.get_rdd_id()) {
        return;
    }
    for dep in rdd.get_dependencies() {
        match dep {
            Dependency::Shuffle(shuf) => out.push(shuf),
            Dependency::OneToOne { rdd_base } | Dependency::Range { rdd_base, .. } => {
                collect_reduce_side_shuffles(&rdd_base, out, visited)
            }
            Dependency::CoalescedSplitDep { rdd, .. } => {
                collect_reduce_side_shuffles(&rdd, out, visited)
            }
        }
    }
}
