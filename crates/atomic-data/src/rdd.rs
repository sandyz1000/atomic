use crate::{
    data::Data, dependency::Dependency, error::BaseResult, partitioner::Partitioner, split::Split,
};

use std::sync::Arc;

/// Base trait for all RDDs, providing structural metadata without type parameters.
///
/// Every RDD in Atomic implements this trait. It exposes the RDD's identity,
/// dependency graph edges, partitioning, and preferred placement for data-local
/// scheduling. Methods that compute data are on the typed [`Rdd`] trait.
pub trait RddBase: Send + Sync {
    fn get_rdd_id(&self) -> usize;

    fn get_op_name(&self) -> String {
        "unknown".to_owned()
    }

    fn register_op_name(&self, _name: &str) {
        log::debug!("couldn't register op name")
    }
    fn get_dependencies(&self) -> Vec<Dependency>;

    fn preferred_locations(&self, _split: Box<dyn Split>) -> Vec<std::net::Ipv4Addr> {
        Vec::new()
    }
    fn partitioner(&self) -> Option<Partitioner> {
        None
    }
    fn splits(&self) -> Vec<Box<dyn Split>>;

    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>>;

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        self.iterator_any(split)
    }

    fn is_pinned(&self) -> bool {
        false
    }

    /// Extract a pre-built staged pipeline carried by this RDD, if any.
    ///
    /// `TypedRdd` overrides this to propagate its `StagedPipeline` through the
    /// streaming layer (which receives `Arc<dyn Rdd<Item=T>>`). All other RDDs
    /// return `None`.
    ///
    /// Returns `(source_partition_bytes, ops)` when present.
    fn extract_staged_pipeline(
        &self,
    ) -> Option<(Vec<Vec<u8>>, Vec<crate::distributed::PipelineOp>)> {
        None
    }
}

/// Typed RDD trait — the core compute primitive.
///
/// Implemented by every RDD node in the DAG. `compute` returns a lazy iterator
/// over one partition's elements; the scheduler calls it for each partition in
/// parallel, then aggregates results on the driver.
///
/// For most user code, work through the high-level [`TypedRdd`](atomic_compute::rdd::TypedRdd)
/// wrapper rather than implementing this trait directly.
pub trait Rdd: RddBase + 'static {
    type Item: Data;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn get_rdd_base(&self) -> Arc<dyn RddBase>;

    fn compute(&self, split: Box<dyn Split>) -> BaseResult<Box<dyn Iterator<Item = Self::Item>>>;

    fn iterator(&self, split: Box<dyn Split>) -> BaseResult<Box<dyn Iterator<Item = Self::Item>>> {
        self.compute(split)
    }
}

/// Iterator adapter that folds elements pairwise using a commutative-associative function.
pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        F: FnMut(T, T) -> T;
}
