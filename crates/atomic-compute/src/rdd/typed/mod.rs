use crate::rdd::cached::CachedRdd;
use crate::rdd::cartesian::CartesianRdd;
use crate::rdd::coalesced::CoalescedRdd;
use crate::rdd::flatmapper::FlatMapperRdd;
use crate::rdd::map_partitions::{MapPartitionsPairRdd, MapPartitionsRdd};
use crate::rdd::mapper::MapperRdd;
use crate::rdd::pair::FlatMappedValuesRdd;
use crate::rdd::parallel_collection::ParallelCollection;
use crate::rdd::partitionwise_sampled::PartitionwiseSampledRdd;
use crate::runtimes::Backend;
use crate::task_traits::{BinaryTask, UnaryTask};
use atomic_data::cache::StorageLevel;
use atomic_data::dependency::Dependency;
use atomic_data::distributed::{
    PipelineOp, TaskAction, TaskEnvelope, TaskRuntime, WireDecode, WireEncode,
};
use atomic_data::error::BaseError;
use atomic_data::partitioner::Partitioner;

use crate::rdd::{Data, Rdd, RddBase};
use atomic_data::split::Split;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::Context;
use crate::rdd::union_rdd::UnionRdd;
use crate::rdd::zip::ZippedPartitionsRdd;

mod actions;
mod joins;
mod pair_ops;
mod persist;
mod sort;
mod task_api;
mod transforms;

/// Accumulated ops waiting to be dispatched to workers as a single pipeline.
///
/// `source_partitions[i]` holds the rkyv-encoded `Vec<T>` for partition `i`.
/// `ops` is the ordered list of operations to apply on each partition.
struct StagedPipeline {
    source_partitions: Vec<Vec<u8>>,
    ops: Vec<PipelineOp>,
}

pub type RddRef<T> = Arc<dyn Rdd<Item = T>>;

/// Typed RDD wrapper that provides transformation and action methods.
///
/// This is the recommended way to work with RDDs in atomic. Unlike trait-based operations,
/// TypedRdd provides methods that can be chained naturally without type erasure issues.
///
/// In distributed mode, `_task` transformations accumulate ops lazily into a
/// `StagedPipeline` and only dispatch them when an action (`collect`, `fold_task`, …)
/// is called — sending the full chain as a single `TaskEnvelope` per partition.
///
/// # Example
/// ```ignore
/// let rdd = context.parallelize_typed(vec![1, 2, 3, 4, 5]);
/// let result = rdd
///     .map(|x| x * 2)
///     .filter(|x| x % 4 == 0)
///     .collect()?;
/// ```
pub struct TypedRdd<T> {
    rdd: RddRef<T>,
    context: Arc<Context>,
    /// Lazily accumulated pipeline of ops (distributed mode only).
    /// `None` means no pipeline has been started yet; the `rdd` field is authoritative.
    staged: Option<StagedPipeline>,
    _marker: PhantomData<T>,
}

impl<T: Clone> TypedRdd<T> {
    pub fn new(rdd: RddRef<T>, context: Arc<Context>) -> Self {
        Self {
            rdd,
            context,
            staged: None,
            _marker: PhantomData,
        }
    }

    pub fn inner(&self) -> &RddRef<T> {
        &self.rdd
    }

    pub fn into_rdd(self) -> RddRef<T> {
        self.rdd
    }

    pub fn num_partitions(&self) -> usize {
        self.rdd.number_of_splits()
    }

    /// Allocate a new RDD id, apply `rdd_fn(id, parent_rdd)` to produce a new concrete RDD,
    /// wrap it in `Arc`, and return a `TypedRdd<U>` sharing the same context.
    ///
    /// Eliminates the repetitive three-line pattern:
    /// ```text
    /// let id = self.context.new_rdd_id();
    /// let ctx = self.context.clone();
    /// TypedRdd::new(Arc::new(SomeRdd::new(id, self.rdd, …)), ctx)
    /// ```
    pub(crate) fn map_rdd<U, R, F>(self, rdd_fn: F) -> TypedRdd<U>
    where
        U: Data + Clone,
        R: Rdd<Item = U> + 'static,
        F: FnOnce(usize, RddRef<T>) -> R,
    {
        let id = self.context.new_rdd_id();
        let ctx = self.context.clone();
        TypedRdd::new(Arc::new(rdd_fn(id, self.rdd)), ctx)
    }
}

impl<T> Clone for TypedRdd<T> {
    fn clone(&self) -> Self {
        TypedRdd {
            rdd: self.rdd.clone(),
            context: self.context.clone(),
            staged: None, // staged pipelines are one-shot; clone starts fresh
            _marker: PhantomData,
        }
    }
}

impl<D: Data> RddBase for TypedRdd<D> {
    fn get_rdd_id(&self) -> usize {
        self.rdd.get_rdd_id()
    }

    fn get_op_name(&self) -> String {
        self.rdd.get_op_name()
    }

    fn register_op_name(&self, name: &str) {
        self.rdd.register_op_name(name)
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.rdd.get_dependencies()
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<std::net::Ipv4Addr> {
        self.rdd.preferred_locations(split)
    }

    fn partitioner(&self) -> Option<Partitioner> {
        self.rdd.partitioner()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.rdd.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.rdd.number_of_splits()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        self.rdd.iterator_any(split)
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        self.rdd.cogroup_iterator_any(split)
    }

    fn is_pinned(&self) -> bool {
        self.rdd.is_pinned()
    }
}

impl<D> Rdd for TypedRdd<D>
where
    D: Data,
{
    type Item = D;

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        self.rdd.clone()
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd.get_rdd_base()
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        self.rdd.compute(split)
    }
}

// CONVERSION TRAIT - For easy Arc<dyn Rdd> -> TypedRdd conversion

/// Extension trait: `rdd.typed(ctx)` wraps any `Arc<dyn Rdd<Item=T>>` in a `TypedRdd<T>`.
pub trait RddExt<T: Data>: Rdd<Item = T> {
    fn typed(self: Arc<Self>, context: Arc<Context>) -> TypedRdd<T>;
}

impl<T: Data + Clone, R: Rdd<Item = T>> RddExt<T> for R {
    fn typed(self: Arc<Self>, context: Arc<Context>) -> TypedRdd<T> {
        TypedRdd::new(self, context)
    }
}
