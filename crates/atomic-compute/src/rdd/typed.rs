use crate::rdd::cached::CachedRdd;
use crate::rdd::cartesian::CartesianRdd;
use crate::rdd::coalesced::CoalescedRdd;
use crate::rdd::flatmapper::FlatMapperRdd;
use crate::rdd::map_partitions::{MapPartitionsPairRdd, MapPartitionsRdd};
use crate::rdd::mapper::MapperRdd;
use crate::rdd::pair::FlatMappedValuesRdd;
use crate::rdd::parallel_collection::ParallelCollection;
use crate::rdd::partitionwise_sampled::PartitionwiseSampledRdd;
use crate::backend::Backend;
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


impl<T: Data + Clone + 'static> TypedRdd<T> {
    /// Persist this RDD's partitions in memory so they are not recomputed
    /// across multiple actions.
    ///
    /// Equivalent to `persist(StorageLevel::MemoryOnly)`.
    pub fn cache(self) -> Self {
        self.persist(StorageLevel::MemoryOnly)
    }

    /// Persist this RDD's partitions using the given storage level.
    ///
    /// - `MemoryOnly` / `MemoryOnlySer`: memoises in the global `PartitionStore` (LRU-bounded).
    /// - `MemoryAndDisk` / `DiskOnly`: accepted but fall back to memory semantics unless `T`
    ///   implements `bincode::Encode + bincode::Decode<()>`. For actual disk spill, call
    ///   `persist_with_disk(level)` instead.
    pub fn persist(self, level: StorageLevel) -> Self {
        let ctx = self.context.clone();
        let cached = Arc::new(CachedRdd::new_with_level(self.into_rdd(), level));
        TypedRdd::new(cached as RddRef<T>, ctx)
    }

    /// Persist with real disk spill for `MemoryAndDisk` and `DiskOnly` levels.
    ///
    /// Requires `T: bincode::Encode + bincode::Decode<()>` so partitions can be
    /// serialized to `{work_dir}/rdd-cache/{rdd_id}/{partition}.bin`.
    ///
    /// - `MemoryAndDisk`: memory-first; on LRU eviction falls back to disk; on miss reads disk.
    /// - `DiskOnly`: always reads from disk; never occupies `PartitionStore` memory.
    /// - Other levels: identical to `persist(level)`.
    pub fn persist_with_disk(self, level: StorageLevel) -> Self
    where
        T: bincode::Encode + bincode::Decode<()>,
    {
        use atomic_data::cache::{disk_write_partition, PARTITION_CACHE};

        match level {
            StorageLevel::MemoryAndDisk => {
                let ctx = self.context.clone();
                let num_parts = self.rdd.number_of_splits();
                // Wrap lazily — spill registration replaces eager materialisation.
                let cached = Arc::new(CachedRdd::new_with_level(
                    self.rdd.clone(),
                    StorageLevel::MemoryAndDisk,
                ));
                let rdd_id = cached.rdd_id();
                // Pre-register per-partition spill handlers so that:
                //   * put() writes to disk on LRU eviction
                //   * get() reads from disk on a memory miss
                if let Some(store) = PARTITION_CACHE.get() {
                    for part_idx in 0..num_parts {
                        if let Some(path) = cached.spill_path(part_idx) {
                            store.register_spill_path::<T>(rdd_id, part_idx, path);
                        }
                    }
                }
                TypedRdd::new(cached as RddRef<T>, ctx)
            }

            StorageLevel::DiskOnly => {
                let ctx = self.context.clone();
                let num_parts = self.rdd.number_of_splits();
                let cached = Arc::new(CachedRdd::new_with_level(
                    self.rdd.clone(),
                    StorageLevel::DiskOnly,
                ));
                // Eagerly write all partitions to disk (DiskOnly skips memory cache).
                for part_idx in 0..num_parts {
                    let splits = self.rdd.splits();
                    if let Ok(items) = self.rdd.compute(splits[part_idx].clone()) {
                        let data: Vec<T> = items.collect();
                        if let Some(path) = cached.spill_path(part_idx) {
                            let _ = disk_write_partition(&path, &data);
                        }
                    }
                }
                TypedRdd::new(cached as RddRef<T>, ctx)
            }

            other => self.persist(other),
        }
    }

    /// Remove all cached partitions for this RDD from the global `PartitionStore`.
    ///
    /// After `unpersist()` the next action on the returned RDD will recompute all
    /// partitions from scratch.  Equivalent to Spark's `RDD.unpersist()`.
    pub fn unpersist(self) -> Self {
        if let Some(store) = atomic_data::cache::PARTITION_CACHE.get() {
            let rdd_id = self.rdd.get_rdd_id();
            let n = self.rdd.number_of_splits();
            store.remove_rdd(rdd_id, n);
        }
        self
    }

    /// Returns `true` if at least one partition of this RDD is currently held in
    /// the global `PartitionStore` (i.e., the RDD has been cached and not evicted).
    pub fn is_cached(&self) -> bool {
        atomic_data::cache::PARTITION_CACHE
            .get()
            .map(|store| {
                let rdd_id = self.rdd.get_rdd_id();
                let n = self.rdd.number_of_splits();
                (0..n).any(|p| store.contains(rdd_id, p))
            })
            .unwrap_or(false)
    }

    /// Materialise this RDD, write each partition to `dir` (local path or `s3://`),
    /// and return a new `TypedRdd` backed by a `CheckpointRdd` — fully truncating
    /// the upstream lineage.
    ///
    /// Partitions are written to `{dir}/{rdd_id}/{partition}.bin` (bincode-encoded).
    ///
    /// Requires `T: bincode::Encode + bincode::Decode<()>`.
    pub fn checkpoint(self, dir: impl AsRef<str>) -> Result<TypedRdd<T>, BaseError>
    where
        T: bincode::Encode + bincode::Decode<()>,
    {
        use atomic_data::cache::disk_write_partition;
        use crate::rdd::checkpoint::{CheckpointRdd, CheckpointStore};

        let store = CheckpointStore::from_uri(dir.as_ref());
        let ctx = self.context.clone();
        let rdd_id = self.rdd.get_rdd_id();
        let partitions = self.collect_partitions()?;
        let num_partitions = partitions.len();

        for (idx, data) in partitions.iter().enumerate() {
            match &store {
                CheckpointStore::Local(base) => {
                    let path = base.join(format!("{rdd_id}")).join(format!("{idx}.bin"));
                    disk_write_partition(&path, data)
                        .map_err(|e| BaseError::Other(format!("checkpoint write failed: {e}")))?;
                }

                #[cfg(feature = "s3")]
                CheckpointStore::S3 { bucket, prefix } => {
                    use crate::io::s3::s3_impl::write_text;
                    let bytes = bincode::encode_to_vec(data, bincode::config::standard())
                        .map_err(|e| BaseError::Other(format!("checkpoint encode: {e}")))?;
                    let b64 =
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                    let key = format!("{prefix}/{rdd_id}/{idx}.bin");
                    write_text(bucket, &key, b64).map_err(|e| BaseError::Other(e))?;
                }
            }
        }

        let checkpoint_rdd = Arc::new(CheckpointRdd::new(ctx.new_rdd_id(), store, num_partitions));
        Ok(TypedRdd::new(checkpoint_rdd as RddRef<T>, ctx))
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

impl<T: Data + Clone> TypedRdd<T> {
    /// Collect all elements from all partitions into a Vec.
    ///
    /// In distributed mode, if a lazy pipeline has been staged by `map_task` /
    /// `filter_task` / `flat_map_task`, this dispatches the full pipeline in one
    /// round-trip per partition. Otherwise falls back to the driver scheduler path.
    ///
    /// **Warning**: This brings all data to the driver. Only use on small datasets.
    pub fn collect(&self) -> Result<Vec<T>, BaseError>
    where
        Vec<T>: WireDecode,
    {
        if self.context.is_distributed() {
            if let Some(ref staged) = self.staged {
                let result_bytes = self
                    .context
                    .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                return result_bytes
                    .into_iter()
                    .map(|bytes| {
                        Vec::<T>::decode_wire(&bytes)
                            .map_err(|e| BaseError::DowncastFailure(e.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vecs| vecs.into_iter().flatten().collect());
            }

            // If the RDD has shuffle dependencies, run the shuffle map stage on
            // workers first. ShuffledRdd::compute will then fetch via HTTP.
            let rdd_base = self.rdd.get_rdd_base();
            let has_shuffle = rdd_base.get_dependencies().iter().any(|d| d.is_shuffle());
            if has_shuffle {
                self.context
                    .run_pending_shuffle_stages(&rdd_base, vec![])
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                // Fall through to run_job — ShuffledRdd::compute calls ShuffleFetcher::fetch.
            }
        }
        let cl = |iter: Box<dyn Iterator<Item = T>>| iter.collect::<Vec<T>>();
        let results = self.context.run_job(self.rdd.clone(), cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<T>| a + b.len());
        Ok(results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            }))
    }

    /// Collect each partition as a separate `Vec<T>`, preserving partition boundaries.
    ///
    /// Returns `Vec<Vec<T>>` where index `i` holds the elements of partition `i`.
    /// Useful for `save_as_text_file` and `checkpoint` which write one file per partition.
    pub fn collect_partitions(&self) -> Result<Vec<Vec<T>>, BaseError> {
        let cl = |iter: Box<dyn Iterator<Item = T>>| iter.collect::<Vec<T>>();
        Ok(self.context.run_job(self.rdd.clone(), cl)?)
    }

    /// Stream elements partition-by-partition to the driver without holding all partitions
    /// in memory simultaneously.
    ///
    /// Unlike `collect()` which materialises every partition before returning, this method
    /// fetches one partition at a time and yields its elements before fetching the next.
    /// This reduces peak driver-side memory for large datasets where the caller processes
    /// elements incrementally.
    pub fn to_local_iterator(&self) -> Result<impl Iterator<Item = T>, BaseError> {
        let n = self.num_partitions();
        let mut result: Vec<T> = Vec::new();
        for i in 0..n {
            let partition_data = self
                .context
                .run_job_with_partitions(self.rdd.clone(), |iter| iter.collect::<Vec<T>>(), [i])?;
            result.extend(partition_data.into_iter().flatten());
        }
        Ok(result.into_iter())
    }

    /// Count the number of elements in the RDD.
    ///
    /// In distributed mode, if a lazy pipeline is staged, it dispatches to workers
    /// and counts the returned elements on the driver. In local mode runs on driver.
    pub fn count(&self) -> Result<u64, BaseError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() && self.staged.is_some() {
            let elements = self.collect_distributed()?;
            return Ok(elements.len() as u64);
        }
        let counting_func = |iter: Box<dyn Iterator<Item = T>>| iter.count() as u64;
        Ok(self
            .context
            .run_job(self.rdd.clone(), counting_func)?
            .into_iter()
            .sum())
    }

    /// Take the first n elements from the RDD.
    ///
    /// In distributed mode, dispatches any staged pipeline to workers and takes
    /// the first `n` elements from the collected results. In local mode uses a
    /// partition-scanning strategy to minimise data read.
    pub fn take(&self, num: usize) -> Result<Vec<T>, BaseError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if num == 0 {
            return Ok(vec![]);
        }
        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            return Ok(elements.into_iter().take(num).collect());
        }
        // Local: partition-scanning strategy to minimise data read.
        const SCALE_UP_FACTOR: f64 = 2.0;
        let mut buf = vec![];
        let total_parts = self.num_partitions() as u32;
        let mut parts_scanned = 0_u32;

        while buf.len() < num && parts_scanned < total_parts {
            let mut num_parts_to_try = 1u32;
            let left = num - buf.len();
            if parts_scanned > 0 {
                let parts_scanned_f64 = f64::from(parts_scanned);
                num_parts_to_try = if buf.is_empty() {
                    (parts_scanned_f64 * SCALE_UP_FACTOR).ceil() as u32
                } else {
                    let num_parts =
                        (1.5 * left as f64 * parts_scanned_f64 / (buf.len() as f64)).ceil();
                    num_parts.min(parts_scanned_f64 * SCALE_UP_FACTOR) as u32
                };
            }

            let partitions: Vec<_> = (parts_scanned as usize
                ..total_parts.min(parts_scanned + num_parts_to_try) as usize)
                .collect();
            let num_partitions = partitions.len() as u32;
            let take_from_partition =
                move |iter: Box<dyn Iterator<Item = T>>| iter.take(left).collect::<Vec<T>>();

            let res = self.context.run_job_with_partitions(
                self.rdd.clone(),
                take_from_partition,
                partitions,
            )?;

            res.into_iter().for_each(|r| {
                let take = num - buf.len();
                buf.extend(r.into_iter().take(take));
            });

            parts_scanned += num_partitions;
        }

        Ok(buf)
    }

    /// Get the first element of the RDD.
    ///
    /// Returns an error if the RDD is empty.
    pub fn first(&self) -> Result<T, BaseError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if let Some(result) = self.take(1)?.into_iter().next() {
            Ok(result)
        } else {
            Err(BaseError::DowncastFailure("empty collection".to_string()))
        }
    }

    /// Returns `true` if the RDD contains no elements.
    pub fn is_empty(&self) -> Result<bool, BaseError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            return Ok(elements.is_empty());
        }
        Ok(self.take(1)?.is_empty())
    }

    /// Approximate count within a time budget.
    ///
    /// Samples `max(1, ceil(confidence × num_partitions))` partitions and extrapolates.
    /// `confidence` must be in `(0.0, 1.0]`; use `1.0` for a full (non-approximate) scan.
    /// The result is an estimate — the actual count may differ from the return value.
    pub fn count_approx(&self, confidence: f64) -> Result<u64, BaseError> {
        let n = self.num_partitions();
        let sample_n = ((confidence.clamp(0.001, 1.0) * n as f64).ceil() as usize)
            .max(1)
            .min(n);
        let sample_indices: Vec<usize> = (0..sample_n).collect();
        let counts = self
            .context
            .run_job_with_partitions(self.rdd.clone(), |iter| iter.count() as u64, sample_indices)
            .map_err(BaseError::from)?;
        let sampled_total: u64 = counts.iter().sum();
        let estimate = (sampled_total as f64 * n as f64 / sample_n as f64).round() as u64;
        Ok(estimate)
    }

    /// Aggregate elements with different accumulator and result types.
    ///
    /// In distributed mode, collects all elements from workers then applies
    /// `seq_fn` on the driver. `comb_fn` is unused in distributed mode since
    /// all elements are aggregated in a single pass on the driver.
    pub fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U, BaseError>
    where
        U: Data + Clone,
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            return Ok(elements.into_iter().fold(init, seq_fn));
        }
        let zero = init.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = T>>| iter.fold(zero.clone(), &seq_fn);
        let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
        Ok(results.into_iter().fold(init, comb_fn))
    }

    /// Reduce elements using a balanced binary tree of merge operations.
    ///
    /// More numerically stable than a linear `reduce` for large datasets, because partial
    /// results are merged in a balanced tree rather than accumulated left-to-right.
    /// `depth` controls the number of tree levels (default 2 is usually sufficient).
    ///
    /// Returns `None` if the RDD is empty.
    pub fn tree_reduce<F>(&self, f: F, depth: usize) -> Result<Option<T>, BaseError>
    where
        T: Clone,
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        let f_job = f.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = T>>| iter.reduce(|a, b| f_job(a, b));
        let mut partials: Vec<T> = self
            .context
            .run_job(self.rdd.clone(), reduce_partition)?
            .into_iter()
            .flatten()
            .collect();

        let levels = depth.max(1);
        for _ in 0..levels {
            if partials.len() <= 1 {
                break;
            }
            let mut next = Vec::with_capacity(partials.len() / 2 + 1);
            let mut iter = partials.into_iter();
            loop {
                match (iter.next(), iter.next()) {
                    (Some(a), Some(b)) => next.push(f(a, b)),
                    (Some(a), None) => next.push(a),
                    _ => break,
                }
            }
            partials = next;
        }
        Ok(partials.into_iter().next())
    }

    /// Aggregate elements using a balanced binary tree of combine operations.
    ///
    /// `seq_fn(acc, elem)` accumulates elements within each partition.
    /// `comb_fn(acc, acc)` merges partition accumulators in a balanced tree.
    /// `depth` controls the number of tree merge levels (default 2).
    pub fn tree_aggregate<U, SF, CF>(
        &self,
        zero: U,
        seq_fn: SF,
        comb_fn: CF,
        depth: usize,
    ) -> Result<U, BaseError>
    where
        U: Data + Clone,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        let z = zero.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = T>>| iter.fold(z.clone(), &seq_fn);
        let mut partials: Vec<U> = self.context.run_job(self.rdd.clone(), reduce_partition)?;

        let levels = depth.max(1);
        for _ in 0..levels {
            if partials.len() <= 1 {
                break;
            }
            let mut next = Vec::with_capacity(partials.len() / 2 + 1);
            let mut iter = partials.into_iter();
            loop {
                match (iter.next(), iter.next()) {
                    (Some(a), Some(b)) => next.push(comb_fn(a, b)),
                    (Some(a), None) => next.push(a),
                    _ => break,
                }
            }
            partials = next;
        }
        Ok(partials.into_iter().next().unwrap_or(zero))
    }

    /// Apply a function to each element (for side effects).
    ///
    /// In distributed mode, collects all elements from workers and applies `f`
    /// on the driver. The function runs on the driver, not on workers.
    pub fn for_each<F>(&self, f: F) -> Result<(), BaseError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        F: Fn(&T) + Clone + Send + Sync + 'static,
    {
        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            elements.iter().for_each(&f);
            return Ok(());
        }
        let for_each_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            iter.for_each(|x| f(&x));
        };
        self.context.run_job(self.rdd.clone(), for_each_partition)?;
        Ok(())
    }

    /// Apply a function to each partition (for side effects).
    ///
    /// In distributed mode, collects all elements from workers and passes them
    /// as a single iterator to `f` on the driver (partition boundaries are not
    /// preserved across the wire).
    pub fn for_each_partition<F>(&self, f: F) -> Result<(), BaseError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        F: Fn(Box<dyn Iterator<Item = T>>) + Clone + Send + Sync + 'static,
    {
        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            f(Box::new(elements.into_iter()));
            return Ok(());
        }
        self.context.run_job(self.rdd.clone(), f)?;
        Ok(())
    }

    /// Count the number of occurrences of each unique value.
    ///
    /// In distributed mode, collects all elements from workers then counts on
    /// the driver.
    pub fn count_by_value(&self) -> Result<std::collections::HashMap<T, u64>, BaseError>
    where
        T: Eq + std::hash::Hash + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        use std::collections::HashMap;

        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            let mut counts = HashMap::new();
            for item in elements {
                *counts.entry(item).or_insert(0) += 1;
            }
            return Ok(counts);
        }

        let count_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            let mut counts = HashMap::new();
            for item in iter {
                *counts.entry(item).or_insert(0) += 1;
            }
            counts
        };

        let partition_counts = self.context.run_job(self.rdd.clone(), count_partition)?;

        let mut final_counts = HashMap::new();
        for counts in partition_counts {
            for (k, v) in counts {
                *final_counts.entry(k).or_insert(0) += v;
            }
        }

        Ok(final_counts)
    }

    /// Return the maximum element.
    ///
    /// In distributed mode, if a lazy pipeline is staged (from `map_task` etc.),
    /// workers execute it and return partition results; the driver picks the global max.
    /// In local mode uses driver-local `iter.max()`.
    ///
    /// # Example
    /// ```ignore
    /// let max_val = rdd.max()?;
    /// ```
    pub fn max(&self) -> Result<Option<T>, BaseError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            // Collect the staged pipeline's output (or the raw partitions if no pipeline),
            // then compute max on the driver from the returned elements.
            let elements = self.collect_distributed()?;
            return Ok(elements.into_iter().max());
        }
        let max_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.max();
        let partition_maxes = self.context.run_job(self.rdd.clone(), max_partition)?;
        Ok(partition_maxes.into_iter().flatten().max())
    }

    /// Return the minimum element.
    ///
    /// In distributed mode dispatches the staged pipeline to workers (if any),
    /// then picks the global minimum on the driver.
    /// In local mode uses driver-local `iter.min()`.
    ///
    /// # Example
    /// ```ignore
    /// let min_val = rdd.min()?;
    /// ```
    pub fn min(&self) -> Result<Option<T>, BaseError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            return Ok(elements.into_iter().min());
        }
        let min_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.min();
        let partition_mins = self.context.run_job(self.rdd.clone(), min_partition)?;
        Ok(partition_mins.into_iter().flatten().min())
    }

    /// Internal helper: dispatch the staged pipeline (or raw partitions) to workers
    /// and return all elements as a flat `Vec<T>`. Used by `max`, `min`, `count`.
    fn collect_distributed(&self) -> Result<Vec<T>, BaseError>
    where
        T: WireEncode,
        Vec<T>: WireEncode + WireDecode,
    {
        let (source, ops) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.ops.clone()),
        };
        let result_bytes = self
            .context
            .dispatch_pipeline(source, ops)
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
        result_bytes
            .into_iter()
            .map(|b| {
                Vec::<T>::decode_wire(&b).map_err(|e| BaseError::DowncastFailure(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|vecs| vecs.into_iter().flatten().collect())
    }

    /// Return the top k elements in descending order.
    ///
    /// In distributed mode, collects all elements from workers then sorts on the driver.
    pub fn top(&self, k: usize) -> Result<Vec<T>, BaseError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let mut all_items = self.collect_distributed()?;
            all_items.sort_by(|a, b| b.cmp(a));
            all_items.truncate(k);
            return Ok(all_items);
        }
        let top_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            let mut items: Vec<T> = iter.collect();
            items.sort_by(|a, b| b.cmp(a));
            items.truncate(k);
            items
        };
        let partition_tops = self.context.run_job(self.rdd.clone(), top_partition)?;
        let mut all_items: Vec<T> = partition_tops.into_iter().flatten().collect();
        all_items.sort_by(|a, b| b.cmp(a));
        all_items.truncate(k);
        Ok(all_items)
    }

    /// Return the first k elements in ascending order.
    ///
    /// In distributed mode, collects all elements from workers then sorts on the driver.
    pub fn take_ordered(&self, k: usize) -> Result<Vec<T>, BaseError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let mut all_items = self.collect_distributed()?;
            all_items.sort();
            all_items.truncate(k);
            return Ok(all_items);
        }
        let take_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            let mut items: Vec<T> = iter.collect();
            items.sort();
            items.truncate(k);
            items
        };
        let partition_tops = self.context.run_job(self.rdd.clone(), take_partition)?;
        let mut all_items: Vec<T> = partition_tops.into_iter().flatten().collect();
        all_items.sort();
        all_items.truncate(k);
        Ok(all_items)
    }
}

use crate::rdd::union_rdd::UnionRdd;
use crate::rdd::zip::ZippedPartitionsRdd;

impl<T: Data> TypedRdd<T> {
    /// Union with another RDD - combine elements from both.
    ///
    /// # Example
    /// ```ignore
    /// let rdd1 = ctx.parallelize_typed(vec![1, 2, 3]);
    /// let rdd2 = ctx.parallelize_typed(vec![4, 5, 6]);
    /// let combined = rdd1.union(rdd2);
    /// ```
    pub fn union(self, other: TypedRdd<T>) -> TypedRdd<T>
    where
        T: Clone,
    {
        let id = self.context.new_rdd_id();
        let rdds = vec![self.rdd, other.rdd];
        // Two non-empty RDDs are always passed; UnionRdd::new only errors on empty input.
        let union_rdd = UnionRdd::new(id, &rdds)
            .expect("UnionRdd::new with two RDDs should never fail");
        TypedRdd::new(Arc::new(union_rdd), self.context)
    }

    /// Cartesian product with another RDD - all pairs (a, b).
    ///
    /// # Example
    /// ```ignore
    /// let rdd1 = ctx.parallelize_typed(vec![1, 2]);
    /// let rdd2 = ctx.parallelize_typed(vec!['a', 'b']);
    /// let pairs = rdd1.cartesian(rdd2); // [(1,'a'), (1,'b'), (2,'a'), (2,'b')]
    /// ```
    pub fn cartesian<U: Data + Clone>(self, other: TypedRdd<U>) -> TypedRdd<(T, U)>
    where
        T: Clone,
    {
        let id = self.context.new_rdd_id();
        let cart_rdd = CartesianRdd::new(id, self.rdd, other.rdd);
        TypedRdd::new(Arc::new(cart_rdd), self.context)
    }

    /// Zip this RDD with another element-wise.
    ///
    /// Both RDDs must have the same number of partitions and elements.
    ///
    /// # Example
    /// ```ignore
    /// let rdd1 = ctx.parallelize_typed(vec![1, 2, 3]);
    /// let rdd2 = ctx.parallelize_typed(vec!['a', 'b', 'c']);
    /// let zipped = rdd1.zip(rdd2); // [(1,'a'), (2,'b'), (3,'c')]
    /// ```
    pub fn zip<U: Data + Clone>(self, other: TypedRdd<U>) -> TypedRdd<(T, U)>
    where
        T: Clone,
    {
        let id = self.context.new_rdd_id();
        let zipped_rdd = ZippedPartitionsRdd::new(id, self.rdd, other.rdd);
        TypedRdd::new(Arc::new(zipped_rdd), self.context)
    }

    pub fn distinct(self) -> TypedRdd<T>
    where
        T: Eq + std::hash::Hash + Clone,
    {
        use std::collections::HashSet;

        let dedup =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set: HashSet<T> = iter.collect();
                Box::new(set.into_iter())
            };

        self.map_rdd(|id, rdd| MapPartitionsRdd::new(id, rdd, dedup))
    }

    /// Return a new RDD containing elements only in this RDD but not in `other`.
    pub fn subtract(self, other: TypedRdd<T>) -> TypedRdd<T>
    where
        T: Eq + std::hash::Hash + Clone,
    {
        use std::collections::HashSet;
        let other_elems = self
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<T>>())
            .unwrap_or_default();
        let other_set: Arc<HashSet<T>> = Arc::new(other_elems.into_iter().flatten().collect());
        let other_set_clone = Arc::clone(&other_set);
        let filter_fn =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set = Arc::clone(&other_set_clone);
                Box::new(
                    iter.filter(move |x| !set.contains(x))
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            };
        let id = self.context.new_rdd_id();
        let subtract_rdd = MapPartitionsRdd::new(id, self.rdd, filter_fn);
        TypedRdd::new(Arc::new(subtract_rdd), self.context)
    }

    pub fn intersection(self, other: TypedRdd<T>) -> TypedRdd<T>
    where
        T: Eq + std::hash::Hash + Clone,
    {
        use std::collections::HashSet;
        let other_elems = self
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<T>>())
            .unwrap_or_default();
        let other_set: Arc<HashSet<T>> = Arc::new(other_elems.into_iter().flatten().collect());
        let other_set_clone = Arc::clone(&other_set);
        let filter_fn =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set = Arc::clone(&other_set_clone);
                Box::new(
                    iter.filter(move |x| set.contains(x))
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            };
        let id = self.context.new_rdd_id();
        let intersect_rdd = MapPartitionsRdd::new(id, self.rdd, filter_fn);
        TypedRdd::new(Arc::new(intersect_rdd), self.context)
    }
}

impl<T: Data + Clone> TypedRdd<T> {
    /// Reduce the number of partitions by coalescing.
    ///
    /// This is a narrow transformation if reducing partitions.
    ///
    /// # Example
    /// ```ignore
    /// let coalesced = rdd.coalesce(2, false);
    /// ```
    /// `shuffle=true` reassigns whole partitions only (no element-level redistribution).
    /// For element-level redistribution use [`repartition_shuffle`].
    /// `shuffle=true` reassigns whole partitions only (no element-level redistribution).
    /// For element-level redistribution use [`repartition_shuffle`].
    pub fn coalesce(self, num_partitions: usize, _shuffle: bool) -> TypedRdd<T>
    where
        T: Clone,
    {
        // Both shuffle=true and shuffle=false use CoalescedRdd (whole-partition reassignment).
        // Use repartition_shuffle for element-level redistribution.
        self.map_rdd(|id, rdd| CoalescedRdd::new(id, rdd, num_partitions))
    }

    pub fn repartition(self, num_partitions: usize) -> TypedRdd<T> {
        self.coalesce(num_partitions, true)
    }

    /// Repartition by shuffling individual elements across `num_partitions` partitions,
    /// eliminating data skew. Unlike `repartition`, elements are redistributed evenly rather
    /// than whole partitions being reassigned.
    ///
    /// Requires T to be bincode-serializable (needed for the shuffle wire format).
    /// The call site must also have `register_shuffle_map!(usize, T)` in scope so the shuffle
    /// type registry is populated.
    pub fn repartition_shuffle(self, num_partitions: usize) -> TypedRdd<T>
    where
        T: Data + Clone + bincode::Encode + bincode::Decode<()>,
        Vec<(usize, T)>: WireEncode,
    {
        use atomic_data::partitioner::CustomPartitioner;

        // Identity partitioner: bucket i → partition i (bypasses hash skew on small integers).
        struct BucketPartitioner(usize);
        impl CustomPartitioner for BucketPartitioner {
            fn num_partitions(&self) -> usize {
                self.0
            }
            fn get_partition_for_key(&self, key: &dyn std::any::Any) -> usize {
                *key.downcast_ref::<usize>().unwrap_or(&0) % self.0
            }
        }

        let np = num_partitions;
        let partitioner = Partitioner::from_custom(BucketPartitioner(np));

        self.map_partitions_to_pair(move |idx, iter| {
            Box::new(iter.enumerate().map(move |(i, elem)| {
                let bucket = (idx * np + i) % np;
                (bucket, elem)
            })) as Box<dyn Iterator<Item = (usize, T)>>
        })
        .combine_by_key_with_partitioner(
            |v| vec![v],
            |mut buf, v| {
                buf.push(v);
                buf
            },
            |mut a, mut b| {
                a.append(&mut b);
                a
            },
            partitioner,
        )
        .map_partitions(|iter| {
            Box::new(iter.flat_map(|(_, vs)| vs.into_iter())) as Box<dyn Iterator<Item = T>>
        })
    }

    /// Apply a function to each partition.
    ///
    /// # Example
    /// ```ignore
    /// let processed = rdd.map_partitions(|iter| {
    ///     Box::new(iter.map(|x| x * 2))
    /// });
    /// ```
    pub fn map_partitions<U, F>(self, f: F) -> TypedRdd<U>
    where
        U: Data + Clone,
        F: Fn(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>>
            + Send
            + Sync
            + Clone
            + 'static,
    {
        let ignore_idx = move |_index: usize, items: Box<dyn Iterator<Item = T>>| f(items);
        self.map_rdd(|id, rdd| MapPartitionsRdd::new(id, rdd, ignore_idx))
    }

    /// Apply a function to each partition with partition index.
    ///
    /// # Example
    /// ```ignore
    /// let indexed = rdd.map_partitions_with_index(|idx, iter| {
    ///     Box::new(iter.map(move |x| (idx, x)))
    /// });
    /// ```
    pub fn map_partitions_with_index<U, F>(self, f: F) -> TypedRdd<U>
    where
        U: Data + Clone,
        F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>>
            + Send
            + Sync
            + Clone
            + 'static,
    {
        self.map_rdd(|id, rdd| MapPartitionsRdd::new(id, rdd, f))
    }

    /// Apply a function to each partition, producing `(K, V)` pair items.
    ///
    /// Unlike `map_partitions`, the result correctly participates in `reduce_by_key`, `join`,
    /// and `cogroup` operations because the pair-aware `cogroup_iterator_any` protocol is
    /// implemented.
    pub fn map_partitions_to_pair<K, V, F>(self, f: F) -> TypedRdd<(K, V)>
    where
        K: Data + Clone,
        V: Data + Clone,
        F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = (K, V)>>
            + Send
            + Sync
            + Clone
            + 'static,
    {
        self.map_rdd(|id, rdd| MapPartitionsPairRdd::new(id, rdd, f))
    }

    /// Collect each partition into a `Vec<T>`, yielding one `Vec` per partition.
    pub fn glom(self) -> TypedRdd<Vec<T>>
    where
        T: Clone,
    {
        let func = |_index: usize, iter: Box<dyn Iterator<Item = T>>| {
            Box::new(std::iter::once(iter.collect::<Vec<_>>())) as Box<dyn Iterator<Item = Vec<T>>>
        };
        self.map_rdd(|id, rdd| MapPartitionsRdd::new(id, rdd, func))
    }
}

impl<K, V> TypedRdd<(K, V)>
where
    K: Data + Eq + std::hash::Hash + Clone,
    V: Data + Clone,
{
    pub fn keys(self) -> TypedRdd<K> {
        self.map_rdd(|id, rdd| MapperRdd::new(id, rdd, |(k, _v)| k))
    }

    pub fn values(self) -> TypedRdd<V> {
        self.map_rdd(|id, rdd| MapperRdd::new(id, rdd, |(_k, v)| v))
    }

    pub fn map_values<U, F>(self, f: F) -> TypedRdd<(K, U)>
    where
        U: Data + Clone,
        F: Fn(V) -> U + Clone + Send + Sync + 'static,
    {
        self.map_rdd(|id, rdd| MapperRdd::new(id, rdd, move |(k, v)| (k, f(v))))
    }

    /// Like `flat_map` but only applied to values — keys are preserved and emitted once per output value.
    pub fn flat_map_values<U, F>(self, f: F) -> TypedRdd<(K, U)>
    where
        K: Data + Clone,
        V: Data + Clone,
        U: Data + Clone,
        F: Fn(V) -> Box<dyn Iterator<Item = U>> + Clone + Send + Sync + 'static,
    {
        self.map_rdd(|id, rdd| FlatMappedValuesRdd::new(id, rdd, f))
    }

    /// Combine values for each key using three aggregation functions.
    ///
    /// - `create_combiner(V) -> C`: starts a combiner for the first value of a key.
    /// - `merge_value(C, V) -> C`: merges a new value into an existing combiner.
    /// - `merge_combiners(C, C) -> C`: merges two combiners (for cross-partition merging).
    ///
    /// This is the generalisation of `reduce_by_key` (`C = V`) and `group_by_key` (`C = Vec<V>`).
    pub fn combine_by_key<C, CC, MV, MC>(
        self,
        create_combiner: CC,
        merge_value: MV,
        merge_combiners: MC,
        num_partitions: usize,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        CC: Fn(V) -> C + Clone + Send + Sync + 'static,
        MV: Fn(C, V) -> C + Clone + Send + Sync + 'static,
        MC: Fn(C, C) -> C + Clone + Send + Sync + 'static,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let mv2 = merge_value.clone();
        let mc2 = merge_combiners.clone();
        let aggregator = Arc::new(Aggregator::<K, V, C>::new(
            Arc::new(move |v: V| create_combiner(v)),
            Arc::new(move |c: &mut C, v: V| *c = mv2(c.clone(), v)),
            Arc::new(move |c1: &mut C, c2: C| *c1 = mc2(c1.clone(), c2)),
        ));

        let partitioner = Partitioner::hash::<K>(num_partitions.max(1));
        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();
        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));

        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };

        let shuffled = ShuffledRdd::<K, V, C>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Re-partition this pair RDD using a user-defined `CustomPartitioner`.
    ///
    /// All existing `(K, V)` pairs are preserved; only the partition assignment changes.
    /// Triggers a shuffle.
    pub fn partition_by<P>(self, partitioner: P) -> TypedRdd<(K, V)>
    where
        P: atomic_data::partitioner::CustomPartitioner + 'static,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let p = Partitioner::from_custom(partitioner);
        self.combine_by_key_with_partitioner(|v| v, |_, v| v, |c, _| c, p)
    }

    /// Internal: `combine_by_key` with an explicit `Partitioner` instead of hash.
    fn combine_by_key_with_partitioner<C, CC, MV, MC>(
        self,
        create_combiner: CC,
        merge_value: MV,
        merge_combiners: MC,
        partitioner: Partitioner,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        CC: Fn(V) -> C + Clone + Send + Sync + 'static,
        MV: Fn(C, V) -> C + Clone + Send + Sync + 'static,
        MC: Fn(C, C) -> C + Clone + Send + Sync + 'static,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let mv2 = merge_value.clone();
        let mc2 = merge_combiners.clone();
        let aggregator = Arc::new(Aggregator::<K, V, C>::new(
            Arc::new(move |v: V| create_combiner(v)),
            Arc::new(move |c: &mut C, v: V| *c = mv2(c.clone(), v)),
            Arc::new(move |c1: &mut C, c2: C| *c1 = mc2(c1.clone(), c2)),
        ));

        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();
        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));

        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };

        let shuffled = ShuffledRdd::<K, V, C>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Fold values for each key with an initial zero value.
    ///
    /// Equivalent to `combine_by_key` where the combiner type equals the value type.
    /// `zero` must be a neutral element: `f(zero.clone(), v) == v`.
    pub fn fold_by_key<F>(self, zero: V, f: F, num_partitions: usize) -> TypedRdd<(K, V)>
    where
        F: Fn(V, V) -> V + Clone + Send + Sync + 'static,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let f1 = f.clone();
        let f2 = f.clone();
        let f3 = f;
        let z1 = zero.clone();
        self.combine_by_key(
            move |v| f1(z1.clone(), v),
            move |c, v| f2(c, v),
            move |c1, c2| f3(c1, c2),
            num_partitions,
        )
    }

    /// Aggregate values for each key with a different accumulator type.
    ///
    /// `zero` is the initial accumulator value per partition.
    /// `seq_fn(acc, value)` merges a value into the partition accumulator.
    /// `comb_fn(acc1, acc2)` merges two partition accumulators on the driver.
    pub fn aggregate_by_key<C, SF, CF>(
        self,
        zero: C,
        seq_fn: SF,
        comb_fn: CF,
        num_partitions: usize,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        SF: Fn(C, V) -> C + Clone + Send + Sync + 'static,
        CF: Fn(C, C) -> C + Clone + Send + Sync + 'static,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let z = zero;
        self.combine_by_key(move |_v| z.clone(), seq_fn, comb_fn, num_partitions)
    }

    /// Return elements whose key is NOT present in `other`.
    ///
    /// Collects all keys from `other` to the driver, then filters `self` to exclude them.
    pub fn subtract_by_key<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, V)>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashSet;
        let ctx = self.context.clone();
        let id = ctx.new_rdd_id();

        let other_parts = other
            .context
            .run_job(other.rdd, |iter| iter.map(|(k, _)| k).collect::<Vec<K>>())
            .unwrap_or_default();
        let excluded: Arc<HashSet<K>> = Arc::new(other_parts.into_iter().flatten().collect());

        let rdd = Arc::new(MapPartitionsRdd::new(id, self.rdd, move |_idx, iter| {
            let excl = excluded.clone();
            Box::new(iter.filter(move |(k, _)| !excl.contains(k)))
                as Box<dyn Iterator<Item = (K, V)>>
        }));
        TypedRdd::new(rdd, ctx)
    }

    /// Reduce values for each key using an associative function.
    ///
    /// Produces a globally correct result by creating a shuffle dependency (like Spark).
    /// The shuffle stage repartitions data by key; each output partition is independently
    /// reduced. `collect()` triggers the full map → shuffle → reduce pipeline.
    ///
    /// # Example
    /// ```ignore
    /// let sums = pair_rdd.reduce_by_key(|a, b| a + b);
    /// ```
    pub fn reduce_by_key<F>(self, f: F) -> TypedRdd<(K, V)>
    where
        F: Fn(V, V) -> V + Clone + Send + Sync + 'static,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let f2 = f.clone();
        let _f3 = f.clone();
        let aggregator = Arc::new(Aggregator::<K, V, V>::new(
            Arc::new(|v: V| v),
            Arc::new(move |c: &mut V, v: V| *c = f(c.clone(), v)),
            Arc::new(move |c1: &mut V, c2: V| *c1 = f2(c1.clone(), c2)),
        ));

        let num_output_partitions = self.context.default_parallelism().max(1);
        let partitioner = Partitioner::hash::<K>(num_output_partitions);
        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();

        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));

        // In distributed mode, if a staged pipeline precedes the shuffle, carry its
        // source partitions + ops into the ShuffleDependencyBox so the workers receive
        // real data and the correct preceding ops (instead of the placeholder RDD).
        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };

        let shuffled = ShuffledRdd::<K, V, V>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Group values for each key.
    ///
    /// Produces a globally correct result by creating a shuffle dependency (like Spark).
    /// All values for a key are gathered from across partitions into a single `Vec<V>`
    /// per key after the shuffle stage completes.
    ///
    /// # Example
    /// ```ignore
    /// let grouped = pair_rdd.group_by_key();
    /// ```
    pub fn group_by_key(self) -> TypedRdd<(K, Vec<V>)>
    where
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let n = self.context.default_parallelism().max(1);
        self.group_by_key_n(n)
    }

    /// Like `group_by_key` but shuffles into exactly `num_partitions` output partitions.
    /// Used internally by `cogroup_shuffle` to guarantee both sides use the same partitioner.
    fn group_by_key_n(self, num_partitions: usize) -> TypedRdd<(K, Vec<V>)>
    where
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let aggregator = Arc::new(Aggregator::<K, V, Vec<V>>::default());
        let partitioner = Partitioner::hash::<K>(num_partitions);
        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();
        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));
        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };
        let shuffled = ShuffledRdd::<K, V, Vec<V>>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    pub fn count_by_key(&self) -> Result<std::collections::HashMap<K, u64>, BaseError> {
        use std::collections::HashMap;

        let count_partition = move |iter: Box<dyn Iterator<Item = (K, V)>>| {
            let mut counts: HashMap<K, u64> = HashMap::new();
            for (k, _v) in iter {
                *counts.entry(k).or_insert(0) += 1;
            }
            counts
        };

        let partition_counts = self.context.run_job(self.rdd.clone(), count_partition)?;

        let mut final_counts = HashMap::new();
        for counts in partition_counts {
            for (k, v) in counts {
                *final_counts.entry(k).or_insert(0) += v;
            }
        }

        Ok(final_counts)
    }

    pub fn lookup(&self, key: &K) -> Result<Vec<V>, BaseError>
    where
        K: Clone,
    {
        let key_clone = key.clone();
        let lookup_partition = move |iter: Box<dyn Iterator<Item = (K, V)>>| {
            iter.filter(|(k, _)| k == &key_clone)
                .map(|(_, v)| v)
                .collect::<Vec<V>>()
        };

        let partition_values = self.context.run_job(self.rdd.clone(), lookup_partition)?;
        Ok(partition_values.into_iter().flatten().collect())
    }

    /// Collect a pair RDD into a `HashMap<K, V>`.
    ///
    /// When a key appears multiple times, the last value encountered wins.
    /// Equivalent to Spark's `collectAsMap()`.
    pub fn collect_as_map(&self) -> Result<std::collections::HashMap<K, V>, BaseError>
    where
        K: std::hash::Hash + Eq,
    {
        let partitions = self
            .context
            .run_job(self.rdd.clone(), |iter| iter.collect::<Vec<(K, V)>>())?;
        let mut map = std::collections::HashMap::new();
        for pairs in partitions {
            for (k, v) in pairs {
                map.insert(k, v);
            }
        }
        Ok(map)
    }

    /// Inner join — collects both sides to the driver (small-data path).
    ///
    /// For distributed workloads, use `join` which shuffles both sides.
    pub fn join_local<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, (V, U))>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashMap;
        let ctx = self.context.clone();
        let num_partitions = self.rdd.number_of_splits();

        let left_parts = ctx
            .run_job(self.rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();
        let mut left_map: HashMap<K, Vec<V>> = HashMap::new();
        for partition in left_parts {
            for (k, v) in partition {
                left_map.entry(k).or_default().push(v);
            }
        }

        let right_parts = other
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<(K, U)>>())
            .unwrap_or_default();
        let mut result: Vec<(K, (V, U))> = Vec::new();
        for partition in right_parts {
            for (k, u) in partition {
                if let Some(vs) = left_map.get(&k) {
                    for v in vs {
                        result.push((k.clone(), (v.clone(), u.clone())));
                    }
                }
            }
        }
        ctx.parallelize_typed(result, num_partitions)
    }

    /// Left outer join — collects both sides to the driver (small-data path).
    pub fn left_outer_join_local<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, (V, Option<U>))>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashMap;
        let ctx = self.context.clone();
        let num_partitions = self.rdd.number_of_splits();

        let right_parts = other
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<(K, U)>>())
            .unwrap_or_default();
        let mut right_map: HashMap<K, Vec<U>> = HashMap::new();
        for partition in right_parts {
            for (k, u) in partition {
                right_map.entry(k).or_default().push(u);
            }
        }

        let left_parts = ctx
            .run_job(self.rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();
        let mut result: Vec<(K, (V, Option<U>))> = Vec::new();
        for partition in left_parts {
            for (k, v) in partition {
                match right_map.get(&k) {
                    Some(us) => {
                        for u in us {
                            result.push((k.clone(), (v.clone(), Some(u.clone()))));
                        }
                    }
                    None => result.push((k.clone(), (v.clone(), None))),
                }
            }
        }
        ctx.parallelize_typed(result, num_partitions)
    }

    /// Right outer join — collects both sides to the driver (small-data path).
    pub fn right_outer_join_local<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, (Option<V>, U))>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashMap;
        let ctx = self.context.clone();
        let num_partitions = self.rdd.number_of_splits();

        let left_parts = ctx
            .run_job(self.rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();
        let mut left_map: HashMap<K, Vec<V>> = HashMap::new();
        for partition in left_parts {
            for (k, v) in partition {
                left_map.entry(k).or_default().push(v);
            }
        }

        let right_parts = other
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<(K, U)>>())
            .unwrap_or_default();
        let mut result: Vec<(K, (Option<V>, U))> = Vec::new();
        for partition in right_parts {
            for (k, u) in partition {
                match left_map.get(&k) {
                    Some(vs) => {
                        for v in vs {
                            result.push((k.clone(), (Some(v.clone()), u.clone())));
                        }
                    }
                    None => result.push((k.clone(), (None, u))),
                }
            }
        }
        ctx.parallelize_typed(result, num_partitions)
    }

    /// Full outer join — collects both sides to the driver (small-data path).
    pub fn full_outer_join_local<U>(
        self,
        other: TypedRdd<(K, U)>,
    ) -> TypedRdd<(K, (Option<V>, Option<U>))>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashMap;
        let ctx = self.context.clone();
        let num_partitions = self.rdd.number_of_splits();

        let left_parts = ctx
            .run_job(self.rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();
        let right_parts = other
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<(K, U)>>())
            .unwrap_or_default();

        let mut left_map: HashMap<K, Vec<V>> = HashMap::new();
        for partition in left_parts {
            for (k, v) in partition {
                left_map.entry(k).or_default().push(v);
            }
        }
        let mut right_map: HashMap<K, Vec<U>> = HashMap::new();
        for partition in right_parts {
            for (k, u) in partition {
                right_map.entry(k).or_default().push(u);
            }
        }

        let mut result: Vec<(K, (Option<V>, Option<U>))> = Vec::new();
        for (k, vs) in &left_map {
            match right_map.get(k) {
                Some(us) => {
                    for v in vs {
                        for u in us {
                            result.push((k.clone(), (Some(v.clone()), Some(u.clone()))));
                        }
                    }
                }
                None => {
                    for v in vs {
                        result.push((k.clone(), (Some(v.clone()), None)));
                    }
                }
            }
        }
        for (k, us) in &right_map {
            if !left_map.contains_key(k) {
                for u in us {
                    result.push((k.clone(), (None, Some(u.clone()))));
                }
            }
        }
        ctx.parallelize_typed(result, num_partitions)
    }

    /// Cogroup — collects both sides to the driver (small-data path).
    pub fn cogroup_local<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, Vec<V>, Vec<U>)>
    where
        U: Data + Clone,
        K: Eq + std::hash::Hash,
        Vec<(K, V)>: Data + Clone,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashMap;
        let ctx = self.context.clone();
        let num_partitions = self.rdd.number_of_splits();

        let left_parts = ctx
            .run_job(self.rdd, |iter| iter.collect::<Vec<(K, V)>>())
            .unwrap_or_default();
        let right_parts = other
            .context
            .run_job(other.rdd, |iter| iter.collect::<Vec<(K, U)>>())
            .unwrap_or_default();

        let mut agg: HashMap<K, (Vec<V>, Vec<U>)> = HashMap::new();
        for partition in left_parts {
            for (k, v) in partition {
                agg.entry(k).or_default().0.push(v);
            }
        }
        for partition in right_parts {
            for (k, u) in partition {
                agg.entry(k).or_default().1.push(u);
            }
        }

        let result: Vec<(K, Vec<V>, Vec<U>)> =
            agg.into_iter().map(|(k, (vs, us))| (k, vs, us)).collect();
        ctx.parallelize_typed(result, num_partitions)
    }

    /// Co-group two pair RDDs using two shuffle stages — no full-dataset collect on the driver.
    ///
    /// Both sides are shuffled into `num_partitions` buckets using the same hash partitioner.
    /// Since both use identical partitioning, partition `i` from the left and partition `i`
    /// from the right contain exactly the same set of keys.  A narrow `CoGroupedRdd` merge
    /// then combines them locally per partition.
    ///
    /// Returns `(K, Vec<V>, Vec<U>)` for every key that appears in either side.
    pub fn cogroup_shuffle<U>(
        self,
        other: TypedRdd<(K, U)>,
        num_partitions: usize,
    ) -> TypedRdd<(K, Vec<V>, Vec<U>)>
    where
        U: Data + Clone + bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
        Vec<(K, U)>: WireEncode,
    {
        use crate::rdd::co_grouped::CoGroupedRdd;
        let ctx = self.context.clone();

        let left_grouped = self.group_by_key_n(num_partitions);
        let right_grouped = other.group_by_key_n(num_partitions);

        let id = ctx.new_rdd_id();
        let cogroup_rdd =
            CoGroupedRdd::<K, Vec<V>, Vec<U>>::new(id, left_grouped.rdd, right_grouped.rdd);

        // After group_by_key_n each key appears at most once per partition, so outer vecs
        // have length 0 (absent) or 1 (present). Flatten to (K, Vec<V>, Vec<U>).
        TypedRdd::new(Arc::new(cogroup_rdd), ctx).map_partitions(|iter| {
            Box::new(iter.map(|(k, vvs, vus)| {
                let vs: Vec<V> = vvs.into_iter().flatten().collect();
                let us: Vec<U> = vus.into_iter().flatten().collect();
                (k, vs, us)
            })) as Box<dyn Iterator<Item = (K, Vec<V>, Vec<U>)>>
        })
    }

    /// Cogroup two pair RDDs via shuffle — distributed, no driver-side collect.
    ///
    /// Equivalent to `cogroup_shuffle(other, self.num_partitions())`.
    pub fn cogroup<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, Vec<V>, Vec<U>)>
    where
        U: Data + Clone + bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
        Vec<(K, U)>: WireEncode,
    {
        let n = self.rdd.number_of_splits();
        self.cogroup_shuffle(other, n)
    }

    /// Inner join via shuffle — distributed, no driver-side collect.
    pub fn join<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, (V, U))>
    where
        U: Data + Clone + bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
        Vec<(K, U)>: WireEncode,
    {
        let n = self.rdd.number_of_splits();
        self.cogroup_shuffle(other, n).map_partitions(|iter| {
            Box::new(iter.flat_map(|(k, vs, us)| {
                vs.into_iter().flat_map(move |v| {
                    let k = k.clone();
                    let us = us.clone();
                    us.into_iter().map(move |u| (k.clone(), (v.clone(), u)))
                })
            })) as Box<dyn Iterator<Item = (K, (V, U))>>
        })
    }

    /// Left outer join via shuffle — every left key is preserved; missing right keys produce `None`.
    pub fn left_outer_join<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, (V, Option<U>))>
    where
        U: Data + Clone + bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
        Vec<(K, U)>: WireEncode,
    {
        let n = self.rdd.number_of_splits();
        self.cogroup_shuffle(other, n).map_partitions(|iter| {
            Box::new(iter.flat_map(|(k, vs, us)| {
                if us.is_empty() {
                    let k = k.clone();
                    Box::new(vs.into_iter().map(move |v| (k.clone(), (v, None))))
                        as Box<dyn Iterator<Item = (K, (V, Option<U>))>>
                } else {
                    Box::new(vs.into_iter().flat_map(move |v| {
                        let k = k.clone();
                        let us = us.clone();
                        us.into_iter()
                            .map(move |u| (k.clone(), (v.clone(), Some(u))))
                    })) as Box<dyn Iterator<Item = (K, (V, Option<U>))>>
                }
            })) as Box<dyn Iterator<Item = (K, (V, Option<U>))>>
        })
    }

    /// Right outer join via shuffle — every right key is preserved; missing left keys produce `None`.
    pub fn right_outer_join<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, (Option<V>, U))>
    where
        U: Data + Clone + bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
        Vec<(K, U)>: WireEncode,
    {
        let n = self.rdd.number_of_splits();
        self.cogroup_shuffle(other, n).map_partitions(|iter| {
            Box::new(iter.flat_map(|(k, vs, us)| {
                if vs.is_empty() {
                    let k = k.clone();
                    Box::new(us.into_iter().map(move |u| (k.clone(), (None, u))))
                        as Box<dyn Iterator<Item = (K, (Option<V>, U))>>
                } else {
                    Box::new(us.into_iter().flat_map(move |u| {
                        let k = k.clone();
                        let vs = vs.clone();
                        vs.into_iter()
                            .map(move |v| (k.clone(), (Some(v), u.clone())))
                    })) as Box<dyn Iterator<Item = (K, (Option<V>, U))>>
                }
            })) as Box<dyn Iterator<Item = (K, (Option<V>, U))>>
        })
    }

    /// Full outer join via shuffle — all keys from both sides are preserved.
    pub fn full_outer_join<U>(
        self,
        other: TypedRdd<(K, U)>,
    ) -> TypedRdd<(K, (Option<V>, Option<U>))>
    where
        U: Data + Clone + bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
        Vec<(K, U)>: WireEncode,
    {
        let n = self.rdd.number_of_splits();
        self.cogroup_shuffle(other, n).map_partitions(|iter| {
            Box::new(
                iter.flat_map(|(k, vs, us)| match (vs.is_empty(), us.is_empty()) {
                    (true, false) => {
                        let k = k.clone();
                        Box::new(us.into_iter().map(move |u| (k.clone(), (None, Some(u)))))
                            as Box<dyn Iterator<Item = (K, (Option<V>, Option<U>))>>
                    }
                    (false, true) => {
                        let k = k.clone();
                        Box::new(vs.into_iter().map(move |v| (k.clone(), (Some(v), None))))
                            as Box<dyn Iterator<Item = (K, (Option<V>, Option<U>))>>
                    }
                    _ => Box::new(vs.into_iter().flat_map(move |v| {
                        let k = k.clone();
                        let us = us.clone();
                        us.into_iter()
                            .map(move |u| (k.clone(), (Some(v.clone()), Some(u))))
                    }))
                        as Box<dyn Iterator<Item = (K, (Option<V>, Option<U>))>>,
                }),
            ) as Box<dyn Iterator<Item = (K, (Option<V>, Option<U>))>>
        })
    }
}

impl<T: Data> TypedRdd<T> {
    pub fn key_by<K, F>(self, f: F) -> TypedRdd<(K, T)>
    where
        K: Data + Clone,
        T: Clone,
        F: Fn(&T) -> K + Clone + Send + Sync + 'static,
    {
        let id = self.context.new_rdd_id();
        TypedRdd::new(
            Arc::new(MapperRdd::new(id, self.rdd, move |x| {
                let key = f(&x);
                (key, x)
            })),
            self.context,
        )
    }

    /// Write each partition as a text file.
    ///
    /// URI schemes:
    /// - `s3://bucket/prefix` — uploads `part-N` objects to that prefix (requires `s3` feature).
    /// - Local path — creates the directory and writes `part-N` files inside it.
    ///
    /// Each element is converted to a string via `Display` and written as one line.
    pub fn save_as_text_file(&self, uri: &str) -> Result<(), BaseError>
    where
        T: std::fmt::Display + Clone,
    {
        if uri.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                use crate::io::s3::s3_impl::{S3Uri, write_text};
                let s3uri = S3Uri::parse(uri).ok_or_else(|| {
                    BaseError::Other(format!("save_as_text_file: invalid S3 URI: {uri}"))
                })?;
                for (idx, partition) in self.collect_partitions()?.into_iter().enumerate() {
                    let key = format!("{}/part-{idx}", s3uri.key.trim_end_matches('/'));
                    let content: String = partition
                        .into_iter()
                        .map(|item| format!("{item}\n"))
                        .collect();
                    write_text(&s3uri.bucket, &key, content).map_err(|e| BaseError::Other(e))?;
                }
                return Ok(());
            }
            #[cfg(not(feature = "s3"))]
            {
                return Err(BaseError::Other(
                    "save_as_text_file: s3:// URI requires the 's3' feature flag".to_owned(),
                ));
            }
        }

        let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
        std::fs::create_dir_all(path).map_err(|e| {
            BaseError::Other(format!(
                "save_as_text_file: cannot create dir {}: {e}",
                path.display()
            ))
        })?;
        for (idx, partition) in self.collect_partitions()?.into_iter().enumerate() {
            use std::io::Write;
            let file_path = path.join(format!("part-{idx}"));
            let mut f = std::fs::File::create(&file_path).map_err(|e| {
                BaseError::Other(format!("save_as_text_file: {}: {e}", file_path.display()))
            })?;
            for item in partition {
                writeln!(f, "{item}").map_err(|e| BaseError::Other(e.to_string()))?;
            }
        }
        Ok(())
    }
}

impl<T: Data> TypedRdd<T> {
    /// Return a multi-line string describing the RDD's lineage (DAG).
    ///
    /// Each line shows one RDD node in the dependency chain, indented by depth.
    /// Shuffle boundaries are annotated with `[Shuffle]`; narrow dependencies with `[Narrow]`.
    ///
    /// Useful for understanding what transformations will run and where shuffles occur.
    pub fn to_debug_string(&self) -> String {
        fn describe(rdd: &dyn RddBase, depth: usize, out: &mut String) {
            let indent = "  ".repeat(depth);
            let name = rdd.get_op_name();
            out.push_str(&format!(
                "{indent}({depth}) {name} [id={}]\n",
                rdd.get_rdd_id()
            ));
            for dep in rdd.get_dependencies() {
                match &dep {
                    Dependency::OneToOne { rdd_base } => {
                        out.push_str(&format!("{indent}  +- [Narrow]\n"));
                        describe(rdd_base.as_ref(), depth + 1, out);
                    }
                    Dependency::Range {
                        rdd_base,
                        in_start,
                        out_start,
                        length,
                    } => {
                        out.push_str(&format!(
                            "{indent}  +- [Range in={in_start}..{} out={out_start}]\n",
                            in_start + length
                        ));
                        describe(rdd_base.as_ref(), depth + 1, out);
                    }
                    Dependency::CoalescedSplitDep { rdd: inner, .. } => {
                        out.push_str(&format!("{indent}  +- [Coalesced]\n"));
                        describe(inner.as_ref(), depth + 1, out);
                    }
                    Dependency::Shuffle(sd) => {
                        out.push_str(&format!(
                            "{indent}  +- [Shuffle id={}] partitions={}\n",
                            sd.get_shuffle_id(),
                            sd.get_num_output_partitions()
                        ));
                        describe(sd.get_rdd_base().as_ref(), depth + 1, out);
                    }
                }
            }
        }
        let mut out = String::new();
        describe(self.rdd.as_ref(), 0, &mut out);
        out
    }
}

impl<T: Data + Clone + 'static> TypedRdd<T> {
    /// Return a sampled subset of this RDD.
    ///
    /// - `with_replacement = true`  → Poisson sampling (each element may appear multiple times)
    /// - `with_replacement = false` → Bernoulli sampling (each element included at most once)
    ///
    /// Works in both local and distributed mode — sampling runs per-partition.
    pub fn sample(self, with_replacement: bool, fraction: f64) -> TypedRdd<T> {
        use atomic_utils::random::BernoulliSampler;
        use atomic_utils::random::PoissonSampler;
        let id = self.context.new_rdd_id();
        let sampler: Arc<dyn atomic_utils::random::RandomSampler<T>> = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true))
        } else {
            Arc::new(BernoulliSampler::new(fraction))
        };
        let rdd = PartitionwiseSampledRdd::new(id, self.rdd, sampler, false);
        TypedRdd::new(Arc::new(rdd), self.context)
    }

    /// Sort elements using a key function, returning a new RDD.
    ///
    /// Collects all data to the driver, sorts, and re-parallelizes into the same number
    /// of partitions. For distributed mode this is a driver-side global sort; a proper
    /// range-shuffle sort is tracked in ROADMAP P2.
    pub fn sort_by<K, F>(self, key_fn: F, ascending: bool) -> Self
    where
        K: Ord,
        F: Fn(&T) -> K,
        Vec<T>: WireDecode,
        T: WireEncode,
    {
        let num_partitions = self.rdd.number_of_splits();
        let ctx = self.context.clone();
        let mut data = self.collect().unwrap_or_default();
        if ascending {
            data.sort_by(|a, b| key_fn(a).cmp(&key_fn(b)));
        } else {
            data.sort_by(|a, b| key_fn(b).cmp(&key_fn(a)));
        }
        ctx.parallelize_typed(data, num_partitions)
    }
}

impl<K, V> TypedRdd<(K, V)>
where
    K: Data + Ord + Clone,
    V: Data + Clone,
{
    /// Sort pair RDD elements by key (ascending or descending).
    ///
    /// Uses sampling + range-partition shuffle + per-partition sort:
    /// 1. Sample ~20 keys per partition to estimate the key distribution.
    /// 2. Build a `RangePartitioner` from the sampled boundaries.
    /// 3. Shuffle via `combine_by_key_with_partitioner` into range-ordered buckets
    ///    (each partition holds all pairs for one key range).
    /// 4. Sort within each partition — O(partition_size) memory per worker.
    ///
    /// Result: globally sorted pair RDD where partition 0 has the smallest keys and
    /// partition N has the largest. No full-dataset collect on the driver.
    pub fn sort_by_key(self, ascending: bool) -> Self
    where
        Vec<(K, V)>: WireDecode + WireEncode,
        Vec<K>: WireDecode,
        K: WireEncode + bincode::Encode + bincode::Decode<()> + Clone + std::hash::Hash + Eq,
        V: WireEncode + bincode::Encode + bincode::Decode<()> + Clone,
        Vec<(K, Vec<V>)>: WireEncode,
    {
        let num_partitions = self.rdd.number_of_splits();
        let ctx = self.context.clone();

        let sample_rdd = TypedRdd::<(K, V)>::new(self.rdd.clone(), ctx.clone());
        let sample_keys: Vec<K> = sample_rdd
            .map_partitions_with_index(move |_idx, iter| {
                Box::new(iter.take(20).map(|(k, _)| k)) as Box<dyn Iterator<Item = K>>
            })
            .collect()
            .unwrap_or_default();

        if sample_keys.is_empty() || num_partitions <= 1 {
            let mut data = self.collect().unwrap_or_default();
            if ascending {
                data.sort_by(|(a, _), (b, _)| a.cmp(b));
            } else {
                data.sort_by(|(a, _), (b, _)| b.cmp(a));
            }
            return ctx.parallelize_typed(data, num_partitions);
        }

        let mut sorted_sample = sample_keys;
        if ascending {
            sorted_sample.sort();
        } else {
            sorted_sample.sort_by(|a, b| b.cmp(a));
        }
        let step = (sorted_sample.len() / num_partitions).max(1);
        let bounds: Vec<K> = (1..num_partitions)
            .filter_map(|i| sorted_sample.get(i * step).cloned())
            .collect();

        let partitioner = Partitioner::range(bounds, ascending);

        let asc = ascending;
        self.combine_by_key_with_partitioner(
            |v| vec![v],
            |mut buf, v| {
                buf.push(v);
                buf
            },
            |mut a, mut b| {
                a.append(&mut b);
                a
            },
            partitioner,
        )
        .map_partitions(move |iter| {
            let mut pairs: Vec<(K, Vec<V>)> = iter.collect();
            if asc {
                pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
            } else {
                pairs.sort_by(|(a, _), (b, _)| b.cmp(a));
            }
            Box::new(
                pairs
                    .into_iter()
                    .flat_map(|(k, vs)| vs.into_iter().map(move |v| (k.clone(), v))),
            ) as Box<dyn Iterator<Item = (K, V)>>
        })
    }

    /// Sort pair RDD elements by key using range-based partitioning.
    ///
    /// Samples the input RDD to estimate the key distribution, derives
    /// `num_partitions - 1` split-point bounds via a `RangePartitioner`, collects
    /// and sorts all data, then distributes it so partition `i` contains only keys
    /// in the i-th range.  The result is globally sorted across partitions.
    ///
    /// Returns a `TypedRdd<(K,V)>` with `num_partitions` partitions where each
    /// partition covers a contiguous, non-overlapping key range.
    pub fn sort_by_key_range(self, num_partitions: usize, ascending: bool) -> Self
    where
        Vec<(K, V)>: WireDecode,
        K: WireEncode,
        V: WireEncode,
    {
        use atomic_data::partitioner::Partitioner;
        let ctx = self.context.clone();

        let mut data = self.collect().unwrap_or_default();
        if ascending {
            data.sort_by(|(a, _), (b, _)| a.cmp(b));
        } else {
            data.sort_by(|(a, _), (b, _)| b.cmp(a));
        }

        let step = (data.len() / num_partitions).max(1);
        let bounds: Vec<K> = (1..num_partitions)
            .filter_map(|i| data.get(i * step).map(|(k, _)| k.clone()))
            .collect();

        let partitioner = Partitioner::range(bounds, ascending);
        let mut partitions: Vec<Vec<(K, V)>> = vec![vec![]; num_partitions];
        for item in data {
            let p = partitioner.get_partition(&item.0 as &dyn std::any::Any);
            partitions[p].push(item);
        }

        // Flatten in partition order → globally sorted vec → re-parallelize.
        let sorted: Vec<(K, V)> = partitions.into_iter().flatten().collect();
        ctx.parallelize_typed(sorted, num_partitions)
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

impl<T: Data + Clone> TypedRdd<T> {
    /// Group elements by a key function, returning `TypedRdd<(K, Vec<T>)>`.
    pub fn group_by<K, F>(self, f: F) -> TypedRdd<(K, Vec<T>)>
    where
        K: Data + Eq + std::hash::Hash + Clone + bincode::Encode + bincode::Decode<()>,
        T: Clone + bincode::Encode + bincode::Decode<()>,
        F: Fn(&T) -> K + Clone + Send + Sync + 'static,
        Vec<(K, T)>: WireEncode,
    {
        self.key_by(f).group_by_key()
    }
}

impl<T> TypedRdd<T>
where
    T: Data + Clone + WireEncode + WireDecode,
    Vec<T>: WireEncode + WireDecode,
{
    /// Apply a `#[task]`-registered unary function element-wise.
    ///
    /// In **distributed** mode this is lazy — the op is accumulated into the
    /// `StagedPipeline` and only dispatched when an action (`collect`, `fold_task`) fires.
    /// In **local** mode it falls back to an in-process `MapperRdd`.
    ///
    /// # Example
    /// ```ignore
    /// let doubled = ctx.parallelize_typed(data, 2).map_task(Double).collect()?;
    /// ```
    pub fn map_task<U, F>(self, _task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<T, U>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            return TypedRdd::new(Arc::new(MapperRdd::new(id, self.rdd, F::call)), context);
        }

        let op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("map_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<U>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// Filter using a `#[task]`-registered predicate (return type `bool`).
    ///
    /// Lazy in distributed mode; eager (in-process `MapPartitionsRdd`) in local mode.
    ///
    /// # Example
    /// ```ignore
    /// let positives = ctx.parallelize_typed(data, 2).filter_task(IsPositive).collect()?;
    /// ```
    pub fn filter_task<F>(self, _task: F) -> TypedRdd<T>
    where
        F: UnaryTask<T, bool>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            let filter_fn = move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| {
                Box::new(iter.filter(|x| F::call(x.clone()))) as Box<dyn Iterator<Item = T>>
            };
            return TypedRdd::new(
                Arc::new(MapPartitionsRdd::new(id, self.rdd, filter_fn)),
                context,
            );
        }

        let op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Filter,
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("filter_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<T>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// FlatMap using a `#[task]`-registered function returning `Vec<U>`.
    ///
    /// Lazy in distributed mode; eager (in-process `FlatMapperRdd`) in local mode.
    ///
    /// # Example
    /// ```ignore
    /// let mirrored = ctx.parallelize_typed(data, 2).flat_map_task(Mirror).collect()?;
    /// ```
    pub fn flat_map_task<U, F>(self, _task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<T, Vec<U>>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            return TypedRdd::new(
                Arc::new(FlatMapperRdd::new(id, self.rdd, |x| {
                    Box::new(F::call(x).into_iter())
                })),
                context,
            );
        }

        let op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::FlatMap,
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("flat_map_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<U>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// Fold all elements using a `#[task]`-registered binary function.
    ///
    /// This is an **action** — it triggers computation and returns a single value.
    /// In distributed mode it dispatches the full staged pipeline (if any) plus
    /// the fold op as one round-trip, then combines per-partition fold results on
    /// the driver via a Reduce step.
    ///
    /// # Example
    /// ```ignore
    /// let total = ctx.parallelize_typed(data, 2).fold_task(0i32, Add)?;
    /// ```
    pub fn fold_task<F>(&self, init: T, _task: F) -> Result<T, BaseError>
    where
        F: BinaryTask<T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let f_clone = F::call;
            let zero = init.clone();
            let reduce_partition =
                move |iter: Box<dyn Iterator<Item = T>>| iter.fold(zero.clone(), &f_clone);
            let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
            return Ok(results.into_iter().fold(init, F::call));
        }

        // Build pipeline: existing staged ops (if any) + fold op.
        let fold_payload = init
            .encode_wire()
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
        let fold_op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Fold,
            runtime: TaskRuntime::Native,
            payload: fold_payload,
        };

        let (source_partitions, mut ops) = match &self.staged {
            None => {
                let encoded = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                (encoded, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.ops.clone()),
        };
        ops.push(fold_op);

        let partition_results_raw = self
            .context
            .dispatch_pipeline(source_partitions, ops.clone())
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;

        let mut partition_values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|bytes| {
                T::decode_wire(&bytes).map_err(|e| BaseError::DowncastFailure(e.to_string()))
            })
            .collect::<Result<_, _>>()?;

        if partition_values.is_empty() {
            return Ok(init);
        }
        if partition_values.len() == 1 {
            return Ok(partition_values.remove(0));
        }

        Ok(partition_values.into_iter().reduce(F::call).unwrap_or(init))
    }

    /// Reduce all elements using a `#[task]`-registered binary function.
    ///
    /// Works identically in **local** and **distributed** mode. Returns `None`
    /// if the RDD is empty. Prefer [`fold_task`] when a known identity value exists.
    ///
    /// In distributed mode, dispatches the full staged pipeline (if any) plus a
    /// `Reduce` op to workers; each partition is reduced to a single element.
    /// The driver then combines the per-partition results with a second local Reduce.
    ///
    /// # Example
    /// ```ignore
    /// let total = ctx.parallelize_typed(data, 2).reduce_task(task_fn!(|a: i32, b: i32| a + b))?;
    /// ```
    pub fn reduce_task<F>(&self, _task: F) -> Result<Option<T>, BaseError>
    where
        F: BinaryTask<T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let reduce_partition = |iter: Box<dyn Iterator<Item = T>>| {
                iter.reduce(F::call).into_iter().collect::<Vec<_>>()
            };
            let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
            return Ok(results.into_iter().flatten().reduce(F::call));
        }

        let reduce_op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Reduce,
            runtime: TaskRuntime::Native,
            payload: vec![],
        };

        let (source_partitions, mut ops) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.ops.clone()),
        };
        ops.push(reduce_op);

        let partition_results_raw = self
            .context
            .dispatch_pipeline(source_partitions, ops)
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;

        let mut values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|b| T::decode_wire(&b).map_err(|e| BaseError::DowncastFailure(e.to_string())))
            .collect::<Result<_, _>>()?;

        match values.len() {
            0 => Ok(None),
            1 => Ok(Some(values.remove(0))),
            _ => {
                let combined = values
                    .encode_wire()
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                let driver_ops = vec![PipelineOp {
                    op_id: F::NAME.to_string(),
                    action: TaskAction::Reduce,
                    runtime: TaskRuntime::Native,
                    payload: vec![],
                }];
                let task = TaskEnvelope::new(
                    0,
                    0,
                    0,
                    0,
                    0,
                    "driver-reduce".to_string(),
                    driver_ops,
                    combined,
                );
                let result = crate::backend::NativeBackend::default()
                    .execute("local-driver", &task)
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                match result.status {
                    atomic_data::distributed::ResultStatus::Success => T::decode_wire(&result.data)
                        .map(Some)
                        .map_err(|e| BaseError::DowncastFailure(e.to_string())),
                    _ => Err(BaseError::DowncastFailure(
                        result
                            .error
                            .unwrap_or_else(|| "reduce_task failed".to_string()),
                    )),
                }
            }
        }
    }

    /// Build (or extend) a `StagedPipeline` for distributed lazy dispatch.
    ///
    /// If a pipeline was already staged, appends `op` and returns it.
    /// Otherwise encodes the source `rdd` partitions once and starts a new pipeline.
    fn stage_op(
        staged: Option<StagedPipeline>,
        rdd: &RddRef<T>,
        op: PipelineOp,
    ) -> Result<StagedPipeline, crate::error::ComputeError> {
        match staged {
            Some(mut s) => {
                s.ops.push(op);
                Ok(s)
            }
            None => {
                let source_partitions = Context::encode_rdd_partitions(rdd.clone())?;
                Ok(StagedPipeline {
                    source_partitions,
                    ops: vec![op],
                })
            }
        }
    }
}
