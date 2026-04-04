use crate::rdd::cartesian::CartesianRdd;
use crate::rdd::coalesced::CoalescedRdd;
use crate::rdd::flatmapper::FlatMapperRdd;
use crate::rdd::map_partitions::MapPartitionsRdd;
use crate::rdd::mapper::MapperRdd;
use crate::rdd::wasm::WasmRddExt;
use atomic_data::dependency::Dependency;
use atomic_data::distributed::{AggregateActionConfig, FoldActionConfig};
use atomic_data::distributed::{RkyvWireSerializer, RkyvWireStrategy, RkyvWireValidator};
use atomic_data::error::BaseError;
use atomic_data::fn_traits::{RddFlatMapFn, RddFn};
use atomic_data::partitioner::Partitioner;
use rkyv::bytecheck::CheckBytes;

use crate::rdd::{Data, Rdd, RddBase};
use atomic_data::split::Split;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::{Context, ExecutionRuntime};

/// Type alias for Arc-wrapped RDD trait objects
pub type RddRef<T> = Arc<dyn Rdd<Item = T>>;

/// Typed RDD wrapper that provides transformation and action methods.
///
/// This is the recommended way to work with RDDs in atomic. Unlike trait-based operations,
/// TypedRdd provides methods that can be chained naturally without type erasure issues.
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
    _marker: PhantomData<T>,
}

impl<T: Clone> TypedRdd<T> {
    /// Create a new TypedRdd wrapping an existing RDD
    pub fn new(rdd: RddRef<T>, context: Arc<Context>) -> Self {
        Self {
            rdd,
            context,
            _marker: PhantomData,
        }
    }

    /// Get the inner RDD reference
    pub fn inner(&self) -> &RddRef<T> {
        &self.rdd
    }

    /// Get the execution context
    pub fn get_context(&self) -> Arc<Context> {
        self.context.clone()
    }

    /// Convert back to Arc<dyn Rdd> for interop with existing code
    pub fn into_rdd(self) -> RddRef<T> {
        self.rdd
    }

    /// Get number of partitions
    pub fn num_partitions(&self) -> usize {
        self.rdd.number_of_splits()
    }
}

impl<T> Clone for TypedRdd<T> {
    fn clone(&self) -> Self {
        TypedRdd {
            rdd: self.rdd.clone(),
            context: self.context.clone(),
            _marker: PhantomData,
        }
    }
}

// Implement RddBase trait for TypedRdd by delegating to inner RDD
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

// Keep backward compatibility - TypedRdd can still implement RddOperation
// but its main API is through direct methods, not trait methods
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

// ============================================================================
// TRANSFORMATION METHODS - These are the preferred API for TypedRdd
// ============================================================================

impl<T: Data> TypedRdd<T> {
    /// Apply a function to each element, returning a new TypedRdd with transformed elements.
    ///
    /// This is a narrow transformation (no shuffle).
    ///
    /// # Example
    /// ```ignore
    /// let rdd = context.parallelize_typed(vec![1, 2, 3]);
    /// let doubled = rdd.map(|x| x * 2);
    /// ```
    pub fn map<U, F>(self, f: F) -> TypedRdd<U>
    where
        U: Data + Clone,
        F: RddFn<T, U>,
    {
        let id = self.context.new_rdd_id();
        let mapped_rdd = MapperRdd::new(id, self.rdd, f);
        TypedRdd::new(Arc::new(mapped_rdd), self.context)
    }

    /// Filter elements using a predicate function.
    ///
    /// Returns a new TypedRdd containing only elements for which the predicate returns true.
    ///
    /// # Example
    /// ```ignore
    /// let rdd = context.parallelize_typed(vec![1, 2, 3, 4, 5]);
    /// let evens = rdd.filter(|x| x % 2 == 0);
    /// ```
    pub fn filter<F>(self, f: F) -> TypedRdd<T>
    where
        T: Clone,
        F: Fn(&T) -> bool + Send + Sync + Clone + 'static,
    {
        let f = Arc::new(f);
        let filter_fn = move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| {
            let f = Arc::clone(&f);
            Box::new(iter.filter(move |x| f(x))) as Box<dyn Iterator<Item = T>>
        };
        let id = self.context.new_rdd_id();
        let filtered_rdd = MapPartitionsRdd::new(id, self.rdd, filter_fn);
        TypedRdd::new(Arc::new(filtered_rdd), self.context)
    }

    /// FlatMap each element to multiple elements.
    ///
    /// Each input element can produce zero or more output elements.
    ///
    /// # Example
    /// ```ignore
    /// let rdd = context.parallelize_typed(vec![1, 2, 3]);
    /// let expanded = rdd.flat_map(|x| Box::new(vec![x, x * 10].into_iter()));
    /// ```
    pub fn flat_map<U, F>(self, f: F) -> TypedRdd<U>
    where
        U: Data + Clone,
        F: RddFlatMapFn<T, U>,
    {
        let id = self.context.new_rdd_id();
        let flatmapped_rdd = FlatMapperRdd::new(id, self.rdd, f);
        TypedRdd::new(Arc::new(flatmapped_rdd), self.context)
    }
}

// ============================================================================
// ACTION METHODS
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecMode {
    Local,
    Wasm(String),
}

impl<T: Data + Clone> TypedRdd<T> {
    fn wasm_ops(&self, action: &str) -> Vec<String> {
        let op_name = self.rdd.get_op_name();
        vec![format!("{}.{}.v1", op_name, action), format!("{}.{}", op_name, action)]
    }

    fn exec_mode(&self, action: &str) -> Result<ExecMode, BaseError> {
        match self.context.runtime() {
            ExecutionRuntime::Local => Ok(ExecMode::Local),
            ExecutionRuntime::Wasm => self
                .context
                .resolve_registered_wasm_operation(self.wasm_ops(action))
                .map(ExecMode::Wasm)
                .ok_or_else(|| {
                    BaseError::Other(format!(
                        "missing registered wasm operation for action '{}' on '{}'",
                        action,
                        self.rdd.get_op_name()
                    ))
                }),
            ExecutionRuntime::Docker => Err(BaseError::Other(format!(
                "typed action '{}' does not yet support the docker runtime",
                action
            ))),
        }
    }

    /// Collect all elements from all partitions into a Vec.
    ///
    /// **Warning**: This brings all data to the driver. Only use on small datasets.
    ///
    /// # Example
    /// ```ignore
    /// let rdd = context.parallelize_typed(vec![1, 2, 3]);
    /// let result = rdd.collect()?;
    /// assert_eq!(result, vec![1, 2, 3]);
    /// ```
    pub fn collect_local(&self) -> Result<Vec<T>, BaseError> {
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

    pub fn collect(&self) -> Result<Vec<T>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Vec<T>: rkyv::Archive,
        <Vec<T> as rkyv::Archive>::Archived:
            for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Vec<T>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("collect")? {
            ExecMode::Local => self.collect_local(),
            ExecMode::Wasm(operation_id) => self.collect_wasm_rkyv::<T>(&operation_id),
        }
    }

    pub fn try_collect_wasm(&self) -> Result<Option<Vec<T>>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Vec<T>: rkyv::Archive,
        <Vec<T> as rkyv::Archive>::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Vec<T>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("collect")? {
            ExecMode::Local => Ok(None),
            ExecMode::Wasm(operation_id) => {
                self.collect_wasm_rkyv::<T>(&operation_id).map(Some)
            }
        }
    }

    pub fn collect_auto_wasm_rkyv(&self) -> Result<Option<Vec<T>>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Vec<T>: rkyv::Archive,
        <Vec<T> as rkyv::Archive>::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Vec<T>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.try_collect_wasm()
    }

    /// Count the number of elements in the RDD.
    ///
    /// # Example
    /// ```ignore
    /// let count = rdd.count()?;
    /// ```
    pub fn count_local(&self) -> Result<u64, BaseError> {
        let counting_func = |iter: Box<dyn Iterator<Item = T>>| iter.count() as u64;
        Ok(self
            .context
            .run_job(self.rdd.clone(), counting_func)?
            .into_iter()
            .sum())
    }

    pub fn count(&self) -> Result<u64, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("count")? {
            ExecMode::Local => self.count_local(),
            ExecMode::Wasm(operation_id) => {
                Ok(self.run_wasm::<u64>(&operation_id)?.into_iter().sum())
            }
        }
    }

    pub fn try_count_wasm(&self) -> Result<Option<u64>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("count")? {
            ExecMode::Local => Ok(None),
            ExecMode::Wasm(operation_id) => {
                Ok(Some(self.run_wasm::<u64>(&operation_id)?.into_iter().sum()))
            }
        }
    }

    pub fn count_auto_wasm_rkyv(&self) -> Result<Option<u64>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.try_count_wasm()
    }

    /// Take the first n elements from the RDD.
    ///
    /// # Example
    /// ```ignore
    /// let first_three = rdd.take(3)?;
    /// ```
    pub fn take(&self, num: usize) -> Result<Vec<T>, BaseError> {
        const SCALE_UP_FACTOR: f64 = 2.0;
        if num == 0 {
            return Ok(vec![]);
        }
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
    pub fn first(&self) -> Result<T, BaseError> {
        if let Some(result) = self.take(1)?.into_iter().next() {
            Ok(result)
        } else {
            Err(BaseError::DowncastFailure("empty collection".to_string()))
        }
    }

    /// Reduce the elements using an associative and commutative function.
    ///
    /// # Example
    /// ```ignore
    /// let sum = rdd.reduce(|a, b| a + b)?;
    /// ```
    pub fn reduce_local<F>(&self, f: F) -> Result<Option<T>, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        let f_clone = f.clone();
        let reduce_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            let acc = iter.reduce(&f_clone);
            match acc {
                None => vec![],
                Some(e) => vec![e],
            }
        };
        let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
        Ok(results.into_iter().flatten().reduce(f))
    }

    pub fn reduce<F>(&self, f: F) -> Result<Option<T>, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Option<T>: rkyv::Archive,
        <Option<T> as rkyv::Archive>::Archived:
            for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Option<T>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("reduce")? {
            ExecMode::Local => self.reduce_local(f),
            ExecMode::Wasm(operation_id) => {
                let partials = self.run_wasm::<Option<T>>(&operation_id)?;
                Ok(partials.into_iter().flatten().reduce(f))
            }
        }
    }

    pub fn try_reduce_wasm<F>(&self, f: F) -> Result<Option<Option<T>>, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Option<T>: rkyv::Archive,
        <Option<T> as rkyv::Archive>::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Option<T>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("reduce")? {
            ExecMode::Local => Ok(None),
            ExecMode::Wasm(operation_id) => {
                let partials = self.run_wasm::<Option<T>>(&operation_id)?;
                Ok(Some(partials.into_iter().flatten().reduce(f)))
            }
        }
    }

    pub fn reduce_auto_wasm_rkyv<F>(&self, f: F) -> Result<Option<Option<T>>, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Option<T>: rkyv::Archive,
        <Option<T> as rkyv::Archive>::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Option<T>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.try_reduce_wasm(f)
    }

    /// Fold/aggregate the elements using an initial value and associative function.
    ///
    /// # Example
    /// ```ignore
    /// let sum = rdd.fold(0, |acc, x| acc + x)?;
    /// ```
    pub fn fold_local<F>(&self, init: T, f: F) -> Result<T, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        let f_clone = f.clone();
        let zero = init.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = T>>| iter.fold(zero.clone(), &f_clone);
        let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
        Ok(results.into_iter().fold(init, f))
    }

    pub fn fold<F>(&self, init: T, f: F) -> Result<T, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        T: rkyv::Archive,
        T::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<T, RkyvWireStrategy>,
        FoldActionConfig<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("fold")? {
            ExecMode::Local => self.fold_local(init, f),
            ExecMode::Wasm(operation_id) => {
                let config = FoldActionConfig { zero: init.clone() };
                let partials = self.run_wasm_cfg::<T, _>(&operation_id, &config)?;
                Ok(partials.into_iter().fold(init, f))
            }
        }
    }

    pub fn try_fold_wasm<F>(&self, init: T, f: F) -> Result<Option<T>, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        T: rkyv::Archive,
        T::Archived: for<'a> CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<T, RkyvWireStrategy>,
        FoldActionConfig<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match self.exec_mode("fold")? {
            ExecMode::Local => Ok(None),
            ExecMode::Wasm(operation_id) => {
                let config = FoldActionConfig { zero: init.clone() };
                let partials = self.run_wasm_cfg::<T, _>(&operation_id, &config)?;
                Ok(Some(partials.into_iter().fold(init, f)))
            }
        }
    }

    pub fn fold_auto_wasm_rkyv<F>(&self, init: T, f: F) -> Result<Option<T>, BaseError>
    where
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        T: rkyv::Archive,
        T::Archived: for<'a> CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<T, RkyvWireStrategy>,
        FoldActionConfig<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.try_fold_wasm(init, f)
    }

    /// Check if the RDD is empty.
    pub fn is_empty(&self) -> Result<bool, BaseError> {
        Ok(self.take(1)?.is_empty())
    }

    /// Aggregate elements with different accumulator and result types.
    ///
    /// This is a more general version of fold that allows different types for
    /// the accumulator (U) and the elements (T).
    ///
    /// # Arguments
    /// * `init` - Initial value for the accumulator
    /// * `seq_fn` - Function to accumulate results within a partition (U, T) -> U
    /// * `comb_fn` - Function to combine results from different partitions (U, U) -> U
    ///
    /// # Example
    /// ```ignore
    /// // Sum of squares
    /// let sum_of_squares = rdd.aggregate(
    ///     0,
    ///     |acc, x| acc + x * x,  // seq_fn: accumulate within partition
    ///     |acc1, acc2| acc1 + acc2,  // comb_fn: combine partitions
    /// )?;
    /// ```
    pub fn aggregate_local<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U, BaseError>
    where
        U: Data + Clone,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        let zero = init.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = T>>| iter.fold(zero.clone(), &seq_fn);
        let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
        Ok(results.into_iter().fold(init, comb_fn))
    }

    pub fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U, BaseError>
    where
        U: Data + Clone + rkyv::Archive,
        U::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<U, RkyvWireStrategy>,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        AggregateActionConfig<U>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        match self.exec_mode("aggregate")? {
            ExecMode::Local => self.aggregate_local(init, seq_fn, comb_fn),
            ExecMode::Wasm(operation_id) => {
                let _ = seq_fn;
                let config = AggregateActionConfig { zero: init.clone() };
                let partials = self.run_wasm_cfg::<U, _>(&operation_id, &config)?;
                Ok(partials.into_iter().fold(init, comb_fn))
            }
        }
    }

    pub fn try_agg_wasm<U, CF>(
        &self,
        init: U,
        comb_fn: CF,
    ) -> Result<Option<U>, BaseError>
    where
        U: Data + Clone + rkyv::Archive,
        U::Archived: for<'a> CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<U, RkyvWireStrategy>,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        AggregateActionConfig<U>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        match self.exec_mode("aggregate")? {
            ExecMode::Local => Ok(None),
            ExecMode::Wasm(operation_id) => {
                let config = AggregateActionConfig { zero: init.clone() };
                let partials = self.run_wasm_cfg::<U, _>(&operation_id, &config)?;
                Ok(Some(partials.into_iter().fold(init, comb_fn)))
            }
        }
    }

    pub fn aggregate_auto_wasm_rkyv<U, CF>(
        &self,
        init: U,
        comb_fn: CF,
    ) -> Result<Option<U>, BaseError>
    where
        U: Data + Clone + rkyv::Archive,
        U::Archived: for<'a> CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<U, RkyvWireStrategy>,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        AggregateActionConfig<U>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        self.try_agg_wasm(init, comb_fn)
    }

    /// Apply a function to each element (for side effects).
    ///
    /// # Example
    /// ```ignore
    /// rdd.for_each(|x| println!("{}", x))?;
    /// ```
    pub fn for_each<F>(&self, f: F) -> Result<(), BaseError>
    where
        F: Fn(&T) + Clone + Send + Sync + 'static,
    {
        let for_each_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            iter.for_each(|x| f(&x));
        };
        self.context.run_job(self.rdd.clone(), for_each_partition)?;
        Ok(())
    }

    /// Apply a function to each partition (for side effects).
    ///
    /// # Example
    /// ```ignore
    /// rdd.for_each_partition(|iter| {
    ///     for x in iter {
    ///         println!("{}", x);
    ///     }
    /// })?;
    /// ```
    pub fn for_each_partition<F>(&self, f: F) -> Result<(), BaseError>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) + Clone + Send + Sync + 'static,
    {
        self.context.run_job(self.rdd.clone(), f)?;
        Ok(())
    }

    /// Count the number of occurrences of each unique value.
    ///
    /// # Example
    /// ```ignore
    /// let counts = rdd.count_by_value()?;
    /// ```
    pub fn count_by_value(&self) -> Result<std::collections::HashMap<T, u64>, BaseError>
    where
        T: Eq + std::hash::Hash + Clone,
    {
        use std::collections::HashMap;

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
    /// # Example
    /// ```ignore
    /// let max_val = rdd.max()?;
    /// ```
    pub fn max(&self) -> Result<Option<T>, BaseError>
    where
        T: Ord + Clone,
    {
        let max_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.max();
        let partition_maxes = self.context.run_job(self.rdd.clone(), max_partition)?;
        Ok(partition_maxes.into_iter().flatten().max())
    }

    /// Return the minimum element.
    ///
    /// # Example
    /// ```ignore
    /// let min_val = rdd.min()?;
    /// ```
    pub fn min(&self) -> Result<Option<T>, BaseError>
    where
        T: Ord + Clone,
    {
        let min_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.min();
        let partition_mins = self.context.run_job(self.rdd.clone(), min_partition)?;
        Ok(partition_mins.into_iter().flatten().min())
    }

    /// Return the top k elements in descending order.
    ///
    /// # Example
    /// ```ignore
    /// let top_5 = rdd.top(5)?;
    /// ```
    pub fn top(&self, k: usize) -> Result<Vec<T>, BaseError>
    where
        T: Ord + Clone,
    {
        let top_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            let mut items: Vec<T> = iter.collect();
            items.sort_by(|a, b| b.cmp(a)); // Descending
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
    /// # Example
    /// ```ignore
    /// let ordered = rdd.take_ordered(5)?;
    /// ```
    pub fn take_ordered(&self, k: usize) -> Result<Vec<T>, BaseError>
    where
        T: Ord + Clone,
    {
        let take_partition = move |iter: Box<dyn Iterator<Item = T>>| {
            let mut items: Vec<T> = iter.collect();
            items.sort(); // Ascending
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

// ============================================================================
// SET OPERATIONS
// ============================================================================

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
        let union_rdd = UnionRdd::new(id, &rdds).expect("Failed to create union RDD");
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

    /// Return a new RDD containing only distinct elements.
    ///
    /// # Example
    /// ```ignore
    /// let distinct = rdd.distinct();
    /// ```
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

        let id = self.context.new_rdd_id();
        let distinct_rdd = MapPartitionsRdd::new(id, self.rdd, dedup);
        TypedRdd::new(Arc::new(distinct_rdd), self.context)
    }

    /// Return a new RDD containing elements only in this RDD but not in the other RDD.
    ///
    /// # Example
    /// ```ignore
    /// let difference = rdd1.subtract(rdd2);
    /// ```
    pub fn subtract(self, other: TypedRdd<T>) -> TypedRdd<T>
    where
        T: Eq + std::hash::Hash + Clone,
    {
        use std::collections::HashSet;

        // Collect other RDD elements into a set
        let other_set: Arc<std::sync::Mutex<HashSet<T>>> = Arc::new(std::sync::Mutex::new(HashSet::new()));

        // Map this to filter based on other_set
        let other_set_clone = other_set.clone();
        let filter_fn =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set = other_set_clone.lock().unwrap();
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

    /// Return a new RDD containing only elements found in both RDDs.
    ///
    /// # Example
    /// ```ignore
    /// let common = rdd1.intersection(rdd2);
    /// ```
    pub fn intersection(self, other: TypedRdd<T>) -> TypedRdd<T>
    where
        T: Eq + std::hash::Hash + Clone,
    {
        use std::collections::HashSet;

        // Collect other RDD elements into a set
        let other_set: Arc<std::sync::Mutex<HashSet<T>>> = Arc::new(std::sync::Mutex::new(HashSet::new()));

        // Map this to filter based on other_set
        let other_set_clone = other_set.clone();
        let filter_fn =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set = other_set_clone.lock().unwrap();
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

// ============================================================================
// PARTITION OPERATIONS
// ============================================================================

impl<T: Data + Clone> TypedRdd<T> {
    /// Reduce the number of partitions by coalescing.
    ///
    /// This is a narrow transformation if reducing partitions.
    ///
    /// # Example
    /// ```ignore
    /// let coalesced = rdd.coalesce(2, false);
    /// ```
    pub fn coalesce(self, num_partitions: usize, shuffle: bool) -> TypedRdd<T>
    where
        T: Clone,
    {
        let id = self.context.new_rdd_id();
        if shuffle {
            // TODO: Implement shuffle-based coalesce
            // For now, just use CoalescedRdd
            let coalesced_rdd = CoalescedRdd::new(id, self.rdd, num_partitions);
            TypedRdd::new(Arc::new(coalesced_rdd), self.context)
        } else {
            let coalesced_rdd = CoalescedRdd::new(id, self.rdd, num_partitions);
            TypedRdd::new(Arc::new(coalesced_rdd), self.context)
        }
    }

    /// Repartition to have a different number of partitions (always shuffles).
    ///
    /// # Example
    /// ```ignore
    /// let repartitioned = rdd.repartition(10);
    /// ```
    pub fn repartition(self, num_partitions: usize) -> TypedRdd<T> {
        self.coalesce(num_partitions, true)
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
        let id = self.context.new_rdd_id();
        let mapped_rdd = MapPartitionsRdd::new(id, self.rdd, ignore_idx);
        TypedRdd::new(Arc::new(mapped_rdd), self.context)
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
        let id = self.context.new_rdd_id();
        let mapped_rdd = MapPartitionsRdd::new(id, self.rdd, f);
        TypedRdd::new(Arc::new(mapped_rdd), self.context)
    }

    /// Group all elements within each partition into a Vec.
    ///
    /// # Example
    /// ```ignore
    /// let grouped = rdd.glom(); // Each partition becomes Vec<T>
    /// ```
    pub fn glom(self) -> TypedRdd<Vec<T>>
    where
        T: Clone,
    {
        let func = |_index: usize, iter: Box<dyn Iterator<Item = T>>| {
            Box::new(std::iter::once(iter.collect::<Vec<_>>())) as Box<dyn Iterator<Item = Vec<T>>>
        };
        let id = self.context.new_rdd_id();
        let glom_rdd = MapPartitionsRdd::new(id, self.rdd, func);
        TypedRdd::new(Arc::new(glom_rdd), self.context)
    }
}

// ============================================================================
// PAIR RDD OPERATIONS (for TypedRdd<(K, V)>)
// ============================================================================

impl<K, V> TypedRdd<(K, V)>
where
    K: Data + Eq + std::hash::Hash + Clone,
    V: Data + Clone,
{
    /// Extract just the keys from a pair RDD.
    ///
    /// # Example
    /// ```ignore
    /// let keys = pair_rdd.keys();
    /// ```
    pub fn keys(self) -> TypedRdd<K> {
        self.map(|(k, _v)| k)
    }

    /// Extract just the values from a pair RDD.
    ///
    /// # Example
    /// ```ignore
    /// let values = pair_rdd.values();
    /// ```
    pub fn values(self) -> TypedRdd<V> {
        self.map(|(_k, v)| v)
    }

    /// Transform only the values in a pair RDD, keeping the keys unchanged.
    ///
    /// # Example
    /// ```ignore
    /// let mapped = pair_rdd.map_values(|v| v * 2);
    /// ```
    pub fn map_values<U, F>(self, f: F) -> TypedRdd<(K, U)>
    where
        U: Data + Clone,
        F: Fn(V) -> U + Clone + Send + Sync + 'static,
    {
        self.map(move |(k, v)| (k, f(v)))
    }

    /// Reduce values for each key using an associative function.
    ///
    /// # Example
    /// ```ignore
    /// let sums = pair_rdd.reduce_by_key(|a, b| a + b);
    /// ```
    pub fn reduce_by_key<F>(self, f: F) -> TypedRdd<(K, V)>
    where
        F: Fn(V, V) -> V + Clone + Send + Sync + 'static,
    {
        use std::collections::HashMap;

        let reduce_partition = move |_idx: usize, iter: Box<dyn Iterator<Item = (K, V)>>| {
            let mut map: HashMap<K, V> = HashMap::new();
            for (k, v) in iter {
                map.entry(k)
                    .and_modify(|existing| *existing = f(existing.clone(), v.clone()))
                    .or_insert(v);
            }
            Box::new(map.into_iter()) as Box<dyn Iterator<Item = (K, V)>>
        };

        let id = self.context.new_rdd_id();
        let reduced_rdd = MapPartitionsRdd::new(id, self.rdd, reduce_partition);
        TypedRdd::new(Arc::new(reduced_rdd), self.context)
    }

    /// Group values for each key.
    ///
    /// # Example
    /// ```ignore
    /// let grouped = pair_rdd.group_by_key();
    /// ```
    pub fn group_by_key(self) -> TypedRdd<(K, Vec<V>)> {
        use std::collections::HashMap;

        let group_partition = move |_idx: usize, iter: Box<dyn Iterator<Item = (K, V)>>| {
            let mut map: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in iter {
                map.entry(k).or_insert_with(Vec::new).push(v);
            }
            Box::new(map.into_iter()) as Box<dyn Iterator<Item = (K, Vec<V>)>>
        };

        let id = self.context.new_rdd_id();
        let grouped_rdd = MapPartitionsRdd::new(id, self.rdd, group_partition);
        TypedRdd::new(Arc::new(grouped_rdd), self.context)
    }

    /// Count the number of values for each key.
    ///
    /// # Example
    /// ```ignore
    /// let counts = pair_rdd.count_by_key()?;
    /// ```
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

    /// Lookup values for a given key.
    ///
    /// # Example
    /// ```ignore
    /// let values = pair_rdd.lookup(&key)?;
    /// ```
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
}

// ============================================================================
// UTILITY TRANSFORMATIONS
// ============================================================================

impl<T: Data> TypedRdd<T> {
    /// Create a pair RDD by applying a function to generate keys.
    ///
    /// # Example
    /// ```ignore
    /// let pair_rdd = rdd.key_by(|x| x % 10);
    /// ```
    pub fn key_by<K, F>(self, f: F) -> TypedRdd<(K, T)>
    where
        K: Data + Clone,
        T: Clone,
        F: Fn(&T) -> K + Clone + Send + Sync + 'static,
    {
        self.map(move |x| {
            let key = f(&x);
            (key, x)
        })
    }

    /// Convert each element to a string and save to a text file.
    ///
    /// Note: This is a placeholder implementation. Full implementation would require
    /// file system integration.
    ///
    /// # Example
    /// ```ignore
    /// rdd.save_as_text_file("/path/to/output")?;
    /// ```
    pub fn save_as_text_file(&self, _path: &str) -> Result<(), BaseError>
    where
        T: std::fmt::Display,
    {
        // TODO: Implement actual file saving
        // For now, just return Ok
        Ok(())
    }
}

// ============================================================================
// CONVERSION TRAIT - For easy Arc<dyn Rdd> -> TypedRdd conversion
// ============================================================================

/// Extension trait to convert Arc<dyn Rdd<Item = T>> to TypedRdd<T>.
///
/// This allows ergonomic conversion: `rdd.typed(ctx)`.
pub trait RddExt<T: Data>: Rdd<Item = T> {
    /// Convert this RDD into a TypedRdd with the given context.
    ///
    /// # Example
    /// ```ignore
    /// let typed_rdd = some_rdd.typed(ctx);
    /// let result = typed_rdd.map(|x| x * 2).collect()?;
    /// ```
    fn typed(self: Arc<Self>, context: Arc<Context>) -> TypedRdd<T>;
}

impl<T: Data + Clone, R: Rdd<Item = T>> RddExt<T> for R {
    fn typed(self: Arc<Self>, context: Arc<Context>) -> TypedRdd<T> {
        TypedRdd::new(self, context)
    }
}

// ============================================================================
// ARTIFACT STUB METHODS — type-safe WASM / Docker dispatch
// ============================================================================

use atomic_data::distributed::ExecutionBackend;
use atomic_data::stub::ArtifactStub;

impl<T: Data + Clone> TypedRdd<T> {
    /// Execute a prebuilt artifact (WASM or Docker) that maps each partition `T → U`.
    ///
    /// The stub's `Input` type must match `T` and `Output` must match `U`.
    /// The compiler enforces this — a type mismatch is a compile error, not a runtime error.
    ///
    /// # Example
    /// ```ignore
    /// let stub: WasmStub<u8, u32> = WasmStub::from_manifest("manifest.toml", "demo.map.v1")?;
    /// let result: Vec<u32> = rdd.map_via(&stub)?;
    /// ```
    pub fn map_via<U>(&self, stub: &ArtifactStub<T, U>) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<U, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match stub.backend() {
            ExecutionBackend::Wasm => self.run_wasm(stub.operation_id()),
            ExecutionBackend::Docker | ExecutionBackend::LocalThread => Err(BaseError::Other(
                format!(
                    "map_via does not yet support the {:?} backend for operation '{}'",
                    stub.backend(),
                    stub.operation_id()
                ),
            )),
        }
    }

    /// Execute a prebuilt artifact that collects each partition into `Vec<U>`.
    ///
    /// Use this when the artifact returns a full `Vec<U>` per partition
    /// (as opposed to a single `U` value per partition from `map_via`).
    ///
    /// # Example
    /// ```ignore
    /// let stub: WasmStub<u8, u8> = WasmStub::from_manifest("manifest.toml", "demo.collect.v1")?;
    /// let result: Vec<u8> = rdd.collect_via(&stub)?;
    /// ```
    pub fn collect_via<U>(&self, stub: &ArtifactStub<T, U>) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Vec<U>: rkyv::Archive,
        <Vec<U> as rkyv::Archive>::Archived:
            for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
                + rkyv::Deserialize<Vec<U>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match stub.backend() {
            ExecutionBackend::Wasm => self.collect_wasm_rkyv(stub.operation_id()),
            ExecutionBackend::Docker | ExecutionBackend::LocalThread => Err(BaseError::Other(
                format!(
                    "collect_via does not yet support the {:?} backend for operation '{}'",
                    stub.backend(),
                    stub.operation_id()
                ),
            )),
        }
    }

    /// Execute a prebuilt artifact with a per-partition config value.
    ///
    /// The config is rkyv-serialized and sent alongside the partition data.
    ///
    /// # Example
    /// ```ignore
    /// let stub: WasmStub<u8, u32> = WasmStub::from_manifest("manifest.toml", "demo.fold.v1")?;
    /// let result: Vec<u32> = rdd.map_via_cfg(&stub, &FoldConfig { zero: 0 })?;
    /// ```
    pub fn map_via_cfg<U, C>(&self, stub: &ArtifactStub<T, U>, config: &C) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<U, RkyvWireStrategy>,
        C: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        match stub.backend() {
            ExecutionBackend::Wasm => self.run_wasm_cfg(stub.operation_id(), config),
            ExecutionBackend::Docker | ExecutionBackend::LocalThread => Err(BaseError::Other(
                format!(
                    "map_via_cfg does not yet support the {:?} backend for operation '{}'",
                    stub.backend(),
                    stub.operation_id()
                ),
            )),
        }
    }

    /// Group elements by a key function, returning `TypedRdd<(K, Vec<T>)>`.
    ///
    /// This is an in-partition grouping (no shuffle). For cross-partition grouping
    /// with shuffle, use `.key_by(f).group_by_key()`.
    ///
    /// # Example
    /// ```ignore
    /// let grouped = rdd.group_by(|x| x % 3);
    /// ```
    pub fn group_by<K, F>(self, f: F) -> TypedRdd<(K, Vec<T>)>
    where
        K: Data + Eq + std::hash::Hash + Clone,
        T: Clone,
        F: Fn(&T) -> K + Clone + Send + Sync + 'static,
    {
        self.key_by(f).group_by_key()
    }
}
