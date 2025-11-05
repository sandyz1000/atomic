use std::{collections::HashMap, sync::Arc, time::Duration};

use ember_utils::bounded_double::BoundedDouble;

use crate::{
    data::Data,
    dependency::Dependency,
    error::BaseResult,
    partitioner::Partitioner,
    split::Split,
};

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
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn splits(&self) -> Vec<Box<dyn Split>>;

    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }

    // Analyse whether this is required or not. It requires downcasting while executing tasks which could hurt performance.
    // TODO: Rename the below method to use only the prefix noun and remove any suffix _any
    // fn iterator_any(
    //     &self,
    //     split: Box<dyn Split>,
    // ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>>;

    // fn cogroup_iterator_any(
    //     &self,
    //     split: Box<dyn Split>,
    // ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>> {
    //     self.iterator_any(split)
    // }

    fn is_pinned(&self) -> bool {
        false
    }
}

// Rdd containing methods associated with processing
pub trait Rdd: RddBase + 'static {
    type Item: Data;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn get_rdd_base(&self) -> Arc<dyn RddBase>;

    fn compute(&self, split: Box<dyn Split>) -> BaseResult<Box<dyn Iterator<Item = Self::Item>>>;

    fn iterator(&self, split: Box<dyn Split>) -> BaseResult<Box<dyn Iterator<Item = Self::Item>>> {
        self.compute(split)
    }
}

pub trait RddOperation: Rdd {
    
    /// Return a new RDD containing only the elements that satisfy a predicate.
    fn filter<F>(&self, predicate: F) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn map<U: Data, F>(&self, f: F) -> Arc<dyn Rdd<Item = U>>;

    fn flat_map<U: Data, F>(&self, f: F) -> Arc<dyn Rdd<Item = U>>;

    /// Return a new RDD by applying a function to each partition of this RDD.
    fn map_partitions<U: Data, F>(&self, func: F) -> Arc<dyn Rdd<Item = U>>;

    /// Return a new RDD by applying a function to each partition of this RDD,
    /// while tracking the index of the original partition.
    fn map_partitions_with_index<U: Data, F>(&self, f: F) -> Arc<dyn Rdd<Item = U>>
    where
        F: Fn(usize, Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>;

    /// Return an RDD created by coalescing all elements within each partition into an array.
    #[allow(clippy::type_complexity)]
    fn glom(&self) -> Arc<dyn Rdd<Item = Vec<Self::Item>>>;

    fn reduce<F>(&self, func: F) -> BaseResult<Option<Self::Item>>
    where
        F: Fn(Self::Item, Self::Item) -> Self::Item;

    /// Aggregate the elements of each partition, and then the BaseResults for all the partitions, using a
    /// given associative function and a neutral "initial value". The function
    /// Fn(t1, t2) is allowed to modify t1 and return it as its BaseResult value to avoid object
    /// allocation; however, it should not modify t2.
    ///
    /// This behaves somewhat differently from fold operations implemented for non-distributed
    /// collections. This fold operation may be applied to partitions individually, and then fold
    /// those BaseResults into the final BaseResult, rather than apply the fold to each element sequentially
    /// in some defined ordering. For functions that are not commutative, the BaseResult may differ from
    /// that of a fold applied to a non-distributed collection.
    ///
    /// # Arguments
    ///
    /// * `init` - an initial value for the accumulated BaseResult of each partition for the `op`
    ///                  operator, and also the initial value for the combine BaseResults from different
    ///                  partitions for the `f` function - this will typically be the neutral
    ///                  element (e.g. `0` for summation)
    /// * `f` - a function used to both accumulate BaseResults within a partition and combine BaseResults
    ///                  from different partitions
    fn fold<F>(&self, init: Self::Item, f: F) -> BaseResult<Self::Item>
    where
        F: Fn(Self::Item, Self::Item) -> Self::Item;

    /// Aggregate the elements of each partition, and then the BaseResults for all the partitions, using
    /// given combine functions and a neutral "initial value". This function can return a different BaseResult
    /// type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
    /// and one operation for merging two U's, as in Rust Iterator fold method. Both of these functions are
    /// allowed to modify and return their first argument instead of creating a new U to avoid memory
    /// allocation.
    ///
    /// # Arguments
    ///
    /// * `init` - an initial value for the accumulated BaseResult of each partition for the `seq_fn` function,
    ///                  and also the initial value for the combine BaseResults from
    ///                  different partitions for the `comb_fn` function - this will typically be the
    ///                  neutral element (e.g. `vec![]` for vector aggregation or `0` for summation)
    /// * `seq_fn` - a function used to accumulate BaseResults within a partition
    /// * `comb_fn` - an associative function used to combine BaseResults from different partitions
    fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> BaseResult<U>
    where
        U: Data,
        SF: Fn(U, Self::Item) -> U,
        CF: Fn(U, U) -> U;

    /// Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
    /// elements (a, b) where a is in `this` and b is in `other`.
    fn cartesian<U: Data>(
        &self,
        other: Arc<dyn Rdd<Item = U>>,
    ) -> Arc<dyn Rdd<Item = (Self::Item, U)>>;

    /// Return a new RDD that is reduced into `num_partitions` partitions.
    ///
    /// This BaseResults in a narrow dependency, e.g. if you go from 1000 partitions
    /// to 100 partitions, there will not be a shuffle, instead each of the 100
    /// new partitions will claim 10 of the current partitions. If a larger number
    /// of partitions is requested, it will stay at the current number of partitions.
    ///
    /// However, if you're doing a drastic coalesce, e.g. to num_partitions = 1,
    /// this may BaseResult in your computation taking place on fewer nodes than
    /// you like (e.g. one node in the case of num_partitions = 1). To avoid this,
    /// you can pass shuffle = true. This will add a shuffle step, but means the
    /// current upstream partitions will be executed in parallel (per whatever
    /// the current partitioning is).
    ///
    /// # Notes
    ///
    /// With shuffle = true, you can actually coalesce to a larger number
    /// of partitions. This is useful if you have a small number of partitions,
    /// say 100, potentially with a few partitions being abnormally large. Calling
    /// coalesce(1000, shuffle = true) will BaseResult in 1000 partitions with the
    /// data distributed using a hash partitioner. The optional partition coalescer
    /// passed in must be serializable.
    fn coalesce(&self, num_partitions: usize, shuffle: bool) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn collect(&self) -> BaseResult<Vec<Self::Item>>;

    fn count(&self) -> BaseResult<u64>;

    /// Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
    fn count_by_value(&self) -> Arc<dyn Rdd<Item = (Self::Item, u64)>>;

    /// Approximate version of `count_by_value`.
    ///
    /// # Arguments
    /// * `timeout` - maximum time to wait for the job, in milliseconds
    /// * `confidence` - the desired statistical confidence in the BaseResult
    fn count_by_value_aprox(
        &self,
        timeout: Duration,
        confidence: Option<f64>,
    ) -> BaseResult<BaseResult<HashMap<Self::Item, BoundedDouble>>>;

    /// Return a new RDD containing the distinct elements in this RDD.
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) -> Arc<dyn Rdd<Item = Self::Item>>;

    /// Return a new RDD containing the distinct elements in this RDD.
    fn distinct(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    /// Return the first element in this RDD.
    fn first(&self) -> BaseResult<Self::Item>;

    /// Return a new RDD that has exactly num_partitions partitions.
    ///
    /// Can increase or decrease the level of parallelism in this RDD. Internally, this uses
    /// a shuffle to redistribute data.
    ///
    /// If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
    /// which can avoid performing a shuffle.
    fn repartition(&self, num_partitions: usize) -> Arc<dyn Rdd<Item = Self::Item>>;

    /// Take the first num elements of the RDD. It works by first scanning one partition, and use the
    /// BaseResults from that partition to estimate the number of additional partitions needed to satisfy
    /// the limit.
    ///
    /// This method should only be used if the BaseResulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn take(&self, num: usize) -> BaseResult<Vec<Self::Item>>;

    /// Randomly splits this RDD with the provided weights.
    fn random_split(
        &self,
        weights: Vec<f64>,
        seed: Option<u64>,
    ) -> Vec<Arc<dyn Rdd<Item = Self::Item>>>;

    /// Return a sampled subset of this RDD.
    ///
    /// # Arguments
    ///
    /// * `with_replacement` - can elements be sampled multiple times (replaced when sampled out)
    /// * `fraction` - expected size of the sample as a fraction of this RDD's size
    /// ** if without replacement: probability that each element is chosen; fraction must be [0, 1]
    /// ** if with replacement: expected number of times each element is chosen; fraction must be greater than or equal to 0
    /// * seed for the random number generator
    ///
    /// # Notes
    ///
    /// This is NOT guaranteed to provide exactly the fraction of the count of the given RDD.
    ///
    /// Replacement requires extra allocations due to the nature of the used sampler (Poisson distribution).
    /// This implies a performance penalty but should be negligible unless fraction and the dataset are rather large.
    fn sample(&self, with_replacement: bool, fraction: f64) -> Arc<dyn Rdd<Item = Self::Item>>;

    /// Return a fixed-size sampled subset of this RDD in an array.
    ///
    /// # Arguments
    ///
    /// `with_replacement` - can elements be sampled multiple times (replaced when sampled out)
    ///
    /// # Notes
    ///
    /// This method should only be used if the BaseResulting array is expected to be small,
    /// as all the data is loaded into the driver's memory.
    ///
    /// Replacement requires extra allocations due to the nature of the used sampler (Poisson distribution).
    /// This implies a performance penalty but should be negligible unless fraction and the dataset are rather large.
    fn take_sample(
        &self,
        with_replacement: bool,
        num: u64,
        seed: Option<u64>,
    ) -> BaseResult<Vec<Self::Item>>;

    /// Applies a function f to all elements of this RDD.
    fn for_each<F: Fn(Self::Item)>(&self, func: F) -> BaseResult<Vec<()>>;

    /// Applies a function f to each partition of this RDD.
    fn for_each_partition<F: Fn(Box<dyn Iterator<Item = Self::Item>>)>(
        &self,
        func: F,
    ) -> BaseResult<Vec<()>>;

    fn union(
        &self,
        other: Arc<dyn Rdd<Item = Self::Item>>,
    ) -> BaseResult<Arc<dyn Rdd<Item = Self::Item>>>;

    fn zip<S: Data>(&self, second: Arc<dyn Rdd<Item = S>>) -> Arc<dyn Rdd<Item = (Self::Item, S)>>;

    fn intersection<T: Rdd<Item = Self::Item>>(
        &self,
        other: Arc<T>,
    ) -> Arc<dyn Rdd<Item = Self::Item>>;

    /// subtract function, same as the one found in apache spark
    /// example of subtract can be found in subtract.rs
    /// performs a full outer join followed by and intersection with self to get subtraction.
    fn subtract<T: Rdd<Item = Self::Item>>(&self, other: Arc<T>)
    -> Arc<dyn Rdd<Item = Self::Item>>;

    fn subtract_with_num_partition<T: Rdd<Item = Self::Item>>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn intersection_with_num_partitions<T: Rdd<Item = Self::Item>>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = Self::Item>>;

    /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    /// mapping to that key. The ordering of elements within each group is not guaranteed, and
    /// may even differ each time the BaseResulting RDD is evaluated.
    ///
    /// # Notes
    ///
    /// This operation may be very expensive. If you are grouping in order to perform an
    /// aggregation (such as a sum or average) over each key, using `aggregate_by_key`
    /// or `reduce_by_key` will provide much better performance.
    fn group_by<K, F>(&self, func: F) -> Arc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        K: Data + std::hash::Hash + Eq,
        F: Fn(&Self::Item) -> K;

    /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    /// mapping to that key. The ordering of elements within each group is not guaranteed, and
    /// may even differ each time the BaseResulting RDD is evaluated.
    ///
    /// # Notes
    ///
    /// This operation may be very expensive. If you are grouping in order to perform an
    /// aggregation (such as a sum or average) over each key, using `aggregate_by_key`
    /// or `reduce_by_key` will provide much better performance.
    fn group_by_with_num_partitions<K, F>(
        &self,
        func: F,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        K: Data + std::hash::Hash + Eq,
        F: Fn(&Self::Item) -> K;

    /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    /// mapping to that key. The ordering of elements within each group is not guaranteed, and
    /// may even differ each time the BaseResulting RDD is evaluated.
    ///
    /// # Notes
    ///
    /// This operation may be very expensive. If you are grouping in order to perform an
    /// aggregation (such as a sum or average) over each key, using `aggregate_by_key`
    /// or `reduce_by_key` will provide much better performance.
    fn group_by_with_partitioner<K, F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> Arc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        K: Data + std::hash::Hash + Eq,
        F: Fn(&Self::Item) -> K;

    /// Approximate version of count() that returns a potentially incomplete BaseResult
    /// within a timeout, even if not all tasks have finished.
    ///
    /// The confidence is the probability that the error bounds of the BaseResult will
    /// contain the true value. That is, if count_approx were called repeatedly
    /// with confidence 0.9, we would expect 90% of the BaseResults to contain the
    /// true count. The confidence must be in the range [0,1] or an exception will
    /// be thrown.
    ///
    /// # Arguments
    /// * `timeout` - maximum time to wait for the job, in milliseconds
    /// * `confidence` - the desired statistical confidence in the BaseResult
    fn count_approx(
        &self,
        timeout: Duration,
        confidence: Option<f64>,
    ) -> BaseResult<BaseResult<BoundedDouble>>;

    /// Creates tuples of the elements in this RDD by applying `f`.
    fn key_by<T, F>(&self, func: F) -> Arc<dyn Rdd<Item = (Self::Item, T)>>
    where
        T: Data,
        F: Fn(&Self::Item) -> T;

    /// Check if the RDD contains no elements at all. Note that an RDD may be empty even when it
    /// has at least 1 partition.
    fn is_empty(&self) -> bool;

    /// Returns the max element of this RDD.
    fn max(&self) -> BaseResult<Option<Self::Item>>;

    /// Returns the min element of this RDD.
    fn min(&self) -> BaseResult<Option<Self::Item>>;

    /// Returns the first k (largest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [take_ordered](#take_ordered).
    /// # Notes
    /// This method should only be used if the BaseResulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn top(&self, num: usize) -> BaseResult<Vec<Self::Item>>;

    /// Returns the first k (smallest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [top()](#top).
    /// # Notes
    /// This method should only be used if the BaseResulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn take_ordered(&self, num: usize) -> BaseResult<Vec<Self::Item>>;
}

pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T;
}
