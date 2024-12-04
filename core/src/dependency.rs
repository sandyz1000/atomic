use crate::aggregator::Aggregator;
use crate::partitioner::Partitioner;
use crate::rdd::rdd::RddBase;
use crate::ser_data::{Data, SerFunc};
use crate::{env, Rdd};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

// Revise if enum is good choice. Considering enum since down casting one trait
// object to another trait object is difficult.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Dependency<N, S> {
    NarrowDependency(Arc<N>),
    ShuffleDependency(Arc<S>),
}

pub trait NarrowDependencyTrait: Send + Sync {
    type R: RddBase;

    fn get_parents(&self, partition_id: usize) -> Vec<usize>;
    fn get_rdd_base(&self) -> Arc<Self::R>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OneToOneDependency<RDD> {
    rdd_base: Arc<RDD>,
}

impl<RDD: RddBase> OneToOneDependency<RDD> {
    pub fn new(rdd_base: Arc<RDD>) -> Self {
        OneToOneDependency { rdd_base }
    }
}

impl<RDD: RddBase> NarrowDependencyTrait for OneToOneDependency<RDD> {
    type R = RDD;

    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        vec![partition_id]
    }

    fn get_rdd_base(&self) -> Arc<Self::R> {
        self.rdd_base.clone()
    }
    
}

/// Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
#[derive(Serialize, Deserialize, Clone)]
pub struct RangeDependency<RDD> {
    rdd_base: Arc<RDD>,
    /// the start of the range in the child RDD
    out_start: usize,
    /// the start of the range in the parent RDD
    in_start: usize,
    /// the length of the range
    length: usize,
}

impl<RDD: RddBase> RangeDependency<RDD> {
    pub fn new(rdd_base: Arc<RDD>, in_start: usize, out_start: usize, length: usize) -> Self {
        RangeDependency {
            rdd_base,
            in_start,
            out_start,
            length,
        }
    }
}

impl<RDD: RddBase> NarrowDependencyTrait for RangeDependency<RDD> {
    type R = RDD;
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        if partition_id >= self.out_start && partition_id < self.out_start + self.length {
            vec![partition_id - self.out_start + self.in_start]
        } else {
            Vec::new()
        }
    }

    fn get_rdd_base(&self) -> Arc<Self::R> {
        self.rdd_base.clone()
    }
    
    
}

pub trait ShuffleDependencyTrait: Send + Sync {
    type R: RddBase;

    fn get_shuffle_id(&self) -> usize;
    fn get_rdd_base(&self) -> Arc<Self::R>;
    fn is_shuffle(&self) -> bool;
    fn do_shuffle_task(&self, rdd_base: Arc<Self::R>, partition: usize) -> String;
}

// impl PartialOrd for dyn ShuffleDependencyTrait {
//     fn partial_cmp(&self, other: &dyn ShuffleDependencyTrait) -> Option<Ordering> {
//         Some(self.get_shuffle_id().cmp(&other.get_shuffle_id()))
//     }
// }

// impl PartialEq for dyn ShuffleDependencyTrait {
//     fn eq(&self, other: &dyn ShuffleDependencyTrait) -> bool {
//         self.get_shuffle_id() == other.get_shuffle_id()
//     }
// }

// impl Eq for dyn ShuffleDependencyTrait {}

// impl Ord for dyn ShuffleDependencyTrait {
//     fn cmp(&self, other: &dyn ShuffleDependencyTrait) -> Ordering {
//         self.get_shuffle_id().cmp(&other.get_shuffle_id())
//     }
// }

#[derive(Debug, Clone, Serialize)]
pub struct ShuffleDependency<K: Data, V: Data, C: Data, RDD, PR, F1, F2, F3> {
    pub shuffle_id: usize,
    pub is_cogroup: bool,
    pub rdd_base: Arc<RDD>, // this need to serializable
    pub aggregator: Arc<Aggregator<K, V, C, F1, F2, F3>>, // this need to serializable
    pub partitioner: Box<PR>, // this need to serializable
    is_shuffle: bool,
}

impl<K: Data, V: Data, C: Data, RDD, PR, F1, F2, F3> ShuffleDependency<K, V, C, RDD, PR, F1, F2, F3>
where
    RDD: RddBase,
    PR: Partitioner,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>,
{
    pub fn new(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd_base: Arc<RDD>,
        aggregator: Arc<Aggregator<K, V, C, F1, F2, F3>>,
        partitioner: Box<PR>,
    ) -> Self {
        ShuffleDependency {
            shuffle_id,
            is_cogroup,
            rdd_base,
            aggregator,
            partitioner,
            is_shuffle: true,
        }
    }
}

impl<K, V, C, RDD, PR, F1, F2, F3> ShuffleDependencyTrait
    for ShuffleDependency<K, V, C, RDD, PR, F1, F2, F3>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
    RDD: RddBase,
    PR: Partitioner,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>,
{
    type R = RDD;

    fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    fn is_shuffle(&self) -> bool {
        self.is_shuffle
    }

    fn get_rdd_base(&self) -> Arc<Self::R> {
        self.rdd_base.clone()
    }

    /// NOTE: This is the actual task that run
    /// The function is a method that performs a Shuffle task. It takes an RDD object and a partition
    /// index as parameters and returns a string representing the server URI for the Shuffle task.
    ///
    /// The method first retrieves the specified partition's Split from the RDD. Then it iterates over
    /// all the elements in that Split and uses the Partitioner to get the corresponding Bucket ID based
    /// on the key-value pair's key.
    /// Next, the method adds the key-value pair to the respective Bucket. If the key already exists in
    /// the Bucket, it calls the merge_value method of the Aggregator to merge the new value with the old
    //// value. Otherwise, it calls the create_combiner method to create a new combiner.
    ///
    /// Finally, the method serializes the key-value pairs in each Bucket into byte arrays and stores them
    /// in the SHUFFLE_CACHE environment variable. The key is a tuple of Shuffle ID, partition index, and
    /// Bucket ID, and the value is the byte array.
    ///
    /// The method returns the server URI for the Shuffle task so that the client can send requests to it
    /// to retrieve Shuffle data.
    ///
    /// Here is an example usage of the function in Rust:
    /// ```
    /// use std::sync::Arc;
    /// use rdd::*;
    ///
    /// // create an RDD
    /// let rdd = sc.parallelize(vec![("a", 1), ("b", 2), ("c", 3)], 2);
    ///
    /// // create a shuffle dependency
    /// let shuffle_dep = ShuffleDependency::new(Arc::new(rdd.clone()), Box::new(|x| x as u64 % 2), 2);
    ///
    /// // create a shuffle map task
    /// let shuffle_map_task = ShuffleMapTask::new(0, Arc::new(shuffle_dep), Box::new(SumAggregator::new()));
    ///
    /// // execute the shuffle map task for partition 0
    /// let server_uri = shuffle_map_task.do_shuffle_task(Arc::new(rdd), 0);
    /// ```
    fn do_shuffle_task(&self, rdd_base: Arc<Self::R>, partition: usize) -> String {
        log::debug!(
            "executing shuffle task #{} for partition #{}",
            self.shuffle_id,
            partition
        );
        let split = rdd_base.splits()[partition].clone();
        let aggregator = self.aggregator.clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();
        log::debug!("is cogroup rdd: {}", self.is_cogroup);
        log::debug!("number of output splits: {}", num_output_splits);
        let partitioner = self.partitioner.clone();
        let mut buckets: Vec<HashMap<K, C>> = (0..num_output_splits)
            .map(|_| HashMap::new())
            .collect::<Vec<_>>();
        log::debug!(
            "before iterating while executing shuffle map task for partition #{}",
            partition
        );
        log::debug!("split index: {}", split.get_index());

        let iter = if self.is_cogroup {
            rdd_base.cogroup_iterator_any(split)
        } else {
            rdd_base.iterator_any(split.clone())
        };

        for (count, i) in iter.unwrap().enumerate() {
            let b = i.into_any().downcast::<(K, V)>().unwrap();
            let (k, v) = *b;
            if count == 0 {
                log::debug!(
                    "iterating inside dependency map task after downcasting: key: {:?}, value: {:?}",
                    k,
                    v
                );
            }
            let bucket_id = partitioner.get_partition(&k);
            let bucket = &mut buckets[bucket_id];
            if let Some(old_v) = bucket.get_mut(&k) {
                let input = ((old_v.clone(), v),);
                let output = aggregator.merge_value.call(input);
                *old_v = output;
            } else {
                bucket.insert(k, aggregator.create_combiner.call((v,)));
            }
        }

        for (i, bucket) in buckets.into_iter().enumerate() {
            let set: Vec<(K, C)> = bucket.into_iter().collect();
            let ser_bytes = bincode::serialize(&set).unwrap();
            log::debug!(
                "shuffle dependency map task set from bucket #{} in shuffle id #{}, partition #{}: {:?}",
                i,
                self.shuffle_id,
                partition,
                set.get(0)
            );
            env::SHUFFLE_CACHE.insert((self.shuffle_id, partition, i), ser_bytes);
        }
        log::debug!(
            "returning shuffle address for shuffle task #{}",
            self.shuffle_id
        );
        env::Env::get().shuffle_manager.get_server_uri()
    }
    
}
