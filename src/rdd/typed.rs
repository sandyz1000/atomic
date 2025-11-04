use crate::partial::bounded_double::BoundedDouble;
use crate::partial::result::PartialResult;
use crate::rdd::RddOperation;
use crate::rdd::cartesian::CartesianRdd;
use crate::rdd::map_partitions::MapPartitionsRdd;
use crate::task::TaskContext;
use crate::{
    rdd::{Data, Rdd, RddBase},
    utils::bpq::BoundedPriorityQueue,
};
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::split::Split;
use crate::rdd::coalesced::CoalescedRdd;
use crate::partial::group_count_eval::GroupedCountEvaluator;

/// Type alias for Arc-wrapped RDD trait objects
pub type RddRef<T> = Arc<dyn Rdd<Item = T>>;

pub struct TypedRdd<T> {
    rdd: RddRef<T>,
    _marker: PhantomData<T>,
}

impl<T> TypedRdd<T> {
    pub fn new(rdd: RddRef<T>) -> Self {
        Self {
            rdd,
            _marker: PhantomData
        }
    }
}

impl <D> Rdd for TypedRdd<D>
where
    D: Data + Ord + Eq + std::hash::Hash
{
    type Item = D;
    
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        todo!()
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        todo!()
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        todo!()
    }
}


impl<D: Data + Ord + Eq + std::hash::Hash> RddOperation for TypedRdd<D> {
    type Item = D;

    /// Returns the first k (largest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [take_ordered](#take_ordered).
    /// # Notes
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn top(&self, num: usize) -> Result<Vec<Self::Item>> {
        Ok(self
            .map(|x| std::cmp::Reverse(x))
            .take_ordered(num)?
            .into_iter()
            .map(|x| x.0)
            .collect())
    }

    fn map_partitions<U: Data, F>(&self, func: F) -> Arc<dyn Rdd<Item = U>>
    where
        F: Fn(Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
    {
        let ignore_idx = move |_index: usize,
                               items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> { (func)(items) };
        Arc::new(MapPartitionsRdd::new(self.get_rdd(), ignore_idx))
    }

    /// Returns the first k (smallest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [top()](#top).
    /// # Notes
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn take_ordered(&self, num: usize) -> Result<Vec<Self::Item>> {
        if num == 0 {
            Ok(vec![])
        } else {
            let first_k_func = move |partition: Box<dyn Iterator<Item = Self::Item>>| -> Box<
                dyn Iterator<Item = BoundedPriorityQueue<Self::Item>>,
            > {
                let mut queue = BoundedPriorityQueue::new(num);
                partition.for_each(|item: Self::Item| queue.append(item));
                Box::new(std::iter::once(queue))
            };

            let queue = self
                .map_partitions(first_k_func)
                .reduce(
                    move |queue1: BoundedPriorityQueue<Self::Item>,
                          queue2: BoundedPriorityQueue<Self::Item>|
                          -> BoundedPriorityQueue<Self::Item> {
                        queue1.merge(queue2)
                    },
                )?
                .ok_or_else(|| crate::error::Error::Other)?;

            let queue = queue as BoundedPriorityQueue<Self::Item>;

            Ok(queue.into())
        }
    }

    

    fn iterator(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        self.compute(split)
    }

    fn filter<F>(&self, predicate: F) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        F: Fn(&Self::Item) -> bool + Copy,
    {
        let filter_fn =
            move |_index: usize,
                  items: Box<dyn Iterator<Item = Self::Item>>|
                  -> Box<dyn Iterator<Item = _>> { Box::new(items.filter(predicate)) };
        Arc::new(MapPartitionsRdd::new(self.get_rdd(), filter_fn))
    }

    fn map<U: super::Data, F>(&self, f: F) -> Arc<dyn Rdd<Item = U>>
    where
        F: Fn(Self::Item) -> U,
        Self: Sized,
    {
        Arc::new(MapperRdd::new(self.get_rdd(), f))
    }

    fn flat_map<U: super::Data, F>(&self, f: F) -> Arc<dyn Rdd<Item = U>>
    where
        F: Fn(Self::Item) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        Arc::new(FlatMapperRdd::new(self.get_rdd(), f))
    }

    fn map_partitions_with_index<U: super::Data, F>(&self, f: F) -> Arc<dyn Rdd<Item = U>>
    where
        F: Fn(usize, Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        Arc::new(MapPartitionsRdd::new(self.get_rdd(), f))
    }

    fn glom(&self) -> Arc<dyn Rdd<Item = Vec<Self::Item>>> {
        let func = |_index: usize, iter: Box<dyn Iterator<Item = Self::Item>>| {
            Box::new(std::iter::once(iter.collect::<Vec<_>>()))
                // as Box<dyn Iterator<Item = Vec<Self::Item>>>
        };
        let rdd = MapPartitionsRdd::new(self.get_rdd(), Box::new(func));
        rdd.register_op_name("gloom");
        Arc::new(rdd)
    }

    fn save_as_text_file(&self, path: String) -> Result<Vec<()>> {
        let cl = move |(ctx, iter)| super::save::<Self::Item>(ctx, iter, path.to_string());
        self.get_context().run_job_with_context(self.get_rdd(), cl)
    }

    fn reduce<F>(&self, func: F) -> Result<Option<Self::Item>>
    where
        F: Fn(Self::Item, Self::Item) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = func.clone();
        let reduce_partition = move |iter: Box<dyn Iterator<Item = Self::Item>>| {
            let acc = iter.reduce(&cf);
            match acc {
                None => std::vec![],
                Some(e) => std::vec![e],
            }
        };
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        Ok(results?.into_iter().flatten().reduce(func))
    }

    fn fold<F>(&self, init: Self::Item, f: F) -> Result<Self::Item>
    where
        F: Fn(Self::Item, Self::Item) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &cf);
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        Ok(results?.into_iter().fold(init, f))
    }

    fn aggregate<U: super::Data, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U>
    where
        SF: Fn(U, Self::Item) -> U,
        CF: Fn(U, U) -> U,
    {
        let zero = init.clone();
        let reduce_partition =
            move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.fold(zero.clone(), &seq_fn);
        let results = self.get_context().run_job(self.get_rdd(), reduce_partition);
        Ok(results?.into_iter().fold(init, comb_fn))
    }

    fn cartesian<U: super::Data>(
        &self,
        other: Arc<dyn Rdd<Item = U>>,
    ) -> Arc<dyn Rdd<Item = (Self::Item, U)>> {
        Arc::new(CartesianRdd::new(self.get_rdd(), other.into()))
    }

    fn coalesce(&self, num_partitions: usize, shuffle: bool) -> Arc<dyn Rdd<Item = Self::Item>> {
        
        if shuffle {
            // Distributes elements evenly across output partitions, starting from a random partition.
            let distributed_partition =
                move |index: usize, items: Box<dyn Iterator<Item = Self::Item>>| {
                    let mut hasher = FxHasher::default();
                    index.hash(&mut hasher);
                    let mut rand = crate::utils::random::get_default_rng_from_seed(hasher.finish());
                    let mut position = rand.gen_range(0, num_partitions);
                    Box::new(items.map(move |t| {
                        // Note that the hash code of the key will just be the key itself.
                        // The HashPartitioner will mod it with the number of total partitions.
                        position += 1;
                        (position, t)
                    })) as Box<dyn Iterator<Item = (usize, Self::Item)>>
                };

            let map_steep: Arc<dyn Rdd<Item = (usize, Self::Item)>> =
                Arc::new(MapPartitionsRdd::new(self.get_rdd(), distributed_partition));
            let partitioner = Box::new(HashPartitioner::<usize>::new(num_partitions));
            Arc::new(CoalescedRdd::new(
                Arc::new(map_steep.partition_by_key(partitioner)),
                num_partitions,
            ))
        } else {
            Arc::new(CoalescedRdd::new(self.get_rdd(), num_partitions))
        }
    }

    fn collect(&self) -> Result<Vec<Self::Item>> {
        let cl = |iter: Box<dyn Iterator<Item = Self::Item>>| iter.collect::<Vec<Self::Item>>();
        let results = self.get_context().run_job(self.get_rdd(), cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<Self::Item>| a + b.len());
        Ok(results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            }))
    }

    fn count(&self) -> Result<u64> {
        let context = self.get_context();
        let counting_func = |iter: Box<dyn Iterator<Item = Self::Item>>| iter.count() as u64;
        Ok(context
            .run_job(self.get_rdd(), counting_func)?
            .into_iter()
            .sum())
    }

    fn count_by_value(&self) -> Arc<dyn Rdd<Item = (Self::Item, u64)>> {
        self.map(|x| (x, 1u64)).reduce_by_key(
            Box::new(|(x, y)| x + y) as Box<dyn Fn((u64, u64)) -> u64>,
            self.number_of_splits(),
        )
    }

    fn count_by_value_aprox(
        &self,
        timeout: std::time::Duration,
        confidence: Option<f64>,
    ) -> Result<PartialResult<HashMap<Self::Item, BoundedDouble>>> {
        let confidence = if let Some(confidence) = confidence {
            confidence
        } else {
            0.95
        };
        assert!(0.0 <= confidence && confidence <= 1.0);

        let count_partition = |(_ctx, iter): (
            TaskContext,
            Box<dyn Iterator<Item = Self::Item>>,
        )|
         -> HashMap<Self::Item, usize> {
            let mut map = HashMap::new();
            iter.for_each(|e| {
                *map.entry(e).or_insert(0) += 1;
            });
            map
        };

        let evaluator = GroupedCountEvaluator::new(self.number_of_splits(), confidence);
        let rdd = self.get_rdd();
        rdd.register_op_name("count_by_value_approx");
        self.get_context()
            .run_approximate_job(count_partition, rdd, evaluator, timeout)
    }

    fn distinct_with_num_partitions(&self, num_partitions: usize) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self::Item: super::Data + Eq + std::hash::Hash,
    {
        let func = Box::new(|x| (Some(x), None))
            as Box<dyn Fn(Self::Item) -> (Option<Self::Item>, Option<Self::Item>)>;
        self.map(func)
            .reduce_by_key(Box::new(|(_x, y)| y), num_partitions)
            .map(Box::new(|x: (Option<Self::Item>, Option<Self::Item>)| {
                let (x, _y) = x;
                x.unwrap()
            }))
    }

    fn distinct(&self) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: super::Data + Eq + std::hash::Hash,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    fn first(&self) -> Result<Self::Item>
    where
        Self: Sized,
    {
        if let Some(result) = self.take(1)?.into_iter().next() {
            Ok(result)
        } else {
            Err(Error::UnsupportedOperation("empty collection"))
        }
    }

    fn repartition(&self, num_partitions: usize) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        self.coalesce(num_partitions, true)
    }

    fn take(&self, num: usize) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        // TODO: in original spark this is configurable; see rdd/RDD.scala:1397
        // Math.max(conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
        const SCALE_UP_FACTOR: f64 = 2.0;
        if num == 0 {
            return Ok(std::vec![]);
        }
        let mut buf = std::vec![];
        let total_parts = self.number_of_splits() as u32;
        let mut parts_scanned = 0_u32;
        while buf.len() < num && parts_scanned < total_parts {
            // The number of partitions to try in this iteration. It is ok for this number to be
            // greater than total_parts because we actually cap it at total_parts in run_job.
            let mut num_parts_to_try = 1u32;
            let left = num - buf.len();
            if parts_scanned > 0 {
                // If we didn't find any rows after the previous iteration, quadruple and retry.
                // Otherwise, interpolate the number of partitions we need to try, but overestimate
                // it by 50%. We also cap the estimation in the end.
                let parts_scanned = f64::from(parts_scanned);
                num_parts_to_try = if buf.is_empty() {
                    (parts_scanned * SCALE_UP_FACTOR).ceil() as u32
                } else {
                    let num_parts_to_try =
                        (1.5 * left as f64 * parts_scanned / (buf.len() as f64)).ceil();
                    num_parts_to_try.min(parts_scanned * SCALE_UP_FACTOR) as u32
                };
            }

            let partitions: Vec<_> = (parts_scanned as usize
                ..total_parts.min(parts_scanned + num_parts_to_try) as usize)
                .collect();
            let num_partitions = partitions.len() as u32;
            let take_from_partion = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| {
                iter.take(left).collect::<Vec<Self::Item>>()
            });

            let res = self.get_context().run_job_with_partitions(
                self.get_rdd(),
                take_from_partion,
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

    fn random_split(
        &self,
        weights: Vec<f64>,
        seed: Option<u64>,
    ) -> Vec<Arc<dyn Rdd<Item = Self::Item>>>
    where
        Self: Sized,
    {
        let sum: f64 = weights.iter().sum();
        assert!(
            weights.iter().all(|&x| x >= 0.0),
            format!("Weights must be nonnegative, but got {:?}", weights)
        );
        assert!(
            sum > 0.0,
            format!("Sum of weights must be positive, but got {:?}", weights)
        );

        let seed_val: u64 = seed.unwrap_or(rand::random::<u64>());

        let mut full_bounds = std::vec![0.0f64];
        let bounds = weights
            .into_iter()
            .map(|weight| weight / sum)
            .scan(0.0f64, |state, x| {
                *state = *state + x;
                Some(*state)
            });
        full_bounds.extend(bounds);

        let mut splitted_rdds: Vec<Arc<dyn Rdd<Item = Self::Item>>> = Vec::new();

        for bound in full_bounds.windows(2) {
            let (lower_bound, upper_bound) = (bound[0], bound[1]);
            let func = Fn!(move |index: usize,
                                 partition: Box<dyn Iterator<Item = Self::Item>>|
                  -> Box<dyn Iterator<Item = Self::Item>> {
                let bcs = Arc::new(BernoulliCellSampler::new(lower_bound, upper_bound, false))
                    as Arc<dyn RandomSampler<Self::Item>>;

                let sampler_func = bcs.get_sampler(Some(seed_val + index as u64));

                Box::new(sampler_func(partition).into_iter())
            });
            let rdd = Arc::new(MapPartitionsRdd::new(self.get_rdd(), func));
            splitted_rdds.push(rdd.clone());
        }

        splitted_rdds
    }

    fn sample(&self, with_replacement: bool, fraction: f64) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        Arc::new(PartitionwiseSampledRdd::new(self.get_rdd(), sampler, true))
    }

    fn take_sample(
        &self,
        with_replacement: bool,
        num: u64,
        seed: Option<u64>,
    ) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        const NUM_STD_DEV: f64 = 10.0f64;
        const REPETITION_GUARD: u8 = 100;
        // TODO: this could be const eval when the support is there for the necessary functions
        let max_sample_size = std::u64::MAX - (NUM_STD_DEV * (std::u64::MAX as f64).sqrt()) as u64;
        assert!(num <= max_sample_size);

        if num == 0 {
            return Ok(std::vec![]);
        }

        let initial_count = self.count()?;
        if initial_count == 0 {
            return Ok(std::vec![]);
        }

        // The original implementation uses java.util.Random which is a LCG pseudorng,
        // not cryptographically secure and some problems;
        // Here we choose Pcg64, which is a proven good performant pseudorng although without
        // strong cryptographic guarantees, which ain't necessary here.
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };

        if !with_replacement && num >= initial_count {
            let mut sample = self.collect()?;
            utils::randomize_in_place(&mut sample, &mut rng);
            Ok(sample)
        } else {
            let fraction = utils::random::compute_fraction_for_sample_size(
                num,
                initial_count,
                with_replacement,
            );
            let mut samples = self.sample(with_replacement, fraction).collect()?;

            // If the first sample didn't turn out large enough, keep trying to take samples;
            // this shouldn't happen often because we use a big multiplier for the initial size.
            let mut num_iters = 0;
            while samples.len() < num as usize && num_iters < REPETITION_GUARD {
                log::warn!(
                    "Needed to re-sample due to insufficient sample size. Repeat #{}",
                    num_iters
                );
                samples = self.sample(with_replacement, fraction).collect()?;
                num_iters += 1;
            }

            if num_iters >= REPETITION_GUARD {
                std::panic!("Repeated sampling {} times; aborting", REPETITION_GUARD)
            }

            utils::randomize_in_place(&mut samples, &mut rng);
            Ok(samples.into_iter().take(num as usize).collect::<Vec<_>>())
        }
    }

    fn for_each<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: Fn(Self::Item),
        Self: Sized,
    {
        let func = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| iter.for_each(&func));
        self.get_context().run_job(self.get_rdd(), func)
    }

    fn for_each_partition<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: Fn(Box<dyn Iterator<Item = Self::Item>>),
        Self: Sized + 'static,
    {
        let func = Fn!(move |iter: Box<dyn Iterator<Item = Self::Item>>| (&func)(iter));
        self.get_context().run_job(self.get_rdd(), func)
    }

    fn union(
        &self,
        other: Arc<dyn Rdd<Item = Self::Item>>,
    ) -> Result<Arc<dyn Rdd<Item = Self::Item>>>
    where
        Self: Clone,
    {
        Ok(Arc::new(crate::context::Context::union(&[
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            other,
        ])?))
    }

    fn zip<S: super::Data>(
        &self,
        second: Arc<dyn Rdd<Item = S>>,
    ) -> Arc<dyn Rdd<Item = (Self::Item, S)>>
    where
        Self: Clone,
    {
        Arc::new(ZippedPartitionsRdd::<Self::Item, S>::new(
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            second,
        ))
    }

    fn intersection<T>(&self, other: Arc<T>) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: super::Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        self.intersection_with_num_partitions(other, self.number_of_splits())
    }

    fn subtract<T>(&self, other: Arc<T>) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: super::Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        self.subtract_with_num_partition(other, self.number_of_splits())
    }

    fn subtract_with_num_partition<T>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: super::Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )))
            .clone();
        let rdd = self
            .map(Box::new(Fn!(|x| -> (Self::Item, Option<Self::Item>) {
                (x, None)
            })))
            .cogroup(
                other,
                Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>,
            )
            .map(Box::new(Fn!(|(x, (v1, v2)): (
                Self::Item,
                (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>)
            )|
             -> Option<Self::Item> {
                if (v1.len() >= 1) ^ (v2.len() >= 1) {
                    Some(x)
                } else {
                    None
                }
            })))
            .map_partitions(Box::new(Fn!(|iter: Box<
                dyn Iterator<Item = Option<Self::Item>>,
            >|
             -> Box<
                dyn Iterator<Item = Self::Item>,
            > {
                Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                    as Box<dyn Iterator<Item = Self::Item>>
            })));

        let subtraction = self.intersection(Arc::new(rdd));
        (&*subtraction).register_op_name("subtraction");
        subtraction
    }

    fn intersection_with_num_partitions<T>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: super::Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )))
            .clone();
        let rdd = self
            .map(Box::new(Fn!(|x| -> (Self::Item, Option<Self::Item>) {
                (x, None)
            })))
            .cogroup(
                other,
                Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>,
            )
            .map(Box::new(Fn!(|(x, (v1, v2)): (
                Self::Item,
                (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>)
            )|
             -> Option<Self::Item> {
                if v1.len() >= 1 && v2.len() >= 1 {
                    Some(x)
                } else {
                    None
                }
            })))
            .map_partitions(Box::new(Fn!(|iter: Box<
                dyn Iterator<Item = Option<Self::Item>>,
            >|
             -> Box<
                dyn Iterator<Item = Self::Item>,
            > {
                Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                    as Box<dyn Iterator<Item = Self::Item>>
            })));
        (&*rdd).register_op_name("intersection");
        rdd
    }

    fn group_by<K, F>(&self, func: F) -> Arc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        Self: Sized,
        K: super::Data + Hash + Eq,
        F: Fn(&Self::Item) -> K,
    {
        self.group_by_with_num_partitions(func, self.number_of_splits())
    }

    fn group_by_with_num_partitions<K, F>(
        &self,
        func: F,
        num_splits: usize,
    ) -> Arc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        Self: Sized,
        K: super::Data + Hash + Eq,
        F: Fn(&Self::Item) -> K,
    {
        self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })))
        .group_by_key(num_splits)
    }

    fn group_by_with_partitioner<K, F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> Arc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        Self: Sized,
        K: super::Data + Hash + Eq,
        F: Fn(&Self::Item) -> K,
    {
        self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })))
        .group_by_key_using_partitioner(partitioner)
    }

    /// Approximate version of count() that returns a potentially incomplete result
    /// within a timeout, even if not all tasks have finished.
    ///
    /// The confidence is the probability that the error bounds of the result will
    /// contain the true value. That is, if count_approx were called repeatedly
    /// with confidence 0.9, we would expect 90% of the results to contain the
    /// true count. The confidence must be in the range [0,1] or an exception will
    /// be thrown.
    ///
    /// # Arguments
    /// * `timeout` - maximum time to wait for the job, in milliseconds
    /// * `confidence` - the desired statistical confidence in the result
    fn count_approx(
        &self,
        timeout: Duration,
        confidence: Option<f64>,
    ) -> Result<PartialResult<BoundedDouble>>
    where
        Self: Sized,
    {
        let confidence = if let Some(confidence) = confidence {
            confidence
        } else {
            0.95
        };
        assert!(0.0 <= confidence && confidence <= 1.0);

        let count_elements = Fn!(|(_ctx, iter): (
            TaskContext,
            Box<dyn Iterator<Item = Self::Item>>
        )|
         -> usize { iter.count() });

        let evaluator = CountEvaluator::new(self.number_of_splits(), confidence);
        let rdd = self.get_rdd();
        rdd.register_op_name("count_approx");
        self.get_context()
            .run_approximate_job(count_elements, rdd, evaluator, timeout)
    }

    /// Creates tuples of the elements in this RDD by applying `f`.
    fn key_by<T, F>(&self, func: F) -> Arc<dyn Rdd<Item = (Self::Item, T)>>
    where
        Self: Sized,
        T: super::Data,
        F: Fn(&Self::Item) -> T,
    {
        self.map(Fn!(move |k: Self::Item| -> (Self::Item, T) {
            let t = (func)(&k);
            (k, t)
        }))
    }

    /// Check if the RDD contains no elements at all. Note that an RDD may be empty even when it
    /// has at least 1 partition.
    fn is_empty(&self) -> bool
    where
        Self: Sized,
    {
        self.number_of_splits() == 0 || self.take(1).unwrap().len() == 0
    }

    /// Returns the max element of this RDD.
    fn max(&self) -> Result<Option<Self::Item>>
    where
        Self: Sized,
        Self::Item: super::Data + Ord,
    {
        let max_fn = |x: Self::Item, y: Self::Item| x.max(y);

        self.reduce(max_fn)
    }

    /// Returns the min element of this RDD.
    fn min(&self) -> Result<Option<Self::Item>> {
        let min_fn = |x: Self::Item, y: Self::Item| x.min(y);
        self.reduce(min_fn)
    }

}
