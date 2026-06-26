use super::*;

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

        // Sort-shuffle: the comparator makes the shuffle write sorted runs and the reduce
        // k-way sort-merge them, so each range partition is already key-ordered — no
        // post-shuffle re-sort needed, just flatten the grouped values back to pairs.
        let cmp: atomic_data::dependency::KeyComparator<K> = if ascending {
            Arc::new(|a: &K, b: &K| a.cmp(b))
        } else {
            Arc::new(|a: &K, b: &K| b.cmp(a))
        };
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
            Some(cmp),
        )
        .map_partitions(move |iter| {
            Box::new(iter.flat_map(|(k, vs): (K, Vec<V>)| {
                vs.into_iter()
                    .map(move |v| (k.clone(), v))
                    .collect::<Vec<_>>()
            })) as Box<dyn Iterator<Item = (K, V)>>
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
        K: WireEncode + bincode::Encode,
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

    /// Repartition by a registered [`NamedPartitioner`] **and** sort by key within
    /// each resulting partition — in a single shuffle.
    ///
    /// Unlike [`sort_by_key`](Self::sort_by_key) (global order via a range partitioner),
    /// this uses *your* partitioner to place keys and only orders within each bucket.
    /// It is the "secondary sort" primitive: partition by the primary part of a
    /// composite key, then iterate each partition in key order. Doing it as one
    /// shuffle is cheaper than `partition_by_named(...).sort_by_key(...)` (two shuffles)
    /// and, unlike a per-partition re-sort, the ordering is produced by the shuffle's
    /// sort-merge itself.
    ///
    /// The partitioner ships to workers by name (register it with
    /// `atomic_compute::register_partitioner!(P)`); for the distributed sorted result,
    /// register the value wire type with `register_sort_shuffle_map!(K, V)`.
    ///
    /// # Example
    /// ```ignore
    /// // Partition by `key % 4`, key-sorted within each partition.
    /// let secondary = pairs.repartition_and_sort(ModPartitioner { n: 4 }, true);
    /// ```
    pub fn repartition_and_sort<P>(self, partitioner: P, ascending: bool) -> TypedRdd<(K, V)>
    where
        P: atomic_data::partitioner::NamedPartitioner,
        Vec<(K, V)>: WireDecode + WireEncode,
        K: WireEncode + bincode::Encode + bincode::Decode<()> + std::hash::Hash + Eq,
        V: WireEncode + bincode::Encode + bincode::Decode<()>,
        Vec<(K, Vec<V>)>: WireEncode,
    {
        let p = Partitioner::from_named::<P>(partitioner.num_partitions());

        // Sort-shuffle: the comparator makes each partition's bucket key-ordered via
        // the reduce-side k-way merge — no post-shuffle re-sort. Grouped values are
        // then flattened back to pairs in key order.
        let cmp: atomic_data::dependency::KeyComparator<K> = if ascending {
            Arc::new(|a: &K, b: &K| a.cmp(b))
        } else {
            Arc::new(|a: &K, b: &K| b.cmp(a))
        };
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
            p,
            Some(cmp),
        )
        .map_partitions(move |iter| {
            let flattened_vals = iter.flat_map(|(k, vs): (K, Vec<V>)| {
                vs.into_iter()
                    .map(move |v| (k.clone(), v))
                    .collect::<Vec<_>>()
            });
            Box::new(flattened_vals) as Box<dyn Iterator<Item = (K, V)>>
        })
    }
}

impl<T> TypedRdd<T>
where
    T: Data + Clone + WireEncode + WireDecode,
    Vec<T>: WireEncode + WireDecode,
{
    /// Distributed sort of a non-pair RDD by a **registered** key-value task.
    ///
    /// The closure-based [`sort_by`](TypedRdd::sort_by) must collect to the driver
    /// because a `Fn(&T) -> K` cannot be shipped. `sort_by_task` instead takes a
    /// `#[task]`-registered function that pairs each element with its sort key
    /// (`T -> (K, T)`); that op ships by id, so the sort runs as a range-partitioned
    /// shuffle with no full-dataset driver collect — the same path as
    /// [`sort_by_key`](TypedRdd::sort_by_key).
    ///
    /// Register the `(K, T)` shuffle with `register_sort_shuffle_map!(K, T)` for a
    /// globally-ordered distributed result.
    ///
    /// ```ignore
    /// #[task] fn by_len(s: String) -> (usize, String) { (s.len(), s) }
    /// atomic_compute::register_sort_shuffle_map!(usize, String);
    /// let sorted = rdd.sort_by_task(ByLen, true).collect()?;
    /// ```
    pub fn sort_by_task<F, K>(self, key_value_task: F, ascending: bool) -> TypedRdd<T>
    where
        F: UnaryTask<T, (K, T)>,
        K: Data
            + Ord
            + Eq
            + std::hash::Hash
            + Clone
            + WireEncode
            + WireDecode
            + bincode::Encode
            + bincode::Decode<()>,
        T: bincode::Encode + bincode::Decode<()>,
        (K, T): Data + Clone + WireEncode + WireDecode,
        Vec<(K, T)>: WireEncode + WireDecode,
        Vec<K>: WireDecode,
        Vec<(K, Vec<T>)>: WireEncode,
    {
        self.map_task(key_value_task)
            .sort_by_key(ascending)
            .values()
    }
}
