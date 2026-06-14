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
}
