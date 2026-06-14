use super::*;

/// Result element of a full outer join: a key with optionally-present values from each side.
type FullOuterJoined<K, V, U> = (K, (Option<V>, Option<U>));

impl<K, V> TypedRdd<(K, V)>
where
    K: Data + Eq + std::hash::Hash + Clone,
    V: Data + Clone,
{
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
    ) -> TypedRdd<FullOuterJoined<K, V, U>>
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

        let mut result: Vec<FullOuterJoined<K, V, U>> = Vec::new();
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
    pub fn full_outer_join<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<FullOuterJoined<K, V, U>>
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
