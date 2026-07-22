use rand::RngExt;

use super::*;

impl<T: Data> TypedRdd<T> {
    /// Build a pair RDD keyed by a registered unary task applied to each element — the
    /// content-addressed form of `key_by`. Produces `(task(x), x)`.
    pub fn key_by_task<K, B>(self, task: B) -> TypedRdd<(K, T)>
    where
        K: Data + Clone,
        T: Clone,
        B: UnaryTask<T, K>,
    {
        self.map_rdd(move |id, rdd| MapperRdd::new(id, rdd, move |x| (task.call(x.clone()), x)))
    }

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
        let union_rdd =
            UnionRdd::new(id, &rdds).expect("UnionRdd::new with two RDDs should never fail");
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

    /// Return the globally-distinct elements.
    ///
    /// Mirrors Spark's `distinct` (`map(x => (x, ())).reduce_by_key(keep).map(key)`): each
    /// element is keyed by itself and shuffled, so every equal element lands in the same
    /// reduce partition and collapses to one — duplicates across *different* input
    /// partitions are removed, not just within a partition. Like any shuffle op, the
    /// element wire type must be registered once in the binary: `register_shuffle_map!(T, ())`.
    pub fn distinct(self) -> TypedRdd<T>
    where
        T: Eq + std::hash::Hash + Clone + bincode::Encode + bincode::Decode<()>,
        Vec<(T, ())>: WireEncode,
    {
        let num_partitions = self.rdd.number_of_splits().max(1);
        self.map_partitions_to_pair(|_idx, iter| {
            Box::new(iter.map(|x| (x, ()))) as Box<dyn Iterator<Item = (T, ())>>
        })
        .combine_by_key(|_v| (), |_c, _v| (), |_c1, _c2| (), num_partitions)
        .keys()
    }

    /// Return a new RDD containing elements only in this RDD but not in `other`.
    /// Materialise the other side of a binary set operation into a hash set,
    /// choosing the distributed op path or local run_job as appropriate.
    fn materialize_other_set(
        &self,
        other: &TypedRdd<T>,
    ) -> Result<Arc<std::collections::HashSet<T>>, DataError>
    where
        T: Eq + std::hash::Hash + Clone + WireEncode,
        Vec<T>: WireEncode + WireDecode,
    {
        let ctx = self.context.clone();
        let items: Vec<T> = if ctx.is_distributed() {
            other.collect_distributed()?
        } else {
            ctx.run_job(other.rdd.clone(), |iter| iter.collect::<Vec<T>>())?
                .into_iter()
                .flatten()
                .collect()
        };
        Ok(Arc::new(items.into_iter().collect()))
    }

    pub fn subtract(self, other: TypedRdd<T>) -> Result<TypedRdd<T>, DataError>
    where
        T: Eq + std::hash::Hash + Clone + WireEncode,
        Vec<T>: WireEncode + WireDecode,
    {
        let other_set = self.materialize_other_set(&other)?;
        let filter_fn =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set = Arc::clone(&other_set);
                Box::new(
                    iter.filter(move |x| !set.contains(x))
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            };
        let id = self.context.new_rdd_id();
        Ok(TypedRdd::new(
            Arc::new(MapPartitionsRdd::new(id, self.rdd, filter_fn)),
            self.context,
        ))
    }

    pub fn intersection(self, other: TypedRdd<T>) -> Result<TypedRdd<T>, DataError>
    where
        T: Eq + std::hash::Hash + Clone + WireEncode,
        Vec<T>: WireEncode + WireDecode,
    {
        let other_set = self.materialize_other_set(&other)?;
        let filter_fn =
            move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let set = Arc::clone(&other_set);
                Box::new(
                    iter.filter(move |x| set.contains(x))
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            };
        let id = self.context.new_rdd_id();
        Ok(TypedRdd::new(
            Arc::new(MapPartitionsRdd::new(id, self.rdd, filter_fn)),
            self.context,
        ))
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
            None,
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

impl<T: Data> TypedRdd<T> {
    /// Write each partition as a text file.
    ///
    /// URI schemes:
    /// - `s3://bucket/prefix` — uploads `part-N` objects to that prefix.
    /// - Local path — creates the directory and writes `part-N` files inside it.
    ///
    /// Each element is converted to a string via `Display` and written as one line.
    pub fn save_as_text_file(&self, uri: &str) -> Result<(), DataError>
    where
        T: std::fmt::Display + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if uri.starts_with("s3://") {
            return self.write_s3(uri);
        }

        let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
        std::fs::create_dir_all(path).map_err(|e| {
            DataError::Other(format!(
                "save_as_text_file: cannot create dir {}: {e}",
                path.display()
            ))
        })?;
        for (idx, partition) in self.collect_partitions()?.into_iter().enumerate() {
            use std::io::Write;
            let file_path = path.join(format!("part-{idx}"));
            let mut f = std::fs::File::create(&file_path).map_err(|e| {
                DataError::Other(format!("save_as_text_file: {}: {e}", file_path.display()))
            })?;
            for item in partition {
                writeln!(f, "{item}")?;
            }
        }
        Ok(())
    }

    fn write_s3(&self, uri: &str) -> Result<(), DataError>
    where
        T: std::fmt::Display + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        use crate::io::s3::{S3Uri, write_text};
        let s3uri = S3Uri::parse(uri)
            .ok_or_else(|| DataError::Other(format!("save_as_text_file: invalid S3 URI: {uri}")))?;
        for (idx, partition) in self.collect_partitions()?.into_iter().enumerate() {
            let key = format!("{}/part-{idx}", s3uri.key.trim_end_matches('/'));
            let content: String = partition
                .into_iter()
                .map(|item| format!("{item}\n"))
                .collect();
            write_text(&s3uri.bucket, &key, content)
                .map_err(|e| DataError::Other(e.to_string()))?;
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

    /// Return a fixed-size random sample of exactly `num` elements (or fewer if the RDD is
    /// smaller and `with_replacement` is false) as a driver-side `Vec<T>`.
    ///
    /// This is an **action**: it materialises the RDD and samples on the driver, seeded by
    /// `seed` for reproducibility. `with_replacement = true` draws with replacement (elements
    /// may repeat); `false` draws distinct elements.
    pub fn take_sample(
        &self,
        with_replacement: bool,
        num: usize,
        seed: u64,
    ) -> Result<Vec<T>, DataError>
    where
        T: Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        use rand::SeedableRng;

        // This workspace's patched `rand` exposes only `RngExt::random`, not the `seq`
        // sampling helpers (`choose_multiple`/`shuffle`), so both branches sample using a
        // single uniform `random::<f64>()` draw per element.
        let all: Vec<T> = self.collect()?;
        if all.is_empty() || num == 0 {
            return Ok(vec![]);
        }
        let mut rng = rand_pcg::Pcg64::seed_from_u64(seed);
        if with_replacement {
            let n = all.len();
            Ok((0..num)
                .map(|_| {
                    let i = ((rng.random::<f64>() * n as f64) as usize).min(n - 1);
                    all[i].clone()
                })
                .collect())
        } else {
            // Assign each element a random key, sort by it, take the first `num` —
            // an unbiased fixed-size sample without replacement.
            let take = num.min(all.len());
            let mut keyed: Vec<(f64, T)> =
                all.into_iter().map(|x| (rng.random::<f64>(), x)).collect();
            keyed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            Ok(keyed.into_iter().take(take).map(|(_, x)| x).collect())
        }
    }
}
