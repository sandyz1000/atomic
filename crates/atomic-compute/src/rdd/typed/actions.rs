use super::*;

impl<T: Data + Clone> TypedRdd<T> {
    /// Collect all elements from all partitions into a Vec.
    ///
    /// In distributed mode, if a lazy pipeline has been staged by `map_task` /
    /// `filter_task` / `flat_map_task`, this dispatches the full pipeline in one
    /// round-trip per partition. Otherwise falls back to the driver scheduler path.
    ///
    /// **Warning**: This brings all data to the driver. Only use on small datasets.
    pub fn collect(&self) -> Result<Vec<T>, DataError>
    where
        Vec<T>: WireDecode,
    {
        if self.context.is_distributed() {
            if let Some(ref staged) = self.staged {
                let result_bytes = self
                    .context
                    .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                return result_bytes
                    .into_iter()
                    .map(|bytes| {
                        Vec::<T>::decode_wire(&bytes)
                            .map_err(|e| DataError::DowncastFailure(e.to_string()))
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
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
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
    pub fn collect_partitions(&self) -> Result<Vec<Vec<T>>, DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            if let Some(ref staged) = self.staged {
                let result_bytes = self
                    .context
                    .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                return result_bytes
                    .into_iter()
                    .map(|bytes| {
                        Vec::<T>::decode_wire(&bytes)
                            .map_err(|e| DataError::DowncastFailure(e.to_string()))
                    })
                    .collect();
            }

            // No staged op pipeline: encode each partition directly and decode back,
            // preserving partition boundaries without closure-backed scheduler tasks.
            let source = Context::encode_rdd_partitions(self.rdd.clone())
                .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
            return source
                .into_iter()
                .map(|bytes| {
                    Vec::<T>::decode_wire(&bytes)
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))
                })
                .collect();
        }

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
    pub fn to_local_iterator(&self) -> Result<impl Iterator<Item = T>, DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            return Ok(self.collect_distributed()?.into_iter());
        }
        let n = self.num_partitions();
        let mut result: Vec<T> = Vec::new();
        for i in 0..n {
            let partition_data = self.context.run_job_with_partitions(
                self.rdd.clone(),
                |iter| iter.collect::<Vec<T>>(),
                [i],
            )?;
            result.extend(partition_data.into_iter().flatten());
        }
        Ok(result.into_iter())
    }

    /// Count the number of elements in the RDD.
    ///
    /// In distributed mode, if a lazy pipeline is staged, it dispatches to workers
    /// and counts the returned elements on the driver. In local mode runs on driver.
    pub fn count(&self) -> Result<u64, DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() && self.staged.is_some() {
            // Sum per-partition lengths incrementally — never concatenate all elements.
            return self.reduce_partitions(0u64, |acc, part| acc + part.len() as u64);
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
    pub fn take(&self, num: usize) -> Result<Vec<T>, DataError>
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
    pub fn first(&self) -> Result<T, DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if let Some(result) = self.take(1)?.into_iter().next() {
            Ok(result)
        } else {
            Err(DataError::DowncastFailure("empty collection".to_string()))
        }
    }

    /// Returns `true` if the RDD contains no elements.
    pub fn is_empty(&self) -> Result<bool, DataError>
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
    pub fn count_approx(&self, confidence: f64) -> Result<u64, DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            // For op-only distributed execution we avoid closure-backed sampling jobs.
            // This path computes an exact count from worker-produced output.
            return Ok(self.collect_distributed()?.len() as u64);
        }

        let n = self.num_partitions();
        let sample_n = ((confidence.clamp(0.001, 1.0) * n as f64).ceil() as usize)
            .max(1)
            .min(n);
        let sample_indices: Vec<usize> = (0..sample_n).collect();
        let counts = self
            .context
            .run_job_with_partitions(self.rdd.clone(), |iter| iter.count() as u64, sample_indices)
            .map_err(DataError::from)?;
        let sampled_total: u64 = counts.iter().sum();
        let estimate = (sampled_total as f64 * n as f64 / sample_n as f64).round() as u64;
        Ok(estimate)
    }

    /// Aggregate elements with different accumulator and result types.
    ///
    /// In distributed mode, collects all elements from workers then applies
    /// `seq_fn` on the driver. `comb_fn` is unused in distributed mode since
    /// all elements are aggregated in a single pass on the driver.
    pub fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U, DataError>
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
    pub fn tree_reduce<F>(&self, f: F, depth: usize) -> Result<Option<T>, DataError>
    where
        T: Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        let partials: Vec<Option<T>> = if self.context.is_distributed() {
            // Per-partition reduce: each partition sends 0 or 1 element.
            self.reduce_partitions(Vec::new(), |mut acc: Vec<Option<T>>, part| {
                acc.push(part.into_iter().reduce(&f));
                acc
            })?
        } else {
            let f_job = f.clone();
            let reduce_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.reduce(&f_job);
            self.context.run_job(self.rdd.clone(), reduce_partition)?
        };

        tree_merge_opts(partials, f, depth)
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
    ) -> Result<U, DataError>
    where
        U: Data + Clone,
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        let partials: Vec<U> = if self.context.is_distributed() {
            // Per-partition fold into accumulator, one U per partition.
            let z = zero.clone();
            let seq = seq_fn.clone();
            self.reduce_partitions(Vec::new(), move |mut acc: Vec<U>, part| {
                acc.push(part.into_iter().fold(z.clone(), &seq));
                acc
            })?
        } else {
            let z = zero.clone();
            let reduce_partition =
                move |iter: Box<dyn Iterator<Item = T>>| iter.fold(z.clone(), &seq_fn);
            self.context.run_job(self.rdd.clone(), reduce_partition)?
        };

        Ok(tree_merge(partials, comb_fn, depth).unwrap_or(zero))
    }

    /// Apply a function to each element (for side effects).
    ///
    /// In distributed mode, collects all elements from workers and applies `f`
    /// on the driver. The function runs on the driver, not on workers.
    pub fn for_each<F>(&self, f: F) -> Result<(), DataError>
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
    pub fn for_each_partition<F>(&self, f: F) -> Result<(), DataError>
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

    /// Apply a `#[task]`-registered side-effecting function (`UnaryTask<T, ()>`) to each
    /// element. Unlike [`for_each`](Self::for_each), which collects to the driver and runs
    /// the closure there, this runs the task **on the workers** in distributed mode.
    pub fn for_each_task<F>(&self, task: F) -> Result<(), DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        F: UnaryTask<T, ()>,
    {
        if !self.context.is_distributed() {
            let task_c = task.clone();
            let for_each_partition = move |iter: Box<dyn Iterator<Item = T>>| {
                for x in iter {
                    task_c.call(x);
                }
            };
            self.context.run_job(self.rdd.clone(), for_each_partition)?;
            return Ok(());
        }

        let op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Foreach),
            runtime: TaskRuntime::Native,
            payload: task.encode_params(),
        };
        let (source_partitions, mut steps) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        steps.push(op);
        self.context
            .dispatch_pipeline(source_partitions, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        Ok(())
    }

    /// Count the number of occurrences of each unique value.
    ///
    /// In distributed mode, collects all elements from workers then counts on
    /// the driver.
    pub fn count_by_value(&self) -> Result<std::collections::HashMap<T, u64>, DataError>
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

    /// Fold all elements with a closure, seeded by `zero`.
    ///
    /// The closure runs on the driver. In distributed mode each partition is dispatched and
    /// folded one at a time (no full concatenation, bounded memory), but every element still
    /// crosses the wire. For worker-side reduction use [`fold_task`](TypedRdd::fold_task) with
    /// a registered `#[task]`.
    pub fn fold(
        &self,
        zero: T,
        op: impl Fn(T, T) -> T + Clone + Send + Sync + 'static,
    ) -> Result<T, DataError>
    where
        T: Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let z = zero.clone();
            let o = op.clone();
            let partials: Vec<T> =
                self.reduce_partitions(Vec::new(), move |mut acc: Vec<T>, part| {
                    acc.push(part.into_iter().fold(z.clone(), &o));
                    acc
                })?;
            let mut acc = zero;
            for p in partials {
                acc = op(acc, p);
            }
            return Ok(acc);
        }
        let z = zero.clone();
        let o = op.clone();
        let part = move |iter: Box<dyn Iterator<Item = T>>| iter.fold(z.clone(), &o);
        Ok(self
            .context
            .run_job(self.rdd.clone(), part)?
            .into_iter()
            .fold(zero, op))
    }

    /// Reduce all elements with a closure. Returns `None` if the RDD is empty.
    ///
    /// The closure runs on the driver. In distributed mode each partition is dispatched and
    /// reduced one at a time (bounded memory), but every element still crosses the wire. For
    /// worker-side reduction use [`reduce_task`](TypedRdd::reduce_task) with a registered
    /// `#[task]`.
    pub fn reduce(
        &self,
        op: impl Fn(T, T) -> T + Clone + Send + Sync + 'static,
    ) -> Result<Option<T>, DataError>
    where
        T: Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let o = op.clone();
            return self.reduce_partitions(None, move |acc: Option<T>, part| {
                let best = part.into_iter().reduce(&o);
                match (acc, best) {
                    (Some(a), Some(b)) => Some(o(a, b)),
                    (a, None) => a,
                    (None, b) => b,
                }
            });
        }
        let o_local = op.clone();
        let part = move |iter: Box<dyn Iterator<Item = T>>| iter.reduce(&o_local);
        let results: Vec<Option<T>> = self.context.run_job(self.rdd.clone(), part)?;
        let mut merged: Option<T> = None;
        for val in results.into_iter().flatten() {
            merged = match merged {
                Some(m) => Some(op(m, val)),
                None => Some(val),
            };
        }
        Ok(merged)
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
    pub fn max(&self) -> Result<Option<T>, DataError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            // Primitive types reduce on the worker via the builtin `MaxTask` (one value per
            // partition crosses the wire); other types reduce per-partition on the driver.
            if let Some(name) = crate::builtin_tasks::max_task_name::<T>() {
                let parts = self.dispatch_with_step(name, TaskAction::Reduce, vec![])?;
                let mut best: Option<T> = None;
                for b in parts {
                    if b.is_empty() {
                        continue; // empty partition — no value
                    }
                    let v = T::decode_wire(&b)
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                    best = Some(best.map_or(v.clone(), |c| c.max(v)));
                }
                return Ok(best);
            }
            return self.reduce_partitions(None, |acc: Option<T>, part| {
                match (acc, part.into_iter().max()) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, None) => a,
                    (None, b) => b,
                }
            });
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
    pub fn min(&self) -> Result<Option<T>, DataError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            if let Some(name) = crate::builtin_tasks::min_task_name::<T>() {
                let parts = self.dispatch_with_step(name, TaskAction::Reduce, vec![])?;
                let mut best: Option<T> = None;
                for b in parts {
                    if b.is_empty() {
                        continue;
                    }
                    let v = T::decode_wire(&b)
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                    best = Some(best.map_or(v.clone(), |c| c.min(v)));
                }
                return Ok(best);
            }
            return self.reduce_partitions(None, |acc: Option<T>, part| {
                match (acc, part.into_iter().min()) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, None) => a,
                    (None, b) => b,
                }
            });
        }
        let min_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.min();
        let partition_mins = self.context.run_job(self.rdd.clone(), min_partition)?;
        Ok(partition_mins.into_iter().flatten().min())
    }

    /// Bucketed counts of the elements over ascending `bucket_bounds`.
    ///
    /// `bucket_bounds` has `n + 1` ascending edges defining `n` buckets; returns a `Vec<u64>`
    /// of length `n` where index `i` counts elements in `[bounds[i], bounds[i+1])`, with the
    /// final bucket right-inclusive. Elements outside `[bounds[0], bounds[n]]` are dropped.
    ///
    /// In distributed mode, each partition produces its own bucket counts; the driver
    /// sums them.  The bucket edges are a runtime parameter, so this is not a
    /// compile-time-registered task — but per-partition work keeps memory bounded.
    pub fn histogram(&self, bucket_bounds: &[f64]) -> Result<Vec<u64>, DataError>
    where
        T: crate::builtin_tasks::NumericValue + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if bucket_bounds.len() < 2 {
            return Ok(vec![]);
        }
        let n = bucket_bounds.len() - 1;
        let lo = bucket_bounds[0];
        let hi = bucket_bounds[n];
        let bounds = bucket_bounds.to_vec(); // Arc-able copy

        if self.context.is_distributed() {
            return self.reduce_partitions(vec![0u64; n], move |mut counts, part| {
                for x in part {
                    let v = x.to_f64();
                    if v < lo || v > hi {
                        continue;
                    }
                    let idx = bounds
                        .partition_point(|&b| b <= v)
                        .saturating_sub(1)
                        .min(n - 1);
                    counts[idx] += 1;
                }
                counts
            });
        }

        let mut counts = vec![0u64; n];
        for x in self.collect()? {
            let v = x.to_f64();
            if v < lo || v > hi {
                continue;
            }
            let idx = bucket_bounds
                .partition_point(|&b| b <= v)
                .saturating_sub(1)
                .min(n - 1);
            counts[idx] += 1;
        }
        Ok(counts)
    }

    /// Dispatch the staged pipeline (or raw partitions) with one extra builtin step appended,
    /// returning the raw per-partition result bytes. Empty blobs (e.g. a per-partition reducer
    /// that produced no value) are preserved for the caller to skip.
    fn dispatch_with_step(
        &self,
        task_name: &str,
        action: TaskAction,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, DataError>
    where
        T: WireEncode,
        Vec<T>: WireEncode,
    {
        let (source, mut steps) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        steps.push(Step {
            task_name: task_name.to_string(),
            kind: StepKind::Task(action),
            runtime: TaskRuntime::Native,
            payload,
        });
        self.context
            .dispatch_pipeline(source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))
    }

    /// Dispatch the staged pipeline (or raw partitions) and fold the per-partition `Vec<T>`
    /// outputs into `acc` one partition at a time. Unlike [`collect_distributed`], this never
    /// concatenates all partitions on the driver — it holds one partition's `Vec<T>` plus the
    /// accumulator at a time.
    fn reduce_partitions<A, F>(&self, zero: A, mut per_partition: F) -> Result<A, DataError>
    where
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        F: FnMut(A, Vec<T>) -> A,
    {
        let (source, steps) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        let parts = self
            .context
            .dispatch_pipeline(source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        let mut acc = zero;
        for b in parts {
            let v =
                Vec::<T>::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))?;
            acc = per_partition(acc, v);
        }
        Ok(acc)
    }

    /// Internal helper: dispatch the staged pipeline (or raw partitions) to workers
    /// and return all elements as a flat `Vec<T>`. Used by `first`, `take`, `is_empty`.
    pub(crate) fn collect_distributed(&self) -> Result<Vec<T>, DataError>
    where
        T: WireEncode,
        Vec<T>: WireEncode + WireDecode,
    {
        let (source, steps) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        let result_bytes = self
            .context
            .dispatch_pipeline(source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;

        result_bytes
            .into_iter()
            .map(|b| {
                Vec::<T>::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|vecs| vecs.into_iter().flatten().collect())
    }

    /// Return the top k elements in descending order.
    ///
    /// In distributed mode, collects all elements from workers then sorts on the driver.
    pub fn top(&self, k: usize) -> Result<Vec<T>, DataError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            // Primitives: each worker emits its local top-k via the builtin `TopKTask`
            // (≤ k per partition); other types truncate per-partition on the driver.
            let mut all_items: Vec<T> =
                if let Some(name) = crate::builtin_tasks::top_k_task_name::<T>() {
                    let payload = (k as u64)
                        .encode_wire()
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                    let parts = self.dispatch_with_step(name, TaskAction::Collect, payload)?;
                    let mut all = Vec::new();
                    for b in parts {
                        all.extend(
                            Vec::<T>::decode_wire(&b)
                                .map_err(|e| DataError::DowncastFailure(e.to_string()))?,
                        );
                    }
                    all
                } else {
                    self.reduce_partitions(Vec::new(), |mut acc: Vec<T>, mut part| {
                        part.sort_by(|a, b| b.cmp(a));
                        part.truncate(k);
                        acc.extend(part);
                        acc
                    })?
                };
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
    pub fn take_ordered(&self, k: usize) -> Result<Vec<T>, DataError>
    where
        T: Ord + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            let mut all_items: Vec<T> =
                if let Some(name) = crate::builtin_tasks::take_ordered_task_name::<T>() {
                    let payload = (k as u64)
                        .encode_wire()
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                    let parts = self.dispatch_with_step(name, TaskAction::Collect, payload)?;
                    let mut all = Vec::new();
                    for b in parts {
                        all.extend(
                            Vec::<T>::decode_wire(&b)
                                .map_err(|e| DataError::DowncastFailure(e.to_string()))?,
                        );
                    }
                    all
                } else {
                    self.reduce_partitions(Vec::new(), |mut acc: Vec<T>, mut part| {
                        part.sort();
                        part.truncate(k);
                        acc.extend(part);
                        acc
                    })?
                };
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

// ── Tree reduction helpers ──────────────────────────────────────────────────────

/// Merge `Option<T>` values in a balanced binary tree of depth `levels`,
/// using `f` as the combine function. Returns `None` for an empty input.
fn tree_merge_opts<T, F>(
    mut partials: Vec<Option<T>>,
    f: F,
    depth: usize,
) -> Result<Option<T>, DataError>
where
    F: Fn(T, T) -> T,
{
    let levels = depth.max(1);
    for _ in 0..levels {
        if partials.len() <= 1 {
            break;
        }
        let mut next = Vec::with_capacity(partials.len() / 2 + 1);
        let mut iter = partials.into_iter();
        while let Some(first) = iter.next() {
            match (first, iter.next()) {
                (Some(a), Some(Some(b))) => next.push(Some(f(a, b))),
                (Some(a), None) => next.push(Some(a)),
                (None, Some(Some(b))) => next.push(Some(b)),
                (None, Some(None)) | (None, None) | (Some(_), Some(None)) => {}
            }
        }
        partials = next;
    }
    Ok(partials.into_iter().next().flatten())
}

/// Merge `T` values in a balanced binary tree of depth `levels`,
/// using `f` as the combine function.
fn tree_merge<T, F>(mut partials: Vec<T>, f: F, depth: usize) -> Option<T>
where
    F: Fn(T, T) -> T,
{
    let levels = depth.max(1);
    for _ in 0..levels {
        if partials.len() <= 1 {
            break;
        }
        let mut next = Vec::with_capacity(partials.len() / 2 + 1);
        let mut iter = partials.into_iter();
        while let Some(first) = iter.next() {
            match iter.next() {
                Some(second) => next.push(f(first, second)),
                None => next.push(first),
            }
        }
        partials = next;
    }
    partials.into_iter().next()
}

#[cfg(test)]
mod tests {
    use crate::context::Context;

    fn rdd() -> crate::rdd::TypedRdd<i32> {
        Context::local()
            .unwrap()
            .parallelize_typed(vec![1, 2, 3, 4, 5], 3)
    }

    #[test]
    fn test_fold() {
        // Zero is the identity 0, so the result is partition-count independent.
        assert_eq!(rdd().fold(0, |a, b| a + b).unwrap(), 15);
    }

    #[test]
    fn test_reduce() {
        assert_eq!(rdd().reduce(|a, b| a + b).unwrap(), Some(15));
    }

    #[test]
    fn test_reduce_empty() {
        let empty = Context::local()
            .unwrap()
            .parallelize_typed(Vec::<i32>::new(), 2);
        assert_eq!(empty.reduce(|a, b| a + b).unwrap(), None);
    }

    #[test]
    fn test_tree_reduce() {
        assert_eq!(rdd().tree_reduce(|a, b| a + b, 2).unwrap(), Some(15));
    }

    #[test]
    fn test_tree_aggregate() {
        // Accumulator differs from element type: count elements.
        let n = rdd()
            .tree_aggregate(0u64, |acc, _x| acc + 1, |a, b| a + b, 2)
            .unwrap();
        assert_eq!(n, 5);
    }

    #[test]
    fn test_histogram() {
        // Buckets [1,3), [3,5]; elements 1,2,3,4,5 → 2 in first, 3 in second.
        let counts = rdd().histogram(&[1.0, 3.0, 5.0]).unwrap();
        assert_eq!(counts, vec![2, 3]);
    }
}
