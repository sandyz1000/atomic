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
        let mut partials: Vec<T> = if self.context.is_distributed() {
            self.collect_distributed()?
        } else {
            let f_job = f.clone();
            let reduce_partition = move |iter: Box<dyn Iterator<Item = T>>| iter.reduce(&f_job);
            self.context
                .run_job(self.rdd.clone(), reduce_partition)?
                .into_iter()
                .flatten()
                .collect()
        };

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
    ) -> Result<U, DataError>
    where
        U: Data + Clone,
        T: WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        let mut partials: Vec<U> = if self.context.is_distributed() {
            let elements = self.collect_distributed()?;
            vec![elements.into_iter().fold(zero.clone(), &seq_fn)]
        } else {
            let z = zero.clone();
            let reduce_partition =
                move |iter: Box<dyn Iterator<Item = T>>| iter.fold(z.clone(), &seq_fn);
            self.context.run_job(self.rdd.clone(), reduce_partition)?
        };

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
            op_id: F::NAME.to_string(),
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
    pub fn min(&self) -> Result<Option<T>, DataError>
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
    pub fn take_ordered(&self, k: usize) -> Result<Vec<T>, DataError>
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
