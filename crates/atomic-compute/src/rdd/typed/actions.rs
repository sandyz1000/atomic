use super::*;

impl<T: Data + Clone> TypedRdd<T> {
    /// Collect all elements from all partitions into a Vec.
    ///
    /// Dispatches via `Context::dispatch_pipeline`, so the same call runs in-process
    /// (local mode) or ships to workers (distributed mode) — the driver code is identical
    /// either way. If a lazy pipeline was staged by `map_task`/`filter_task`/`flat_map_task`,
    /// dispatches it directly; otherwise encodes the raw RDD partitions first (running any
    /// pending shuffle-map stage so `ShuffledRdd::compute` can fetch its input).
    ///
    /// **Warning**: This brings all data to the driver. Only use on small datasets.
    pub fn collect(&self) -> Result<Vec<T>, DataError>
    where
        T: WireSerde,
    {
        let (source, steps) = self.resolve_pipeline()?;
        let result_bytes = self
            .context
            .dispatch_pipeline(self.rdd.clone(), source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;

        result_bytes
            .into_iter()
            .map(|b| {
                Vec::<T>::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|vecs| vecs.into_iter().flatten().collect())
    }

    /// Collect each partition as a separate `Vec<T>`, preserving partition boundaries.
    ///
    /// Returns `Vec<Vec<T>>` where index `i` holds the elements of partition `i`.
    /// Useful for `save_as_text_file` and `checkpoint` which write one file per partition.
    pub fn collect_partitions(&self) -> Result<Vec<Vec<T>>, DataError>
    where
        T: WireSerde,
    {
        let (source, steps) = self.resolve_pipeline()?;
        let result_bytes = self
            .context
            .dispatch_pipeline(self.rdd.clone(), source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        result_bytes
            .into_iter()
            .map(|bytes| {
                Vec::<T>::decode_wire(&bytes).map_err(|e| DataError::DowncastFailure(e.to_string()))
            })
            .collect()
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
        T: WireSerde,
    {
        let (source, steps) = self.resolve_pipeline()?;
        let mut result: Vec<T> = Vec::new();
        for partition in source {
            let parts = self
                .context
                .dispatch_pipeline(self.rdd.clone(), vec![partition], steps.clone())
                .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
            for b in parts {
                result.extend(
                    Vec::<T>::decode_wire(&b)
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))?,
                );
            }
        }
        Ok(result.into_iter())
    }

    /// Count the number of elements in the RDD.
    ///
    /// Sums per-partition lengths incrementally — never concatenates all elements on
    /// the driver.
    pub fn count(&self) -> Result<u64, DataError>
    where
        T: WireSerde,
    {
        self.reduce_partitions(0u64, |acc, part| acc + part.len() as u64)
    }

    /// Take the first n elements from the RDD.
    ///
    /// Uses a partition-scanning strategy in both modes: starts with one partition and
    /// scales up geometrically until `num` elements are found, rather than dispatching
    /// every partition up front — bounded by the pipeline dispatch granularity (a whole
    /// partition per round-trip; unlike the old closure-driven local path, a partition
    /// can no longer stop mid-iteration once `num` is reached).
    pub fn take(&self, num: usize) -> Result<Vec<T>, DataError>
    where
        T: WireSerde,
    {
        if num == 0 {
            return Ok(vec![]);
        }
        const SCALE_UP_FACTOR: f64 = 2.0;
        let (source, steps) = self.resolve_pipeline()?;
        let total_parts = source.len() as u32;
        let mut buf = vec![];
        let mut parts_scanned = 0_u32;

        while buf.len() < num && parts_scanned < total_parts {
            let mut num_parts_to_try = 1u32;
            if parts_scanned > 0 {
                let parts_scanned_f64 = f64::from(parts_scanned);
                num_parts_to_try = if buf.is_empty() {
                    (parts_scanned_f64 * SCALE_UP_FACTOR).ceil() as u32
                } else {
                    let left = num - buf.len();
                    let num_parts =
                        (1.5 * left as f64 * parts_scanned_f64 / (buf.len() as f64)).ceil();
                    num_parts.min(parts_scanned_f64 * SCALE_UP_FACTOR) as u32
                };
            }

            let end = total_parts.min(parts_scanned + num_parts_to_try) as usize;
            let subset = source[parts_scanned as usize..end].to_vec();
            let num_partitions = subset.len() as u32;

            let res = self
                .context
                .dispatch_pipeline(self.rdd.clone(), subset, steps.clone())
                .map_err(|e| DataError::DowncastFailure(e.to_string()))?;

            for b in res {
                let part = Vec::<T>::decode_wire(&b)
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                let take = num - buf.len();
                buf.extend(part.into_iter().take(take));
            }

            parts_scanned += num_partitions;
        }

        Ok(buf)
    }

    /// Get the first element of the RDD.
    ///
    /// Returns an error if the RDD is empty.
    pub fn first(&self) -> Result<T, DataError>
    where
        T: WireSerde,
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
        T: WireSerde,
    {
        Ok(self.take(1)?.is_empty())
    }

    /// Approximate count within a time budget.
    ///
    /// Samples `max(1, ceil(confidence × num_partitions))` partitions and extrapolates.
    /// `confidence` must be in `(0.0, 1.0]`; use `1.0` for a full (non-approximate) scan.
    /// The result is an estimate — the actual count may differ from the return value.
    pub fn count_approx(&self, confidence: f64) -> Result<u64, DataError>
    where
        T: WireSerde,
    {
        let (source, steps) = self.resolve_pipeline()?;
        let n = source.len();
        let sample_n = ((confidence.clamp(0.001, 1.0) * n as f64).ceil() as usize)
            .max(1)
            .min(n);
        let subset = source[..sample_n].to_vec();
        let result_bytes = self
            .context
            .dispatch_pipeline(self.rdd.clone(), subset, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        let mut sampled_total = 0u64;
        for b in result_bytes {
            let part =
                Vec::<T>::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))?;
            sampled_total += part.len() as u64;
        }
        let estimate = (sampled_total as f64 * n as f64 / sample_n as f64).round() as u64;
        Ok(estimate)
    }

    /// Aggregate elements with different accumulator and result types.
    ///
    /// `seq_fn(acc, elem)` folds within each partition; `comb_fn(acc, acc)` merges the
    /// per-partition accumulators on the driver.
    pub fn aggregate<U, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U, DataError>
    where
        U: Data + Clone,
        T: WireSerde,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        let z = init.clone();
        let partials: Vec<U> =
            self.reduce_partitions(Vec::new(), move |mut acc: Vec<U>, part| {
                acc.push(part.into_iter().fold(z.clone(), &seq_fn));
                acc
            })?;
        Ok(partials.into_iter().fold(init, comb_fn))
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
        T: Clone + WireSerde,
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        // Per-partition reduce: each partition sends 0 or 1 element.
        let partials: Vec<Option<T>> =
            self.reduce_partitions(Vec::new(), |mut acc: Vec<Option<T>>, part| {
                acc.push(part.into_iter().reduce(&f));
                acc
            })?;

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
        T: WireSerde,
        SF: Fn(U, T) -> U + Clone + Send + Sync + 'static,
        CF: Fn(U, U) -> U + Clone + Send + Sync + 'static,
    {
        // Per-partition fold into accumulator, one U per partition.
        let z = zero.clone();
        let seq = seq_fn.clone();
        let partials: Vec<U> =
            self.reduce_partitions(Vec::new(), move |mut acc: Vec<U>, part| {
                acc.push(part.into_iter().fold(z.clone(), &seq));
                acc
            })?;

        Ok(tree_merge(partials, comb_fn, depth).unwrap_or(zero))
    }

    /// Apply a function to each element (for side effects).
    ///
    /// The function runs on the driver, not on workers — one partition's worth of
    /// elements crosses the wire at a time (bounded memory), never the whole RDD at once.
    pub fn for_each<F>(&self, f: F) -> Result<(), DataError>
    where
        T: WireSerde,
        F: Fn(&T) + Clone + Send + Sync + 'static,
    {
        self.reduce_partitions((), move |(), part| {
            part.iter().for_each(&f);
        })
    }

    /// Apply a function to each partition (for side effects).
    ///
    /// `f` is called once per partition, with that partition's elements as an iterator —
    /// partition boundaries are preserved.
    pub fn for_each_partition<F>(&self, f: F) -> Result<(), DataError>
    where
        T: WireSerde,
        F: Fn(Box<dyn Iterator<Item = T>>) + Clone + Send + Sync + 'static,
    {
        self.reduce_partitions((), move |(), part| {
            f(Box::new(part.into_iter()));
        })
    }

    /// Apply a `#[task]`-registered side-effecting function (`UnaryTask<T, ()>`) to each
    /// element. Unlike [`for_each`](Self::for_each), which runs the closure on the driver,
    /// this dispatches the task itself — it runs on the worker (or in-process thread) that
    /// owns each partition.
    pub fn for_each_task<F>(&self, task: F) -> Result<(), DataError>
    where
        T: WireSerde,
        F: UnaryTask<T, ()>,
    {
        let (source_partitions, mut steps) = self.resolve_pipeline()?;
        steps.push(Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Foreach),
            runtime: TaskRuntime::Native,
            payload: task.encode_params(),
        });
        self.context
            .dispatch_pipeline(self.rdd.clone(), source_partitions, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        Ok(())
    }

    /// Count the number of occurrences of each unique value.
    ///
    /// Each partition's counts are folded in one at a time (bounded memory), then merged.
    pub fn count_by_value(&self) -> Result<std::collections::HashMap<T, u64>, DataError>
    where
        T: Eq + std::hash::Hash + Clone + WireSerde,
    {
        use std::collections::HashMap;

        self.reduce_partitions(HashMap::new(), |mut acc: HashMap<T, u64>, part| {
            for item in part {
                *acc.entry(item).or_insert(0) += 1;
            }
            acc
        })
    }

    /// Fold all elements with a closure, seeded by `zero`.
    ///
    /// The closure runs on the driver. Each partition is dispatched and folded one at a
    /// time (no full concatenation, bounded memory), but every element still crosses the
    /// wire. For worker-side reduction use [`fold_task`](TypedRdd::fold_task) with a
    /// registered `#[task]`.
    pub fn fold(
        &self,
        zero: T,
        op: impl Fn(T, T) -> T + Clone + Send + Sync + 'static,
    ) -> Result<T, DataError>
    where
        T: Clone + WireSerde,
    {
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
        Ok(acc)
    }

    /// Reduce all elements with a closure. Returns `None` if the RDD is empty.
    ///
    /// The closure runs on the driver. Each partition is dispatched and reduced one at a
    /// time (bounded memory), but every element still crosses the wire. For worker-side
    /// reduction use [`reduce_task`](TypedRdd::reduce_task) with a registered `#[task]`.
    pub fn reduce(
        &self,
        op: impl Fn(T, T) -> T + Clone + Send + Sync + 'static,
    ) -> Result<Option<T>, DataError>
    where
        T: Clone + WireSerde,
    {
        let o = op.clone();
        self.reduce_partitions(None, move |acc: Option<T>, part| {
            let best = part.into_iter().reduce(&o);
            match (acc, best) {
                (Some(a), Some(b)) => Some(o(a, b)),
                (a, None) => a,
                (None, b) => b,
            }
        })
    }

    /// Return the maximum element.
    ///
    /// If a lazy pipeline is staged (from `map_task` etc.), dispatches it and picks the
    /// global max from the partition results.
    ///
    /// # Example
    /// ```ignore
    /// let max_val = rdd.max()?;
    /// ```
    pub fn max(&self) -> Result<Option<T>, DataError>
    where
        T: Ord + Clone + WireSerde,
    {
        // Primitive types reduce on the worker via the builtin `MaxTask` (one value per
        // partition crosses the wire); other types reduce per-partition on the driver.
        if let Some(name) = crate::builtin_tasks::max_task_name::<T>() {
            let parts = self.dispatch_with_step(name, TaskAction::Reduce, vec![])?;
            let mut best: Option<T> = None;
            for b in parts {
                if b.is_empty() {
                    continue; // empty partition — no value
                }
                let v =
                    T::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                best = Some(best.map_or(v.clone(), |c| c.max(v)));
            }
            return Ok(best);
        }
        self.reduce_partitions(None, |acc: Option<T>, part| {
            match (acc, part.into_iter().max()) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, None) => a,
                (None, b) => b,
            }
        })
    }

    /// Return the minimum element.
    ///
    /// Dispatches the staged pipeline (if any), then picks the global minimum from the
    /// partition results.
    ///
    /// # Example
    /// ```ignore
    /// let min_val = rdd.min()?;
    /// ```
    pub fn min(&self) -> Result<Option<T>, DataError>
    where
        T: Ord + Clone + WireSerde,
    {
        if let Some(name) = crate::builtin_tasks::min_task_name::<T>() {
            let parts = self.dispatch_with_step(name, TaskAction::Reduce, vec![])?;
            let mut best: Option<T> = None;
            for b in parts {
                if b.is_empty() {
                    continue;
                }
                let v =
                    T::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                best = Some(best.map_or(v.clone(), |c| c.min(v)));
            }
            return Ok(best);
        }
        self.reduce_partitions(None, |acc: Option<T>, part| {
            match (acc, part.into_iter().min()) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, None) => a,
                (None, b) => b,
            }
        })
    }

    /// Bucketed counts of the elements over ascending `bucket_bounds`.
    ///
    /// `bucket_bounds` has `n + 1` ascending edges defining `n` buckets; returns a `Vec<u64>`
    /// of length `n` where index `i` counts elements in `[bounds[i], bounds[i+1])`, with the
    /// final bucket right-inclusive. Elements outside `[bounds[0], bounds[n]]` are dropped.
    ///
    /// Each partition produces its own bucket counts; the driver sums them. The bucket
    /// edges are a runtime parameter, so this is not a compile-time-registered task — but
    /// per-partition work keeps memory bounded.
    pub fn histogram(&self, bucket_bounds: &[f64]) -> Result<Vec<u64>, DataError>
    where
        T: crate::builtin_tasks::NumericValue + WireSerde,
    {
        if bucket_bounds.len() < 2 {
            return Ok(vec![]);
        }
        let n = bucket_bounds.len() - 1;
        let lo = bucket_bounds[0];
        let hi = bucket_bounds[n];
        let bounds = bucket_bounds.to_vec(); // Arc-able copy

        self.reduce_partitions(vec![0u64; n], move |mut counts, part| {
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
        })
    }

    /// Resolve `(source_partitions, steps)` for dispatching this RDD's pipeline: the staged
    /// pipeline's own source+steps if one was built by `_task` methods, or the raw RDD
    /// partitions (freshly encoded) with no steps otherwise. The un-staged case first runs
    /// any pending shuffle-map stage (in either mode) so a `ShuffledRdd` node in the lineage
    /// has output registered with `MapOutputTracker` before `encode_rdd_partitions`'
    /// `.compute()` tries to fetch it — shared by every un-staged dispatcher
    /// (`dispatch_with_step`, `reduce_partitions`, `collect`) so each gets it once instead
    /// of missing it independently.
    pub(super) fn resolve_pipeline(&self) -> Result<(Vec<Vec<u8>>, Vec<Step>), DataError>
    where
        T: WireSerde,
    {
        match &self.staged {
            Some(s) => Ok((s.source_partitions.clone(), s.steps.clone())),
            None => {
                let rdd_base = self.rdd.get_rdd_base();
                // Detect shuffles anywhere upstream, not just as a direct dep — a narrow hop
                // (`.values()`/`.map_values()`/`.filter()`) between the shuffle and this
                // terminal action would otherwise hide it, leaving the shuffle-map
                // undispatched and the reduce-side fetch below to hang.
                let has_shuffle =
                    !atomic_data::dependency::reduce_side_shuffles(&rdd_base).is_empty();
                if has_shuffle {
                    self.context
                        .run_pending_shuffle_stages(&rdd_base, vec![])
                        .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                }
                // A shuffle-crossing `.compute()` below needs a Tokio reactor for the
                // `ShuffleFetcher` HTTP pull — enter one the same way `run_pending_shuffle_stages`
                // does, since that call's own guard is already dropped by the time we get here.
                let rdd = self.rdd.clone();
                let src = crate::env::Env::run_in_async_rt(|| Context::encode_rdd_partitions(rdd))
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                Ok((src, vec![]))
            }
        }
    }

    /// Dispatch the staged pipeline (or raw partitions) with one extra builtin step appended,
    /// returning the raw per-partition result bytes. Empty blobs (e.g. a per-partition reducer
    /// that produced no value) are preserved for the caller to skip.
    pub(super) fn dispatch_with_step(
        &self,
        task_name: &str,
        action: TaskAction,
        payload: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, DataError>
    where
        T: WireSerde,
    {
        let (source, mut steps) = self.resolve_pipeline()?;
        steps.push(Step {
            task_name: task_name.to_string(),
            kind: StepKind::Task(action),
            runtime: TaskRuntime::Native,
            payload,
        });
        self.context
            .dispatch_pipeline(self.rdd.clone(), source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))
    }

    /// Dispatch the staged pipeline (or raw partitions) and fold the per-partition `Vec<T>`
    /// outputs into `acc` one partition at a time. Unlike [`collect`](TypedRdd::collect), this
    /// never concatenates all partitions on the driver — it holds one partition's `Vec<T>`
    /// plus the accumulator at a time.
    pub(super) fn reduce_partitions<A, F>(
        &self,
        zero: A,
        mut per_partition: F,
    ) -> Result<A, DataError>
    where
        T: WireSerde,
        F: FnMut(A, Vec<T>) -> A,
    {
        let (source, steps) = self.resolve_pipeline()?;
        let parts = self
            .context
            .dispatch_pipeline(self.rdd.clone(), source, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        let mut acc = zero;
        for b in parts {
            let v =
                Vec::<T>::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string()))?;
            acc = per_partition(acc, v);
        }
        Ok(acc)
    }

    /// Return the top k elements in descending order.
    pub fn top(&self, k: usize) -> Result<Vec<T>, DataError>
    where
        T: Ord + Clone + WireSerde,
    {
        // Primitives: each worker emits its local top-k via the builtin `TopKTask`
        // (≤ k per partition); other types truncate per-partition on the driver.
        let mut all_items: Vec<T> = if let Some(name) = crate::builtin_tasks::top_k_task_name::<T>()
        {
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
        Ok(all_items)
    }

    /// Return the first k elements in ascending order.
    pub fn take_ordered(&self, k: usize) -> Result<Vec<T>, DataError>
    where
        T: Ord + Clone + WireSerde,
    {
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

    #[test]
    fn test_approx_distinct_sd() {
        // 5 distinct values; small-cardinality linear counting is exact here.
        assert_eq!(rdd().count_approx_distinct_sd(0.05).unwrap(), 5);
    }
}
