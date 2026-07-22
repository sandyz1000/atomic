use super::*;

impl<T> TypedRdd<T>
where
    T: Data + Clone + WireEncode + WireDecode,
    Vec<T>: WireEncode + WireDecode,
{
    /// Apply a `#[task]`-registered unary function element-wise.
    ///
    /// In **distributed** mode this is lazy — the op is accumulated into the
    /// `StagedPipeline` and only dispatched when an action (`collect`, `fold_task`) fires.
    /// In **local** mode it falls back to an in-process `MapperRdd`.
    ///
    /// # Example
    /// ```ignore
    /// let doubled = ctx.parallelize_typed(data, 2).map_task(Double).collect()?;
    /// ```
    pub fn map_task<U, F>(self, task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<T, U>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            return TypedRdd::new(
                Arc::new(MapperRdd::new(id, self.rdd, move |x| task.call(x))),
                context,
            );
        }

        let op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Map),
            runtime: TaskRuntime::Native,
            payload: task.encode_params(),
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("map_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<U>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// Filter using a `#[task]`-registered predicate (return type `bool`).
    ///
    /// Lazy in distributed mode; eager (in-process `MapPartitionsRdd`) in local mode.
    ///
    /// # Example
    /// ```ignore
    /// let positives = ctx.parallelize_typed(data, 2).filter_task(IsPositive).collect()?;
    /// ```
    pub fn filter_task<F>(self, task: F) -> TypedRdd<T>
    where
        F: UnaryTask<T, bool>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            let filter_fn = move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| {
                let task = task.clone();
                Box::new(iter.filter(move |x| task.call(x.clone()))) as Box<dyn Iterator<Item = T>>
            };
            return TypedRdd::new(
                Arc::new(MapPartitionsRdd::new(id, self.rdd, filter_fn)),
                context,
            );
        }

        let op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Filter),
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("filter_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<T>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// FlatMap using a `#[task]`-registered function returning `Vec<U>`.
    ///
    /// Lazy in distributed mode; eager (in-process `FlatMapperRdd`) in local mode.
    ///
    /// # Example
    /// ```ignore
    /// let mirrored = ctx.parallelize_typed(data, 2).flat_map_task(Mirror).collect()?;
    /// ```
    pub fn flat_map_task<U, F>(self, task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<T, Vec<U>>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            return TypedRdd::new(
                Arc::new(FlatMapperRdd::new(id, self.rdd, move |x| {
                    Box::new(task.call(x).into_iter())
                })),
                context,
            );
        }

        let op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::FlatMap),
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("flat_map_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<U>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// Apply a `#[task]`-registered `Vec<T> -> Vec<U>` function to each whole partition.
    ///
    /// The task is a `UnaryTask<Vec<T>, Vec<U>>` — the "element" is the entire partition.
    /// Unlike [`map_partitions`](Self::map_partitions) (closure, driver-only), this dispatches
    /// to the worker holding the partition in distributed mode.
    ///
    /// # Example
    /// ```ignore
    /// #[task] fn top2(mut items: Vec<i32>) -> Vec<i32> { items.sort(); items.into_iter().rev().take(2).collect() }
    /// let out = ctx.parallelize_typed(data, 2).map_partitions_task(Top2).collect()?;
    /// ```
    pub fn map_partitions_task<U, F>(self, task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<Vec<T>, Vec<U>>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            let f = move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| {
                let items: Vec<T> = iter.collect();
                Box::new(task.call(items).into_iter()) as Box<dyn Iterator<Item = U>>
            };
            return TypedRdd::new(Arc::new(MapPartitionsRdd::new(id, self.rdd, f)), context);
        }

        let op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::MapPartitions),
            runtime: TaskRuntime::Native,
            payload: task.encode_params(),
        };
        let staged = Self::stage_op(self.staged, &self.rdd, op)
            .expect("map_partitions_task: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<U>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }

    /// Fold all elements using a `#[task]`-registered binary function.
    ///
    /// This is an **action** — it triggers computation and returns a single value.
    /// In distributed mode it dispatches the full staged pipeline (if any) plus
    /// the fold op as one round-trip, then combines per-partition fold results on
    /// the driver via a Reduce step.
    ///
    /// # Example
    /// ```ignore
    /// let total = ctx.parallelize_typed(data, 2).fold_task(0i32, Add)?;
    /// ```
    pub fn fold_task<F>(&self, init: T, task: F) -> Result<T, DataError>
    where
        F: BinaryTask<T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let task_c = task.clone();
            let zero = init.clone();
            let reduce_partition = move |iter: Box<dyn Iterator<Item = T>>| {
                iter.fold(zero.clone(), |a, b| task_c.call(a, b))
            };
            let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
            return Ok(results.into_iter().fold(init, |a, b| task.call(a, b)));
        }

        // Build pipeline: existing staged steps (if any) + fold op.
        let fold_payload = init
            .encode_wire()
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        let fold_op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Fold),
            runtime: TaskRuntime::Native,
            payload: fold_payload,
        };

        let (source_partitions, mut steps) = match &self.staged {
            None => {
                let encoded = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (encoded, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        steps.push(fold_op);

        let raw_partition_outputs = self
            .context
            .dispatch_pipeline(source_partitions, steps.clone())
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;

        let mut part_values: Vec<T> = raw_partition_outputs
            .into_iter()
            .map(|bytes| {
                T::decode_wire(&bytes).map_err(|e| DataError::DowncastFailure(e.to_string()))
            })
            .collect::<Result<_, _>>()?;

        if part_values.is_empty() {
            return Ok(init);
        }
        if part_values.len() == 1 {
            return Ok(part_values.remove(0));
        }

        Ok(part_values
            .into_iter()
            .reduce(|a, b| task.call(a, b))
            .unwrap_or(init))
    }

    /// Reduce all elements using a `#[task]`-registered binary function.
    ///
    /// Works identically in **local** and **distributed** mode. Returns `None`
    /// if the RDD is empty. Prefer [`fold_task`] when a known identity value exists.
    ///
    /// In distributed mode, dispatches the full staged pipeline (if any) plus a
    /// `Reduce` op to workers; each partition is reduced to a single element.
    /// The driver then combines the per-partition results with a second local Reduce.
    ///
    /// # Example
    /// ```ignore
    /// let total = ctx.parallelize_typed(data, 2).reduce_task(task_fn!(|a: i32, b: i32| a + b))?;
    /// ```
    pub fn reduce_task<F>(&self, task: F) -> Result<Option<T>, DataError>
    where
        F: BinaryTask<T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let task_c = task.clone();
            let reduce_partition = move |iter: Box<dyn Iterator<Item = T>>| {
                iter.reduce(|a, b| task_c.call(a, b))
                    .into_iter()
                    .collect::<Vec<_>>()
            };
            let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
            return Ok(results.into_iter().flatten().reduce(|a, b| task.call(a, b)));
        }

        let reduce_op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Reduce),
            runtime: TaskRuntime::Native,
            payload: vec![],
        };

        let (source_partitions, mut steps) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        steps.push(reduce_op);

        let partition_results_raw = self
            .context
            .dispatch_pipeline(source_partitions, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;

        let mut values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|b| T::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string())))
            .collect::<Result<_, _>>()?;

        match values.len() {
            0 => Ok(None),
            1 => Ok(Some(values.remove(0))),
            _ => {
                let combined = values
                    .encode_wire()
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                let driver_ops = vec![Step {
                    task_name: F::NAME.to_string(),
                    kind: StepKind::Task(TaskAction::Reduce),
                    runtime: TaskRuntime::Native,
                    payload: vec![],
                }];
                let task = TaskEnvelope::new(
                    0,
                    0,
                    0,
                    0,
                    0,
                    "driver-reduce".to_string(),
                    driver_ops,
                    combined,
                );
                let result = crate::runtimes::ComputeEngine::default()
                    .execute("local-driver", &task)
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                match result.status {
                    atomic_data::distributed::ResultStatus::Success => T::decode_wire(&result.data)
                        .map(Some)
                        .map_err(|e| DataError::DowncastFailure(e.to_string())),
                    _ => Err(DataError::DowncastFailure(
                        result
                            .error
                            .unwrap_or_else(|| "reduce_task failed".to_string()),
                    )),
                }
            }
        }
    }

    /// Aggregate with a distinct accumulator type using an
    /// [`AggregateTask<Acc, T>`](crate::task_traits::AggregateTask).
    ///
    /// This is an **action**. Each partition folds its elements into one `Acc` via `seq`
    /// (on workers in distributed mode, starting from the wire-encoded `zero`); the driver
    /// merges the per-partition accumulators with `comb`. This is the general reduction
    /// (`Acc != T`) that `fold_task`/`reduce_task` (`BinaryTask`, `Acc == T`) cannot express.
    ///
    /// The task type must be registered with
    /// [`register_aggregate_task!`](crate::register_aggregate_task) so the worker carries its
    /// handler.
    ///
    /// # Example
    /// ```ignore
    /// // mean via (sum, count)
    /// let (sum, n) = rdd.aggregate_task((0.0f64, 0u64), MeanTask::<i32>::default())?;
    /// ```
    pub fn aggregate_task<Acc, F>(&self, zero: Acc, task: F) -> Result<Acc, DataError>
    where
        Acc: Data + Clone + WireEncode + WireDecode,
        F: AggregateTask<Acc, T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let task_c = task.clone();
            let z = zero.clone();
            let seq_partition = move |iter: Box<dyn Iterator<Item = T>>| {
                iter.fold(z.clone(), |a, x| task_c.seq(a, x))
            };
            let partials = self.context.run_job(self.rdd.clone(), seq_partition)?;
            return Ok(partials.into_iter().fold(zero, |a, b| task.comb(a, b)));
        }

        let payload = zero
            .encode_wire()
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
        let agg_op = Step {
            task_name: F::NAME.to_string(),
            kind: StepKind::Task(TaskAction::Aggregate),
            runtime: TaskRuntime::Native,
            payload,
        };

        let (source_partitions, mut steps) = match &self.staged {
            None => {
                let encoded = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| DataError::DowncastFailure(e.to_string()))?;
                (encoded, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.steps.clone()),
        };
        steps.push(agg_op);

        let raw = self
            .context
            .dispatch_pipeline(source_partitions, steps)
            .map_err(|e| DataError::DowncastFailure(e.to_string()))?;

        let partials: Vec<Acc> = raw
            .into_iter()
            .map(|b| Acc::decode_wire(&b).map_err(|e| DataError::DowncastFailure(e.to_string())))
            .collect::<Result<_, _>>()?;

        Ok(partials.into_iter().fold(zero, |a, b| task.comb(a, b)))
    }

    /// Arithmetic mean of the elements as `f64`.
    ///
    /// Uses the built-in [`MeanTask`](crate::builtin_tasks::mean::MeanTask) — each partition
    /// accumulates `(sum, count)` on the worker; the driver merges and divides. Returns an
    /// error on an empty RDD rather than `NaN`.
    ///
    /// Distributed `f64`/`f32` sums fold in partition-arrival order, so the result is not
    /// bit-identical to local mode.
    pub fn mean(&self) -> Result<f64, DataError>
    where
        crate::builtin_tasks::mean::MeanTask<T>: AggregateTask<(f64, u64), T> + Default,
    {
        let (sum, count) = self.aggregate_task(
            (0.0f64, 0u64),
            crate::builtin_tasks::mean::MeanTask::<T>::default(),
        )?;
        if count == 0 {
            return Err(DataError::DowncastFailure(
                "mean of empty collection".to_string(),
            ));
        }
        Ok(sum / count as f64)
    }

    /// Population variance of the elements as `f64`.
    ///
    /// Uses the built-in [`VarianceTask`](crate::builtin_tasks::variance::VarianceTask) —
    /// each partition accumulates Welford `(count, mean, m2)` on the worker; the driver merges
    /// them and returns `m2 / count`. Returns an error on an empty RDD.
    ///
    /// Distributed float accumulation folds in partition-arrival order, so the result is not
    /// bit-identical to local mode.
    pub fn variance(&self) -> Result<f64, DataError>
    where
        crate::builtin_tasks::variance::VarianceTask<T>:
            AggregateTask<(u64, f64, f64), T> + Default,
    {
        let (count, _mean, m2) = self.aggregate_task(
            (0u64, 0.0f64, 0.0f64),
            crate::builtin_tasks::variance::VarianceTask::<T>::default(),
        )?;
        if count == 0 {
            return Err(DataError::DowncastFailure(
                "variance of empty collection".to_string(),
            ));
        }
        Ok(m2 / count as f64)
    }

    /// Population standard deviation of the elements as `f64` — `sqrt(variance())`.
    pub fn stdev(&self) -> Result<f64, DataError>
    where
        crate::builtin_tasks::variance::VarianceTask<T>:
            AggregateTask<(u64, f64, f64), T> + Default,
    {
        Ok(self.variance()?.sqrt())
    }

    /// Build (or extend) a `StagedPipeline` for distributed lazy dispatch.
    ///
    /// If a pipeline was already staged, appends `op` and returns it.
    /// Otherwise encodes the source `rdd` partitions once and starts a new pipeline.
    fn stage_op(
        staged: Option<StagedPipeline>,
        rdd: &RddRef<T>,
        op: Step,
    ) -> Result<StagedPipeline, crate::error::ComputeError> {
        match staged {
            Some(mut s) => {
                s.steps.push(op);
                Ok(s)
            }
            None => {
                let source_partitions = Context::encode_rdd_partitions(rdd.clone())?;
                Ok(StagedPipeline {
                    source_partitions,
                    steps: vec![op],
                })
            }
        }
    }
}

impl TypedRdd<String> {
    /// Dispatch a framework-native sub-agent over each partition of string inputs.
    ///
    /// The registered [`AgentRunner`][crate::task_registry::AgentRunner] (installed
    /// by `atomic-nlq` at startup via [`register_agent_runner`][crate::register_agent_runner])
    /// executes a multi-round plan→execute→evaluate loop per partition, returning one
    /// [`AgentFindings`][atomic_data::distributed::AgentFindings] per input string.
    ///
    /// **Lazy in distributed mode** — appends an `AgentStep` op to the staged pipeline
    /// and dispatches on the next action.  **Local mode** runs the registered runner
    /// in-process.
    ///
    /// Stages are automatically marked non-speculatable by the scheduler so no
    /// partition runs twice and accrues double LLM cost.
    pub fn agent_step(
        self,
        config: atomic_data::distributed::AgentStepPayload,
    ) -> TypedRdd<atomic_data::distributed::AgentFindings> {
        use atomic_data::distributed::{AgentFindings, TaskRuntime, WireDecode};

        let context = self.context.clone();
        let payload_bytes =
            serde_json::to_vec(&config).expect("AgentStepPayload serialization failed");

        if !context.is_distributed() {
            let runner = crate::task_registry::AGENT_RUNNER_REGISTRY.get().expect(
                "agent_step: no agent runner registered; \
                     call `atomic_compute::register_agent_runner(...)` at startup",
            );
            let source_partitions = Context::encode_rdd_partitions(self.rdd.clone())
                .expect("agent_step: failed to encode source partitions");
            let flat: Vec<AgentFindings> = source_partitions
                .into_iter()
                .flat_map(|part_bytes| {
                    let raw = runner
                        .run_partition(&config, &part_bytes)
                        .expect("agent_step local runner failed");
                    Vec::<AgentFindings>::decode_wire(&raw)
                        .expect("agent_step: failed to decode AgentFindings from runner output")
                })
                .collect();
            let id = context.new_rdd_id();
            return TypedRdd::new(Arc::new(ParallelCollection::new(id, flat, 1)), context);
        }

        let op = Step {
            task_name: String::new(),
            kind: StepKind::Engine(EngineStep::AgentStep),
            runtime: TaskRuntime::Native,
            payload: payload_bytes,
        };
        let staged = TypedRdd::<String>::stage_op(self.staged, &self.rdd, op)
            .expect("agent_step: failed to encode source partitions");
        let id = context.new_rdd_id();
        TypedRdd {
            rdd: Arc::new(ParallelCollection::new(id, Vec::<AgentFindings>::new(), 1)),
            context,
            staged: Some(staged),
            _marker: PhantomData,
        }
    }
}
