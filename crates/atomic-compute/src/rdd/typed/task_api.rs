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
    pub fn map_task<U, F>(self, _task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<T, U>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            return TypedRdd::new(Arc::new(MapperRdd::new(id, self.rdd, F::call)), context);
        }

        let op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
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
    pub fn filter_task<F>(self, _task: F) -> TypedRdd<T>
    where
        F: UnaryTask<T, bool>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            let filter_fn = move |_idx: usize, iter: Box<dyn Iterator<Item = T>>| {
                Box::new(iter.filter(|x| F::call(x.clone()))) as Box<dyn Iterator<Item = T>>
            };
            return TypedRdd::new(
                Arc::new(MapPartitionsRdd::new(id, self.rdd, filter_fn)),
                context,
            );
        }

        let op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Filter,
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
    pub fn flat_map_task<U, F>(self, _task: F) -> TypedRdd<U>
    where
        U: Data + Clone + WireEncode + WireDecode,
        Vec<U>: WireEncode + WireDecode,
        F: UnaryTask<T, Vec<U>>,
    {
        let context = self.context.clone();
        if !context.is_distributed() {
            let id = context.new_rdd_id();
            return TypedRdd::new(
                Arc::new(FlatMapperRdd::new(id, self.rdd, |x| {
                    Box::new(F::call(x).into_iter())
                })),
                context,
            );
        }

        let op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::FlatMap,
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
    pub fn fold_task<F>(&self, init: T, _task: F) -> Result<T, BaseError>
    where
        F: BinaryTask<T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let f_clone = F::call;
            let zero = init.clone();
            let reduce_partition =
                move |iter: Box<dyn Iterator<Item = T>>| iter.fold(zero.clone(), &f_clone);
            let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
            return Ok(results.into_iter().fold(init, F::call));
        }

        // Build pipeline: existing staged ops (if any) + fold op.
        let fold_payload = init
            .encode_wire()
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
        let fold_op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Fold,
            runtime: TaskRuntime::Native,
            payload: fold_payload,
        };

        let (source_partitions, mut ops) = match &self.staged {
            None => {
                let encoded = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                (encoded, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.ops.clone()),
        };
        ops.push(fold_op);

        let partition_results_raw = self
            .context
            .dispatch_pipeline(source_partitions, ops.clone())
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;

        let mut partition_values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|bytes| {
                T::decode_wire(&bytes).map_err(|e| BaseError::DowncastFailure(e.to_string()))
            })
            .collect::<Result<_, _>>()?;

        if partition_values.is_empty() {
            return Ok(init);
        }
        if partition_values.len() == 1 {
            return Ok(partition_values.remove(0));
        }

        Ok(partition_values.into_iter().reduce(F::call).unwrap_or(init))
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
    pub fn reduce_task<F>(&self, _task: F) -> Result<Option<T>, BaseError>
    where
        F: BinaryTask<T>,
        Vec<T>: WireEncode + WireDecode,
    {
        if !self.context.is_distributed() {
            let reduce_partition = |iter: Box<dyn Iterator<Item = T>>| {
                iter.reduce(F::call).into_iter().collect::<Vec<_>>()
            };
            let results = self.context.run_job(self.rdd.clone(), reduce_partition)?;
            return Ok(results.into_iter().flatten().reduce(F::call));
        }

        let reduce_op = PipelineOp {
            op_id: F::NAME.to_string(),
            action: TaskAction::Reduce,
            runtime: TaskRuntime::Native,
            payload: vec![],
        };

        let (source_partitions, mut ops) = match &self.staged {
            None => {
                let src = Context::encode_rdd_partitions(self.rdd.clone())
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                (src, vec![])
            }
            Some(s) => (s.source_partitions.clone(), s.ops.clone()),
        };
        ops.push(reduce_op);

        let partition_results_raw = self
            .context
            .dispatch_pipeline(source_partitions, ops)
            .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;

        let mut values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|b| T::decode_wire(&b).map_err(|e| BaseError::DowncastFailure(e.to_string())))
            .collect::<Result<_, _>>()?;

        match values.len() {
            0 => Ok(None),
            1 => Ok(Some(values.remove(0))),
            _ => {
                let combined = values
                    .encode_wire()
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                let driver_ops = vec![PipelineOp {
                    op_id: F::NAME.to_string(),
                    action: TaskAction::Reduce,
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
                    .map_err(|e| BaseError::DowncastFailure(e.to_string()))?;
                match result.status {
                    atomic_data::distributed::ResultStatus::Success => T::decode_wire(&result.data)
                        .map(Some)
                        .map_err(|e| BaseError::DowncastFailure(e.to_string())),
                    _ => Err(BaseError::DowncastFailure(
                        result
                            .error
                            .unwrap_or_else(|| "reduce_task failed".to_string()),
                    )),
                }
            }
        }
    }

    /// Build (or extend) a `StagedPipeline` for distributed lazy dispatch.
    ///
    /// If a pipeline was already staged, appends `op` and returns it.
    /// Otherwise encodes the source `rdd` partitions once and starts a new pipeline.
    fn stage_op(
        staged: Option<StagedPipeline>,
        rdd: &RddRef<T>,
        op: PipelineOp,
    ) -> Result<StagedPipeline, crate::error::ComputeError> {
        match staged {
            Some(mut s) => {
                s.ops.push(op);
                Ok(s)
            }
            None => {
                let source_partitions = Context::encode_rdd_partitions(rdd.clone())?;
                Ok(StagedPipeline {
                    source_partitions,
                    ops: vec![op],
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

        let op = PipelineOp {
            op_id: String::new(),
            action: TaskAction::AgentStep,
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
