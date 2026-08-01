use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use atomic_data::data::Data;
use atomic_data::distributed::{
    ResultStatus, Step, StepKind, TaskAction, TaskEnvelope, TaskRuntime, WireDecode, WireEncode,
    WireSerde,
};
use atomic_data::partial::{ApproximateEvaluator, result::PartialResult};
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::task_context::PartitionTask;
use atomic_scheduler::Schedulers;

use crate::env;
use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, ComputeEngine};

use super::Context;

impl Context {
    /// Dispatch a `#[task]`-registered Map/Filter/FlatMap over every partition,
    /// returning decoded `Vec<U>` per partition.
    pub fn run_native_job_map<T, U>(
        self: &Arc<Self>,
        task_name: &str,
        action: TaskAction,
        payload: Vec<u8>,
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<Vec<Vec<U>>>
    where
        T: Data + Clone + WireSerde,
        U: Data + Clone + WireSerde,
    {
        let steps = vec![Step {
            task_name: task_name.to_string(),
            kind: StepKind::Task(action),
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd.clone())?;
        let result_bytes = self.dispatch_pipeline(rdd, encoded, steps)?;
        result_bytes
            .into_iter()
            .map(|bytes| Vec::<U>::decode_wire(&bytes).map_err(ComputeError::from))
            .collect()
    }

    /// Dispatch a `#[task]`-registered binary Fold over every partition,
    /// then combine the per-partition results into a single value on the driver.
    pub fn run_native_job_fold<T>(
        self: &Arc<Self>,
        task_name: &str,
        zero: T,
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<T>
    where
        T: Data + Clone + WireSerde,
    {
        let payload = zero.encode_wire()?;
        let steps = vec![Step {
            task_name: task_name.to_string(),
            kind: StepKind::Task(TaskAction::Fold),
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd.clone())?;
        let partition_results_raw = self.dispatch_pipeline(rdd, encoded, steps.clone())?;

        let mut partition_values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|bytes| T::decode_wire(&bytes).map_err(ComputeError::from))
            .collect::<ComputeResult<_, _>>()?;

        if partition_values.is_empty() {
            return Ok(zero);
        }
        if partition_values.len() == 1 {
            return Ok(partition_values.remove(0));
        }

        // Combine partition results via Reduce on the driver.
        let combined_data = partition_values.encode_wire()?;
        let reduce_ops = vec![Step {
            task_name: task_name.to_string(),
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
            format!("driver-reduce-{}", task_name),
            reduce_ops,
            combined_data,
        );
        let result = ComputeEngine::default().execute("local-driver", &task)?;
        match result.status {
            ResultStatus::Success => Ok(T::decode_wire(&result.data)?),
            _ => Err(ComputeError::InvalidPayload(
                result.error.unwrap_or_else(|| "reduce failed".to_string()),
            )),
        }
    }

    /// Dispatch a full pipeline of steps over pre-encoded partition bytes, as one `Stage`
    /// job through `Schedulers::run_pipeline_job` — the scheduler's own `submit_stage`/
    /// `get_missing_parent_stages` recursion discovers and runs any shuffle-map parent stage
    /// (walking `final_rdd`'s dependency DAG) before the result stage runs, via a real
    /// tracked `Stage` wait, not call order. `Local` mode runs each task on a blocking
    /// thread; `Distributed` mode ships it to a worker — that's the only difference.
    ///
    /// `final_rdd` supplies only the shape the planner walks (`RddBase`, dependencies, split
    /// count) — its actual data is irrelevant here, since `source_partitions` (already
    /// encoded) is what a `PipelineTask` actually runs `steps` over.
    ///
    /// An empty pipeline (no `steps`) is a no-op by definition — `source_partitions` is
    /// returned unchanged without submitting a job at all.
    ///
    /// Returns raw result bytes per partition; callers decode into the concrete type.
    pub fn dispatch_pipeline<T: Data>(
        &self,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        source_partitions: Vec<Vec<u8>>,
        steps: Vec<Step>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        if steps.is_empty() {
            return Ok(source_partitions);
        }
        let broadcasts = self.broadcast_snapshot();
        let partitions: Vec<usize> = (0..source_partitions.len()).collect();
        let pipeline_data = atomic_scheduler::PipelineJobData {
            result_steps: steps,
            source_partitions,
            broadcasts,
        };
        // `run_pipeline_job` dispatches tasks via `tokio::task::spawn_blocking` (local mode)
        // or its own tokio-backed RPC path (distributed mode), both of which need an active
        // Tokio runtime in scope — same requirement `run_job`/`run_job_with_partitions`
        // already wrap for below.
        Ok(env::Env::run_in_async_rt(|| {
            self.scheduler
                .run_pipeline_job(final_rdd, pipeline_data, partitions)
        })?)
    }

    /// Encode every partition of an RDD into rkyv bytes.
    pub(crate) fn encode_rdd_partitions<T>(
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<Vec<Vec<u8>>>
    where
        T: Data + Clone + WireSerde,
    {
        rdd.splits()
            .iter()
            .map(|split| {
                let items: Vec<T> = rdd.compute(split.clone())?.collect();
                Ok(items.encode_wire()?)
            })
            .collect()
    }

    /// Run all pending `ShuffleDependency` map stages and register outputs with
    /// `MapOutputTracker` so the reduce phase can fetch them — needed by any un-staged
    /// (plain-closure) RDD chain whose lineage crosses a shuffle boundary, in either mode.
    ///
    /// - **Distributed**: dispatches one `TaskEnvelope` per map partition to workers.
    /// - **Local**: runs each map partition in-process via
    ///   [`ShuffleDependency::do_shuffle_task`](atomic_data::dependency::ShuffleDependency::do_shuffle_task)
    ///   — the same call `LocalScheduler`'s own `ShuffleMapTask::run` makes — so the reduce
    ///   side's HTTP fetch (same `ShuffleManager` server local mode already runs) finds a
    ///   registered map output regardless of executor.
    pub fn run_pending_shuffle_stages(
        self: &Arc<Self>,
        rdd: &Arc<dyn RddBase>,
        preceding_steps: Vec<Step>,
    ) -> ComputeResult<()> {
        let sched = match &self.scheduler {
            Schedulers::Distributed(s) => s.clone(),
            Schedulers::Local(_) => return self.run_shuffle_stages_local(rdd),
        };

        let dispatched = env::Env::run_in_async_rt(|| {
            futures::executor::block_on(sched.run_pending_shuffle_stages(rdd, preceding_steps))
        })?;

        for stage in dispatched {
            let num_output_partitions = stage.dep.get_num_output_partitions();
            self.active_shuffle_stages.insert(
                stage.shuffle_id,
                super::ActiveShuffleStage {
                    shuffle_id: stage.shuffle_id,
                    dep: stage.dep,
                    steps: stage.steps,
                },
            );
            log::info!(
                "shuffle map stage complete: shuffle_id={} num_reduce_partitions={}",
                stage.shuffle_id,
                num_output_partitions,
            );
        }

        Ok(())
    }

    fn run_shuffle_stages_local(&self, rdd: &Arc<dyn RddBase>) -> ComputeResult<()> {
        let Some(tracker) = atomic_data::env::get_map_output_tracker() else {
            return Ok(());
        };
        // Walk narrow deps too: a shuffle can sit upstream of a `.values()`/`.map_values()`
        // hop, where a direct-dep scan would miss it (see `reduce_side_shuffles`).
        for shuffle_dep in atomic_data::dependency::reduce_side_shuffles(rdd) {
            let shuffle_id = shuffle_dep.get_shuffle_id();
            let num_map_partitions = shuffle_dep.get_rdd_base().number_of_splits();
            let uris: Vec<Option<String>> = (0..num_map_partitions)
                .map(|p| Some(shuffle_dep.do_shuffle_task(p)))
                .collect();
            tracker.register_shuffle(shuffle_id, num_map_partitions);
            tracker.register_map_outputs(shuffle_id, uris);
            log::info!(
                "shuffle map stage complete (local): shuffle_id={shuffle_id} \
                 num_map_partitions={num_map_partitions}"
            );
        }
        Ok(())
    }

    /// Wire the driver's fetch-failure recovery: when a reduce task reports a lost
    /// map output, recompute that one map partition on a live worker and re-register
    /// its fresh shuffle URI so only the fetching stage retries. Without this, the
    /// local scheduler would recompute the map partition on the driver — wrong for
    /// staged pipelines, whose driver-side parent RDD is an empty placeholder.
    pub(super) fn install_map_output_recovery(
        driver_scheduler: &atomic_scheduler::LocalScheduler,
        dist: Arc<atomic_scheduler::DistributedScheduler>,
        stages: super::ActiveShuffleStages,
    ) {
        driver_scheduler.set_map_output_recovery(Arc::new(move |shuffle_id, map_id| {
            let (dep, steps) = match stages.get(&shuffle_id) {
                Some(entry) => (Arc::clone(&entry.dep), entry.steps.clone()),
                None => {
                    log::error!(
                        "map-output recovery: no dispatched stage recorded for \
                         shuffle {shuffle_id}"
                    );
                    return false;
                }
            };
            let partition = match dep.encode_partitions() {
                Ok(mut parts) if map_id < parts.len() => parts.swap_remove(map_id),
                Ok(parts) => {
                    log::error!(
                        "map-output recovery: map {map_id} out of bounds for \
                         shuffle {shuffle_id} ({} partitions)",
                        parts.len()
                    );
                    return false;
                }
                Err(e) => {
                    log::error!(
                        "map-output recovery: re-encoding shuffle {shuffle_id} input \
                         failed: {e}"
                    );
                    return false;
                }
            };

            // The hook runs inside the local scheduler's blocking event loop, so the
            // dispatch future is spawned onto the shared runtime and awaited over a
            // channel instead of a nested `block_on`.
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let dist = Arc::clone(&dist);
            env::Env::run_in_async_rt(move || {
                tokio::spawn(async move {
                    let res = dist
                        .rerun_shuffle_map_partition(shuffle_id, map_id, steps, partition)
                        .await;
                    let _ = tx.send(res);
                });
            });
            match rx.recv() {
                Ok(Ok(())) => true,
                Ok(Err(e)) => {
                    log::error!("map-output recovery dispatch failed: {e}");
                    false
                }
                Err(e) => {
                    log::error!("map-output recovery task dropped: {e}");
                    false
                }
            }
        }));
    }

    /// Legacy closure-dispatch path, not a pattern for new code — see
    /// [`Context::driver_scheduler`]'s doc comment. Backs the built-in RDD actions only.
    pub fn run_job<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> ComputeResult<Vec<U>>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U + Send + Sync + 'static,
    {
        let cl = move |(_task_context, iter)| (func)(iter);
        let sched = self.driver_scheduler.clone();
        let partitions = (0..rdd.number_of_splits()).collect();
        let res =
            env::Env::run_in_async_rt(|| sched.run_job(Arc::new(cl), rdd, partitions, false))?;
        Ok(res)
    }

    /// Legacy closure-dispatch path, not a pattern for new code — see
    /// [`Context::driver_scheduler`]'s doc comment. Backs the built-in RDD actions only.
    pub fn run_job_with_partitions<T: Data, U: Data + Clone, F, P>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
        partitions: P,
    ) -> ComputeResult<Vec<U>>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U + Send + Sync + 'static,
        P: IntoIterator<Item = usize>,
    {
        let cl = move |(_task_context, iter)| (func)(iter);
        let sched = self.driver_scheduler.clone();
        let partitions: Vec<usize> = partitions.into_iter().collect();
        let res =
            env::Env::run_in_async_rt(|| sched.run_job(Arc::new(cl), rdd, partitions, false))?;
        Ok(res)
    }

    /// Legacy closure-dispatch path, not a pattern for new code — see
    /// [`Context::driver_scheduler`]'s doc comment. Backs the built-in RDD actions only.
    pub fn run_job_with_context<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> ComputeResult<Vec<U>>
    where
        F: PartitionTask<T, U>,
    {
        let func = Arc::new(func);
        let sched = self.driver_scheduler.clone();
        let partitions = (0..rdd.number_of_splits()).collect();
        let res = env::Env::run_in_async_rt(|| sched.run_job(func, rdd, partitions, false))?;
        Ok(res)
    }

    /// Run `func` over every partition, merging each partition's result into `evaluator` as it
    /// arrives, and return a [`PartialResult<R>`] once either every partition has reported or
    /// `timeout` elapses — whichever comes first. `PartialResult::is_final` tells you which.
    ///
    /// Concrete evaluators: [`CountEvaluator`](atomic_data::partial::CountEvaluator) for a
    /// scalar approximate count, [`GroupedCountEvaluator`](atomic_data::partial::GroupedCountEvaluator)
    /// for counts by key. See `examples/approx_count` for a runnable end-to-end example.
    ///
    /// **Local scheduler only.** Under `Context::distributed(..)` this returns
    /// `ComputeError`/`SchedulerError::UnsupportedOperation` — approximate jobs need the
    /// local scheduler's polling event loop, which the distributed dispatch path doesn't have.
    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: &Arc<Self>,
        func: F,
        rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> ComputeResult<PartialResult<R>>
    where
        F: PartitionTask<T, U>,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        let res = self
            .scheduler
            .run_approximate_job(Arc::new(func), rdd, evaluator, timeout)?;
        Ok(res)
    }

    /// Collect all elements of an RDD into a `Vec`, distribution-aware.
    pub fn collect_rdd<T>(self: &Arc<Self>, rdd: Arc<dyn Rdd<Item = T>>) -> ComputeResult<Vec<T>>
    where
        T: Data + Clone + WireSerde,
    {
        use crate::rdd::TypedRdd;
        if matches!(self.scheduler, Schedulers::Distributed(_))
            && let Some((partitions, steps)) = rdd.extract_staged_pipeline()
        {
            let raw = self.dispatch_pipeline(rdd.clone(), partitions, steps)?;
            let mut out: Vec<T> = Vec::new();
            for bytes in raw {
                let decoded = Vec::<T>::decode_wire(&bytes).map_err(|e| {
                    ComputeError::InvalidPayload(format!("collect_rdd decode: {e}"))
                })?;
                out.extend(decoded);
            }
            return Ok(out);
        }
        Ok(TypedRdd::new(rdd, self.clone()).collect()?)
    }
}
