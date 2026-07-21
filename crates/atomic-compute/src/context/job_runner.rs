use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use atomic_data::data::Data;
use atomic_data::distributed::{
    ResultStatus, Step, StepKind, TaskAction, TaskEnvelope, TaskRuntime, WireDecode, WireEncode,
};
use atomic_data::partial::{ApproximateEvaluator, result::PartialResult};
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::task_context::TaskContext;
use atomic_scheduler::Schedulers;

use crate::env;
use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, ComputeEngine};

use super::Context;
use super::pipeline_executor::{DistributedExecutor, LocalExecutor, PipelineExecutor};

impl Context {
    /// Dispatch a `#[task]`-registered Map/Filter/FlatMap over every partition,
    /// returning decoded `Vec<U>` per partition.
    pub fn run_native_job_map<T, U>(
        self: &Arc<Self>,
        op_id: &str,
        action: TaskAction,
        payload: Vec<u8>,
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<Vec<Vec<U>>>
    where
        T: Data + Clone + WireEncode,
        Vec<T>: WireEncode,
        U: Data + Clone + WireDecode,
        Vec<U>: WireDecode,
    {
        let steps = vec![Step {
            op_id: op_id.to_string(),
            kind: StepKind::Task(action),
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let result_bytes = self.dispatch_pipeline(encoded, steps)?;
        result_bytes
            .into_iter()
            .map(|bytes| Vec::<U>::decode_wire(&bytes).map_err(ComputeError::from))
            .collect()
    }

    /// Dispatch a `#[task]`-registered binary Fold over every partition,
    /// then combine the per-partition results into a single value on the driver.
    pub fn run_native_job_fold<T>(
        self: &Arc<Self>,
        op_id: &str,
        zero: T,
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<T>
    where
        T: Data + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        let payload = zero.encode_wire()?;
        let steps = vec![Step {
            op_id: op_id.to_string(),
            kind: StepKind::Task(TaskAction::Fold),
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let partition_results_raw = self.dispatch_pipeline(encoded, steps.clone())?;

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
            op_id: op_id.to_string(),
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
            format!("driver-reduce-{}", op_id),
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

    /// Dispatch a full pipeline of steps over pre-encoded partition bytes.
    ///
    /// - **Local mode**: runs all steps via `NativeBackend` in-process.
    /// - **Distributed mode**: sends one `TaskEnvelope` per partition to a worker via TCP.
    ///
    /// Returns raw result bytes per partition; callers decode into the concrete type.
    pub fn dispatch_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        steps: Vec<Step>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let broadcasts = self.broadcast_snapshot();
        self.pipeline_executor()
            .run_pipeline(source_partitions, steps, broadcasts)
    }

    /// Build the [`PipelineExecutor`] for the active runtime — a local thread pool
    /// ([`LocalExecutor`]) or remote workers ([`DistributedExecutor`]). Upstream calls
    /// [`dispatch_pipeline`](Self::dispatch_pipeline) either way.
    pub(crate) fn pipeline_executor(&self) -> Box<dyn PipelineExecutor> {
        match &self.scheduler {
            Schedulers::Local(_) => Box::new(LocalExecutor {
                accumulator_store: self.accumulator_store.clone(),
            }),
            Schedulers::Distributed(s) => Box::new(DistributedExecutor {
                scheduler: s.clone(),
            }),
        }
    }

    /// Encode every partition of an RDD into rkyv bytes.
    pub(crate) fn encode_rdd_partitions<T>(
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<Vec<Vec<u8>>>
    where
        T: Data + Clone + WireEncode,
        Vec<T>: WireEncode,
    {
        rdd.splits()
            .iter()
            .map(|split| {
                let items: Vec<T> = rdd.compute(split.clone())?.collect();
                Ok(items.encode_wire()?)
            })
            .collect()
    }

    /// In distributed mode, run all pending `ShuffleDependency` map stages on workers
    /// and register outputs with `MapOutputTracker` so the reduce phase can fetch them.
    pub fn run_pending_shuffle_stages(
        self: &Arc<Self>,
        rdd: &Arc<dyn RddBase>,
        preceding_steps: Vec<Step>,
    ) -> ComputeResult<()> {
        let sched = match &self.scheduler {
            Schedulers::Distributed(s) => s.clone(),
            Schedulers::Local(_) => return Ok(()),
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

    pub fn run_job_with_context<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> ComputeResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
    {
        let func = Arc::new(func);
        let sched = self.driver_scheduler.clone();
        let partitions = (0..rdd.number_of_splits()).collect();
        let res = env::Env::run_in_async_rt(|| sched.run_job(func, rdd, partitions, false))?;
        Ok(res)
    }

    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: &Arc<Self>,
        func: F,
        rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> ComputeResult<PartialResult<R>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
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
        T: Data + Clone + WireDecode,
        Vec<T>: WireDecode,
    {
        use crate::rdd::TypedRdd;
        if matches!(self.scheduler, Schedulers::Distributed(_))
            && let Some((partitions, steps)) = rdd.extract_staged_pipeline()
        {
            let raw = self.dispatch_pipeline(partitions, steps)?;
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
