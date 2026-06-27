use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use atomic_data::data::Data;
use atomic_data::distributed::{
    PipelineOp, ResultStatus, TaskAction, TaskEnvelope, TaskRuntime, WireDecode, WireEncode,
};
use atomic_data::partial::{ApproximateEvaluator, result::PartialResult};
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::task_context::TaskContext;
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
        let ops = vec![PipelineOp {
            op_id: op_id.to_string(),
            action,
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let result_bytes = self.dispatch_pipeline(encoded, ops)?;
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
        let ops = vec![PipelineOp {
            op_id: op_id.to_string(),
            action: TaskAction::Fold,
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let partition_results_raw = self.dispatch_pipeline(encoded, ops.clone())?;

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
        let reduce_ops = vec![PipelineOp {
            op_id: op_id.to_string(),
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

    /// Dispatch a full pipeline of ops over pre-encoded partition bytes.
    ///
    /// - **Local mode**: runs all ops via `NativeBackend` in-process.
    /// - **Distributed mode**: sends one `TaskEnvelope` per partition to a worker via TCP.
    ///
    /// Returns raw result bytes per partition; callers decode into the concrete type.
    pub fn dispatch_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        ops: Vec<PipelineOp>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let broadcasts = self.broadcast_snapshot();
        match &self.scheduler {
            Schedulers::Local(_) => {
                // Some ops (e.g. AgentStep) need a reachable Tokio runtime to `block_on`
                // their async work; local mode otherwise runs on a plain native thread.
                env::Env::run_in_async_rt(|| {
                    let backend = ComputeEngine::default();
                    source_partitions
                        .into_iter()
                        .enumerate()
                        .map(|(part_id, data)| {
                            let task = TaskEnvelope::new(
                                0,
                                0,
                                part_id,
                                0,
                                part_id,
                                format!("local-pipeline-{}", part_id),
                                ops.clone(),
                                data,
                            )
                            .with_broadcasts(broadcasts.clone());
                            let result = backend.execute("local-driver", &task)?;
                            match result.status {
                                ResultStatus::Success => {
                                    if !result.accumulator_deltas.is_empty() {
                                        self.merge_accumulator_deltas(&result.accumulator_deltas);
                                    }
                                    Ok(result.data)
                                }
                                _ => Err(ComputeError::InvalidPayload(
                                    result.error.unwrap_or_else(|| "task failed".to_string()),
                                )),
                            }
                        })
                        .collect()
                })
            }
            Schedulers::Distributed(sched) => Ok(env::Env::run_in_async_rt(|| {
                futures::executor::block_on(sched.run_native_job_with_broadcasts(
                    ops,
                    source_partitions,
                    broadcasts,
                ))
            })?),
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
        preceding_ops: Vec<PipelineOp>,
    ) -> ComputeResult<()> {
        use atomic_data::dependency::Dependency;

        let sched = match &self.scheduler {
            Schedulers::Distributed(s) => s.clone(),
            Schedulers::Local(_) => return Ok(()),
        };

        for dep in rdd.get_dependencies() {
            if let Dependency::Shuffle(shuffle_dep) = dep {
                let parent_partitions = shuffle_dep.encode_partitions()?;
                let spec = &shuffle_dep.spec;

                let shuffle_op = PipelineOp {
                    op_id: format!("shuffle-map-{}", spec.shuffle_id),
                    action: TaskAction::ShuffleMap {
                        shuffle_id: spec.shuffle_id,
                        num_output_partitions: spec.num_output_partitions,
                    },
                    runtime: TaskRuntime::Native,
                    payload: bincode::encode_to_vec(
                        crate::shuffle_map::ShuffleMapPayload {
                            type_id: spec.type_id.to_string(),
                            partitioner_spec: spec.partitioner_spec.clone(),
                        },
                        bincode::config::standard(),
                    )
                    .map_err(|e| {
                        ComputeError::InvalidPayload(format!("shuffle-map payload encode: {e}"))
                    })?,
                };

                let dep_preceding = spec.preceding_ops.clone();
                let base_ops = if !dep_preceding.is_empty() {
                    dep_preceding
                } else {
                    preceding_ops.clone()
                };
                let mut ops = base_ops;
                ops.push(shuffle_op);

                let shuffle_id = spec.shuffle_id;
                let num_output_partitions = spec.num_output_partitions;
                env::Env::run_in_async_rt(|| {
                    futures::executor::block_on(sched.run_shuffle_map_stage(
                        shuffle_id,
                        ops,
                        parent_partitions,
                    ))
                })?;

                log::info!(
                    "shuffle map stage complete: shuffle_id={shuffle_id} \
                     num_reduce_partitions={num_output_partitions}",
                );
            }
        }

        Ok(())
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
            && let Some((partitions, ops)) = rdd.extract_staged_pipeline()
        {
            let raw = self.dispatch_pipeline(partitions, ops)?;
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
