//! Pipeline execution dispatches to [`LocalExecutor`] (thread pool) or
//! [`DistributedExecutor`] (remote workers) through the shared
//! [`PipelineExecutor`] trait. Callers use [`Context::dispatch_pipeline`].

use std::sync::Arc;

use atomic_data::distributed::{ResultStatus, Step, TaskEnvelope};
use atomic_scheduler::DistributedScheduler;

use crate::env;
use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, ComputeEngine};

use super::AccumulatorStore;
use super::broadcast::merge_deltas_into;

/// Run a pipeline of steps over pre-encoded partition bytes, returning per-partition output.
///
/// This is the one execution entry point the runtime routes to. The local runtime runs each
/// partition on a thread; the distributed runtime ships each partition to a worker.
pub(crate) trait PipelineExecutor: Send + Sync {
    /// Run a pipeline of steps over pre-encoded partition bytes, returning per-partition output.
    ///
    /// Executors are responsible for merging any accumulator deltas produced by tasks
    /// (see [`TaskResultEnvelope::accumulator_deltas`]); the caller receives only the
    /// per-partition result bytes.
    fn run_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        steps: Vec<Step>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> ComputeResult<Vec<Vec<u8>>>;
}

/// Local runtime executor: each partition's pipeline runs on its own Tokio blocking
/// thread, with concurrency capped to `num_cpus::get()` to avoid over-subscribing the
/// system when the source has many partitions.
pub(crate) struct LocalExecutor {
    pub(crate) accumulator_store: AccumulatorStore,
}

impl PipelineExecutor for LocalExecutor {
    fn run_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        steps: Vec<Step>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let store = self.accumulator_store.clone();
        env::Env::run_in_async_rt(|| {
            let engine = Arc::new(ComputeEngine::default());
            let steps = Arc::new(steps);
            let broadcasts = Arc::new(broadcasts);
            futures::executor::block_on(async {
                let sem = Arc::new(tokio::sync::Semaphore::new(num_cpus::get()));
                let mut handles = Vec::with_capacity(source_partitions.len());
                for (part_id, data) in source_partitions.into_iter().enumerate() {
                    let engine = engine.clone();
                    let steps = steps.clone();
                    let broadcasts = broadcasts.clone();
                    let permit = sem
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore not closed");
                    handles.push(tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        let task = TaskEnvelope::new(
                            0,
                            0,
                            part_id,
                            0,
                            part_id,
                            format!("local-pipeline-{part_id}"),
                            (*steps).clone(),
                            data,
                        )
                        .with_broadcasts((*broadcasts).clone());
                        engine.execute("local-driver", &task)
                    }));
                }
                // Join in partition order; accumulator deltas merge on the driver.
                let mut out = Vec::with_capacity(handles.len());
                for h in handles {
                    let result = h.await.map_err(|e| {
                        ComputeError::InvalidPayload(format!("local task thread panicked: {e}"))
                    })??;
                    match result.status {
                        ResultStatus::Success => {
                            if !result.accumulator_deltas.is_empty() {
                                merge_deltas_into(&store, &result.accumulator_deltas);
                            }
                            out.push(result.data);
                        }
                        _ => {
                            return Err(ComputeError::InvalidPayload(
                                result.error.unwrap_or_else(|| "task failed".to_string()),
                            ));
                        }
                    }
                }
                Ok(out)
            })
        })
    }
}

/// Distributed runtime executor: each partition's pipeline is shipped to a remote worker via
/// the `DistributedScheduler` (which owns worker dispatch, speculation, and fault tolerance).
pub(crate) struct DistributedExecutor {
    pub(crate) scheduler: Arc<DistributedScheduler>,
}

impl PipelineExecutor for DistributedExecutor {
    fn run_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        steps: Vec<Step>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let sched = self.scheduler.clone();
        Ok(env::Env::run_in_async_rt(|| {
            futures::executor::block_on(sched.run_native_job_with_broadcasts(
                steps,
                source_partitions,
                broadcasts,
            ))
        })?)
    }
}
