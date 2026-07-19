//! The unified pipeline-execution API the scheduler exposes to upstream.
//!
//! [`Context::dispatch_pipeline`](super::Context) builds a [`PipelineExecutor`] from the
//! active runtime and calls [`run_pipeline`](PipelineExecutor::run_pipeline). Upstream code
//! is identical for both runtimes; only the executor differs — a local thread pool
//! ([`LocalExecutor`]) or remote workers ([`DistributedExecutor`]).

use std::sync::Arc;

use atomic_data::distributed::{PipelineOp, ResultStatus, TaskEnvelope};
use atomic_scheduler::DistributedScheduler;

use crate::env;
use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, ComputeEngine};

use super::AccumulatorStore;
use super::broadcast::merge_deltas_into;

/// Run a pipeline of ops over pre-encoded partition bytes, returning per-partition output.
///
/// This is the one execution entry point the runtime routes to. The local runtime runs each
/// partition on a thread; the distributed runtime ships each partition to a worker.
pub(crate) trait PipelineExecutor: Send + Sync {
    fn run_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        ops: Vec<PipelineOp>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> ComputeResult<Vec<Vec<u8>>>;
}

/// Local runtime executor: each partition's pipeline runs on its own Tokio blocking thread.
/// Mirrors the distributed one-task-per-worker model, with a thread as the executor.
pub(crate) struct LocalExecutor {
    pub(crate) accumulator_store: AccumulatorStore,
}

impl PipelineExecutor for LocalExecutor {
    fn run_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        ops: Vec<PipelineOp>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let store = self.accumulator_store.clone();
        env::Env::run_in_async_rt(|| {
            let engine = Arc::new(ComputeEngine::default());
            let ops = Arc::new(ops);
            let broadcasts = Arc::new(broadcasts);
            futures::executor::block_on(async {
                let mut handles = Vec::with_capacity(source_partitions.len());
                for (part_id, data) in source_partitions.into_iter().enumerate() {
                    let engine = engine.clone();
                    let ops = ops.clone();
                    let broadcasts = broadcasts.clone();
                    handles.push(tokio::task::spawn_blocking(move || {
                        let task = TaskEnvelope::new(
                            0,
                            0,
                            part_id,
                            0,
                            part_id,
                            format!("local-pipeline-{part_id}"),
                            (*ops).clone(),
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
        ops: Vec<PipelineOp>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let sched = self.scheduler.clone();
        Ok(env::Env::run_in_async_rt(|| {
            futures::executor::block_on(sched.run_native_job_with_broadcasts(
                ops,
                source_partitions,
                broadcasts,
            ))
        })?)
    }
}
