use atomic_data::{
    dependency::Dependency,
    distributed::{EngineStep, ShuffleMapPayload, Step, StepKind, TaskEnvelope, TaskRuntime},
    rdd::RddBase,
};
use futures::future::try_join_all;
use std::sync::Arc;

use crate::{
    error::{LibResult, SchedulerError},
    planner::StagePlanner,
};

/// One shuffle stage dispatched by `run_pending_shuffle_stages`.
///
/// The compute context stores this for fetch-failure recovery (re-run one lost map partition).
#[derive(Clone)]
pub struct ActiveShuffleStage {
    pub shuffle_id: usize,
    pub dep: Arc<atomic_data::dependency::ShuffleDependency>,
    pub steps: Vec<Step>,
}

use super::DistributedScheduler;

impl DistributedScheduler {
    /// Build the exact op pipeline for one distributed shuffle-map stage:
    /// any preceding staged steps followed by the `ShuffleMap` engine step.
    fn shuffle_map_ops(
        dep: &atomic_data::dependency::ShuffleDependency,
        fallback_preceding: &[Step],
    ) -> LibResult<Vec<Step>> {
        let payload = bincode::encode_to_vec(
            ShuffleMapPayload {
                type_id: dep.type_id.to_string(),
                partitioner_spec: dep.partitioner_spec(),
            },
            bincode::config::standard(),
        )
        .map_err(|e| SchedulerError::TaskFailed(format!("shuffle-map payload encode: {e}")))?;

        let mut steps = if dep.preceding_steps.is_empty() {
            fallback_preceding.to_vec()
        } else {
            dep.preceding_steps.clone()
        };
        steps.push(Step {
            task_name: format!("shuffle-map-{}", dep.get_shuffle_id()),
            kind: StepKind::Engine(EngineStep::ShuffleMap {
                shuffle_id: dep.get_shuffle_id(),
                num_output_partitions: dep.get_num_output_partitions(),
            }),
            runtime: TaskRuntime::Native,
            payload,
        });
        Ok(steps)
    }

    /// Traverse the RDD dependencies and run every pending shuffle-map stage on workers.
    ///
    /// Returns one descriptor per dispatched shuffle stage so callers can persist recovery
    /// metadata (`dep + steps`) alongside scheduler state.
    pub async fn run_pending_shuffle_stages(
        &self,
        rdd: &Arc<dyn RddBase>,
        preceding_steps: Vec<Step>,
    ) -> LibResult<Vec<ActiveShuffleStage>> {
        let mut dispatched = Vec::new();
        for dep in rdd.get_dependencies() {
            if let Dependency::Shuffle(shuffle_dep) = dep {
                let steps = Self::shuffle_map_ops(&shuffle_dep, &preceding_steps)?;
                let parent_partitions = shuffle_dep.encode_partitions().map_err(|e| {
                    SchedulerError::TaskFailed(format!("shuffle input encode: {e}"))
                })?;
                let shuffle_id = shuffle_dep.get_shuffle_id();
                self.run_shuffle_map_stage(shuffle_id, steps.clone(), parent_partitions)
                    .await?;
                dispatched.push(ActiveShuffleStage {
                    shuffle_id,
                    dep: shuffle_dep,
                    steps,
                });
            }
        }
        Ok(dispatched)
    }

    /// Run the shuffle-map phase of a shuffle stage in distributed mode.
    ///
    /// Dispatches one `TaskEnvelope` per input partition; each worker stores its output
    /// buckets in its local `ShuffleCache` and serves them via its `ShuffleManager` HTTP
    /// server. The worker returns its server URI in `TaskResultEnvelope::shuffle_server_uri`.
    /// This method registers all URIs with the driver's `MapOutputTracker` so the reduce
    /// phase can locate and fetch the right buckets from each worker.
    ///
    /// **Fault recovery**: on stage-level failure, stale URIs are cleared from
    /// `MapOutputTracker` and the entire map stage is re-submitted (up to `max_failures` times).
    pub async fn run_shuffle_map_stage(
        &self,
        shuffle_id: usize,
        steps: Vec<Step>,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<()> {
        let max_retries = self.max_failures;
        super::retry::retry_with_backoff(max_retries, 200, || {
            let steps = steps.clone();
            let partitions = partitions.clone();
            async move {
                let result = self
                    .run_shuffle_map_stage_inner(shuffle_id, steps, partitions)
                    .await;
                if let Err(ref e) = result {
                    // Clear stale map output URIs so the reduce phase doesn't try
                    // to fetch from the failed workers on the next attempt.
                    if let Some(tracker) = atomic_data::env::get_map_output_tracker() {
                        tracker.unregister_shuffle(shuffle_id);
                    }
                    log::warn!(
                        "shuffle-map stage for shuffle_id={shuffle_id} failed: {e}; \
                         cleared MapOutputTracker, retrying"
                    );
                }
                result
            }
        })
        .await
    }

    /// Dispatch one shuffle-map `TaskEnvelope`, retrying `RetryableFailure` results
    /// with backoff, and return the worker's shuffle-server URI on success.
    async fn submit_shuffle_map_task(&self, task: &TaskEnvelope) -> LibResult<Option<String>> {
        let mut retry_count = 0usize;
        loop {
            let (result, _worker) = self.submit_native_task(task, None).await?;
            match result.status {
                atomic_data::distributed::ResultStatus::FatalFailure
                | atomic_data::distributed::ResultStatus::CacheMiss => {
                    return Err(SchedulerError::TaskFailed(
                        result
                            .error
                            .unwrap_or_else(|| "shuffle map fatal failure".to_string()),
                    ));
                }
                atomic_data::distributed::ResultStatus::RetryableFailure => {
                    if retry_count < self.max_failures {
                        retry_count += 1;
                        let delay =
                            std::time::Duration::from_millis(200 * (1u64 << retry_count).min(16));
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(SchedulerError::TaskFailed(result.error.unwrap_or_else(
                        || "shuffle map retryable failure exhausted".to_string(),
                    )));
                }
                atomic_data::distributed::ResultStatus::Success => {
                    self.merge_accumulator_deltas(&result.accumulator_deltas);
                    return Ok(result.shuffle_server_uri);
                }
            }
        }
    }

    /// Recompute one lost shuffle-map partition on a live worker and re-register
    /// its fresh URI in the `MapOutputTracker`.
    ///
    /// Called from the driver's fetch-failure recovery hook when the worker that
    /// held `map_id`'s output died between the map and reduce phases. `steps` and
    /// `partition` are the same pipeline and input bytes the original map task ran.
    pub async fn rerun_shuffle_map_partition(
        &self,
        shuffle_id: usize,
        map_id: usize,
        steps: Vec<Step>,
        partition: Vec<u8>,
    ) -> LibResult<()> {
        use std::sync::atomic::Ordering;

        let m = self.state();
        let stage_id = m.get_next_stage_id();
        let task_id = m.get_next_task_id();
        let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let trace_id = format!("shuffle-map-recovery-{shuffle_id}-{map_id}");
        let task = TaskEnvelope::new(
            0, stage_id, task_id, attempt_id, map_id, trace_id, steps, partition,
        );

        let uri = self.submit_shuffle_map_task(&task).await?.ok_or_else(|| {
            SchedulerError::TaskFailed(format!(
                "shuffle-map recovery for shuffle {shuffle_id} map {map_id} \
                 returned no shuffle URI"
            ))
        })?;
        if !m.register_map_output(shuffle_id, map_id, uri.clone()) {
            return Err(SchedulerError::TaskFailed(format!(
                "shuffle-map recovery for shuffle {shuffle_id} map {map_id}: \
                 MapOutputTracker rejected URI {uri}"
            )));
        }
        log::info!(
            "shuffle-map recovery: shuffle {shuffle_id} map {map_id} recomputed, \
             now served from {uri}"
        );
        Ok(())
    }

    async fn run_shuffle_map_stage_inner(
        &self,
        shuffle_id: usize,
        steps: Vec<Step>,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<()> {
        use std::sync::atomic::Ordering;

        let num_partitions = partitions.len();
        let m = self.state();
        let stage_id = {
            let _lock = self.scheduler_lock.lock();
            m.register_shuffle(shuffle_id, num_partitions);
            m.get_next_stage_id()
        };

        let submits = partitions.into_iter().enumerate().map(|(part_id, data)| {
            let task_id = m.get_next_task_id();
            let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
            let trace_id = format!("shuffle-map-{shuffle_id}-{part_id}");
            let task = TaskEnvelope::new(
                0,
                stage_id,
                task_id,
                attempt_id,
                part_id,
                trace_id,
                steps.clone(),
                data,
            );
            async move {
                let uri = self.submit_shuffle_map_task(&task).await?;
                Ok::<_, SchedulerError>((part_id, uri))
            }
        });

        let responses = try_join_all(submits).await?;

        let mut locs: Vec<Option<String>> = vec![None; num_partitions];
        for (part_id, uri_opt) in responses {
            if let Some(uri) = uri_opt {
                locs[part_id] = Some(uri);
            } else {
                log::warn!(
                    "shuffle-map stage {stage_id}: partition {part_id} returned no shuffle URI"
                );
            }
        }
        m.register_map_outputs(shuffle_id, locs);
        Ok(())
    }
}
