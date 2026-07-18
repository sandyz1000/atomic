use atomic_data::distributed::{PipelineOp, TaskEnvelope};
use futures::future::try_join_all;

use crate::{
    error::{LibResult, SchedulerError},
    planner::StagePlanner,
};

use super::DistributedScheduler;

impl DistributedScheduler {
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
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<()> {
        let max_retries = self.max_failures;
        super::retry::retry_with_backoff(max_retries, 200, || {
            let ops = ops.clone();
            let partitions = partitions.clone();
            async move {
                let result = self
                    .run_shuffle_map_stage_inner(shuffle_id, ops, partitions)
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
    /// held `map_id`'s output died between the map and reduce phases. `ops` and
    /// `partition` are the same pipeline and input bytes the original map task ran.
    pub async fn rerun_shuffle_map_partition(
        &self,
        shuffle_id: usize,
        map_id: usize,
        ops: Vec<PipelineOp>,
        partition: Vec<u8>,
    ) -> LibResult<()> {
        use std::sync::atomic::Ordering;

        let m = self.state();
        let stage_id = m.get_next_stage_id();
        let task_id = m.get_next_task_id();
        let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let trace_id = format!("shuffle-map-recovery-{shuffle_id}-{map_id}");
        let task = TaskEnvelope::new(
            0, stage_id, task_id, attempt_id, map_id, trace_id, ops, partition,
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
        ops: Vec<PipelineOp>,
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
                ops.clone(),
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
