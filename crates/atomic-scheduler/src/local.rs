use atomic_data::data::Data;
use atomic_data::dependency::ShuffleDependencyBox;
use atomic_data::partial::result::PartialResult;
use atomic_data::partial::{ApproxListener, ApproximateEvaluator};
use atomic_data::rdd::Rdd;
use atomic_data::task::{TaskOption, TaskResult};
use atomic_data::task_context::TaskContext;
use std::clone::Clone;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::option::Option;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::base::{Mutators, NativeScheduler};
use crate::dag::{CompletionEvent, TastEndReason};
use crate::error::{LibResult, SchedulerError};
use crate::job::JobTracker;
use crate::listener::{
    JobEndListener, JobListener, JobStartListener, LiveListenerBus, NoOpListener,
};
use crate::stage::Stage;

#[derive(Clone, Default)]
pub struct LocalScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    /// Shared mutable state: stage cache, event queues, map-output tracker, and ID counters.
    mutators: Mutators,
    master: bool,
    scheduler_lock: Arc<Mutex<()>>,
    live_listener_bus: LiveListenerBus,
}

impl LocalScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        LocalScheduler {
            mutators: Mutators::new(),
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 50,
            master,
            scheduler_lock: Arc::new(Mutex::new(())),
            live_listener_bus,
        }
    }

    /// Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    /// as they arrive. Returns a partial result object from the evaluator.
    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> LibResult<PartialResult<R>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let selfc = self.clone();
        let _lock = selfc.scheduler_lock.lock();

        // Run async code directly - tokio runtime should already be available
        futures::executor::block_on(async move {
            let partitions: Vec<_> = (0..final_rdd.number_of_splits()).collect();
            let listener = ApproxListener::new(evaluator, timeout, partitions.len());
            let jt =
                JobTracker::from_scheduler(&*self, func, final_rdd.clone(), partitions, listener)
                    .await
                    .map_err(|_| SchedulerError::Other)?;
            if final_rdd.number_of_splits() == 0 {
                // Return immediately if the job is running 0 tasks
                let time = Instant::now();
                self.live_listener_bus.post(Box::new(JobStartListener {
                    job_id: jt.run_id,
                    time,
                    stage_infos: vec![],
                }));
                self.live_listener_bus.post(Box::new(JobEndListener {
                    job_id: jt.run_id,
                    time,
                    job_result: true,
                }));
                let res =
                    PartialResult::new(jt.listener.evaluator.lock().await.current_result(), true);
                return Ok(res);
            }
            tokio::spawn(self.event_process_loop(false, jt.clone()));
            jt.listener
                .get_result()
                .await
                .map_err(|s| SchedulerError::PartialJobError(s))
        })
    }

    pub fn run_job<T: Data, U: Data + Clone, F>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let selfc = self.clone();
        let _lock = selfc.scheduler_lock.lock();

        // Run async code directly - tokio runtime should already be available
        futures::executor::block_on(async move {
            let jt = JobTracker::from_scheduler(
                &*self,
                func,
                final_rdd.clone(),
                partitions,
                NoOpListener,
            )
            .await?;
            self.event_process_loop(allow_local, jt).await
        })
    }

    /// Start the event processing loop for a given job.
    async fn event_process_loop<T: Data, U: Data + Clone, F, L>(
        self: Arc<Self>,
        allow_local: bool,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        L: JobListener,
    {
        // TODO: update cache

        if allow_local {
            if let Some(result) = LocalScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        self.mutators
            .event_queues
            .insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);
        // Per-task failure counter: key = (stage_id, task_id).
        // Prevents a repeatedly-failing task from being retried indefinitely.
        let mut task_failure_counts: HashMap<(usize, usize), usize> = HashMap::new();

        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?;
        log::debug!(
            "pending stages and tasks: {:?}",
            jt.pending_tasks
                .lock()
                .await
                .iter()
                .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        );

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout);
            let start = Instant::now();

            if let Some(evt) = event_option {
                log::debug!("event starting");
                let stage = self
                    .mutators
                    .stage_cache
                    .get(&evt.task.get_stage_id())
                    .unwrap()
                    .clone();
                log::debug!(
                    "removing stage #{} task from pending task #{}",
                    stage.id,
                    evt.task.get_task_id()
                );
                jt.pending_tasks
                    .lock()
                    .await
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use super::dag::TastEndReason::*;
                match evt.reason {
                    Success => {
                        self.on_event_success(evt, &mut results, &mut num_finished, jt.clone())
                            .await?;
                    }
                    FetchFailed(failed_vals) => {
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id())
                            .await;
                        fetch_failure_duration = start.elapsed();
                    }
                    Error(error) => {
                        let key = (evt.task.get_stage_id(), evt.task.get_task_id());
                        let count = task_failure_counts.entry(key).or_insert(0);
                        *count += 1;
                        if *count >= self.max_failures {
                            return Err(SchedulerError::MaxTaskFailures(error.to_string()));
                        }
                        log::warn!(
                            "task {}/{} error (attempt {}/{}): retrying stage",
                            evt.task.get_stage_id(),
                            evt.task.get_task_id(),
                            count,
                            self.max_failures,
                        );
                        let m = self.get_mutators();
                        jt.failed
                            .lock()
                            .await
                            .insert(m.fetch_from_stage_cache(evt.task.get_stage_id()));
                        fetch_failure_duration = start.elapsed();
                    }
                    OtherFailure(msg) => {
                        let key = (evt.task.get_stage_id(), evt.task.get_task_id());
                        let count = task_failure_counts.entry(key).or_insert(0);
                        *count += 1;
                        if *count >= self.max_failures {
                            return Err(SchedulerError::MaxTaskFailures(msg));
                        }
                        log::warn!(
                            "task {}/{} failure (attempt {}/{}): retrying stage",
                            evt.task.get_stage_id(),
                            evt.task.get_task_id(),
                            count,
                            self.max_failures,
                        );
                        let m = self.get_mutators();
                        jt.failed
                            .lock()
                            .await
                            .insert(m.fetch_from_stage_cache(evt.task.get_stage_id()));
                        fetch_failure_duration = start.elapsed();
                    }
                }
            }
        }

        if !jt.failed.lock().await.is_empty()
            && fetch_failure_duration.as_millis() > self.resubmit_timeout
        {
            self.update_cache_locs().await?;
            for stage in jt.failed.lock().await.iter() {
                self.submit_stage(stage.clone(), jt.clone()).await?;
            }
            jt.failed.lock().await.clear();
        }

        self.mutators.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    fn run_task<T: Data, U: Data, F>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        _id_in_job: usize,
        attempt_id: usize,
    ) where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let result = task.run(attempt_id);
        match result {
            Ok(task_result) => {
                // Extract Box<dyn Data> from TaskResult enum
                let result_data = match task_result {
                    TaskResult::ResultTask(data) => data,
                    TaskResult::ShuffleTask(data) => data,
                };
                LocalScheduler::handle_completion_event(
                    event_queues,
                    task,
                    TastEndReason::Success,
                    result_data,
                );
            }
            Err(err) => {
                // Convert error to String to make it Send + Sync
                let err_msg = format!("Task execution failed: {}", err);
                LocalScheduler::handle_completion_event(
                    event_queues,
                    task,
                    TastEndReason::OtherFailure(err_msg),
                    Box::new(()) as Box<dyn Data>, // Placeholder for error case
                );
            }
        }
    }

    fn handle_completion_event(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        reason: TastEndReason,
        result: Box<dyn Data>,
        // TODO: accumvalues needs to be done
    ) {
        let result = Some(result);
        let run_id = match &task {
            TaskOption::ResultTask(rt) => rt.run_id,
            TaskOption::ShuffleMapTask(smt) => smt.run_id,
        };
        if let Some(mut queue) = event_queues.get_mut(&run_id) {
            queue.push_back(CompletionEvent {
                task,
                reason,
                result,
                accum_updates: HashMap::new(),
            });
        } else {
            log::debug!("ignoring completion event for DAG Job");
        }
    }
}

#[async_trait::async_trait]
impl NativeScheduler for LocalScheduler {
    fn get_mutators(&self) -> Mutators {
        self.mutators.clone()
    }

    /// Every single task is run in the local thread pool
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        _server_address: SocketAddrV4,
    ) where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::debug!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.mutators.event_queues.clone();

        // No need to serialize for local execution — runs on a Tokio blocking thread
        let task_id = match &task {
            TaskOption::ResultTask(t) => t.task_id,
            TaskOption::ShuffleMapTask(t) => t.task_id,
        };
        log::info!(
            "[local-worker] spawning task_id={} attempt={} on blocking thread",
            task_id,
            my_attempt_id,
        );
        tokio::task::spawn_blocking(move || {
            log::info!(
                "[local-worker] executing task_id={} thread={:?}",
                task_id,
                std::thread::current().id(),
            );
            LocalScheduler::run_task::<T, U, F>(event_queues, task, id_in_job, my_attempt_id)
        });
    }

    fn next_executor_server(&self, _task: &TaskOption) -> SocketAddrV4 {
        // Just point to the localhost
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)
    }

    async fn update_cache_locs(&self) -> LibResult<()> {
        // For local scheduler, cache locations are managed locally
        // No external cache tracker needed - data is in same process
        // This is just a no-op to satisfy the trait
        Ok(())
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<ShuffleDependencyBox>) -> LibResult<Stage> {
        log::debug!("getting shuffle map stage");
        let stage_id = self
            .mutators
            .shuffle_to_map_stage
            .get(&shuf.get_shuffle_id())
            .map(|s| s.id);
        match stage_id {
            Some(id) => {
                // Return the up-to-date copy from stage_cache — shuffle_to_map_stage holds a
                // stale clone that never sees add_output_loc_to_stage updates.
                Ok(self.mutators.stage_cache.get(&id).unwrap().clone())
            }
            None => {
                log::debug!("started creating shuffle map stage before");
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.mutators
                    .shuffle_to_map_stage
                    .insert(shuf.get_shuffle_id(), stage.clone());
                log::debug!("finished inserting newly created shuffle stage");
                Ok(stage)
            }
        }
    }

    async fn get_missing_parent_stages(&'_ self, stage: Stage) -> LibResult<Vec<Stage>> {
        log::debug!("getting missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: HashSet<usize> = HashSet::new();
        self.visit_missing_parent(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}

// Implement JobListener for ApproxListener
#[async_trait::async_trait]
impl<U, R, E> JobListener for ApproxListener<U, R, E>
where
    U: Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
    E: ApproximateEvaluator<U, R> + Send + Sync,
{
    async fn task_succeeded(&self, _index: usize, result: &dyn Data) -> LibResult<()> {
        // Downcast the result to U and merge it into the evaluator
        if let Some(typed_result) = result.as_any().downcast_ref::<U>() {
            let mut eval = self.evaluator.lock().await;
            eval.merge(_index, typed_result);
        }
        Ok(())
    }
}
