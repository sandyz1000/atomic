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
use ember_data::partial::result::PartialResult;
use ember_data::partial::{ApproximateActionListener, ApproximateEvaluator};
use ember_data::task_context::TaskContext;
use ember_data::data::Data;
use ember_data::dependency::ShuffleDependencyBox;
use ember_data::rdd::Rdd;
use ember_data::task::{TaskOption, TaskResult};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::base::{EventQueue, MutatorsAndGetter, NativeScheduler};
use crate::dag::{CompletionEvent, TastEndReason};
use crate::error::{LibResult, SchedulerError};
use crate::job::{Job, JobTracker};
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
    event_queues: EventQueue,
    mutators: MutatorsAndGetter,
    pub next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<DashMap<usize, Stage>>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    master: bool,
    framework_name: String,
    is_registered: bool, // TODO: check if it is necessary
    active_jobs: HashMap<usize, Job>,
    active_job_queue: Vec<Job>,
    taskid_to_jobid: HashMap<String, usize>,
    taskid_to_slaveid: HashMap<String, String>,
    job_tasks: HashMap<usize, HashSet<String>>,
    slaves_with_executors: HashSet<String>,
    // map_output_tracker: Arc<dyn MapOutputTracker>,
    // TODO: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<()>>,
    live_listener_bus: LiveListenerBus,
}

impl LocalScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        let mutators = MutatorsAndGetter::new();
        LocalScheduler {
            mutators,
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 50,
            event_queues: Arc::new(DashMap::new()),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_run_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            stage_cache: Arc::new(DashMap::new()),
            shuffle_to_map_stage: Arc::new(DashMap::new()),
            cache_locs: Arc::new(DashMap::new()),
            master,
            framework_name: "spark".to_string(),
            is_registered: true, // TODO: check if it is necessary
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            // map_output_tracker: env::Env::get().map_output_tracker.clone(),
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
            let listener = ApproximateActionListener::new(evaluator, timeout, partitions.len());
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
                let res = PartialResult::new(
                    jt.listener.evaluator.lock().await.current_result(),
                    true,
                );
                return Ok(res);
            }
            tokio::spawn(self.event_process_loop(false, jt.clone()));
            jt.listener.get_result().await.map_err(|s| SchedulerError::PartialJobError(s))
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

        self.event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);

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
                    Error(error) => panic!("{}", error),
                    OtherFailure(msg) => panic!("{}", msg),
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

        self.event_queues.remove(&jt.run_id);
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
    fn get_mutators(&self) -> MutatorsAndGetter {
        todo!()
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
        let event_queues = self.event_queues.clone();

        // No need to serialize for local execution
        tokio::task::spawn_blocking(move || {
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
        let stage = self.shuffle_to_map_stage.get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage.clone()),
            None => {
                log::debug!("started creating shuffle map stage before");
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.shuffle_to_map_stage
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
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}

// Implement JobListener for ApproximateActionListener
#[async_trait::async_trait]
impl<U, R, E> JobListener for ApproximateActionListener<U, R, E>
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



// #[async_trait::async_trait]
// impl<E, U, R> JobListener for ApproximateActionListener<U, R, E>
// where
//     E: ApproximateEvaluator<U, R> + Send + Sync,
//     R: Clone + Debug + Send + Sync + 'static,
//     U: Send + Sync + 'static,
// {
//     async fn task_succeeded(&self, index: usize, result: &dyn Data) -> Result<()> {
//         let result = result.as_any().downcast_ref::<U>().ok_or_else(|| {
//             PartialJobError::DowncastFailure(
//                 "failed converting to generic type param @ ApproximateActionListener",
//             )
//         })?;
//         self.evaluator.lock().await.merge(index, result);
//         let current_finished = self.finished_tasks.fetch_add(1, Ordering::SeqCst) + 1;
//         if current_finished == self.total_tasks {
//             // If we had already returned a PartialResult, set its final value
//             if let Some(ref mut value) = *self.result_object.lock().await {
//                 value.set_final_value(self.evaluator.lock().await.current_result())?;
//             }
//         }
//         Ok(())
//     }

//     async fn job_failed(&self, err: Error) {
//         let mut failure = self.failure.lock().await;
//         *failure = Some(err);
//     }
// }
