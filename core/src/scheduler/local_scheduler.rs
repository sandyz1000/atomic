use std::clone::Clone;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use async_trait::async_trait;
use crate::dependency::ShuffleDependencyTrait;
use crate::map_output_tracker::MapOutputTracker;
use crate::rdd::rdd::{Rdd, RddBase};
use crate::scheduler::{
    CompletionEvent, 
    EventQueue, 
    Job, 
    LiveListenerBus, 
    NativeScheduler,
    ResultTask, 
    Stage, 
    TaskBase, 
    TaskContext, 
    TaskOption, 
    TaskResult, 
    TastEndReason,
};
use crate::ser_data::{AnyData, Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use crate::{env, Result};
use dashmap::DashMap;
use parking_lot::Mutex;
use super::live_listener_bus::AsyncEventQueue;
use super::task::TaskBox;

#[derive(Clone, Default)]
pub(crate) struct LocalScheduler<S, RDD, AeQ> 
where 
    S: ShuffleDependencyTrait,
    RDD: RddBase,
    AeQ: AsyncEventQueue
{
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: EventQueue,
    pub(crate) next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    stage_cache: Arc<DashMap<usize, Stage<S, RDD>>>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage<S, RDD>>>,
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
    map_output_tracker: MapOutputTracker,
    // TODO: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<()>>,
    live_listener_bus: LiveListenerBus<AeQ>,
}

impl<S, RDD, AeQ> LocalScheduler<S, RDD, AeQ> 
where 
    S: ShuffleDependencyTrait,
    RDD: RddBase,
    AeQ: AsyncEventQueue
{
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        LocalScheduler {
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
            framework_name: "atomic".to_string(),
            is_registered: true, // TODO: check if it is necessary
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
            scheduler_lock: Arc::new(Mutex::new(())),
            live_listener_bus,
        }
    }

    fn run_task<T: Data, U: Data, F>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Vec<u8>,
        _id_in_job: usize,
        attempt_id: usize,
    ) where
        F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output = U>,
    {
        let des_task: TaskOption = bincode::deserialize(&task).unwrap();
        let result = des_task.run(attempt_id);
        match des_task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    LocalScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_box(),
                    );
                }
            }
            TaskOption::ShuffleMapTask(tsk) => {
                let result = match result {
                    TaskResult::ShuffleTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ShuffleMapTask>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    LocalScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        result.into_box(),
                    );
                }
            }
        };
    }

    fn task_ended(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: Box<dyn TaskBase>,
        reason: TastEndReason,
        result: Box<dyn AnyData>,
        // TODO: accumvalues needs to be done
    ) {
        let result = Some(result);
        if let Some(mut queue) = event_queues.get_mut(&(task.get_run_id())) {
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

#[async_trait]
impl<S, RDD, AeQ> NativeScheduler for LocalScheduler<S, RDD, AeQ> 
where 
    S: ShuffleDependencyTrait,
    RDD: RddBase,
    AeQ: AsyncEventQueue
{
    type RDD = RDD;
    type SDT = S;

    #[inline]
    fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>> {
        &self.event_queues
    }

    #[inline]
    fn get_next_job_id(&self) -> usize {
        self.next_job_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn get_next_stage_id(&self) -> usize {
        self.next_stage_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn get_next_task_id(&self) -> usize {
        self.next_task_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Every single task is run in the local thread pool
    fn submit_task<T: Data, U: Data, F, Tb: TaskBox>(
        &self,
        task: TaskOption<Tb>,
        id_in_job: usize,
        _server_address: SocketAddrV4,
    ) where
        F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output = U>,
    {
        log::debug!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.event_queues.clone();
        let task = bincode::serialize(&task).unwrap();

        tokio::task::spawn_blocking(move || {
            LocalScheduler::run_task::<T, U, F>(event_queues, task, id_in_job, my_attempt_id)
        });
    }

    fn next_executor_server<Tb: TaskBase>(&self, _rdd: &Tb) -> SocketAddrV4 {
        // Just point to the localhost
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)
    }

    async fn update_cache_locs(&self) -> Result<()> {
        self.cache_locs.clear();
        env::Env::get()
            .cache_tracker
            .get_location_snapshot()
            .await?
            .into_iter()
            .for_each(|(k, v)| {
                self.cache_locs.insert(k, v);
            });
        Ok(())
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<Self::SDT>) -> Result<Stage<Self::SDT, Self::RDD>> {
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

    async fn get_missing_parent_stages(&'_ self, stage: Stage<Self::SDT, Self::RDD>) -> Result<Vec<Stage<Self::SDT, Self::RDD>>> {
        log::debug!("getting missing parent stages");
        let mut missing = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }
    
}

impl<S, RDD, AeQ> Drop for LocalScheduler<S, RDD, AeQ> 
where 
    S: ShuffleDependencyTrait,
    RDD: RddBase,
    AeQ: AsyncEventQueue
{
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}
