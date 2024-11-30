use std::collections::{btree_set::BTreeSet, vec_deque::VecDeque, HashMap, HashSet};
use std::iter::FromIterator;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::dependency::ShuffleDependencyTrait;
use crate::env;
use crate::error::{Error, NetworkError, Result};
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
// use capnp::message::ReaderOptions;
// use crate::serialized_data_capnp::serialized_data;
// use capnp_futures::serialize as capnp_serialize;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
//     traversal_limit_in_words: std::u64::MAX,
//     nesting_limit: 64,
// };

// Just for now, creating an entire scheduler functions without dag scheduler trait.
// Later change it to extend from dag scheduler.
#[derive(Clone, Default)]
pub(crate) struct DistributedScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    event_queues: EventQueue,
    next_job_id: Arc<AtomicUsize>,
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
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,
    port: u16,
    map_output_tracker: MapOutputTracker,
    // TODO: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<bool>>,
    live_listener_bus: LiveListenerBus,
}

impl DistributedScheduler {
    pub fn new(
        max_failures: usize,
        master: bool,
        servers: Option<Vec<SocketAddrV4>>,
        port: u16,
    ) -> Self {
        log::debug!(
            "starting distributed scheduler @ port {} (in master mode: {})",
            port,
            master,
        );
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        DistributedScheduler {
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
            server_uris: if let Some(servers) = servers {
                Arc::new(Mutex::new(VecDeque::from_iter(servers)))
            } else {
                Arc::new(Mutex::new(VecDeque::new()))
            },
            port,
            map_output_tracker: env::Env::get().map_output_tracker.clone(),
            scheduler_lock: Arc::new(Mutex::new(true)),
            live_listener_bus,
        }
    }

    async fn receive_results<T: Data, U: Data, F, R>(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        receiver: R,
        task: TaskOption,
        target_port: u16,
    ) where
        F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output = U>,
        R: futures::AsyncRead + std::marker::Unpin,
    {
        let result: TaskResult = {
            // let message = capnp_futures::serialize::read_message(receiver, CAPNP_BUF_READ_OPTS)
            //     .await
            //     .unwrap()
            //     .ok_or_else(|| NetworkError::NoMessageReceived)
            //     .unwrap();
            // let task_data = message.get_root::<serialized_data::Reader>().unwrap();
            let task_data: String = String::new();
            log::debug!(
                "received task #{} result of {} bytes from executor @{}",
                task.get_task_id(),
                task_data,
                target_port
            );
            // bincode::deserialize(&task_data.get_msg().unwrap()).unwrap()
            bincode::deserialize(&task_data).unwrap()
        };

        match task {
            TaskOption::ResultTask(tsk) => {
                let result = match result {
                    TaskResult::ResultTask(r) => r,
                    _ => panic!("wrong result type"),
                };
                if let Ok(task_final) = tsk.downcast::<ResultTask<T, U, F>>() {
                    let task_final = task_final as Box<dyn TaskBase>;
                    DistributedScheduler::task_ended(
                        event_queues,
                        task_final,
                        TastEndReason::Success,
                        // Can break in future. But actually not needed for distributed scheduler since task runs on different processes.
                        // Currently using this because local scheduler needs it. It can be solved by refactoring tasks differently for local and distributed scheduler
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
                    DistributedScheduler::task_ended(
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

#[async_trait::async_trait]
impl NativeScheduler for DistributedScheduler {
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
    /// This function is used to submit a task to remote executor 
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        _id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output = U>,
    {
        if !env::Configuration::get().is_driver {
            return;
        }
        log::debug!("inside submit task");
        let event_queues_clone = self.event_queues.clone();
        tokio::spawn(async move {
            let mut num_retries = 0;
            loop {
                // TODO: Refer TcpStream::connect for other part of the code
                match TcpStream::connect(&target_executor).await {
                    Ok(mut stream) => {
                        let (reader, writer) = stream.split();
                        // let reader = reader.compat();
                        // let writer = writer.compat_write();
                        let task_bytes = bincode::serialize(&task).unwrap();
                        log::debug!(
                            "sending task #{} of {} bytes to exec @{},",
                            task.get_task_id(),
                            task_bytes.len(),
                            target_executor.port(),
                        );

                        // TODO: remove blocking call when possible
                        // tokio::task::spawn_blocking(async {
                        //     let mut message = capnp::message::Builder::new_default();
                        //     let mut task_data = message.init_root::<serialized_data::Builder>();
                        //     task_data.set_msg(&task_bytes);
                        //     capnp_serialize::write_message(writer, message)
                        //         .await
                        //         .map_err(Error::CapnpDeserialization)
                        //         .unwrap();
                            
                        // });
                        // TODO: Serialized `task_bytes` and write `task_bytes` to writer

                        log::debug!("sent data to exec @{}", target_executor.port());

                        // receive results back
                        DistributedScheduler::receive_results::<T, U, F, _>(
                            event_queues_clone,
                            reader,
                            task,
                            target_executor.port(),
                        )
                        .await;
                        break;
                    }
                    Err(_) => {
                        if num_retries > 5 {
                            panic!("executor @{} not initialized", target_executor.port());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        num_retries += 1;
                        continue;
                    }
                }
            }
        });
    }

    fn next_executor_server(&self, task: &dyn TaskBase) -> SocketAddrV4 {
        if !task.is_pinned() {
            // pick the first available server
            let socket_addrs = self.server_uris.lock().pop_back().unwrap();
            self.server_uris.lock().push_front(socket_addrs);
            socket_addrs
        } else {
            // seek and pick the selected host
            let servers = &mut *self.server_uris.lock();
            let location: Ipv4Addr = task.preferred_locations()[0];
            if let Some((pos, _)) = servers
                .iter()
                .enumerate()
                .find(|(_, e)| *e.ip() == location)
            {
                let target_host = servers.remove(pos).unwrap();
                servers.push_front(target_host);
                target_host
            } else {
                unreachable!()
            }
        }
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

    async fn get_shuffle_map_stage(&self, shuf: Arc<dyn ShuffleDependencyTrait>) -> Result<Stage> {
        log::debug!("getting shuffle map stage");
        let stage = self.shuffle_to_map_stage.get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage),
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

    async fn get_missing_parent_stages(&'_ self, stage: Stage) -> Result<Vec<Stage>> {
        log::debug!("getting missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: BTreeSet<Arc<dyn RddBase>> = BTreeSet::new();
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }
}

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}
