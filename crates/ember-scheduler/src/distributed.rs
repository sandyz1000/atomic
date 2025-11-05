use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, atomic::AtomicUsize},
};

use dashmap::DashMap;
use ember_compute::{ApproximateEvaluator, result::PartialResult};
use ember_data::{context::TaskContext, data::Data, rdd::Rdd};
use parking_lot::Mutex;

use crate::{
    base::{EventQueue, MutatorsAndGetter}, error::LibResult, job::Job, listener::LiveListenerBus, stage::Stage
};

#[derive(Clone, Default)]
pub struct DistributedScheduler {
    mutators: MutatorsAndGetter,
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
    // map_output_tracker: Arc<dyn MapOutputTracker>,
    // TODO: fix proper locking mechanism
    scheduler_lock: Arc<Mutex<bool>>,
    live_listener_bus: LiveListenerBus,
}


impl DistributedScheduler {
    pub fn run_job<T: Data, U: Data, F>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync,
    {
        todo!()
    }

    /// Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    /// as they arrive. Returns a partial result object from the evaluator.
    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: std::time::Duration,
    ) -> LibResult<PartialResult<R>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        todo!()
    }
}
