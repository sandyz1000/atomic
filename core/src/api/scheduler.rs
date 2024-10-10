use crate::utils::TaskMetrics;


pub enum AtomicListenerEvent {

    ListenerStageSubmitted {
        stage_info: StageInfo,
        properties: Option<Properties>,
    },
    ListenerStageCompleted {
        stage_info: StageInfo,
    },
    ListenerTaskStart {
        stage_id: i32,
        stage_attempt_id: i32,
        task_info: TaskInfo,
    },
    ListenerTaskGettingResult {
        task_info: TaskInfo,
    },
    ListenerSpeculativeTaskSubmitted {
        stage_id: i32,
        stage_attempt_id: i32,
        task_index: i32,
        partition_id: i32,
    },
    
    ListenerTaskEnd {
        stage_id: i32,
        stage_attempt_id: i32,
        task_type: String,
        reason: TaskEndReason,
        task_info: TaskInfo,
        task_executor_metrics: ExecutorMetrics,
        task_metrics: Option<TaskMetrics>,
    },

    ListenerJobStart {
        job_id: i32,
        time: i64,
        stage_infos: Vec<StageInfo>,
        properties: Option<Properties>,
        stage_ids: Vec<i32>,
    },
    ListenerJobEnd {
        job_id: i32,
        time: i64,
        job_result: JobResult,
    },

    ListenerEnvironmentUpdate {
        environment_details: HashMap<String, Vec<(String, String)>>,
    },

    ListenerBlockManagerAdded {
        time: i64,
        block_manager_id: BlockManagerId,
        max_mem: i64,
        max_on_heap_mem: Option<i64>,
        max_off_heap_mem: Option<i64>,
    },
    ListenerBlockManagerRemoved {
        time: i64,
        block_manager_id: BlockManagerId,
    },
    ListenerUnpersistRDD {
        rdd_id: i32,
    },
    ListenerExecutorAdded {
        time: i64,
        executor_id: String,
        executor_info: ExecutorInfo,
    },
    ListenerExecutorRemoved {
        time: i64,
        executor_id: String,
        reason: String,
    },

    ListenerExecutorExcluded {
        time: i64,
        executor_id: String,
        task_failures: i32,
    },
    
    ListenerExecutorExcludedForStage {
        time: i64,
        executor_id: String,
        task_failures: i32,
        stage_id: i32,
        stage_attempt_id: i32,
    }, 
    ListenerNodeExcludedForStage {
        time: i64,
        host_id: String,
        executor_failures: i32,
        stage_id: i32,
        stage_attempt_id: i32,
    },

    ListenerExecutorUnexcluded {
        time: i64,
        executor_id: String,
    },

    ListenerNodeExcluded {
        time: i64,
        host_id: String,
        executor_failures: i32,
    },

    ListenerNodeUnexcluded {
        time: i64,
        host_id: String,
    },

    ListenerUnschedulableTaskSetAdded {
        stage_id: i32,
        stage_attempt_id: i32,
    },

    ListenerUnschedulableTaskSetRemoved {
        stage_id: i32,
        stage_attempt_id: i32,
    },

    ListenerBlockUpdated {
        block_updated_info: BlockUpdatedInfo,
    },

    ListenerMiscellaneousProcessAdded {
        time: i64,
        process_id: String,
        info: MiscellaneousProcessDetails,
    },

    ListenerExecutorMetricsUpdate {
        exec_id: String,
        accum_updates: Vec<(i64, i32, i32, Vec<AccumulableInfo>)>,
        executor_updates: HashMap<(i32, i32), ExecutorMetrics>,
    },

    ListenerStageExecutorMetrics {
        exec_id: String,
        stage_id: i32,
        stage_attempt_id: i32,
        executor_metrics: ExecutorMetrics,
    },

    ListenerApplicationStart {
        app_name: String,
        app_id: Option<String>,
        time: i64,
        user: String,
        app_attempt_id: Option<String>,
        driver_logs: Option<HashMap<String, String>>,
        driver_attributes: Option<HashMap<String, String>>,
    },

    ListenerApplicationEnd {
        time: i64,
    },

    ListenerLogStart {
        version: String,
    },

    ListenerResourceProfileAdded {
        resource_profile: ResourceProfile,
    }
}

impl AtomicListenerEvent {
    fn log_event(&self) -> bool {
        true
    }
}


/// Interface for listening to events from the scheduler. Most applications should probably
/// extend AtomicListener directly, rather than implementing this class.
/// 
pub trait ListenerInterface {
    /// Called when a stage completes successfully or fails, with information on the completed stage. 
    fn on_stage_completed(&self, stage_completed: AtomicListenerEvent);
    /// Called when a stage is submitted
    fn on_stage_submitted(&self, stage_submitted: AtomicListenerEvent);
    /// Called when a task starts
    fn on_task_start(&self, task_start: AtomicListenerEvent);
    /// Called when a task begins remotely fetching its result (will not be called for tasks that do
    /// not need to fetch the result remotely).
    fn on_task_getting_result(&self, task_getting_result: AtomicListenerEvent);
    /// Called when a task ends
    fn on_task_end(&self, task_end: AtomicListenerEvent);
    /// Called when a job starts
    fn on_job_start(&self, job_start: AtomicListenerEvent);
    /// Called when a job ends
    fn on_job_end(&self, job_end: AtomicListenerEvent);
    /// Called when environment properties have been updated
    fn on_environment_update(&self, environment_update: AtomicListenerEvent);
    /// Called when a new block manager has joined
    fn on_block_manager_added(&self, block_manager_added: AtomicListenerEvent);
    /// Called when an existing block manager has been removed
    fn on_block_manager_removed(&self, block_manager_removed: AtomicListenerEvent);
    /// Called when an RDD is manually unpersisted by the application
    fn on_unpersist_rdd(&self, unpersist_rdd: AtomicListenerEvent);
    /// Called when the application starts
    fn on_application_start(&self, application_start: AtomicListenerEvent);
    /// Called when the application ends
    fn on_application_end(&self, application_end: AtomicListenerEvent);
    /// Called when the driver receives task metrics from an executor in a heartbeat.
    fn on_executor_metrics_update(&self, executor_metrics_update: AtomicListenerEvent);
    /// Called with the peak memory metrics for a given (executor, stage) combination. Note that this
    /// is only present when reading from the event log (as in the history server), and is never
    /// called in a live application.
    fn on_stage_executor_metrics(&self, executor_metrics: AtomicListenerEvent);
    /// Called when the driver registers a new executor.
    fn on_executor_added(&self, executor_added: AtomicListenerEvent);
    /// Called when the driver removes an executor.
    fn on_executor_removed(&self, executor_removed: AtomicListenerEvent);
    /// Called when the driver excludes an executor for a application.
    fn on_executor_excluded(&self, executor_excluded: AtomicListenerEvent);
    /// Called when the driver excludes an executor for a stage.
    fn on_executor_excluded_for_stage(&self, executor_excluded_for_stage: AtomicListenerEvent);

    fn on_node_excluded_for_stage(&self, node_excluded_for_stage: AtomicListenerEvent);
    
    fn on_executor_unexcluded(&self, executor_unexcluded: AtomicListenerEvent);
    
    fn on_node_excluded(&self, node_excluded: AtomicListenerEvent);

    fn on_node_unexcluded(&self, node_unexcluded: AtomicListenerEvent);
    fn on_unschedulable_task_set_added(&self, unschedulable_task_set_added: AtomicListenerEvent);
    fn on_unschedulable_task_set_removed(&self, unschedulable_task_set_removed: AtomicListenerEvent);
    fn on_block_updated(&self, block_updated: AtomicListenerEvent);
    // Called when a speculative task is submitted
    fn on_speculative_task_submitted(&self, speculative_task: AtomicListenerEvent) -> i8;
}