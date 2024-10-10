use std::collections::LinkedList;
use std::fmt::Debug;
use std::fs::File;
use std::io::Result;
use std::option::Option;

pub trait StageInfo: Debug + serde::Serialize + serde::Deserialize {
    fn stage_id(&self) -> i32;
    fn current_attempt_id(&self) -> i32;
    fn submission_time(&self) -> i64;
    fn name(&self) -> String;
    fn num_tasks(&self) -> i32;
    fn num_active_tasks(&self) -> i32;
    fn num_completed_tasks(&self) -> i32;
    fn num_failed_tasks(&self) -> i32;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct StageInfoImpl {
    pub stage_id: i32,
    pub current_attempt_id: i32,
    pub submission_time: i64,
    pub name: String,
    pub num_tasks: i32,
    pub num_active_tasks: i32,
    pub num_completed_tasks: i32,
    pub num_failed_tasks: i32,
}

impl StageInfo for StageInfoImpl {
    fn stage_id(&self) -> i32 {
        todo!()
    }

    fn current_attempt_id(&self) -> i32 {
        todo!()
    }

    fn submission_time(&self) -> i64 {
        todo!()
    }

    fn name(&self) -> String {
        todo!()
    }

    fn num_tasks(&self) -> i32 {
        todo!()
    }

    fn num_active_tasks(&self) -> i32 {
        todo!()
    }

    fn num_completed_tasks(&self) -> i32 {
        todo!()
    }

    fn num_failed_tasks(&self) -> i32 {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct JobInfoImpl {
    pub job_id: i32,
    pub stage_ids: Vec<i32>,
    pub status: JobExecutionStatus,
}

#[derive(Debug, Clone)]
pub struct StageInfoImpl {
    stage_id: i32,
    current_attempt_id: i32,
    submission_time: i64,
    name: String,
    num_tasks: i32,
    num_active_tasks: i32,
    num_completed_tasks: i32,
    num_failed_tasks: i32,
}

#[derive(Debug, Clone)]
pub struct ExecutorInfoImpl {
    pub host: String,
    pub port: i32,
    pub cache_size: i64,
    pub num_running_tasks: i32,
    pub used_on_heap_storage_memory: i64,
    pub used_off_heap_storage_memory: i64,
    pub total_on_heap_storage_memory: i64,
    pub total_off_heap_storage_memory: i64,
}

#[derive(Debug, Clone)]
pub enum JobExecutionStatus {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct AppSummary {
    guid: String,
    num_completed_jobs: i32,
    num_completed_stages: i32,
}

#[derive(Debug, Clone)]
pub struct RDDOperationGraphWrapper {
    stage_id: i32,
    edges: Seq<RDDOperationEdge>,
    outgoing_edges: Seq<RDDOperationEdge>,
    incoming_edges: Seq<RDDOperationEdge>,
    root_cluster: RDDOperationClusterWrapper,
}

#[derive(Debug, Clone)]
pub struct RDDOperationNode;

#[derive(Debug)]
pub struct RDDOperationClusterWrapper {
    id: String,
    name: String,
    child_nodes: Seq<RDDOperationNode>,
    child_clusters: Seq<RDDOperationClusterWrapper>,
}

impl RDDOperationClusterWrapper {
    fn new(
        id: String,
        name: String,
        child_nodes: Seq<RDDOperationNode>,
        child_clusters: Seq<RDDOperationClusterWrapper>,
    ) -> RDDOperationClusterWrapper {
        RDDOperationClusterWrapper {
            id,
            name,
            child_nodes,
            child_clusters,
        }
    }
}

#[derive(Debug)]
pub struct RDDOperationEdge {
    from_id: i32,
    to_id: i32,
}

#[derive(Debug)]
pub struct AppStatusStore<T> {
    store: T,
    listener: Option<AppStatusListener>,
    store_path: Option<File>,
}

impl<T> AppStatusStore<T> {
    pub fn new(store: T, listener: Option<AppStatusListener>, store_path: Option<File>) -> Self {
        AppStatusStore {
            store,
            listener,
            store_path,
        }
    }

    fn application_info(&self) -> ApplicationInfo {
        match self
            .store
            .view(classOf[ApplicationInfoWrapper])
            .max(1)
            .closeableIterator()
        {
            Some(mut it) => it.next().info,
            None => {
                panic!("Failed to get the application information. If you are starting up, please wait a while until it's ready.")
            }
        }
    }

    fn environment_info(&self) -> ApplicationEnvironmentInfo {
        unimplemented!()
    }

    fn resource_profile_info(&self) -> Vec<ResourceProfileInfo> {
        KVUtils::map_to_seq(self.store.view(classOf[ResourceProfileWrapper]), |rp| {
            rp.rpInfo
        })
    }

    pub fn jobs_list(&self, statuses: &LinkedList<JobExecutionStatus>) -> Vec<JobData> {
        unimplemented!()
    }

    pub fn job(&self, job_id: i32) -> JobData {
        unimplemented!()
    }

    /// Returns job data and associated SQL execution ID of certain Job ID.
    /// If there is no related SQL execution, the SQL execution ID part will be None.
    pub fn job_with_associated_sql(&self, job_id: i32) -> (JobData, Option<i64>) {
        unimplemented!()
    }

    fn executor_summary(executor_id: String) -> ExecutorSummary {
        unimplemented!()
    }

    fn stage_data(
        stage_id: i32,
        details: bool,
        task_status: LinkedList<TaskStatus>,
        with_summaries: bool,
        unsorted_quantiles: Vec<f64>,
    ) -> Vec<StageData> {
        unimplemented!()
    }

    fn stage_attempt(
        stage_id: i32,
        stage_attempt_id: i32,
        details: bool,
        task_status: LinkedList<TaskStatus>,
        with_summaries: bool,
        unsorted_quantiles: Vec<f64>,
    ) -> (StageData, Vec<i32>) {
        unimplemented!()
    }

    /// Calculates a summary of the task metrics for the given stage attempt, returning the
    /// requested quantiles for the recorded metrics.
    ///
    /// This method can be expensive if the requested quantiles are not cached; the method
    /// will only cache certain quantiles (every 0.05 step), so it's recommended to stick to
    /// those to avoid expensive scans of all task data.
    fn task_summary(
        stage_id: i32,
        stage_attempt_id: i32,
        unsorted_quantiles: Vec<f64>,
    ) -> Option<TaskMetricDistributions> {
        unimplemented!()
    }

    fn task_list(
        stage_id: i32,
        stage_attempt_id: i32,
        offset: i32,
        length: i32,
        sort_by: TaskSorting,
        statuses: LinkedList<TaskStatus>,
    ) -> Vec<TaskData> {
        unimplemented!()
    }

    fn new_stage_data(
        stage: StageData,
        with_detail: bool,
        task_status: LinkedList<TaskStatus>,
        with_summaries: bool,
        unsorted_quantiles: Vec<f64>,
    ) -> StageData {
        unimplemented!()
    }

    fn stage_executor_summary(
        stage_id: i32,
        stage_attempt_id: i32,
        unsorted_quantiles: Vec<f64>,
    ) -> Option<ExecutorMetricsDistributions> {
        unimplemented!()
    }
}

pub trait KVStore<T, K, V> {
    fn get_metadata<T: 'static>(&self, store_type: T) -> Result<T>;

    fn set_metadata(&self, value: T) -> Result<()>;

    fn read<T>(&self, store_type: T, natural_key: K) -> Result<T>;

    fn write(&self, value: V) -> Result<()>;

    fn delete(&self, store_type: T, natural_key: Object) -> Result<()>;

    pub fn view<T>(&self, store_type: T) -> Result<KVStoreView<T>>;

    fn count(&self, store_type: T) -> Result<i64>;

    fn count(&self, store_type: T, index: String, indexed_value: V) -> Result<i64>;

    fn remove_all_by_index_values<T>(
        &self,
        store_type: T,
        index: String,
        index_values: Vec<T>,
    ) -> Result<bool>;
}
