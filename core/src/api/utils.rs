use crate::models::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, DirEntry};
use std::hash::Hasher;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct StatusTracker {
    store: AppStatusStore,
}

/// Define a default value for driver memory here since this value is referenced across the code
/// base and nearly all files already use Utils.scala
const DEFAULT_DRIVER_MEM_MB: i64 = 1024;

/// Closes the given object, ignoring IOExceptions.
fn close_quietly<T: io::Read + io::Write>(closeable: &mut T) {
    let _ = closeable.flush();
}

pub fn non_negative_hash<T: std::hash::Hash>(obj: &T) -> u64 {
    if obj.is_none() {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    let hash = hasher.finish();
    if hash != std::i64::MIN {
        hash.abs() as u64
    } else {
        0
    }
}

/// Convert the given string to a byte buffer. The resulting buffer can be
/// converted back to the same string through `bytes_to_string()`.
pub fn string_to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Convert the given byte buffer to a string. The resulting string can be
/// converted back to the same byte buffer through `string_to_bytes()`.
pub fn bytes_to_string(b: &[u8]) -> Result<String, std::str::Utf8Error> {
    String::from_utf8(b.to_vec())
}

pub fn byte_string_as_kb(s: &str) -> u64 {
    unimplemented!()
}

pub fn byte_string_as_mb(s: &str) -> u64 {
    unimplemented!()
}

pub fn byte_string_as_gb(s: &str) -> u64 {
    unimplemented!()
}

/// Delete a file or directory and its contents recursively.
/// Don't follow directories if they are symlinks.
pub fn delete_recursively(file: &Path, filter: Option<&str>) -> io::Result<()> {
    if !file.exists() {
        return Ok(());
    }

    if file.is_file() {
        fs::remove_file(file)?;
        return Ok(());
    }

    if file.is_dir() {
        let entries = fs::read_dir(file)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if let Some(filter) = filter {
                if !path.file_name().unwrap().to_str().unwrap().contains(filter) {
                    continue;
                }
            }

            if path.is_dir() {
                delete_recursively_impl(&path, filter)?;
            } else {
                fs::remove_file(&path)?;
            }
        }

        fs::remove_dir(file)?;
    }

    Ok(())
}

pub fn list_files_safely(file: &Path, filter: Option<&str>) -> io::Result<Vec<PathBuf>> {
    if file.exists() {
        let entries = fs::read_dir(file)?;

        let mut files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if let Some(filter) = filter {
                if !path.file_name().unwrap().to_str().unwrap().contains(filter) {
                    continue;
                }
            }

            files.push(path);
        }

        Ok(files)
    } else {
        Ok(Vec::new())
    }
}

pub fn is_symlink(file: &Path) -> io::Result<bool> {
    Ok(fs::symlink_metadata(file)?.file_type().is_symlink())
}

lazy_static! {
    static ref TIME_SUFFIXES: HashMap<&'static str, TimeUnit> = {
        let mut map = HashMap::new();
        map.insert("us", TimeUnit::Microseconds);
        map.insert("ms", TimeUnit::Milliseconds);
        map.insert("s", TimeUnit::Seconds);
        map.insert("m", TimeUnit::Minutes);
        map.insert("min", TimeUnit::Minutes);
        map.insert("h", TimeUnit::Hours);
        map.insert("d", TimeUnit::Days);
        map
    };
    static ref BYTE_SUFFIXES: HashMap<&'static str, ByteUnit> = {
        let mut map = HashMap::new();
        map.insert("b", ByteUnit::Byte);
        map.insert("k", ByteUnit::KiB);
        map.insert("kb", ByteUnit::KiB);
        map.insert("m", ByteUnit::MiB);
        map.insert("mb", ByteUnit::MiB);
        map.insert("g", ByteUnit::GiB);
        map.insert("gb", ByteUnit::GiB);
        map.insert("t", ByteUnit::TiB);
        map.insert("tb", ByteUnit::TiB);
        map.insert("p", ByteUnit::PiB);
        map.insert("pb", ByteUnit::PiB);
        map
    };
}

pub fn time_string_as(str: &str, unit: Instant) -> Result<u64, String> {
    let lower = str.to_lowercase().trim();

    let re = Regex::new(r"(-?[0-9]+)([a-z]+)?").unwrap();
    let captures = re.captures(lower);

    if let Some(captures) = captures {
        let val = captures.get(1).unwrap().as_str().parse::<i64>().unwrap();
        let suffix = captures.get(2).map(|m| m.as_str());

        if let Some(suffix) = suffix {
            if !TIME_SUFFIXES.contains_key(suffix) {
                return Err(format!("Invalid suffix: \"{}\"", suffix));
            }
            let suffix_unit = TIME_SUFFIXES.get(suffix).unwrap();
            Ok(unit.convert(val, *suffix_unit))
        } else {
            Ok(unit.convert(val, unit))
        }
    } else {
        Err(format!("Failed to parse time string: {}", str))
    }
}

pub fn time_string_as_seconds(input: &str) -> f64 {
    // TODO: Raise error "Invalid value for configuration setting '{}': {}", key, value
    todo!()
}

pub fn time_string_as_ms(input: &str) -> f64 {
    // TODO: Raise error "Invalid value for configuration setting '{}': {}", key, value
    todo!()
}

pub fn byte_string_as_bytes(value: &str) -> f64 {
    // TODO: Raise error "Invalid value for configuration setting '{}': {}", key, value
    todo!()
}

impl StatusTracker {
    pub fn new(store: AppStatusStore) -> Self {
        StatusTracker { store }
    }

    pub fn get_job_ids_for_group(&self, job_group: Option<String>) -> Vec<i32> {
        self.store
            .jobs_list(None)
            .into_iter()
            .filter(|job| job.job_group == job_group)
            .map(|job| job.job_id)
            .collect()
    }

    pub fn get_job_ids_for_tag(&self, job_tag: String) -> Vec<i32> {
        self.store
            .jobs_list(None)
            .into_iter()
            .filter(|job| job.job_tags.contains(&job_tag))
            .map(|job| job.job_id)
            .collect()
    }

    pub fn get_active_stage_ids(&self) -> Vec<i32> {
        self.store
            .stage_list(vec![StageStatus::Active])
            .into_iter()
            .map(|stage| stage.stage_id)
            .collect()
    }

    pub fn get_active_job_ids(&self) -> Vec<i32> {
        self.store
            .jobs_list(vec![JobExecutionStatus::Running])
            .into_iter()
            .map(|job| job.job_id)
            .collect()
    }

    pub fn get_job_info(&self, job_id: i32) -> Option<JobInfo> {
        self.store
            .as_option(self.store.job(job_id))
            .map(|job| JobInfoImpl {
                job_id,
                stage_ids: job.stage_ids.into_iter().collect(),
                status: job.status,
            })
    }

    pub fn get_stage_info(&self, stage_id: i32) -> Option<StageInfo> {
        self.store
            .as_option(self.store.last_stage_attempt(stage_id))
            .map(|stage| StageInfoImpl {
                stage_id,
                current_attempt_id: stage.attempt_id,
                submission_time: stage.submission_time.unwrap_or(0),
                name: stage.name,
                num_tasks: stage.num_tasks,
                num_active_tasks: stage.num_active_tasks,
                num_completed_tasks: stage.num_complete_tasks,
                num_failed_tasks: stage.num_failed_tasks,
            })
    }

    pub fn get_executor_infos(&self) -> Vec<ExecutorInfoImpl> {
        self.store
            .executor_list(true)
            .into_iter()
            .map(|exec| {
                let (host, port) = Utils::parse_host_port(exec.host_port);
                let cache_size = exec
                    .memory_metrics
                    .map(|mem| mem.used_on_heap_storage_memory + mem.used_off_heap_storage_memory)
                    .unwrap_or(0);

                ExecutorInfoImpl {
                    host,
                    port,
                    cache_size,
                    num_running_tasks: exec.active_tasks,
                    used_on_heap_storage_memory: exec
                        .memory_metrics
                        .map(|mem| mem.used_on_heap_storage_memory)
                        .unwrap_or(0),
                    used_off_heap_storage_memory: exec
                        .memory_metrics
                        .map(|mem| mem.used_off_heap_storage_memory)
                        .unwrap_or(0),
                    total_on_heap_storage_memory: exec
                        .memory_metrics
                        .map(|mem| mem.total_on_heap_storage_memory)
                        .unwrap_or(0),
                    total_off_heap_storage_memory: exec
                        .memory_metrics
                        .map(|mem| mem.total_off_heap_storage_memory)
                        .unwrap_or(0),
                }
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct SSLOptions {
    enabled: bool,
    port: Option<i32>,
    key_store: Option<PathBuf>,
    key_store_password: Option<String>,
    key_password: Option<String>,
    key_store_type: Option<String>,
    need_client_auth: bool,
    trust_store: Option<PathBuf>,
    trust_store_password: Option<String>,
    trust_store_type: Option<String>,
    protocol: Option<String>,
    enabled_algorithms: Vec<String>,
}

impl SSLOptions {
    fn create_jetty_ssl_context_factory(&self) -> Option<SslContextFactory> {
        if self.enabled {
            let mut ssl_context_factory = SslContextFactory::new_server();

            if let Some(key_store) = &self.key_store {
                ssl_context_factory.set_key_store_path(key_store);
            }

            if let Some(key_store_password) = &self.key_store_password {
                ssl_context_factory.set_key_store_password(key_store_password);
            }

            if let Some(key_password) = &self.key_password {
                ssl_context_factory.set_key_manager_password(key_password);
            }

            if let Some(key_store_type) = &self.key_store_type {
                ssl_context_factory.set_key_store_type(key_store_type);
            }

            if self.need_client_auth {
                if let Some(trust_store) = &self.trust_store {
                    ssl_context_factory.set_trust_store_path(trust_store);
                }

                if let Some(trust_store_password) = &self.trust_store_password {
                    ssl_context_factory.set_trust_store_password(trust_store_password);
                }

                if let Some(trust_store_type) = &self.trust_store_type {
                    ssl_context_factory.set_trust_store_type(trust_store_type);
                }

                ssl_context_factory.set_need_client_auth(self.need_client_auth);
            }

            if let Some(protocol) = &self.protocol {
                ssl_context_factory.set_protocol(protocol);
            }

            Some(ssl_context_factory)
        } else {
            None
        }
    }

    // ...
}

impl Default for SSLOptions {
    fn default() -> Self {
        SSLOptions {
            enabled: false,
            port: None,
            key_store: None,
            key_store_password: None,
            key_password: None,
            key_store_type: None,
            need_client_auth: false,
            trust_store: None,
            trust_store_password: None,
            trust_store_type: None,
            protocol: None,
            enabled_algorithms: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SslContextFactory {
    // ...
}

impl SslContextFactory {
    fn new_server() -> Self {
        SslContextFactory {
            // initialize SSL context factory for server
        }
    }
}

thread_local! {
    static TASK_CONTEXT: RefCell<Option<Rc<dyn TaskContext>>> = RefCell::new(None);
}

pub struct Properties(HashMap<String, String>);

pub trait TaskCompletionListener {
    fn on_task_completion(&self, context: &dyn TaskContext);
}

pub trait TaskFailureListener {
    fn on_task_failure(&self, context: &dyn TaskContext, error: &std::error::Error);
}

pub trait TaskContext {
    fn is_completed(&self) -> bool;

    fn is_interrupted(&self) -> bool;

    fn add_task_completion_listener(&self, listener: Box<dyn TaskCompletionListener>);

    fn add_task_failure_listener(&self, listener: Box<dyn TaskFailureListener>);

    fn run_task_with_listeners<T>(&self, task: Box<dyn Task<T>>) -> T;

    fn stage_id(&self) -> i32;

    fn stage_attempt_number(&self) -> i32;

    fn partition_id(&self) -> i32;

    fn num_partitions(&self) -> i32;

    fn attempt_number(&self) -> i32;

    fn task_attempt_id(&self) -> i64;

    fn get_local_property(&self, key: &str) -> Option<String>;

    fn cpus(&self) -> i32;

    fn resources(&self) -> HashMap<String, ResourceInformation>;

    fn resources_jmap(&self) -> HashMap<String, ResourceInformation>;

    fn task_metrics(&self) -> TaskMetrics;

    fn get_metrics_sources(&self, source_name: &str) -> Vec<Source>;

    fn kill_task_if_interrupted(&self);

    fn get_kill_reason(&self) -> Option<String>;

    fn task_memory_manager(&self) -> TaskMemoryManager;

    fn register_accumulator(&self, a: Box<dyn Accumulator>);

    fn set_fetch_failed(&self, fetch_failed: FetchFailedException);

    fn mark_interrupted(&self, reason: &str);

    fn mark_task_failed(&self, error: Box<dyn std::error::Error>);

    fn mark_task_completed(&self, error: Option<Box<dyn std::error::Error>>);

    fn fetch_failed(&self) -> Option<FetchFailedException>;

    fn get_local_properties(&self) -> Properties;
}

#[derive(Debug, Clone)]
pub struct TaskContextImpl {
    // Implementation details of TaskContext
}

impl TaskContext for TaskContextImpl {
    fn is_completed(&self) -> bool {
        todo!()
    }

    fn is_interrupted(&self) -> bool {
        todo!()
    }

    fn add_task_completion_listener(&self, listener: Box<dyn TaskCompletionListener>) {
        todo!()
    }

    fn add_task_failure_listener(&self, listener: Box<dyn TaskFailureListener>) {
        todo!()
    }

    fn run_task_with_listeners<T>(&self, task: Box<dyn Task<T>>) -> T {
        todo!()
    }

    fn stage_id(&self) -> i32 {
        todo!()
    }

    fn stage_attempt_number(&self) -> i32 {
        todo!()
    }

    fn partition_id(&self) -> i32 {
        todo!()
    }

    fn num_partitions(&self) -> i32 {
        todo!()
    }

    fn attempt_number(&self) -> i32 {
        todo!()
    }

    fn task_attempt_id(&self) -> i64 {
        todo!()
    }

    fn get_local_property(&self, key: &str) -> Option<String> {
        todo!()
    }

    fn cpus(&self) -> i32 {
        todo!()
    }

    fn resources(&self) -> HashMap<String, ResourceInformation> {
        todo!()
    }

    fn resources_jmap(&self) -> HashMap<String, ResourceInformation> {
        todo!()
    }

    fn task_metrics(&self) -> TaskMetrics {
        todo!()
    }

    fn get_metrics_sources(&self, source_name: &str) -> Vec<Source> {
        todo!()
    }

    fn kill_task_if_interrupted(&self) {
        todo!()
    }

    fn get_kill_reason(&self) -> Option<String> {
        todo!()
    }

    fn task_memory_manager(&self) -> TaskMemoryManager {
        todo!()
    }

    fn register_accumulator(&self, a: Box<dyn Accumulator>) {
        todo!()
    }

    fn set_fetch_failed(&self, fetch_failed: FetchFailedException) {
        todo!()
    }

    fn mark_interrupted(&self, reason: &str) {
        todo!()
    }

    fn mark_task_failed(&self, error: Box<dyn std::error::Error>) {
        todo!()
    }

    fn mark_task_completed(&self, error: Option<Box<dyn std::error::Error>>) {
        todo!()
    }

    fn fetch_failed(&self) -> Option<FetchFailedException> {
        todo!()
    }

    fn get_local_properties(&self) -> Properties {
        todo!()
    }
    // Implement TaskContext methods
}

pub struct Task<T> {
    // Implementation details of Task
}

impl<T> Task<T> {
    fn run_task(&self, context: &dyn TaskContext) -> T {
        // Implementation of task execution
    }
}
#[derive(Debug)]
pub struct FetchFailedException {
    // Implementation details of FetchFailedException
}

#[derive(Debug)]
pub struct ResourceInformation {
    // Implementation details of ResourceInformation
}

#[derive(Debug)]
pub struct TaskMetrics {
    // Implementation details of TaskMetrics
}

#[derive(Debug)]
pub struct Source {
    // Implementation details of Source
}

pub trait Accumulator {
    // Implementation details of Accumulator
}
