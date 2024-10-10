use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use crate::conf::AtomicConf;
use crate::utils;
use atomic::scheduler::{DAGScheduler, Scheduler};
use http::uri::Uri;
use std::thread;
use std::path::PathBuf;
use std::time::{Duration, Instant};
// use crate::models::*;

/// CallSite represents a place in user code. It can have a short and a long form.
#[derive(Debug, Clone)]
pub struct CallSite {
    pub long_form: String,
    pub short_form: String
}

// TODO: Find the right configuration
#[derive(Debug, Clone)]
pub struct HadoopConfiguration;

#[derive(Debug, Clone)]
pub struct MetricRegistry;


#[derive(Debug, Clone)]
pub struct LiveListenerBus {
    context: Option<AtomicContext>,
    metrics: LiveListenerBusMetrics,
    started: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    queues: AsyncEventQueue,
}

#[derive(Debug, Clone)]
pub struct LiveListenerBusMetrics {
    source_name: String,
    // TODO: Verify and remove this type
    metric_registry: MetricRegistry,
    num_events_posted: HashMap<String, i32>,
    per_listener_class_timers: Mutex<HashMap<String, Instant>>,
}

impl LiveListenerBusMetrics {
    fn new(conf: AtomicConf) -> LiveListenerBusMetrics {
        LiveListenerBusMetrics {
            source_name: "LiveListenerBus".to_string(),
            metric_registry: MetricRegistry::new(),
            num_events_posted: metric_registry.counter("numEventsPosted"),
            per_listener_class_timers: Mutex::new(HashMap::new()),
        }
    }
}


#[derive(Debug, Clone)]
pub struct AsyncEventQueue;

#[derive(Debug, Clone)]
pub struct AppStatusStore;

#[derive(Debug, Clone)]
pub struct StatusTracker {
    sc: AtomicContext, 
    store: AppStatusStore
}

#[derive(Debug, Clone)]
pub struct AtomicUI;

#[derive(Debug, Clone, Default)]
pub struct ConsoleProgressBar {
    sc: AtomicContext,
    update_period_msec: u64,
    first_delay_msec: u64,
    terminal_width: usize,
    last_finish_time: Instant,
    last_update_time: Instant,
    last_progress_bar: String,
    timer: Timer,
}

impl ConsoleProgressBar {
    fn new(
        sc: AtomicContext,
        update_period_msec: u64,
        first_delay_msec: u64,
        terminal_width: usize,
        last_finish_time: Instant,
        last_update_time: Instant,
        last_progress_bar: String,
        timer: Instant
    ) -> Self {
            self.sc = sc;
            self.update_period_msec = update_period_msec;

        }
}

/// ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
/// status of active stages from the app state store periodically, the progress bar will be showed
/// up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
/// of them will be combined together, showed in one line.
impl ConsoleProgressBar {
    fn new(sc: AtomicContext) -> ConsoleProgressBar {
        let update_period_msec = sc.get_conf().get_ui_console_progress_update_interval();
        let first_delay_msec = 500;
        let terminal_width = env::var("COLUMNS").unwrap_or_else(|_| "80".to_string()).parse().unwrap();
        let last_finish_time = Instant::now();
        let last_update_time = Instant::now();
        let last_progress_bar = String::new();
        let timer = Timer::new();

        // TODO: Fix me
        timer.schedule_repeating(Duration::from_millis(first_delay_msec), Duration::from_millis(update_period_msec), || {
            // Self::refresh();
            unimplemented!()
        });

        ConsoleProgressBar {
            sc,
            update_period_msec,
            first_delay_msec,
            terminal_width,
            last_finish_time,
            last_update_time,
            last_progress_bar,
            timer,
        }
    }

    fn refresh(&mut self) {
        // Implement the logic to refresh the progress bar
    }
}

/// A CoreListener that logs events to persistent storage.
#[derive(Debug, Clone)]
pub struct EventLoggingListener {
    app_id: String,
    app_attempt_id: Option<String>,
    log_base_dir: PathBuf,
    conf: AtomicConf,
    hadoop_conf: Configuration,
}

impl EventLoggingListener {
    fn new(
        app_id: String,
        app_attempt_id: Option<String>,
        log_base_dir: PathBuf,
        atomic_conf: AtomicConf,
        hadoop_conf: Configuration,
    ) -> Self {
        EventLoggingListener {
            app_id,
            app_attempt_id,
            log_base_dir,
            conf: atomic_conf,
            hadoop_conf,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContextCleaner {
    sc: AtomicContext,
    shuffle_driver_component: ShuffleDriverComponents
}


/// Creates a heartbeat thread which will call the specified reportHeartbeat function at
/// intervals of intervalMs.
#[derive(Debug, Clone)]
pub struct HeartBeater {
    report_heartbeat: Box<dyn Fn() + Send + Sync>,
    name: String,
    interval_ms: u64,
    heart_beater: Option<thread::JoinHandle<()>>,
}

impl HeartBeater {
    fn start(&mut self) {
        // Wait a random interval so the heartbeats don't end up in sync
        let initial_delay = self.interval_ms + (rand::random::<f64>() * self.interval_ms as f64) as u64;

        let report_heartbeat = self.report_heartbeat.clone();
        let interval = Duration::from_millis(self.interval_ms);
        let heartbeat_task = move || {
            Utils::log_uncaught_exceptions(|| report_heartbeat());
        };

        let heartbeater_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(initial_delay));
            loop {
                let start = Instant::now();
                heartbeat_task();
                let elapsed = start.elapsed();
                if elapsed < interval {
                    thread::sleep(interval - elapsed);
                }
            }
        });

        self.heart_beater = Some(heartbeater_thread);
    }

    fn stop(&mut self) {
        if let Some(heartbeater_thread) = self.heart_beater.take() {
            heartbeater_thread.join().unwrap();
        }
    }
}

/// Manager of resource profiles. The manager allows one place to keep the actual ResourceProfiles
/// and everywhere else we can use the ResourceProfile Id to save on space.
/// Note we never remove a resource profile at this point. Its expected this number is small
/// so this shouldn't be much overhead.
#[derive(Debug, Clone)]
pub struct ResourceProfileManager {
    conf: AtomicConf,
    listener_bus: LiveListenerBus
}

impl ResourceProfileManager {
    
}

trait PluginContainer {
    fn shutdown(&self);
    fn register_metrics(&self, app_id: &str);
    fn on_task_start(&self);
    fn on_task_succeeded(&self);
    fn on_task_failed(&self, failure_reason: TaskFailedReason);
}


trait ShuffleDriverComponents {
    /// Called once in the driver to bootstrap this module that is specific to this application.
    /// This method is called before submitting executor requests to the cluster manager.
    /// 
    /// This method should prepare the module with its shuffle components i.e. registering against
    /// an external file servers or shuffle services, or creating tables in a shuffle
    /// storage data database.
    /// 
    fn initialize_application(&self) -> HashMap<String, String>;

    /// Called once at the end of the application to clean up any existing shuffle state.
    fn cleanup_application(&self);

    /// Called once per shuffle id when the shuffle id is first generated for a shuffle stage.
    fn register_shuffle(&self, shuffle_id: i32) {}

    /// Removes shuffle data associated with the given shuffle.
    fn remove_shuffle(&self, shuffle_id: i32, blocking: bool) {}

    fn supports_reliable_storage(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub struct Rdd<T> {
    
}

pub trait Partition: Serializable {
    /// Get the partition's index within its parent RDD
    fn index(&self) -> i32;

    /// A better default implementation of `HashCode`
    fn hash_code(&self) -> i32 {
        self.index()
    }

    /// A better default implementation of `equals`
    fn equals(&self, other: &dyn Any) -> bool {
        self as *const _ == other as *const _
    }
}

#[derive(Debug, Clone)]
pub struct AtomicEnv;


// TODO: Fix this
#[derive(Debug, Clone)]
pub struct AtomicContext {
    pub creation_site: CallSite,
    pub stopped: Arc<AtomicBool>,
    pub conf: AtomicConf,
    pub event_log_dir: Option<Uri>,
    pub event_log_codec: Option<String>,
    pub listener_bus: LiveListenerBus,
    pub env: AtomicEnv,
    pub status_tracker: StatusTracker,
    pub progress_bar: Option<ConsoleProgressBar>,
    pub ui: Option<AtomicUI>,
    pub hadoop_configuration: HadoopConfiguration,
    pub executor_memory: i32,
    // scheduler_backend: SchedulerBackend,
    pub task_scheduler: Scheduler,
    pub dag_scheduler: DAGScheduler,
    pub application_id: String,
    pub application_attempt_id: Option<String>,
    pub event_logger: Option<EventLoggingListener>,
    pub executor_allocation_manager: Option<ExecutorAllocationManager>,
    pub cleaner: Option<ContextCleaner>,
    pub listener_bus_started: bool,
    pub files: Vec<String>,
    pub archives: Vec<String>,
    pub shutdown_hook_ref: Option<T>,
    pub status_store: AppStatusStore,
    pub heartbeater: HeartBeater,
    pub resources: HashMap<String, ResourceInformation>,
    pub shuffle_driver_components: Box<dyn ShuffleDriverComponents>,
    pub plugins: Option<Box<dyn PluginContainer>>,
    pub resource_profile_manager: ResourceProfileManager,
}

impl AtomicContext {
    pub const JOB_DESCRIPTION: &str = "job.description";
    pub const JOB_GROUP_ID: &str = "jobGroup.id";
    pub const JOB_INTERRUPT_ON_CANCEL: &str = "job.interruptOnCancel";
    pub const JOB_TAGS: &str = "job.tags";
    pub const SCHEDULER_POOL: &str = "scheduler.pool";
    pub const RDD_SCOPE_KEY: &str = "rdd.scope";
    pub const RDD_SCOPE_NO_OVERRIDE_KEY: &str = "rdd.scope.noOverride";
    pub const DRIVER_IDENTIFIER: &str = "";

    pub fn get_or_create(conf: &AtomicConf) -> Self {
        todo!()
    }

    pub fn get_active() -> bool {
        todo!()
    }

    pub fn new(config: AtomicConf) -> Self {
        let creation_site = utils::get_call_site();
        let stopped = Arc::new(AtomicBool::new(false));
        if !config.get(EXECUTOR_ALLOW_CONTEXT) {
            Self::assert_on_driver();
        }
        Self::mark_partially_constructed();
        let start_time = SystemTime::now();
        let env = AtomicEnv::create_driver_env(
            config.clone(),
            utils::is_local_master(&config),
            listener_bus,
            num_driver_cores,
            this,
        );

        let progress_bar = if config.get("ui.showConsoleProgress") == "true" {
            Some(ConsoleProgressBar::default())
        } else {
            None
        };
        let ui = if config.get(UI_ENABLED) {
            Some(AtomicUI::create(Some(self)))
        } else {
            None
        };

        let status_store = AppStatusStore::new(storeUris);
        let status_tracker = StatusTracker::new(this);

        let dag_scheduler = DAGScheduler::new(this);
        let task_scheduler = AtomicContext.create_task_scheduler(this, master, deployMode, conf);

        let scheduler_backend = task_scheduler::create_scheduler_backend(
            this,
            master,
            deployMode,
            scheduler_conf,
            AtomicContext::num_driver_cores(master, conf),
            listener_bus,
            isLocal,
            conf,
            env.rpcEnv,
            dag_scheduler,
            taskScheduler,
            resourcesFileOpt,
            _resourcesFileEncoding
        );

        // let event_logger =

        fn init_local_properties() -> HashMap<String, String> {
            let mut properties = HashMap::new();
            properties
        }

    }

    /// TODO: Fix argument types 
    pub fn num_driver_cores(master: (), conf: AtomicConf) -> () {
        unimplemented!()
    }

    pub fn create_context() -> Result<(), Box<dyn std::error::Error>> {
        let mut _conf = config.clone();
        _conf.validate_settings()?;
        _conf.set("app.startTime", startTime.to_string());

        if !_conf.contains("master") {
            return Err("A master URL must be set in your configuration".into());
        }
        if !_conf.contains("app.name") {
            return Err("An application name must be set in your configuration".into());
        }
        // This should be set as early as possible.
        // Self::fill_missing_magic_committer_confs_if_needed(_conf)?;

        let _driver_logger = DriverLogger::new(_conf);

        let resources_file_opt = conf.get(DRIVER_RESOURCES_FILE);
        let _resources = get_or_discover_all_resources(_conf, DRIVER_PREFIX, resources_file_opt)?;
        log_resource_info(DRIVER_PREFIX, _resources);

        // log out app.name in the driver logs
        log::info!(format!("Submitted application: {}", appName));

        // System property yarn.app.id must be set if user code ran by AM on a YARN cluster
        if master == "yarn" && deployMode == "cluster" && !_conf.contains("yarn.app.id") {
            let message = "Detected yarn cluster mode, but isn't running on a cluster. 
                Deployment to YARN is not supported directly by AtomicContext. 
                Please use atm-submit.".into();
            return Err(message);
        }

        if _conf.get_boolean("logConf", false) {
            log::info!(format!("Atomic configuration:\n {}", _conf.to_debug_string()));
        }

        // Set driver host and port system properties. This explicitly sets the configuration
        // instead of relying on the default value of the config constant.
        _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS));
        _conf.set_if_missing(DRIVER_PORT, 0);

        _conf.set(EXECUTOR_ID, AtomicContext::DRIVER_IDENTIFIER);

        
        let _files = _conf.get_option(FILES.key).map(|x| x.split(",").filter(|s| !s.is_empty()).collect::<Vec<_>>().to_owned()).unwrap_or(vec![]);
        let _archives = _conf.get_option(ARCHIVES.key).map(|x| utils::stringToSeq(x).to_owned()).unwrap_or(vec![]);

        let _event_log_dir =
            if is_event_log_enabled {
                let unresolved_dir: String = conf.get(EVENT_LOG_DIR).strip_suffix("/").unwrap();
                Some(utils::resolve_uri(unresolved_dir))
            } else {
                None
            };

        let _event_log_codec = {
            let compress = _conf.get(EVENT_LOG_COMPRESS);
            if compress && is_event_log_enabled {
                Some(_conf.get(EVENT_LOG_COMPRESSION_CODEC)).map(CompressionCodec::get_short_name)
            } else {
                None
            }
        };

        let mut _listener_bus = LiveListenerBus::new(_conf);
        let _resource_profile_manager = ResourceProfileManager::new(_conf);
        
        // TODO: Fix rest of the implementation
        unimplemented!()    
        
    }

    
    pub fn get_executor_thread_dump(executor_id: &str) -> Option<Vec<ThreadStackTrace>> {
        if executor_id == AtomicContext::DRIVER_IDENTIFIER {
            Some(Utils::get_thread_dump())
        } else {
            match env.block_manager.master.get_executor_endpoint_ref(executor_id) {
                Some(endpoint_ref) => {
                    Some(endpoint_ref
                        .ask_sync::<Vec<ThreadStackTrace>>(TriggerThreadDump)
                        .ok()?)
                }
                None => {
                    log::warn!(format!("Executor {} might already have stopped and can not request thread dump from it.", executor_id));
                    None
                }
            }
        }
    }
    
    pub fn get_local_properties() -> Properties {
        local_properties.get()
    }
    
    pub fn set_local_properties(props: Properties) {
        local_properties.set(props);
    }
    
    pub fn set_local_property(key: &str, value: &str) {
        if value.is_empty() {
            local_properties.get_mut().remove(key);
        } else {
            local_properties.get_mut().insert(key.to_string(), value.to_string());
        }
    }

    /// Assigns a group ID to all the jobs started by this thread until the group ID is set to a
    /// different value or cleared.
    /// 
    /// Often, a unit of execution in an application consists of multiple actions or jobs.
    /// Application programmers can use this method to group all those jobs together and give a
    /// group description. Once set, the web UI will associate such jobs with this group.
    ///  
    /// The application can also use `cancel_group_job` to cancel all
    /// running jobs in this group. For example,
    /// ```rust
    /// // In the main thread:
    /// sc.setJobGroup("some_job_to_cancel", "some job description")
    /// sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
    /// 
    /// // In a separate thread:
    /// sc.cancelJobGroup("some_job_to_cancel");
    /// ```
    /// ### Arguments
    /// - `interrupt_on_cancel`: If true, then job cancellation will result in `panic!`
    /// being called on the job's executor threads.
    pub fn set_job_group(&self, group_id: &str, description: &str, interrupt_on_cancel: bool) {
        set_local_property(AtomicContext::JOB_DESCRIPTION, description);
        set_local_property(AtomicContext::JOB_GROUP_ID, group_id);
        // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
        // changing several public APIs and allows cancellations outside of the cancelJobGroup
        // APIs to also take advantage of this property (e.g., internal job failures or canceling from
        // JobProgressTab UI) on a per-job basis.
        set_local_property(AtomicContext::JOB_INTERRUPT_ON_CANCEL, interrupt_on_cancel.to_string());
    }
    
    pub fn clear_job_group() {
        set_local_property(AtomicContext::JOB_DESCRIPTION, "");
        set_local_property(AtomicContext::JOB_GROUP_ID, "");
        set_local_property(AtomicContext::JOB_INTERRUPT_ON_CANCEL, "");
    }
    
    /// Execute a block of code in a scope such that all new RDDs created in this body will
    /// be part of the same scope. 
    /// Return statements are NOT allowed in the given body.
    fn with_scope<U, F: FnOnce() -> U>(body: F) -> U {
        RDDOperationScope::with_scope::<U, F>(self, body)
    }
    
    // Methods for creating RDDs
    
    /// Distribute a local Scala collection to form an RDD.
    /// ### Note
    /// - Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
    /// to parallelize and before the first action on the RDD, the resultant RDD will reflect the
    /// modified collection. Pass a copy of the argument to avoid this.
    /// - And avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `empty_rdd` for an
    /// empty RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
    /// ### Arguments
    /// - `seq`: Scala collection to distribute
    /// - `num_slices`:  number of partitions to divide the collection into
    /// ### Returns
    /// RDD representing distributed collection
    pub fn parallelize<T: 'static + Clone + Send>(
        seq: &Vec<T>,
        num_slices: usize,
    ) -> Rdd<T> {
        assert_not_stopped();
        let slices = (0..num_slices).map(|i| (i, vec![])).collect::<HashMap<_, _>>();
        Rdd::new(ParallelCollectionRDD::<T>::new(
            self.clone(),
            seq.clone(),
            num_slices,
            slices,
        ))
    }

    pub fn range(
        start: i64,
        end: i64,
        step: i64,
        num_slices: usize,
    ) -> Rdd<i64> {
        assert_not_stopped();
        // When step is 0, range will run infinitely.
        assert_ne!(step, 0, "step cannot be 0");
        let num_elements = {
            let safe_start = BigInt::from(start);
            let safe_end = BigInt::from(end);
            if (safe_end - safe_start) % step == 0
                || (safe_end > safe_start) != (step > 0)
            {
                (safe_end - safe_start) / step
            } else {
                // the remainder has the same sign with range, could add 1 more.
                (safe_end - safe_start) / step + 1
            }
        };
        let slices = (0..num_slices).collect::<Vec<_>>();
        parallelize(&slices, num_slices).map_partitions_with_index(move |i, _| {
            let partition_start =
                (i * num_elements) / num_slices as i64 * step + start;
            let partition_end =
                (((i + 1) * num_elements) / num_slices as i64) * step + start;
            let get_safe_margin = |bi: BigInt| -> i64 {
                if bi.is_valid_i64() {
                    bi.to_i64().unwrap()
                } else if bi > 0 {
                    i64::MAX
                } else {
                    i64::MIN
                }
            };
            let safe_partition_start = get_safe_margin(BigInt::from(partition_start));
            let safe_partition_end = get_safe_margin(BigInt::from(partition_end));
    
            iterator::from_fn(move || {
                let mut number = safe_partition_start;
                let mut overflow = false;
                Some(loop {
                    let ret = number;
                    number += step;
                    if number < ret ^ step < 0 {
                        // We have i64::MAX + i64::MAX < i64::MAX and
                        // i64::MIN + i64::MIN > i64::MIN, so iff the step causes a step back,
                        // we are pretty sure that we have an overflow.
                        overflow = true;
                    }
                    if !overflow {
                        if step > 0 {
                            if number < safe_partition_end {
                                break ret;
                            }
                        } else if number > safe_partition_end {
                            break ret;
                        }
                    } else {
                        break None;
                    }
                })
            })
        })
    }

    /// Distribute a local Scala collection to form an RDD.
    /// 
    /// This method is identical to `parallelize`.
    /// ### Arguments
    /// - `seq`: Collection to distribute
    /// - `num_slices`: number of partitions to divide the collection into
    /// ### Returns
    /// RDD representing distributed collection
    pub fn make_rdd<T: 'static + Clone + Send>(
        seq: &Vec<T>,
        num_slices: usize,
    ) -> Rdd<T> {
        assert_not_stopped();
        parallelize(seq, num_slices)
    }

    /// Distribute a local Scala collection to form an RDD, with one or more
    /// location preferences (hostnames of nodes) for each object.
    /// Create a new partition for each collection item.
    /// ### Arguments
    /// - `seq`: list of tuples of data and location preferences (hostnames of nodes)
    /// ### Returns
    /// RDD representing data partitioned according to location preferences
    pub fn make_rdd_with_locations<T: 'static + Clone + Send>(
        seq: &Vec<(T, Vec<String>)>,
    ) -> Rdd<T> {
        assert_not_stopped();
        let index_to_prefs = seq.iter().enumerate().map(|(i, (_, prefs))| (i, prefs.clone())).collect::<HashMap<_, _>>();
        let data = seq.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>();
        Rdd::new(ParallelCollectionRDD::<T>::new(data, data.len(), index_to_prefs))
    }

    /**
     * Read a text file from HDFS, a local file system (available on all nodes), or any
     * Hadoop-supported file system URI, and return it as an RDD of Strings.
     * The text files must be encoded as UTF-8.
     *
     * @param path path to the text file on a supported file system
     * @param minPartitions suggested minimum number of partitions for the resulting RDD
     * @return RDD of lines of the text file
     */
    pub fn text_file(
        path: &str,
        min_partitions: usize,
    ) -> Rdd<String> {
        assert_not_stopped();
        hadoop_file(
            path,
            &TextInputFormat::class(),
            &LongWritable::class(),
            &Text::class(),
            min_partitions,
        )
        .map(|pair| pair.1.to_string())
        .set_name(path.to_string())
    }
    
    ///
    /// Read a directory of text files from HDFS, a local file system (available on all nodes), or any
    // Hadoop-supported file system URI. Each file is read as a single record and returned in a
    // key-value pair, where the key is the path of each file, the value is the content of each file.
    // The text files must be encoded as UTF-8.
    //
    // For example, if you have the following files:
    //   hdfs://a-hdfs-path/part-00000
    //   hdfs://a-hdfs-path/part-00001
    //   ...
    //   hdfs://a-hdfs-path/part-nnnnn
    //
    // Do `let rdd = context.whole_text_file("hdfs://a-hdfs-path")`,
    //
    // Then `rdd` contains
    //   (a-hdfs-path/part-00000, its content)
    //   (a-hdfs-path/part-00001, its content)
    //   ...
    //   (a-hdfs-path/part-nnnnn, its content)
    //
    // Small files are preferred, large file is also allowable, but may cause bad performance.
    //
    // On some filesystems, `.../path/*` can be a more efficient way to read all files in a directory
    // rather than `.../path/` or `.../path`.
    //
    // Partitioning is determined by data locality. This may result in too few partitions by default.
    //
    // # Arguments
    //
    // * `path` - Directory to the input data files, the path can be comma separated paths as the list
    //            of inputs.
    // * `min_partitions` - A suggestion value of the minimal splitting number for input data.
    //
    // # Returns
    //
    //RDD representing tuples of file path and the corresponding file content.
    ///
    fn whole_text_files(
        path: &str,
        min_partitions: Option<i32>,
    ) -> Rdd<(String, String)> {
        assert_not_stopped();
        let job = NewHadoopJob::get_instance(hadoop_configuration());
        // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
        // comma separated files as input. 
        NewFileInputFormat::set_input_paths(&job, path);
        let update_conf = job.configuration();
        WholeTextFileRDD::new(
            &context(),
            &WholeTextFileInputFormat::class(),
            &Text::class(),
            &Text::class(),
            update_conf,
            min_partitions.unwrap_or(default_min_partitions()),
        )
        .map(|record| (record._1.to_string(), record._2.to_string()))
        .set_name(path)
    }

    /// Get an RDD for a Hadoop-readable dataset as PortableDataStream for each file (useful for binary data).
    ///
    /// For example, if you have the following files:
    ///   hdfs://a-hdfs-path/part-00000
    ///   hdfs://a-hdfs-path/part-00001
    ///   ...
    ///   hdfs://a-hdfs-path/part-nnnnn
    /// 
    /// Do `let rdd = context.binary_files("hdfs://a-hdfs-path")`,
    ///
    /// Then `rdd` contains
    ///   (a-hdfs-path/part-00000, its content)
    ///   (a-hdfs-path/part-00001, its content)
    ///   ...
    ///   (a-hdfs-path/part-nnnnn, its content)
    /// 
    /// Small files are preferred; very large files may cause bad performance.
    /// 
    /// On some filesystems, `.../path/*` can be a more efficient way to read all files in a directory
    /// rather than `.../path/` or `.../path`.
    /// 
    /// Partitioning is determined by data locality. This may result in too few partitions by default.
    /// 
    /// # Arguments
    /// - `path` - Directory to the input data files, the path can be comma separated paths as the list of inputs.
    /// - `min_partitions` - A suggestion value of the minimal splitting number for input data.
    /// 
    /// # Returns
    /// RDD representing tuples of file path and corresponding file content.
    /// 
    fn binary_files(
        path: &str,
        min_partitions: Option<i32>,
    ) -> Rdd<(String, PortableDataStream)> {
        assert_not_stopped();
        let job = NewHadoopJob::get_instance(hadoop_configuration());
        // Use setInputPaths so that binaryFiles aligns with hadoopFile/textFile in taking
        // comma separated files as input.
        NewFileInputFormat::set_input_paths(&job, path);
        let update_conf = job.configuration();
        BinaryFileRDD::new(
            &context(),
            &StreamInputFormat::class(),
            &String::class(),
            &PortableDataStream::class(),
            update_conf,
            min_partitions.unwrap_or(default_min_partitions()),
        )
        .set_name(path)
    }

    ///  Get an RDD for a Hadoop-readable dataset with fixed length binary records.
    /// 
    ///  # Arguments
    ///  * `path` - Directory to the input data files, the path can be comma separated paths as the
    ///             list of inputs.
    ///  * `record_length` - Length of each record in bytes.
    ///  * `conf` - Hadoop configuration for setting up the dataset.
    ///  
    ///  # Returns
    ///  RDD of byte arrays, where each array is a fixed-length binary record.
    fn binary_records(
        path: &str,
        record_length: i32,
        conf: Option<Configuration>,
    ) -> Rdd<Vec<u8>> {
        assert_not_stopped();
        let conf = conf.unwrap_or(hadoop_configuration());
        conf.set_int(
            &FixedLengthBinaryInputFormat::RECORD_LENGTH_PROPERTY,
            record_length,
        );
        let br = new_api_hadoop_file(
            path,
            &FixedLengthBinaryInputFormat::class(),
            &LongWritable::class(),
            &BytesWritable::class(),
            Some(conf),
        );
        br.map(|(_k, v)| {
            let bytes = v.copy_bytes();
            assert!(
                bytes.len() == record_length as usize,
                "Byte array does not have correct length"
            );
            bytes.to_vec()
        })
    }

    ///  Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
    ///  necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
    ///  using the older MapReduce API (`org.apache.hadoop.mapred`).
    ///  
    ///  # Arguments
    ///  - `conf` - JobConf for setting up the dataset. Note: This will be put into a Broadcast.
    ///             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
    ///             sure you won't modify the conf. A safe approach is always creating a new conf for a
    ///             new RDD.
    ///  - `input_format_class` - Storage format of the data to be read.
    ///  - `key_class` - `Class` of the key associated with the `input_format_class` parameter.
    ///  - `value_class` - `Class` of the value associated with the `input_format_class` parameter.
    ///  - `min_partitions` - Minimum number of Hadoop Splits to generate.
    ///  
    ///  # Returns
    ///  RDD of tuples of key and corresponding value.
    ///  
    ///  # Notes
    ///  Because Hadoop's RecordReader class re-uses the same Writable object for each record, directly
    ///  caching the returned RDD or directly passing it to an aggregation or shuffle operation will
    ///  create many references to the same object.
    ///  If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
    ///  copy them using a `map` function.
    fn hadoop_rdd<K, V>(
        conf: &JobConf,
        input_format_class: &Class<InputFormat<K, V>>,
        key_class: &Class<K>,
        value_class: &Class<V>,
        min_partitions: Option<i32>,
    ) -> Rdd<(K, V)> {
        assert_not_stopped();
        let min_partitions = min_partitions.unwrap_or(default_min_partitions());
        // This is a hack to enforce loading hdfs-site.xml.
        FileSystem::get_local(conf);
    
        // Add necessary security credentials to the JobConf before broadcasting it.
        HadoopUtil::get().add_credentials(conf);
        HadoopRDD::new(
            this,
            conf,
            input_format_class,
            key_class,
            value_class,
            min_partitions,
        )
    }

    /// Get an RDD for a Hadoop file with an arbitrary InputFormat.
    /// 
    ///  # Arguments
    ///  - `path` - Directory to the input data files, the path can be comma separated paths as the
    ///             list of inputs.
    ///  - `input_format_class` - Storage format of the data to be read.
    ///  - `key_class` - `Class` of the key associated with the `input_format_class` parameter.
    ///  - `value_class` - `Class` of the value associated with the `input_format_class` parameter.
    ///  - `min_partitions` - Suggested minimum number of partitions for the resulting RDD.
    /// 
    ///  # Returns
    ///  RDD of tuples of key and corresponding value.
    /// 
    ///  # Notes
    ///  Because Hadoop's RecordReader class re-uses the same Writable object for each
    fn hadoop_file<K: Data, V: Data, F: InputFormat<K, V> + Any + Clone + Send + Sync + 'static>(
        sc: &AtomicContext,
        path: &str,
        min_partitions: i32,
    ) -> Rdd<(K, V)> {
        unimplemented!()
    } 

    pub fn new_api_hadoop_file<K: Data, V: Data, F: NewInputFormat<K, V> + Any + Clone + Send + Sync + 'static>(
        sc: &AtomicContext,
        path: &str,
    ) -> Rdd<(K, V)> {
        new_api_hadoop_file_helper(
            sc,
            path,
            F::class_name(),
            PhantomData::<F>,
            PhantomData::<K>,
            PhantomData::<V>,
        )
    }
    
    fn new_api_hadoop_file_helper<K: Data, V: Data, F: NewInputFormat<K, V> + Any + Clone + Send + Sync + 'static>(
        sc: &AtomicContext,
        path: &str,
        input_format_class: &str,
        _fm: PhantomData<F>,
        _km: PhantomData<K>,
        _vm: PhantomData<V>,
    ) -> Rdd<(K, V)> {
        assert_not_stopped();
        let j_input_format_class = sc.jvm().get().unwrap().load_class(input_format_class).unwrap();
        let j_key_class = jvm_class_tag(&K::tag()).runtime_class();
        let j_value_class = jvm_class_tag(&V::tag()).runtime_class();
        let rdd = todo!();
        Ok(Rdd::new(rdd))
    }
    
    fn new_api_hadoop_file_with_conf<K, V, F>(
        sc: &AtomicContext,
        path: &str,
        input_format_class: &Class<F>,
        key_class: &Class<K>,
        value_class: &Class<V>,
        conf: &Configuration,
    ) -> Rdd<(K, V)> 
    where K: Data, V: Data, F: NewInputFormat<K, V> + Any + Clone + Send + Sync + 'static 
    {
        assert_not_stopped();
        let mut job = NewHadoopJob::get_instance(conf);
        NewFileInputFormat::set_input_paths(&mut job, &[path]);
        let updated_conf = job.get_configuration();
        let rdd = NewHadoopRDD::new(
            sc,
            input_format_class,
            key_class,
            value_class,
            updated_conf,
        )?;
        Ok(Rdd::new(rdd))
    }

}