use crate::exception::AtomicConfigException;
use crate::utils;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use std::env::var;
use std::sync::Arc;
use std::sync::RwLock;

const EXECUTOR_AVA_OPTIONS: &'static str = "";

pub mod config {
    use std::time::Duration;

    pub const DRIVER_PREFIX: &str = "driver";
    pub const EXECUTOR_PREFIX: &str = "executor";
    pub const TASK_PREFIX: &str = "task";
    pub const LISTENER_BUS_EVENT_QUEUE_PREFIX: &str = "scheduler.listenerbus.eventqueue";

    pub const RESOURCES_DISCOVERY_PLUGIN: &str = "resources.discoveryPlugin";
    pub const DRIVER_RESOURCES_FILE: &str = "driver.resourcesFile";
    pub const DRIVER_CLASS_PATH: &str = "driver.extraClassPath";
    pub const DRIVER_OPTIONS: &str = "driver.extraOptions";
    pub const DRIVER_LIBRARY_PATH: &str = "driver.extraLibraryPath";
    pub const DRIVER_USER_CLASS_PATH_FIRST: &str = "driver.userClassPathFirst";
    pub const DRIVER_CORES: &str = "driver.cores";
    pub const DRIVER_MEMORY: &str = "driver.memory";
    pub const DRIVER_MEMORY_OVERHEAD: &str = "driver.memoryOverhead";
    pub const DRIVER_MEMORY_OVERHEAD_FACTOR: &str = "driver.memoryOverheadFactor";
    pub const DRIVER_LOG_LOCAL_DIR: &str = "driver.log.localDir";
    pub const DRIVER_LOG_DFS_DIR: &str = "driver.log.dfsDir";
    pub const DRIVER_LOG_LAYOUT: &str = "driver.log.layout";
    pub const DRIVER_LOG_PERSISTTODFS: &str = "driver.log.persistToDfs.enabled";
    pub const DRIVER_LOG_ALLOW_EC: &str = "driver.log.allowErasureCoding";
    pub const EVENT_LOG_ENABLED: &str = "eventLog.enabled";
    pub const EVENT_LOG_DIR: &str = "eventLog.dir";
    pub const EVENT_LOG_COMPRESS: &str = "eventLog.compress";
    pub const EVENT_LOG_BLOCK_UPDATES: &str = "eventLog.logBlockUpdates.enabled";
    pub const EVENT_LOG_ALLOW_EC: &str = "eventLog.erasureCoding.enabled";
    pub const EVENT_LOG_TESTING: &str = "eventLog.testing";
    pub const EVENT_LOG_OUTPUT_BUFFER_SIZE: &str = "eventLog.buffer.kb";
    pub const EVENT_LOG_STAGE_EXECUTOR_METRICS: &str = "eventLog.logStageExecutorMetrics";
    pub const EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS: &str =
        "eventLog.gcMetrics.youngGenerationGarbageCollectors";
    pub const EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS: &str =
        "eventLog.gcMetrics.oldGenerationGarbageCollectors";
    pub const EVENT_LOG_OVERWRITE: &str = "eventLog.overwrite";
    pub const EVENT_LOG_CALLSITE_LONG_FORM: &str = "eventLog.longForm.enabled";
    /// Whether rolling over event log files is enabled. If set to true, it cuts down 
    /// each event log file to the configured size.
    pub const EVENT_LOG_ENABLE_ROLLING: &str = "eventLog.rolling.enabled";
    pub const EVENT_LOG_ROLLING_MAX_FILE_SIZE: &str = "eventLog.rolling.maxFileSize";
    pub const EXECUTOR_ID: Option<&'static str> = option_env!("executor.id");
    pub const EXECUTOR_CLASS_PATH: Option<&'static str> = option_env!("executor.extraClassPath");
    pub const EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES: bool = true;
    pub const EXECUTOR_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10000);
    pub const EXECUTOR_HEARTBEAT_MAX_FAILURES: i32 = 60;
    /// Whether to collect process tree metrics (from the /proc filesystem) when collecting executor metrics.
    pub const EXECUTOR_PROCESS_TREE_METRICS_ENABLED: bool = false;
    
    /// How often to collect executor metrics (in milliseconds).
    /// If 0, the polling is done on executor heartbeats. 
    /// If positive, the polling is done at this interval. 
    pub const EXECUTOR_METRICS_POLLING_INTERVAL: Duration = Duration::from_millis(0);
    pub const EXECUTOR_METRICS_FILESYSTEM_SCHEMES: &'static str = "file,hdfs";
    pub const EXECUTOR_OPTIONS: Option<&'static str> = option_env!("executor.extraOptions");
    pub const EXECUTOR_LIBRARY_PATH: Option<&'static str> = option_env!("executor.extraLibraryPath");
    pub const EXECUTOR_USER_CLASS_PATH_FIRST: bool = false;
    pub const EXECUTOR_CORES: i32 = 1;
    
    /// Amount of memory to use per executor process, in MiB unless otherwise specified.
    pub const EXECUTOR_MEMORY: &'static str = "1g";
    
    /// The amount of non-heap memory to be allocated per executor, in MiB unless otherwise specified.
    pub const EXECUTOR_MEMORY_OVERHEAD: Option<&'static str> = option_env!("executor.memoryOverhead");
    
    // Fraction of executor memory to be allocated as additional non-heap memory per " +
    // "executor process. This is memory that accounts for things like VM overheads, " +
    // "interned strings, other native overheads, etc. This tends to grow with the container " +
    // "size. This value defaults to 0.10 except for Kubernetes non-JVM jobs, which defaults " +
    // "to 0.40. This is done as non-JVM tasks need more non-JVM heap space and such tasks " +
    // "commonly fail with \"Memory Overhead Exceeded\" errors. This preempts this error " +
    // "with a higher default. This value is ignored if executor.memoryOverhead is set " +
    // "directly.
    const EXECUTOR_MEMORY_OVERHEAD_FACTOR: f64 = 0.1;


    pub const MEMORY_FRACTION: f64 = 0.6;

    pub const STORAGE_UNROLL_MEMORY_THRESHOLD: i64 = 1024 * 1024;

    pub const STORAGE_REPLICATION_PROACTIVE: bool = true;

    pub const STORAGE_MEMORY_MAP_THRESHOLD: &'static str = "2m";

    pub const STORAGE_REPLICATION_POLICY: &'static str =
        "org.apache.storage.RandomBlockReplicationPolicy";

    pub const STORAGE_REPLICATION_TOPOLOGY_MAPPER: &'static str =
        "org.apache.storage.DefaultTopologyMapper";

    pub const STORAGE_CACHED_PEERS_TTL: i32 = 60 * 1000;

    pub const STORAGE_MAX_REPLICATION_FAILURE: i32 = 1;

    pub const STORAGE_DECOMMISSION_ENABLED: bool = false;

    pub const STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED: bool = true;

    pub const STORAGE_DECOMMISSION_SHUFFLE_MAX_THREADS: i32 = 8;

    pub const STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED: bool = true;

    pub const STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK: i32 = 3;

    pub const STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL: &'static str = "30s";

    pub const STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH: Option<&'static str> = None;

}

struct ConfigProvider {
    // Implementation of ConfigProvider goes here
}

impl ConfigProvider {
    fn new(settings: HashMap<String, String>) -> Self {
        todo!()
    }

    fn get(&self, key: &str) -> Option<String> {
        unimplemented!()
    }
}

struct MapProvider {
    // Implementation of MapProvider goes here
}

impl MapProvider {
    fn new(values: HashMap<String, String>) -> Self {
        unimplemented!()
    }
}

struct EnvProvider {
    // Implementation of EnvProvider goes here
}

impl EnvProvider {
    fn new() -> Self {
        unimplemented!()
    }
}

#[derive(Debug)]
struct SystemProvider {
    // Implementation of SystemProvider goes here
}

impl SystemProvider {
    fn new() -> Self {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct ConfigReader {
    conf: Arc<dyn ConfigProvider>,
    bindings: HashMap<String, Arc<dyn ConfigProvider>>,
}

impl ConfigReader {
    fn new(conf: Arc<dyn ConfigProvider>) -> Self {
        // let mut bindings = HashMap::new();
        // Self::bind(&mut bindings, None, conf.clone());
        // Self::bind_env(&mut bindings, Arc::new(EnvProvider::new()));
        // Self::bind_system(&mut bindings, Arc::new(SystemProvider::new()));

        // ConfigReader { conf, bindings }

        todo!()
    }

    fn bind(prefix: Option<&str>, provider: Arc<dyn ConfigProvider>) {
        if let Some(prefix) = prefix {
            provider.insert(prefix.to_string(), provider);
        }
    }

    pub fn bind_env(provider: Arc<dyn ConfigProvider>) {
        Self::bind(Some("env"), provider);
    }

    fn bind_system(provider: Arc<dyn ConfigProvider>) {
        Self::bind(Some("system"), provider);
    }

    fn get(&self, key: &str) -> Option<String> {
        self.conf
            .get(key)
            .map(|value| self.substitute(&value, &HashSet::new()))
    }

    fn substitute(&self, input: &str, used_refs: &HashSet<String>) -> String {
        if let Some(input) = input {
            let ref_re = Regex::new(r"\$\{(?:(.*?):)?(.*?)\}").unwrap();
            ref_re
                .replace_all(input, |caps: &Captures| {
                    let prefix = caps.get(1).map(|m| m.as_str());
                    let name = caps.get(2).map(|m| m.as_str());
                    let ref_str = if let Some(prefix) = prefix {
                        format!("{}:{}", prefix, name)
                    } else {
                        name.to_string()
                    };
                    let ref_str = ref_str.as_str();
                    assert!(
                        !used_refs.contains(ref_str),
                        format!("Circular reference in {}: {}", input, ref_str)
                    );

                    let replacement = self
                        .bindings
                        .get(&prefix)
                        .and_then(|provider| self.get_or_default(provider, name))
                        .map(|value| self.substitute(&value, &(used_refs + ref_str)))
                        .unwrap_or_else(|| caps.get(0).map(|m| m.as_str()));
                        
                        // Regex::replace(replacement.as_str())
                })
                .to_string()
        } else {
            input.to_string()
        }
    }

    fn get_or_default(&self, conf: &dyn ConfigProvider, key: &str) -> Option<String> {
        conf.get(key)
            .or_else(|| match ConfigEntry::find_entry(key) {
                Some(e) => match e {
                    ConfigEntryVariant::ConfigEntryWithDefault(default_value) => {
                        Some(default_value.default_value_string())
                    }
                    ConfigEntryVariant::ConfigEntryWithDefaultString(default_value) => {
                        Some(default_value.default_value_string())
                    }
                    ConfigEntryVariant::ConfigEntryWithDefaultFunction(default_value) => {
                        Some(default_value.default_value_string())
                    }
                    ConfigEntryVariant::FallbackConfigEntry(fallback) => {
                        self.get_or_default(conf, fallback.fallback.key)
                    }
                    _ => None,
                },
                _ => None,
            })
    }
}

fn register_entry<T>(entry: &ConfigEntry<T>) {
    // Implementation of register_entry goes here
    todo!()
}

#[derive(Debug)]
pub enum ConfigEntryVariant {
    ConfigEntryWithDefault(String),
    ConfigEntryWithDefaultString(String),
    ConfigEntryWithDefaultFunction(String),
    FallbackConfigEntry(String),
}

#[derive(Debug)]
pub struct ConfigEntry<T> {
    pub key: String,
    prepended_key: Option<String>,
    prepend_separator: String,
    alternatives: Vec<String>,
    value_converter: Box<dyn Fn(String) -> T>,
    string_converter: Box<dyn Fn(T) -> String>,
    doc: String,
    is_public: bool,
    version: String,
}

impl<T> ConfigEntry<T> {
    fn new(
        key: String,
        prepended_key: Option<String>,
        prepend_separator: String,
        alternatives: Vec<String>,
        value_converter: Box<dyn Fn(String) -> T>,
        string_converter: Box<dyn Fn(T) -> String>,
        doc: String,
        is_public: bool,
        version: String,
    ) -> Self {
        let entry = ConfigEntry {
            key,
            prepended_key,
            prepend_separator,
            alternatives,
            value_converter,
            string_converter,
            doc,
            is_public,
            version,
        };
        register_entry(&entry);
        entry
    }

    fn find_entry(key: &str) -> ConfigEntryVariant {
        todo!()
    }

    fn default_value_string(&self) -> String {
        unimplemented!()
    }

    fn read_string(&self, reader: &ConfigReader) -> Option<String> {
        let values: Vec<Option<String>> = vec![
            self.prepended_key.as_ref().and_then(|k| reader.get(k)),
            self.alternatives
                .iter()
                .fold(reader.get(&self.key), |res, next_key| {
                    res.or_else(|_| reader.get(next_key))
                }),
        ];
        let values: Vec<String> = values.into_iter().flatten().collect();
        if !values.is_empty() {
            Some(values.join(&self.prepend_separator))
        } else {
            None
        }
    }

    fn read_from(&self, reader: &ConfigReader) -> T {
        unimplemented!()
    }

    fn default_value(&self) -> Option<T> {
        None
    }

    fn to_string(&self) -> String {
        format!(
            "ConfigEntry(key={}, defaultValue={}, doc={}, public={}, version={})",
            self.key,
            self.default_value_string(),
            self.doc,
            self.is_public,
            self.version
        )
    }
}

#[derive(Debug, Clone)]
pub struct AtomicConf {
    settings: RwLock<HashMap<String, String>>,
}

lazy_static! {
    static ref READER: ConfigReader = {
        let reader = ConfigReader::new(Arc::new(ConfigProvider::new(settings)));
        reader.bind_env(Arc::new(|key: &str| env::var(key).ok()));
        reader
    };
}

impl AtomicConf {
    const AVRO_NAMESPACE: &'static str = "avro.schema.";

    pub fn new(load_defaults: bool) -> Self {
        let settings = RwLock::new(HashMap::new());
        let conf = Self { settings };
        if load_defaults {
            conf.load_from_system_properties(false);
        }
        conf
    }

    pub fn set(&self, key: &str, value: &str) -> &Self {
        self.set_with_silent(key, value, false)
    }

    fn set_with_silent(&self, key: &str, value: &str, silent: bool) -> &Self {
        if key.is_empty() {
            panic!("null key");
        }
        if value.is_empty() {
            panic!("null value for {}", key);
        }
        if !silent {
            self.log_deprecation_warning(key);
        }
        self.settings
            .write()
            .unwrap()
            .insert(key.to_owned(), value.to_owned());
        self
    }

    /// The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
    /// run locally with 4 cores, or "http://master:7077" to run on a standalone cluster.
    pub fn set_master(&self, master: &str) -> &Self {
        self.set("master", master)
    }

    pub fn set_app_name(&self, name: &str) -> &Self {
        self.set("app.name", name)
    }

    fn load_from_system_properties(&self, silent: bool) -> &Self {
        if silent {
            return self;
        }
        for (key, value) in std::env::vars() {
            if key.starts_with("atomic.") {
                self.set_with_silent(&key, &value, silent);
            }
        }
        self
    }

    fn log_deprecation_warning(&self, key: &str) {
        // TODO: Implement logDeprecationWarning
        unimplemented!()
    }

    /// Set an environment variable to be used when launching executors for this application.
    /// These variables are stored as properties of the form executorEnv.VAR_NAME
    /// (for example executorEnv.PATH) but this method makes them easier to set.
    pub fn set_executor_env(&self, variable: &str, value: &str) -> &Self {
        self.set(&format!("executorEnv.{}", variable), value)
    }

    pub fn set_executor_envs(&self, variables: &[(String, String)]) -> &Self {
        for (k, v) in variables {
            self.set_executor_env(k, v);
        }
        self
    }

    /// Set the location where the module is installed on worker nodes.
    pub fn set_home(&self, home: &str) -> &Self {
        self.set("home", home)
    }

    pub fn set_all(&self, settings: &[(String, String)]) -> &Self {
        self.settings
            .write()
            .unwrap()
            .extend(settings.iter().cloned());
        self
    }

    pub fn set_if_missing(&self, key: &str, value: &str) -> &Self {
        if self
            .settings
            .write()
            .unwrap()
            .insert(key.to_owned(), value.to_owned())
            .is_none()
        {
            self.log_deprecation_warning(key);
        }
        self
    }

    pub fn set_if_missing_entry<T: ToString>(&self, entry: &ConfigEntry<T>, value: T) -> &Self {
        let key = entry.key.to_owned();
        let string_value = entry.string_converter(value.to_string().as_str());
        self.set_if_missing(&key, &string_value)
    }

    pub fn set_if_missing_optional_entry<T: ToString>(
        &self,
        entry: Optional<ConfigEntry<T>>,
        value: T,
    ) -> &Self {
        // let key = entry.key.to_owned();
        // let string_value = entry.raw_string_converter(value.to_string().as_str());
        // self.set_if_missing(&key, &string_value)
        todo!()
    }

    pub fn get_avro_schema_namespace(&self, schema_key: &str) -> Option<String> {
        self.settings
            .read()
            .unwrap()
            .get(&(Self::AVRO_NAMESPACE.to_owned() + schema_key))
            .cloned()
    }

    /// Use serialization and register the given set of Avro schemas so that the generic
    /// record serializer can decrease network IO
    pub fn register_avro_schemas(&self, schemas: &[&str]) -> &Self {
        todo!()
    }

    /// Gets all the avro schemas in the configuration used in the generic Avro record serializer
    pub fn get_avro_schema(&self) -> HashMap<u64, String> {
        self.get_all()
            .into_iter()
            .filter(|(k, _)| k.starts_with(Self::AVRO_NAMESPACE))
            .map(|(k, v)| {
                (
                    k[Self::AVRO_NAMESPACE.len()..].parse::<u64>().unwrap(),
                    v.to_owned(),
                )
            })
            .collect()
    }

    /// Remove a parameter from the configuration
    pub fn remove(&mut self, key: &str) -> &mut Self {
        self.settings.write().unwrap().remove(key);
        self
    }

    pub fn get_or_else(&self, key: &str, default_value: &str) -> &str {
        self.settings.get(key).unwrap_or(default_value)
    }

    /// Retrieves the value of a pre-defined configuration entry.
    ///
    /// - This is an internal API.
    /// - The return type if defined by the configuration entry.
    /// - This will throw an exception is the config is not optional and the value is not set.
    pub fn get<T: Value>(&self, entry: &ConfigEntry<T>) -> T::Value {
        entry.read_from(&READER)
    }

    /// Get a time parameter as seconds
    pub fn get_time_as_seconds(&self, key: &str, default_value: Option<&str>) -> u64 {
        let value = self.get_or_else(key, default_value);
        utils::time_string_as_seconds(&value)
    }

    /// Get a time parameter as milliseconds
    pub fn get_time_as_ms(&self, key: &str, default_value: Option<&str>) -> u64 {
        let value = self.get_or_else(key, default_value);
        utils::time_string_as_ms(&value)
    }

    /// Get a size parameter as bytes
    pub fn get_size_as_bytes(&self, key: &str, default_value: Option<&str>) -> u64 {
        let value = self.get_or_else(key, default_value);
        utils::byte_string_as_bytes(&value)
    }

    pub fn get_size_as_kb(&self, key: &str, default_value: Option<&str>) -> u64 {
        let value = self.get_or_else(key, default_value);
        utils::byte_string_as_kb(&value)
    }

    pub fn get_size_as_mb(&self, key: &str, default_value: Option<&str>) -> u64 {
        let value = self.get_or_else(key, default_value);
        utils::byte_string_as_mb(&value)
    }

    pub fn get_size_as_gb(&self, key: &str, default_value: Option<&str>) -> u64 {
        let value = self.get_or_else(key, default_value);
        utils::byte_string_as_gb(&value)
    }

    /// Get a parameter as an Option
    pub fn get_option(&self, key: &str) -> Option<String> {
        self.settings.read().unwrap().get(key).cloned()
    }

    pub fn get_with_substitution(&self, key: &str) -> Option<String> {
        // TODO: Fix here
        self.get_option(key)
            .map(|value| READER.substitute(&value, todo!()))
    }

    pub fn get_all(&self) -> Vec<(String, String)> {
        self.settings
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get all parameters that start with `prefix`
    pub fn get_all_with_prefix(&self, prefix: &str) -> Vec<(String, String)> {
        self.get_all()
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key[prefix.len()..].to_owned(), value))
            .collect()
    }

    pub fn get_int(&self, key: &str, default_value: i32) -> i32 {
        self.get_option(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(default_value)
    }

    pub fn get_long(&self, key: &str, default_value: i64) -> i64 {
        self.get_option(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(default_value)
    }

    pub fn get_double(&self, key: &str, default_value: f64) -> f64 {
        self.get_option(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(default_value)
    }

    /// Get a parameter as a boolean, falling back to a default if not set
    pub fn get_boolean(&self, key: &str, default_value: bool) -> bool {
        self.get_option(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(default_value)
    }

    /// Get all executor environment variables set on this AtomicConf
    pub fn get_executor_env(&self) -> Vec<(String, String)> {
        self.get_all_with_prefix("executorEnv.")
    }

    /// Returns the application id, valid in the Driver after TaskScheduler registration and
    /// from the start in the Executor.
    pub fn get_app_id(&self) -> Option<String> {
        self.get("app.id")
    }

    // TODO: Fix this
    /// Does the configuration contain a given parameter?
    pub fn contains(&self, key: &str) -> bool {
        self.settings.read().unwrap().contains_key(key)
            || self
                .configs_with_alternatives
                .get(key)
                .map_or(false, |x| x.iter().any(|alt| self.contains(alt.key)))
    }

    pub fn contains_entry(&self, entry: &ConfigEntry) -> bool {
        self.contains(entry.key())
    }

    pub fn clone(&self) -> AtomicConf {
        let mut cloned = AtomicConf::new(false);
        for (key, value) in self.get_all() {
            cloned.set(&key, &value);
        }
        cloned
    }

    pub fn getenv(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }

    fn catch_illegal_value<T, F: FnOnce() -> T>(&self, key: &str, get_value: F) -> T {
        // TODO: Fix me
        match get_value() {
            Ok(value) => value,
            Err(e) => todo!(),
        }
    }

    /// TODO: Fix this
    /// Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    /// idempotent - may mutate this conf object to convert deprecated settings to supported ones.
    pub fn validate_settings(&self) -> Result<(), AtomicConfigException> {
        // TODO: Fix here
        if self.contains("local.dir") {
            let msg = "Note that local.dir will be overridden by the value set by \
                the cluster manager (via LOCAL_DIRS in mesos/standalone/kubernetes and LOCAL_DIRS \
                in YARN).";
            log::warning(msg);
        }

        let executor_opts_key = config::EXECUTOR_AVA_OPTIONS.key();

        // Validate atomic.executor.extraOptions
        if let Some(opts) = self.get_string(executor_opts_key) {
            if opts.contains("-Datomic") {
                let msg = format!(
                    "{} is not allowed to set options (was '{}'). \
                    Set them directly on a AtomicConf or in a properties file \
                    when using ./bin/submit.",
                    executor_opts_key, opts
                );
                return Err(AtomicConfigException::new(msg));
            }
            if opts.contains("-Xmx") {
                let msg = format!(
                    "{} is not allowed to specify max heap memory settings (was '{}'). \
                    Use executor.memory instead.",
                    executor_opts_key, opts
                );
                return Err(AtomicConfigException::new(msg));
            }
        }

        // Validate memory fractions
        for key in &[config::MEMORY_FRACTION, config::MEMORY_STORAGE_FRACTION.key()] {
            let value = self.get_double(key, 0.5)?;
            if value > 1.0 || value < 0.0 {
                let msg = format!("{} should be between 0 and 1 (was '{}').", key, value);
                return Err(AtomicConfigException::new(msg));
            }
        }

        if self.contains(config::SUBMIT_DEPLOY_MODE.key()) {
            match self.get(SUBMIT_DEPLOY_MODE.key()).as_deref() {
                Some("cluster") | Some("client") => {}
                Some(e) => {
                    let msg = format!(
                        "{} can only be \"cluster\" or \"client\".",
                        SUBMIT_DEPLOY_MODE.key()
                    );
                    return Err(AtomicConfigException::new(msg));
                }
                None => {}
            }
        }

        if self.contains(config::CORES_MAX) && self.contains(config::EXECUTOR_CORES) {
            let total_cores = self.get_int(CORES_MAX.key(), 1);
            let executor_cores = self.get(EXECUTOR_CORES.key());
            let left_cores = total_cores % executor_cores;
            if left_cores != 0 {
                let msg = format!(
                    "Total executor cores: {} is not divisible by cores per executor: {}, \
                    the left cores: {} will not be allocated",
                    total_cores, executor_cores, left_cores
                );
                log::warn!(&msg);
            }
        }

        let encryption_enabled =
            self.get(config::NETWORK_CRYPTO_ENABLED) || self.get(config::SASL_ENCRYPTION_ENABLED);
        if encryption_enabled && !self.get(NETWORK_AUTH_ENABLED) {
            let msg = format!(
                "{} must be enabled when enabling encryption.",
                NETWORK_AUTH_ENABLED.key()
            );
            return Err(AtomicConfigException::new(msg));
        }

        let executor_timeout_threshold_ms = self.get(config::NETWORK_TIMEOUT) * 1000;
        let executor_heartbeat_interval_ms = self.get(config::EXECUTOR_HEARTBEAT_INTERVAL);
        let network_timeout = config::NETWORK_TIMEOUT.key();
        if executor_timeout_threshold_ms <= executor_heartbeat_interval_ms {
            let msg = format!(
                "The value of {}={}ms must be greater than the value of {}={}ms.",
                network_timeout,
                executor_timeout_threshold_ms,
                config::EXECUTOR_HEARTBEAT_INTERVAL.key(),
                executor_heartbeat_interval_ms
            );
            return Err(AtomicConfigException::new(msg));
        }

        Ok(())
    }
    /// Return a string listing all keys and values, one per line. This is useful to print the
    /// configuration out for debugging.
    pub fn to_debug_string(&self) -> String {
        let redacted = Utils::redact(self, self.get_all());
        let mut lines: Vec = redacted
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        lines.sort();
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_conf_set() {
        env::set_var("test.key", "test value");
        let conf = AtomicConf::new(true);
        conf.set("test.key", "test value 2");
        assert_eq!(
            conf.settings.read().unwrap().get("test.key"),
            Some(&"test value 2".to_owned())
        );
    }
}
