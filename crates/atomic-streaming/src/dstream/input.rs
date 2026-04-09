use crate::context::StreamingContext;
use crate::dstream::{DStream, DStreamBase, InputStreamBase};
use atomic_compute::rdd::ParallelCollection;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// Rate estimation (stub — Phase 4)
// ─────────────────────────────────────────────────────────────────────────────

pub trait RateEstimator: Send + Sync {
    fn compute(&self, time_ms: u64, elements: u64, work_delay_ms: u64, wait_delay_ms: u64)
        -> Option<f64>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Common state for input DStreams
// ─────────────────────────────────────────────────────────────────────────────

pub struct InputDStreamState {
    pub stream_id: usize,
    pub ssc: Arc<StreamingContext>,
    pub batch_duration: Duration,
}

impl InputDStreamState {
    pub fn new(ssc: Arc<StreamingContext>, stream_id: usize) -> Self {
        let batch_duration = ssc.batch_duration;
        InputDStreamState { stream_id, ssc, batch_duration }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// QueueInputDStream<T>
// ─────────────────────────────────────────────────────────────────────────────

/// An input DStream backed by an in-memory queue of pre-built RDDs.
/// Primarily used for testing.
pub struct QueueInputDStream<T: Data + Clone> {
    state: InputDStreamState,
    queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = T>>>>>,
    one_at_a_time: bool,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = T>>>>,
}

impl<T: Data + Clone> QueueInputDStream<T> {
    pub fn new(
        ssc: Arc<StreamingContext>,
        stream_id: usize,
        queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = T>>>>>,
        one_at_a_time: bool,
    ) -> Self {
        QueueInputDStream {
            state: InputDStreamState::new(ssc, stream_id),
            queue,
            one_at_a_time,
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl<T: Data + Clone> DStreamBase for QueueInputDStream<T> {
    fn slide_duration(&self) -> Duration {
        self.state.batch_duration
    }
    fn id(&self) -> usize {
        self.state.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![]
    }
}

impl<T: Data + Clone> DStream<T> for QueueInputDStream<T> {
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        let rdds: Vec<_> = {
            let mut q = self.queue.lock();
            if self.one_at_a_time {
                q.pop_front().into_iter().collect()
            } else {
                q.drain(..).collect()
            }
        };

        let ctx = &self.state.ssc.sc;
        if rdds.is_empty() {
            let id = ctx.new_rdd_id();
            Some(Arc::new(ParallelCollection::<T>::new(id, vec![], 1)))
        } else if rdds.len() == 1 {
            Some(rdds.into_iter().next().unwrap())
        } else {
            ctx.union(&rdds).ok().map(|u| Arc::new(u) as Arc<dyn Rdd<Item = T>>)
        }
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) {
                return Some(rdd.clone());
            }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

impl<T: Data + Clone> InputStreamBase for QueueInputDStream<T> {}

// ─────────────────────────────────────────────────────────────────────────────
// FileInputDStream
// ─────────────────────────────────────────────────────────────────────────────

/// An input DStream that watches a local directory for new text files.
pub struct FileInputDStream {
    state: InputDStreamState,
    directory: PathBuf,
    filter: Arc<dyn Fn(&std::path::Path) -> bool + Send + Sync>,
    new_files_only: bool,
    seen_files: Mutex<HashSet<PathBuf>>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = String>>>>,
}

impl FileInputDStream {
    pub fn new(
        ssc: Arc<StreamingContext>,
        stream_id: usize,
        directory: impl Into<PathBuf>,
        new_files_only: bool,
    ) -> Self {
        FileInputDStream {
            state: InputDStreamState::new(ssc, stream_id),
            directory: directory.into(),
            filter: Arc::new(|_| true),
            new_files_only,
            seen_files: Mutex::new(HashSet::new()),
            generated: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&std::path::Path) -> bool + Send + Sync + 'static,
    {
        self.filter = Arc::new(filter);
        self
    }
}

impl DStreamBase for FileInputDStream {
    fn slide_duration(&self) -> Duration {
        self.state.batch_duration
    }
    fn id(&self) -> usize {
        self.state.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![]
    }
}

impl DStream<String> for FileInputDStream {
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        let batch_ms = self.state.batch_duration.as_millis() as u64;
        let window_start_ms = valid_time_ms.saturating_sub(batch_ms);

        let new_files: Vec<PathBuf> = {
            let mut seen = self.seen_files.lock();
            match std::fs::read_dir(&self.directory) {
                Ok(entries) => {
                    let candidates: Vec<PathBuf> = entries
                        .filter_map(|e| e.ok())
                        .map(|e| e.path())
                        .filter(|p| p.is_file())
                        .filter(|p| (self.filter)(p))
                        .filter(|p| {
                            if seen.contains(p) {
                                return false;
                            }
                            if self.new_files_only {
                                let modified_ms = std::fs::metadata(p)
                                    .and_then(|m| m.modified())
                                    .ok()
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);
                                modified_ms > window_start_ms && modified_ms <= valid_time_ms
                            } else {
                                true
                            }
                        })
                        .collect();
                    for p in &candidates {
                        seen.insert(p.clone());
                    }
                    candidates
                }
                Err(e) => {
                    log::error!("FileInputDStream: failed to read dir {:?}: {}", self.directory, e);
                    vec![]
                }
            }
        };

        let lines: Vec<String> = new_files
            .iter()
            .flat_map(|p| {
                std::fs::read_to_string(p)
                    .into_iter()
                    .flat_map(|s| s.lines().map(String::from).collect::<Vec<_>>())
            })
            .collect();

        let ctx = &self.state.ssc.sc;
        let id = ctx.new_rdd_id();
        let nparts = lines.len().max(1).min(num_cpus::get());
        Some(Arc::new(ParallelCollection::new(id, lines, nparts)))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) {
                return Some(rdd.clone());
            }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

impl InputStreamBase for FileInputDStream {}

// ─────────────────────────────────────────────────────────────────────────────
// SocketInputDStream
// ─────────────────────────────────────────────────────────────────────────────

/// An input DStream that reads text lines from a TCP socket.
pub struct SocketInputDStream {
    state: InputDStreamState,
    host: String,
    port: u16,
    buffer: Arc<Mutex<Vec<String>>>,
    reader_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = String>>>>,
}

impl SocketInputDStream {
    pub fn new(ssc: Arc<StreamingContext>, stream_id: usize, host: &str, port: u16) -> Self {
        SocketInputDStream {
            state: InputDStreamState::new(ssc, stream_id),
            host: host.to_string(),
            port,
            buffer: Arc::new(Mutex::new(Vec::new())),
            reader_handle: Mutex::new(None),
            stop_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl DStreamBase for SocketInputDStream {
    fn slide_duration(&self) -> Duration {
        self.state.batch_duration
    }
    fn id(&self) -> usize {
        self.state.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![]
    }
    fn is_receiver_input(&self) -> bool {
        true
    }

    fn start(&self) {
        use std::sync::atomic::Ordering;
        let addr = format!("{}:{}", self.host, self.port);
        let buffer = self.buffer.clone();
        let stop = self.stop_flag.clone();

        let handle = std::thread::Builder::new()
            .name(format!("socket-receiver-{}", self.state.stream_id))
            .spawn(move || {
                let stream = match TcpStream::connect(&addr) {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("SocketInputDStream: failed to connect to {}: {}", addr, e);
                        return;
                    }
                };
                let reader = BufReader::new(stream);
                for line in reader.lines() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    match line {
                        Ok(l) => buffer.lock().push(l),
                        Err(e) => {
                            log::error!("SocketInputDStream: read error: {}", e);
                            break;
                        }
                    }
                }
            })
            .expect("failed to spawn socket receiver thread");

        *self.reader_handle.lock() = Some(handle);
    }

    fn stop(&self) {
        use std::sync::atomic::Ordering;
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(h) = self.reader_handle.lock().take() {
            let _ = h.join();
        }
    }
}

impl DStream<String> for SocketInputDStream {
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        let lines: Vec<String> = self.buffer.lock().drain(..).collect();
        let ctx = &self.state.ssc.sc;
        let id = ctx.new_rdd_id();
        let nparts = lines.len().max(1).min(num_cpus::get());
        Some(Arc::new(ParallelCollection::new(id, lines, nparts)))
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) {
                return Some(rdd.clone());
            }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

impl InputStreamBase for SocketInputDStream {}
