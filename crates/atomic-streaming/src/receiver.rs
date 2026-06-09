/// Receiver infrastructure for streaming data ingestion.
///
/// `BlockGenerator` buffers incoming items and periodically emits blocks.
use crate::streaming_support::file_sink::StorageLevel;
use parking_lot::Mutex;
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Block identifier

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamBlockId {
    pub stream_id: usize,
    pub unique_id: u64,
}

impl StreamBlockId {
    pub fn new(stream_id: usize, unique_id: u64) -> Self {
        StreamBlockId { stream_id, unique_id }
    }

    pub fn name(&self) -> String {
        format!("input-{}-{}", self.stream_id, self.unique_id)
    }
}

// ReceivedBlock — what a receiver hands to the block generator

pub enum ReceivedBlock {
    /// A batch of items as boxed Any values.
    DataIterator(Vec<Box<dyn Any + Send>>),
    /// A block already identified by its ID.
    DataBlock { block_id: StreamBlockId, data: Vec<Box<dyn Any + Send>> },
}

// Receiver trait

/// Implemented by each input source to provide data to the streaming engine.
pub trait Receiver: Send + Sync + 'static {
    /// Start receiving data. Called in a background thread.
    fn on_start(&mut self);
    /// Called when the receiver should stop.
    fn on_stop(&mut self);
    /// Store a single item.
    fn store(&self, item: Box<dyn Any + Send>);
    /// Stop the receiver.
    fn stop(&mut self, message: &str) {
        log::info!("Receiver stopping: {}", message);
        self.on_stop();
    }
}

// BlockGeneratorListener

/// Callbacks fired by BlockGenerator on block lifecycle events.
pub trait BlockGeneratorListener: Send + Sync + 'static {
    fn on_add_data(&self, data: &Box<dyn Any + Send>, metadata: Option<&dyn Any>) {}
    fn on_generate_block(&self, block_id: &StreamBlockId, iterator: &[Box<dyn Any + Send>]) {}
    fn on_push_block(&self, block_id: &StreamBlockId) {}
    fn on_error(&self, message: &str, throwable: Option<&dyn std::error::Error>) {
        log::error!("BlockGenerator error: {}", message);
    }
}

// GeneratorState

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeneratorState {
    Initialised,
    Active,
    StoppedAddingData,
    StoppedGeneratingBlocks,
    Stopped,
}

// BlockGenerator

/// Buffers incoming items and periodically pushes them as blocks.
pub struct BlockGenerator {
    stream_id: usize,
    block_interval: Duration,
    current_buffer: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
    state: Mutex<GeneratorState>,
    next_block_id: Arc<AtomicUsize>,
    stopped: Arc<AtomicBool>,
    /// Optional ReceiverTracker to notify when a block is generated.
    receiver_tracker: Option<Arc<crate::scheduler::receiver::ReceiverTracker>>,
}

impl BlockGenerator {
    pub fn new(stream_id: usize, block_interval: Duration) -> Self {
        BlockGenerator {
            stream_id,
            block_interval,
            current_buffer: Arc::new(Mutex::new(Vec::new())),
            state: Mutex::new(GeneratorState::Initialised),
            next_block_id: Arc::new(AtomicUsize::new(0)),
            stopped: Arc::new(AtomicBool::new(false)),
            receiver_tracker: None,
        }
    }

    /// Attach a ReceiverTracker so that each generated block is registered
    /// for metadata tracking (block ID, record count).
    pub fn with_tracker(
        mut self,
        tracker: Arc<crate::scheduler::receiver::ReceiverTracker>,
    ) -> Self {
        self.receiver_tracker = Some(tracker);
        self
    }

    /// Add an item to the current buffer.
    pub fn add_data(&self, item: Box<dyn Any + Send>) {
        let state = self.state.lock();
        if *state != GeneratorState::Active {
            log::warn!("BlockGenerator::add_data called in state {:?}", *state);
            return;
        }
        drop(state);
        self.current_buffer.lock().push(item);
    }

    /// Start the periodic block-push loop in a background thread.
    pub fn start(&self) {
        *self.state.lock() = GeneratorState::Active;
        let buffer = self.current_buffer.clone();
        let interval = self.block_interval;
        let stopped = self.stopped.clone();
        let stream_id = self.stream_id;
        let tracker = self.receiver_tracker.clone();
        let next_block_id = Arc::clone(&self.next_block_id);

        std::thread::Builder::new()
            .name(format!("block-generator-{}", stream_id))
            .spawn(move || {
                while !stopped.load(Ordering::SeqCst) {
                    std::thread::sleep(interval);
                    let block: Vec<_> = buffer.lock().drain(..).collect();
                    if !block.is_empty() {
                        let num_records = block.len() as u64;
                        let unique_id = next_block_id.fetch_add(1, Ordering::Relaxed);
                        let block_id = StreamBlockId::new(stream_id, unique_id as u64);
                        log::debug!(
                            "BlockGenerator[{}]: block {} with {} items",
                            stream_id, unique_id, num_records
                        );
                        if let Some(ref t) = tracker {
                            t.add_block(stream_id, ReceivedBlockInfo {
                                stream_id,
                                block_id,
                                num_records: Some(num_records),
                                metadata: None,
                            });
                        }
                    }
                }
            })
            .expect("failed to spawn block generator thread");
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        *self.state.lock() = GeneratorState::Stopped;
    }

    pub fn is_active(&self) -> bool {
        *self.state.lock() == GeneratorState::Active
    }

    fn next_block_id(&self) -> StreamBlockId {
        StreamBlockId::new(
            self.stream_id,
            self.next_block_id.fetch_add(1, Ordering::Relaxed) as u64,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    // --- next_block_id() increments correctly ---

    #[test]
    fn block_id_increments() {
        let bg = BlockGenerator::new(7, Duration::from_secs(1));
        let a = bg.next_block_id();
        let b = bg.next_block_id();
        let c = bg.next_block_id();
        assert_eq!(a.stream_id, 7);
        assert_eq!(a.unique_id, 0);
        assert_eq!(b.unique_id, 1);
        assert_eq!(c.unique_id, 2);
    }

    // --- The key safety property: Arc<AtomicUsize> is shared correctly ---
    // This directly tests that the counter replacing the raw pointer is visible
    // from both the spawning thread and any thread that clones the Arc.

    #[test]
    fn arc_counter_cross_thread() {
        let bg = BlockGenerator::new(0, Duration::from_secs(1));
        // Clone the Arc just like BlockGenerator::start() does.
        let shared = Arc::clone(&bg.next_block_id);
        let handle = std::thread::spawn(move || {
            shared.fetch_add(5, Ordering::SeqCst);
        });
        handle.join().unwrap();
        // The add from the other thread must be visible here.
        assert_eq!(bg.next_block_id.load(Ordering::SeqCst), 5);
        // next_block_id() must see the update too.
        let id = bg.next_block_id();
        assert_eq!(id.unique_id, 5);
    }

    #[test]
    fn arc_no_lost_updates() {
        let bg = BlockGenerator::new(0, Duration::from_secs(1));
        let mut handles = vec![];
        for _ in 0..8 {
            let shared = Arc::clone(&bg.next_block_id);
            handles.push(std::thread::spawn(move || {
                shared.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(bg.next_block_id.load(Ordering::SeqCst), 8);
    }

    // --- add_data respects the Active state guard ---

    #[test]
    fn add_data_not_active() {
        let bg = BlockGenerator::new(0, Duration::from_secs(1));
        // Generator is Initialised, not Active — data must be silently dropped.
        bg.add_data(Box::new(42u32));
        assert_eq!(bg.current_buffer.lock().len(), 0);
    }

    #[test]
    fn add_data_active() {
        let bg = BlockGenerator::new(0, Duration::from_secs(60));
        bg.start();
        bg.add_data(Box::new(1u32));
        bg.add_data(Box::new(2u32));
        assert_eq!(bg.current_buffer.lock().len(), 2);
        bg.stop();
    }

    // --- start / stop lifecycle ---

    #[test]
    fn active_after_start() {
        let bg = BlockGenerator::new(0, Duration::from_secs(60));
        assert!(!bg.is_active());
        bg.start();
        assert!(bg.is_active());
        bg.stop();
        assert!(!bg.is_active());
    }

    // --- StreamBlockId helpers ---

    #[test]
    fn block_id_name() {
        let id = StreamBlockId::new(3, 17);
        assert_eq!(id.name(), "input-3-17");
    }

    #[test]
    fn block_id_equality() {
        let a = StreamBlockId::new(1, 2);
        let b = StreamBlockId::new(1, 2);
        let c = StreamBlockId::new(1, 3);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}

// RateLimiter (stub)

/// Controls the rate at which a receiver ingests data.
///
pub trait RateLimiter: Send + Sync {
    fn wait_to_push(&self);
    fn get_current_limit(&self) -> f64;
    fn update_rate(&self, new_rate: f64);
}

// ReceivedBlockInfo (used by scheduler/info.rs)

#[derive(Debug, Clone)]
pub struct ReceivedBlockInfo {
    pub stream_id: usize,
    pub block_id: StreamBlockId,
    pub num_records: Option<u64>,
    pub metadata: Option<String>,
}
