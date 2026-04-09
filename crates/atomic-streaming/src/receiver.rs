/// Receiver infrastructure for streaming data ingestion.
///
/// `BlockGenerator` buffers incoming items and periodically emits blocks.
/// Receivers are started/stopped by the `ReceiverTracker` (TODO Phase 4 — distributed).
use crate::utils::fileutils::StorageLevel;
use parking_lot::Mutex;
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// Block identifier
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// ReceivedBlock — what a receiver hands to the block generator
// ─────────────────────────────────────────────────────────────────────────────

pub enum ReceivedBlock {
    /// A batch of items as boxed Any values.
    DataIterator(Vec<Box<dyn Any + Send>>),
    /// A block already identified by its ID.
    DataBlock { block_id: StreamBlockId, data: Vec<Box<dyn Any + Send>> },
}

// ─────────────────────────────────────────────────────────────────────────────
// Receiver trait
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// BlockGeneratorListener
// ─────────────────────────────────────────────────────────────────────────────

/// Callbacks fired by BlockGenerator on block lifecycle events.
pub trait BlockGeneratorListener: Send + Sync + 'static {
    fn on_add_data(&self, data: &Box<dyn Any + Send>, metadata: Option<&dyn Any>) {}
    fn on_generate_block(&self, block_id: &StreamBlockId, iterator: &[Box<dyn Any + Send>]) {}
    fn on_push_block(&self, block_id: &StreamBlockId) {}
    fn on_error(&self, message: &str, throwable: Option<&dyn std::error::Error>) {
        log::error!("BlockGenerator error: {}", message);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GeneratorState
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeneratorState {
    Initialised,
    Active,
    StoppedAddingData,
    StoppedGeneratingBlocks,
    Stopped,
}

// ─────────────────────────────────────────────────────────────────────────────
// BlockGenerator
// ─────────────────────────────────────────────────────────────────────────────

/// Buffers incoming items and periodically pushes them as blocks.
pub struct BlockGenerator {
    stream_id: usize,
    block_interval: Duration,
    current_buffer: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
    state: Mutex<GeneratorState>,
    next_block_id: AtomicUsize,
    stopped: Arc<AtomicBool>,
}

impl BlockGenerator {
    pub fn new(stream_id: usize, block_interval: Duration) -> Self {
        BlockGenerator {
            stream_id,
            block_interval,
            current_buffer: Arc::new(Mutex::new(Vec::new())),
            state: Mutex::new(GeneratorState::Initialised),
            next_block_id: AtomicUsize::new(0),
            stopped: Arc::new(AtomicBool::new(false)),
        }
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

        std::thread::Builder::new()
            .name(format!("block-generator-{}", stream_id))
            .spawn(move || {
                while !stopped.load(Ordering::SeqCst) {
                    std::thread::sleep(interval);
                    let block: Vec<_> = buffer.lock().drain(..).collect();
                    if !block.is_empty() {
                        log::debug!(
                            "BlockGenerator[{}]: pushed block with {} items",
                            stream_id,
                            block.len()
                        );
                        // TODO Phase 4: hand block to BlockManager / ReceiverTracker
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

// ─────────────────────────────────────────────────────────────────────────────
// RateLimiter (stub)
// ─────────────────────────────────────────────────────────────────────────────

/// Controls the rate at which a receiver ingests data.
///
/// TODO Phase 4: implement PID-based back-pressure controller.
pub trait RateLimiter: Send + Sync {
    fn wait_to_push(&self);
    fn get_current_limit(&self) -> f64;
    fn update_rate(&self, new_rate: f64);
}

// ─────────────────────────────────────────────────────────────────────────────
// ReceivedBlockInfo (used by scheduler/info.rs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ReceivedBlockInfo {
    pub stream_id: usize,
    pub block_id: StreamBlockId,
    pub num_records: Option<u64>,
    pub metadata: Option<String>,
}
