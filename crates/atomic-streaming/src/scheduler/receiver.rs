/// ReceiverTracker — manages distributed receiver lifecycle.
///
/// In local mode, receivers are managed directly by the input DStreams.
/// Distributed receiver scheduling is a TODO (Phase 4+).
use crate::receiver::ReceivedBlockInfo;
use parking_lot::Mutex;
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────────
// ReceiverState
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiverState {
    Inactive,
    Scheduled,
    Active,
}

#[derive(Debug, Clone)]
pub struct ReceiverInfo {
    pub stream_id: usize,
    pub state: ReceiverState,
    pub executor_id: Option<String>,
    pub host: Option<String>,
    pub last_error: Option<String>,
}

impl ReceiverInfo {
    pub fn new(stream_id: usize) -> Self {
        ReceiverInfo {
            stream_id,
            state: ReceiverState::Inactive,
            executor_id: None,
            host: None,
            last_error: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReceiverTracker
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks the state of receivers.
///
/// TODO Phase 4 (distributed): schedule receivers on workers, handle failures.
pub struct ReceiverTracker {
    infos: Mutex<HashMap<usize, ReceiverInfo>>,
    allocated_blocks: Mutex<HashMap<usize, Vec<ReceivedBlockInfo>>>,
}

impl ReceiverTracker {
    pub fn new() -> Self {
        ReceiverTracker {
            infos: Mutex::new(HashMap::new()),
            allocated_blocks: Mutex::new(HashMap::new()),
        }
    }

    pub fn register_receiver(&self, stream_id: usize) {
        self.infos
            .lock()
            .insert(stream_id, ReceiverInfo::new(stream_id));
    }

    pub fn deregister_receiver(&self, stream_id: usize) {
        self.infos.lock().remove(&stream_id);
    }

    pub fn receiver_info(&self, stream_id: usize) -> Option<ReceiverInfo> {
        self.infos.lock().get(&stream_id).cloned()
    }

    pub fn add_block(&self, stream_id: usize, block: ReceivedBlockInfo) {
        self.allocated_blocks
            .lock()
            .entry(stream_id)
            .or_default()
            .push(block);
    }

    pub fn get_blocks_and_clear(&self, stream_id: usize) -> Vec<ReceivedBlockInfo> {
        self.allocated_blocks
            .lock()
            .remove(&stream_id)
            .unwrap_or_default()
    }

    /// TODO Phase 4: distribute receivers across workers.
    pub fn start(&self) {
        log::info!("ReceiverTracker started (local mode — receivers managed by input DStreams)");
    }

    pub fn stop(&self) {
        log::info!("ReceiverTracker stopped");
    }
}

impl Default for ReceiverTracker {
    fn default() -> Self {
        Self::new()
    }
}
