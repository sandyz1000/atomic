/// ReceiverTracker — tracks distributed source partition dispatch across batches.
///
/// For the **Direct** (pull) model used by [`DistributedInputDStream`] and
/// [`DirectKafkaInputDStream`], the driver is authoritative for all offsets and
/// file splits.  "Re-planning on worker death" is therefore implicit: the source's
/// [`DistributedSource::commit`] is never called for a failed partition, so the
/// next [`plan_batch`] automatically includes it again.
///
/// This tracker records which descriptors were dispatched per batch (for logging
/// and observability) and logs a warning when a worker is removed while partitions
/// from that batch are still in flight.
use crate::receiver::ReceivedBlockInfo;
use parking_lot::Mutex;
use std::collections::HashMap;

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

/// Tracks receiver / source-partition lifecycle across micro-batches.
pub struct ReceiverTracker {
    distributed: bool,
    infos: Mutex<HashMap<usize, ReceiverInfo>>,
    allocated_blocks: Mutex<HashMap<usize, Vec<ReceivedBlockInfo>>>,
    /// Descriptors of source partitions currently in flight, keyed by stream_id.
    /// An entry is added by `record_dispatched` and removed by `record_batch_committed`.
    /// On worker removal, any remaining entries here may need re-planning.
    inflight_descriptors: Mutex<HashMap<usize, Vec<String>>>,
}

impl ReceiverTracker {
    pub fn new(distributed: bool) -> Self {
        ReceiverTracker {
            distributed,
            infos: Mutex::new(HashMap::new()),
            allocated_blocks: Mutex::new(HashMap::new()),
            inflight_descriptors: Mutex::new(HashMap::new()),
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

    /// Record that `descriptors` source partitions were dispatched for `stream_id`
    /// in the current batch.
    pub fn record_dispatched(&self, stream_id: usize, descriptors: Vec<String>) {
        self.inflight_descriptors
            .lock()
            .insert(stream_id, descriptors);
    }

    /// Mark the batch for `stream_id` as successfully committed (all partitions
    /// processed).  Removes the inflight tracking entry for this stream.
    pub fn record_batch_committed(&self, stream_id: usize) {
        self.inflight_descriptors.lock().remove(&stream_id);
    }

    /// Called when a worker is removed (heartbeat failure or explicit removal).
    ///
    /// Logs a warning if any source partitions were in flight on the removed host.
    /// The actual re-planning is implicit — any uncommitted partition's
    /// `DistributedSource::commit` was never called, so the next `plan_batch`
    /// re-includes it automatically.
    pub fn on_worker_removed(&self, host: &str) {
        let inflight = self.inflight_descriptors.lock();
        let total: usize = inflight.values().map(|v| v.len()).sum();
        if total > 0 {
            log::warn!(
                "ReceiverTracker: worker {host} removed; {total} inflight source \
                 partition(s) will be re-planned on the next batch (at-least-once)"
            );
        }
    }

    pub fn start(&self) {
        if self.distributed {
            log::info!(
                "ReceiverTracker started (distributed mode — source partitions \
                 dispatched via DistributedSource Direct model)"
            );
        } else {
            log::info!(
                "ReceiverTracker started (local mode — receivers managed by input DStreams)"
            );
        }
    }

    pub fn stop(&self) {
        log::info!("ReceiverTracker stopped");
    }
}

impl Default for ReceiverTracker {
    fn default() -> Self {
        Self::new(false)
    }
}
