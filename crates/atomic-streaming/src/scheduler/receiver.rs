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
use crate::wal::WriteAheadLog;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

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
    /// Optional write-ahead log. When set, dispatched descriptors are logged before dispatch
    /// and cleared on commit, so a driver restart can recover the in-flight set.
    wal: Option<Arc<dyn WriteAheadLog>>,
}

impl ReceiverTracker {
    pub fn new(distributed: bool) -> Self {
        ReceiverTracker {
            distributed,
            infos: Mutex::new(HashMap::new()),
            allocated_blocks: Mutex::new(HashMap::new()),
            inflight_descriptors: Mutex::new(HashMap::new()),
            wal: None,
        }
    }

    /// Attach a [`WriteAheadLog`] so dispatched descriptors are durably logged before dispatch.
    pub fn with_wal(mut self, wal: Arc<dyn WriteAheadLog>) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Replay the WAL, returning the `(stream_id, descriptors)` entries logged but not cleared by
    /// a commit — the partitions to re-plan after a driver restart. Empty when no WAL is attached.
    pub fn recover_inflight(&self) -> Vec<(usize, Vec<String>)> {
        let Some(wal) = &self.wal else {
            return Vec::new();
        };
        let records = wal.read_all().unwrap_or_default();
        records.iter().filter_map(|r| decode_entry(r)).collect()
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
    /// in the current batch. When a WAL is attached, the entry is logged before dispatch.
    pub fn record_dispatched(&self, stream_id: usize, descriptors: Vec<String>) {
        if let Some(wal) = &self.wal
            && let Err(e) = wal.write(&encode_entry(stream_id, &descriptors))
        {
            log::warn!("ReceiverTracker: WAL write failed for stream {stream_id}: {e}");
        }
        self.inflight_descriptors
            .lock()
            .insert(stream_id, descriptors);
    }

    /// Mark the batch for `stream_id` as successfully committed (all partitions
    /// processed).  Removes the inflight tracking entry for this stream.
    ///
    /// With a WAL attached, a commit means the logged descriptors are no longer needed for
    /// recovery; when no stream remains in flight the WAL is cleared to bound its growth.
    pub fn record_batch_committed(&self, stream_id: usize) {
        let mut inflight = self.inflight_descriptors.lock();
        inflight.remove(&stream_id);
        if let Some(wal) = &self.wal
            && inflight.is_empty()
            && let Err(e) = wal.clear()
        {
            log::warn!("ReceiverTracker: WAL clear failed: {e}");
        }
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

/// Encode a `(stream_id, descriptors)` WAL entry as `stream_id\tdesc\tdesc…`. Descriptors are
/// opaque source-partition identifiers with no tab characters, so tab is a safe separator.
fn encode_entry(stream_id: usize, descriptors: &[String]) -> Vec<u8> {
    let mut parts = vec![stream_id.to_string()];
    parts.extend(descriptors.iter().cloned());
    parts.join("\t").into_bytes()
}

/// Inverse of [`encode_entry`]. Returns `None` for a malformed record.
fn decode_entry(bytes: &[u8]) -> Option<(usize, Vec<String>)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut parts = text.split('\t');
    let stream_id = parts.next()?.parse::<usize>().ok()?;
    Some((stream_id, parts.map(str::to_string).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::FileWriteAheadLog;

    #[test]
    fn test_entry_roundtrip() {
        let bytes = encode_entry(7, &["a".into(), "b".into()]);
        assert_eq!(
            decode_entry(&bytes),
            Some((7, vec!["a".into(), "b".into()]))
        );
    }

    #[test]
    fn test_wal_recovers_inflight() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(FileWriteAheadLog::open(dir.path()).unwrap());
        let tracker = ReceiverTracker::new(true).with_wal(wal);
        tracker.record_dispatched(1, vec!["p0".into(), "p1".into()]);
        // A fresh tracker over the same WAL replays the uncommitted dispatch.
        let wal2 = Arc::new(FileWriteAheadLog::open(dir.path()).unwrap());
        let recovered = ReceiverTracker::new(true).with_wal(wal2);
        assert_eq!(
            recovered.recover_inflight(),
            vec![(1usize, vec!["p0".to_string(), "p1".to_string()])]
        );
    }

    #[test]
    fn test_wal_cleared_on_commit() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(FileWriteAheadLog::open(dir.path()).unwrap());
        let tracker = ReceiverTracker::new(true).with_wal(wal);
        tracker.record_dispatched(1, vec!["p0".into()]);
        tracker.record_batch_committed(1);
        // Nothing left to recover once the only in-flight stream commits.
        assert!(tracker.recover_inflight().is_empty());
    }
}
