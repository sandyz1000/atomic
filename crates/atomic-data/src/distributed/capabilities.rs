use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::WIRE_SCHEMA_V1;

/// Advertised by a worker on the TCP handshake.
///
/// The driver reads this to decide whether the worker is compatible before
/// dispatching any tasks. See [`DistributedScheduler::register_worker`] for
/// the fingerprint-mismatch check.
#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct WorkerCapabilities {
    pub version: u16,
    pub worker_id: String,
    pub max_tasks: u16,
    /// Op IDs registered in this worker's `TASK_REGISTRY`.
    /// Empty means "unknown / accept all" — used for backwards compatibility with old workers.
    pub registered_ops: Vec<String>,
    /// Port of the worker's ShuffleManager HTTP server. Used by the driver heartbeat
    /// to probe `GET /health`. `None` if the shuffle server is not yet started.
    pub shuffle_server_port: Option<u16>,
    /// FNV-1a fingerprint of all `(op_id, body_hash)` pairs in the worker's
    /// `TASK_REGISTRY`, sorted by op_id. The driver checks this against its own
    /// fingerprint at registration time; a mismatch means the binaries diverged.
    /// Zero means "unknown" (old worker) — driver logs a warning but allows it.
    pub registry_fingerprint: u64,
}

impl WorkerCapabilities {
    pub fn new(worker_id: String, max_tasks: u16, registered_ops: Vec<String>) -> Self {
        Self {
            version: WIRE_SCHEMA_V1,
            worker_id,
            max_tasks,
            registered_ops,
            shuffle_server_port: None,
            registry_fingerprint: 0,
        }
    }

    pub fn with_shuffle_port(mut self, port: u16) -> Self {
        self.shuffle_server_port = Some(port);
        self
    }

    pub fn with_registry_fingerprint(mut self, fingerprint: u64) -> Self {
        self.registry_fingerprint = fingerprint;
        self
    }
}
