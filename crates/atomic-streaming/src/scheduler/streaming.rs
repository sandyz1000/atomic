use super::info::{BatchInfo, OutputOperationInfo};

/// Events emitted by the streaming scheduler for monitoring and UI.
#[derive(Debug)]
pub enum StreamingListenerEvent {
    StreamingStarted {
        time_ms: u64,
    },
    BatchSubmitted {
        batch_info: BatchInfo,
    },
    BatchStarted {
        batch_info: BatchInfo,
    },
    BatchCompleted {
        batch_info: BatchInfo,
    },
    OutputOperationStarted {
        output_operation_info: OutputOperationInfo,
    },
    OutputOperationCompleted {
        output_operation_info: OutputOperationInfo,
    },
    ReceiverStarted {
        stream_id: usize,
    },
    ReceiverStopped {
        stream_id: usize,
        message: String,
    },
    ReceiverError {
        stream_id: usize,
        message: String,
    },
}

/// A listener for streaming lifecycle events.
pub trait StreamingListener: Send + Sync + 'static {
    fn on_event(&self, event: &StreamingListenerEvent) {}
}
