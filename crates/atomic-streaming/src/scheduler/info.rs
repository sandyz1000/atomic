use crate::receiver::ReceivedBlockInfo;
use std::collections::HashMap;
use std::sync::Arc;

// ─────────────────────────────────────────────────────────────────────────────
// StreamInputInfo
// ─────────────────────────────────────────────────────────────────────────────

/// Input information for a single stream at a given batch time.
#[derive(Debug, Clone)]
pub struct StreamInputInfo {
    pub input_stream_id: usize,
    pub num_records: u64,
    pub metadata: HashMap<String, String>,
}

impl StreamInputInfo {
    pub const METADATA_KEY_DESCRIPTION: &'static str = "Description";

    pub fn new(input_stream_id: usize, num_records: u64) -> Self {
        StreamInputInfo {
            input_stream_id,
            num_records,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn description(&self) -> Option<&str> {
        self.metadata
            .get(Self::METADATA_KEY_DESCRIPTION)
            .map(String::as_str)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OutputOperationInfo
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OutputOperationInfo {
    pub batch_time_ms: u64,
    pub output_op_id: usize,
    pub name: String,
    pub description: String,
    pub start_time_ms: Option<u64>,
    pub end_time_ms: Option<u64>,
    pub failure_reason: Option<String>,
}

impl OutputOperationInfo {
    pub fn new(batch_time_ms: u64, output_op_id: usize, name: impl Into<String>) -> Self {
        OutputOperationInfo {
            batch_time_ms,
            output_op_id,
            name: name.into(),
            description: String::new(),
            start_time_ms: None,
            end_time_ms: None,
            failure_reason: None,
        }
    }

    pub fn duration_ms(&self) -> Option<u64> {
        self.end_time_ms
            .zip(self.start_time_ms)
            .map(|(end, start)| end.saturating_sub(start))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BatchInfo
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BatchInfo {
    pub batch_time_ms: u64,
    pub stream_input_info: HashMap<usize, StreamInputInfo>,
    pub submission_time_ms: u64,
    pub processing_start_time_ms: Option<u64>,
    pub processing_end_time_ms: Option<u64>,
    pub output_op_infos: HashMap<usize, OutputOperationInfo>,
}

impl BatchInfo {
    pub fn new(batch_time_ms: u64, submission_time_ms: u64) -> Self {
        BatchInfo {
            batch_time_ms,
            stream_input_info: HashMap::new(),
            submission_time_ms,
            processing_start_time_ms: None,
            processing_end_time_ms: None,
            output_op_infos: HashMap::new(),
        }
    }

    pub fn scheduling_delay_ms(&self) -> Option<u64> {
        self.processing_start_time_ms
            .map(|start| start.saturating_sub(self.submission_time_ms))
    }

    pub fn processing_delay_ms(&self) -> Option<u64> {
        self.processing_end_time_ms
            .zip(self.processing_start_time_ms)
            .map(|(end, start)| end.saturating_sub(start))
    }

    pub fn total_delay_ms(&self) -> Option<u64> {
        self.processing_end_time_ms
            .map(|end| end.saturating_sub(self.submission_time_ms))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// InputInfoTracker
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks input info per batch for reporting and back-pressure.
pub struct InputInfoTracker {
    batch_time_to_input_info: parking_lot::Mutex<HashMap<u64, HashMap<usize, StreamInputInfo>>>,
}

impl InputInfoTracker {
    pub fn new() -> Self {
        InputInfoTracker {
            batch_time_to_input_info: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    pub fn report_info(&self, batch_time_ms: u64, infos: Vec<StreamInputInfo>) {
        let mut map = self.batch_time_to_input_info.lock();
        let entry = map.entry(batch_time_ms).or_default();
        for info in infos {
            entry.insert(info.input_stream_id, info);
        }
    }

    pub fn get_info(&self, batch_time_ms: u64) -> HashMap<usize, StreamInputInfo> {
        self.batch_time_to_input_info
            .lock()
            .get(&batch_time_ms)
            .cloned()
            .unwrap_or_default()
    }

    pub fn cleanup(&self, batch_time_ms: u64) {
        self.batch_time_to_input_info
            .lock()
            .retain(|&t, _| t >= batch_time_ms);
    }
}

impl Default for InputInfoTracker {
    fn default() -> Self {
        Self::new()
    }
}
