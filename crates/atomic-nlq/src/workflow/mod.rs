pub mod agent_loop;
pub mod executor;
pub mod streaming;

pub use agent_loop::{AgentContext, AgentLoop, AgentResult};
pub use executor::WorkflowExecutor;
pub use streaming::{AgentEvent, AgentEventSender, VisualizationSpec};

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A dependency graph of tool calls produced by the LLM planner.
///
/// Steps with an empty `depends_on` are independent and are dispatched in
/// parallel by `WorkflowExecutor`. Steps that list upstream ids wait until
/// those results are available before being submitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowPlan {
    pub steps: Vec<WorkflowStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStep {
    /// Unique identifier for this step within the plan.
    pub id: String,
    /// Name of the tool to call (must be registered in `ToolRegistry`).
    pub tool: String,
    /// Arguments to pass to the tool.
    pub args: HashMap<String, Value>,
    /// IDs of steps whose results must be available before this step runs.
    pub depends_on: Vec<String>,
}

/// The output produced by a single workflow step.
#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_id: String,
    pub output: StepOutput,
}

#[derive(Debug, Clone)]
pub enum StepOutput {
    /// Arrow RecordBatches serialized as IPC bytes (one entry per partition).
    DataFrame(Vec<Vec<u8>>),
    /// Plain text result.
    Text(String),
    Empty,
}

impl StepOutput {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            StepOutput::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, StepOutput::Empty)
    }
}
