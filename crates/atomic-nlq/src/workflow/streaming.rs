use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Structured visualization hint returned by `AgentLoop.evaluate()`.
/// The dashboard frontend maps `chart_type` → the appropriate Recharts component.
/// No LLM code generation involved — purely a data spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualizationSpec {
    /// "bar" | "line" | "pie" | "scatter" | "table"
    pub chart_type: String,
    pub x_axis: Option<String>,
    pub y_axis: Option<String>,
    pub group_by: Option<String>,
    pub title: Option<String>,
}

/// Events emitted by `AgentLoop::run_streaming` as the query executes.
/// Each event is serialized to JSON and sent as an SSE frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    /// LLM produced a plan for this round.
    PlanCreated {
        round: usize,
        step_count: usize,
    },
    /// A step has started execution.
    StepStarted {
        step_id: String,
        tool: String,
        round: usize,
    },
    /// A step completed successfully.
    StepCompleted {
        step_id: String,
        tool: String,
        round: usize,
        /// "dataframe" | "text" | "empty"
        output_kind: String,
        /// For Text: truncated preview. For DataFrame: first 10 rows as JSON array.
        #[serde(skip_serializing_if = "Option::is_none")]
        preview: Option<serde_json::Value>,
    },
    /// A step failed.
    StepFailed {
        step_id: String,
        tool: String,
        round: usize,
        error: String,
    },
    /// All steps in a round completed; LLM is evaluating whether the query is answered.
    RoundEvaluating {
        round: usize,
    },
    /// The agent is done; the final answer and optional visualization spec are ready.
    Done {
        answer: String,
        rounds: usize,
        total_steps: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        visualization: Option<VisualizationSpec>,
    },
    /// The agent hit `max_rounds` without a definitive answer.
    MaxRoundsReached {
        answer: String,
        rounds: usize,
    },
    /// An unrecoverable error occurred.
    Error {
        message: String,
    },
}

pub type AgentEventSender = mpsc::Sender<AgentEvent>;
