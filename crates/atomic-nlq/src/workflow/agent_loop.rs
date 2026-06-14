use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;

use crate::config::NlqConfig;
use crate::errors::{NlqError, Result};
use crate::planner::llm_planner::LlmPlanner;
use crate::registry::ToolRegistry;

use super::streaming::{AgentEvent, AgentEventSender, VisualizationSpec};
use super::{StepResult, WorkflowExecutor};

pub struct AgentLoop {
    planner: Arc<LlmPlanner>,
    executor: Arc<WorkflowExecutor>,
    config: Arc<NlqConfig>,
}

/// Accumulated state passed to the planner on each subsequent round.
#[derive(Default)]
pub struct AgentContext {
    pub previous_results: Vec<StepResult>,
    pub rounds: usize,
}

/// The final answer from the agent after one or more planning-execution rounds.
pub struct AgentResult {
    /// Human-readable answer synthesized from the last round's results.
    pub answer: String,
    /// All step results accumulated across all rounds.
    pub steps: Vec<StepResult>,
    /// Number of planning-execution rounds that ran.
    pub rounds: usize,
    /// Optional hint for how the dashboard should display the result.
    pub visualization: Option<VisualizationSpec>,
}

/// Evaluation decision returned by the LLM after each round.
struct Evaluation {
    is_done: bool,
    answer: String,
    visualization: Option<VisualizationSpec>,
}

impl AgentLoop {
    pub fn new(
        planner: Arc<LlmPlanner>,
        executor: Arc<WorkflowExecutor>,
        config: Arc<NlqConfig>,
    ) -> Self {
        Self {
            planner,
            executor,
            config,
        }
    }

    /// Translate a natural language query into a `WorkflowPlan` without executing it.
    ///
    /// Use this for dry-run / debugging: inspect which tools and SQL steps the planner
    /// would choose before committing to an actual execution.
    pub async fn plan_only(
        &self,
        nl_query: &str,
        tool_registry: &ToolRegistry,
        table_schemas: &HashMap<String, SchemaRef>,
    ) -> Result<crate::workflow::WorkflowPlan> {
        let ctx = AgentContext::default();
        self.planner
            .plan(nl_query, table_schemas, tool_registry, &ctx)
            .await
    }

    /// Run the agentic query loop.
    ///
    /// Each round:
    ///   1. LLM generates a `WorkflowPlan` (dependency graph of tool calls).
    ///   2. `WorkflowExecutor` runs the plan (parallel Atomic jobs).
    ///   3. LLM evaluates whether the results answer the query or another round is needed.
    ///   4. Repeat until done or `max_rounds` is reached.
    pub async fn run(
        &self,
        nl_query: &str,
        tool_registry: &ToolRegistry,
        table_schemas: &HashMap<String, SchemaRef>,
    ) -> Result<AgentResult> {
        let mut ctx = AgentContext::default();

        loop {
            let plan = self
                .planner
                .plan(nl_query, table_schemas, tool_registry, &ctx)
                .await?;

            log::debug!(
                "AgentLoop round {}: plan has {} steps",
                ctx.rounds + 1,
                plan.steps.len()
            );

            let results: HashMap<String, StepResult> = self.executor.execute(plan).await?;
            let step_results: Vec<StepResult> = results.into_values().collect();
            ctx.previous_results.extend(step_results);
            ctx.rounds += 1;

            let eval = self.evaluate(nl_query, &ctx).await?;

            if eval.is_done || ctx.rounds >= self.config.max_rounds {
                if !eval.is_done {
                    log::warn!(
                        "AgentLoop: max_rounds ({}) reached without a definitive answer",
                        self.config.max_rounds
                    );
                }
                return Ok(AgentResult {
                    answer: eval.answer,
                    steps: ctx.previous_results,
                    rounds: ctx.rounds,
                    visualization: eval.visualization,
                });
            }
        }
    }

    /// Streaming variant: emits `AgentEvent` through `tx` as each step executes.
    /// Returns the same `AgentResult` as `run()` when complete.
    pub async fn run_streaming(
        &self,
        nl_query: &str,
        tool_registry: &ToolRegistry,
        table_schemas: &HashMap<String, SchemaRef>,
        tx: AgentEventSender,
    ) -> Result<AgentResult> {
        let mut ctx = AgentContext::default();

        loop {
            let plan = self
                .planner
                .plan(nl_query, table_schemas, tool_registry, &ctx)
                .await?;

            let _ = tx
                .send(AgentEvent::PlanCreated {
                    round: ctx.rounds + 1,
                    step_count: plan.steps.len(),
                })
                .await;

            let results = self
                .executor
                .execute_streaming(plan, ctx.rounds + 1, &tx)
                .await?;

            let step_results: Vec<StepResult> = results.into_values().collect();
            ctx.previous_results.extend(step_results);
            ctx.rounds += 1;

            let _ = tx
                .send(AgentEvent::RoundEvaluating { round: ctx.rounds })
                .await;

            let eval = self.evaluate(nl_query, &ctx).await?;

            if eval.is_done || ctx.rounds >= self.config.max_rounds {
                let total_steps = ctx.previous_results.len();
                if eval.is_done {
                    let _ = tx
                        .send(AgentEvent::Done {
                            answer: eval.answer.clone(),
                            rounds: ctx.rounds,
                            total_steps,
                            visualization: eval.visualization.clone(),
                        })
                        .await;
                } else {
                    let _ = tx
                        .send(AgentEvent::MaxRoundsReached {
                            answer: eval.answer.clone(),
                            rounds: ctx.rounds,
                        })
                        .await;
                }
                return Ok(AgentResult {
                    answer: eval.answer,
                    steps: ctx.previous_results,
                    rounds: ctx.rounds,
                    visualization: eval.visualization,
                });
            }
        }
    }

    async fn evaluate(&self, nl_query: &str, ctx: &AgentContext) -> Result<Evaluation> {
        // Summarize available results for the LLM.
        let results_summary = ctx
            .previous_results
            .iter()
            .map(|r| {
                let kind = match &r.output {
                    super::StepOutput::DataFrame(batches) => {
                        format!("DataFrame ({} IPC buffers)", batches.len())
                    }
                    super::StepOutput::Text(t) => format!("Text: {}", &t[..t.len().min(200)]),
                    super::StepOutput::Empty => "Empty".to_string(),
                };
                format!("  step '{}': {kind}", r.step_id)
            })
            .collect::<Vec<_>>()
            .join("\n");

        let system = "You are evaluating whether a set of workflow step results \
            fully answers the user's original question.\n\
            Reply with a JSON object:\n\
            {\"done\": true/false, \"answer\": \"...\", \"visualization\": {\"chart_type\": \"...\", \"x_axis\": \"...\", \"y_axis\": \"...\", \"group_by\": \"...\", \"title\": \"...\"}}\n\
            - If done=true, provide a concise answer and a visualization hint.\n\
            - chart_type must be one of: bar, line, pie, scatter, table.\n\
            - x_axis, y_axis, group_by, title are optional column names or labels.\n\
            - If the result is pure text with no tabular data, omit the visualization field.\n\
            - If done=false, set answer to a brief explanation of what is still missing and omit visualization.\n\
            Output ONLY valid JSON, no prose.";

        let user = format!("Original question: {nl_query}\n\nResults so far:\n{results_summary}");

        let text = self
            .planner
            .client()
            .chat_with_retry(&self.config.model, system, &user, 512)
            .await?;

        let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            NlqError::PlanParse(format!("evaluation parse error: {e}; got: {text}"))
        })?;

        let is_done = json.get("done").and_then(|v| v.as_bool()).unwrap_or(true);
        let answer = json
            .get("answer")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let visualization = json
            .get("visualization")
            .and_then(|v| serde_json::from_value::<VisualizationSpec>(v.clone()).ok());

        Ok(Evaluation {
            is_done,
            answer,
            visualization,
        })
    }
}
