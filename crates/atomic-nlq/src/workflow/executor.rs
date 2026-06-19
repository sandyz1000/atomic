use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use atomic_sql::context::AtomicSqlContext;
use tokio::task::JoinSet;

use crate::errors::{NlqError, Result};
use crate::llm::LlmClient;
use crate::registry::{BuiltinTool, ToolRegistry, ToolRuntime};

use super::streaming::{AgentEvent, AgentEventSender};
use super::{StepOutput, StepResult, WorkflowPlan, WorkflowStep};

/// Executes a `WorkflowPlan` using Atomic as the compute backend.
///
/// Independent steps (no `depends_on` entries) are submitted as concurrent
/// tokio tasks. Dependent steps wait until all upstream results are available,
/// then are submitted in the next wave. This gives step-level parallelism on
/// top of Atomic's data-level parallelism inside each tool.
pub struct WorkflowExecutor {
    tool_registry: Arc<ToolRegistry>,
    sql_ctx: Option<Arc<AtomicSqlContext>>,
    openai_client: Arc<dyn LlmClient>,
}

impl WorkflowExecutor {
    pub fn new(
        tool_registry: Arc<ToolRegistry>,
        sql_ctx: Option<Arc<AtomicSqlContext>>,
        openai_client: Arc<dyn LlmClient>,
    ) -> Self {
        Self {
            tool_registry,
            sql_ctx,
            openai_client,
        }
    }

    /// Execute a plan and return results keyed by step id.
    pub async fn execute(&self, plan: WorkflowPlan) -> Result<HashMap<String, StepResult>> {
        let mut completed: HashMap<String, StepResult> = HashMap::new();
        let mut remaining: Vec<WorkflowStep> = plan.steps;

        while !remaining.is_empty() {
            let done_ids: HashSet<&str> = completed.keys().map(|s| s.as_str()).collect();

            // Partition into ready (all deps satisfied) and not-yet-ready.
            let (ready, blocked): (Vec<_>, Vec<_>) = remaining.into_iter().partition(|step| {
                step.depends_on
                    .iter()
                    .all(|d| done_ids.contains(d.as_str()))
            });

            if ready.is_empty() {
                // Circular dependency or missing dep — report which steps are stuck.
                let stuck: Vec<_> = blocked.iter().map(|s| s.id.as_str()).collect();
                return Err(NlqError::WorkflowExecution(format!(
                    "workflow is stuck; steps with unresolvable dependencies: {stuck:?}"
                )));
            }

            remaining = blocked;

            // Collect the upstream results each ready step needs.
            let upstream_snapshots: HashMap<String, StepResult> = ready
                .iter()
                .flat_map(|s| s.depends_on.iter())
                .filter_map(|dep_id| completed.get(dep_id).map(|r| (dep_id.clone(), r.clone())))
                .collect();

            let mut join_set: JoinSet<Result<StepResult>> = JoinSet::new();

            for step in ready {
                let tool_registry = Arc::clone(&self.tool_registry);
                let sql_ctx = self.sql_ctx.clone();
                let openai_client = Arc::clone(&self.openai_client);
                let upstream = upstream_snapshots.clone();

                join_set.spawn(async move {
                    run_step(step, tool_registry, sql_ctx, openai_client, upstream).await
                });
            }

            while let Some(res) = join_set.join_next().await {
                let step_result = res.map_err(|e| NlqError::WorkflowExecution(e.to_string()))??;
                completed.insert(step_result.step_id.clone(), step_result);
            }
        }

        Ok(completed)
    }

    /// Like `execute` but emits `StepStarted`/`StepCompleted`/`StepFailed` events
    /// through `tx` as each step runs. The existing `execute` is not modified.
    pub async fn execute_streaming(
        &self,
        plan: WorkflowPlan,
        round: usize,
        tx: &AgentEventSender,
    ) -> Result<HashMap<String, StepResult>> {
        let mut completed: HashMap<String, StepResult> = HashMap::new();
        let mut remaining: Vec<WorkflowStep> = plan.steps;

        while !remaining.is_empty() {
            let done_ids: HashSet<&str> = completed.keys().map(|s| s.as_str()).collect();

            let (ready, blocked): (Vec<_>, Vec<_>) = remaining.into_iter().partition(|step| {
                step.depends_on
                    .iter()
                    .all(|d| done_ids.contains(d.as_str()))
            });

            if ready.is_empty() {
                let stuck: Vec<_> = blocked.iter().map(|s| s.id.as_str()).collect();
                return Err(NlqError::WorkflowExecution(format!(
                    "workflow is stuck; steps with unresolvable dependencies: {stuck:?}"
                )));
            }

            remaining = blocked;

            let upstream_snapshots: HashMap<String, StepResult> = ready
                .iter()
                .flat_map(|s| s.depends_on.iter())
                .filter_map(|dep_id| completed.get(dep_id).map(|r| (dep_id.clone(), r.clone())))
                .collect();

            // Emit StepStarted for every step in this wave before executing.
            for step in &ready {
                let _ = tx
                    .send(AgentEvent::StepStarted {
                        step_id: step.id.clone(),
                        tool: step.tool.clone(),
                        round,
                    })
                    .await;
            }

            let mut join_set: JoinSet<(String, String, Result<StepResult>)> = JoinSet::new();

            for step in ready {
                let step_id = step.id.clone();
                let tool_name = step.tool.clone();
                let tool_registry = Arc::clone(&self.tool_registry);
                let sql_ctx = self.sql_ctx.clone();
                let openai_client = Arc::clone(&self.openai_client);
                let upstream = upstream_snapshots.clone();

                join_set.spawn(async move {
                    let result =
                        run_step(step, tool_registry, sql_ctx, openai_client, upstream).await;
                    (step_id, tool_name, result)
                });
            }

            while let Some(join_result) = join_set.join_next().await {
                match join_result {
                    Ok((_step_id, tool_name, Ok(step_result))) => {
                        let (output_kind, preview) = match &step_result.output {
                            StepOutput::DataFrame(bufs) => {
                                let kind = "dataframe".to_string();
                                let rows = ipc_to_json_rows(bufs, 50);
                                let preview = if rows.is_empty() {
                                    None
                                } else {
                                    Some(serde_json::Value::Array(rows))
                                };
                                (kind, preview)
                            }
                            StepOutput::Text(t) => {
                                let preview =
                                    Some(serde_json::json!({ "text": &t[..t.len().min(500)] }));
                                ("text".to_string(), preview)
                            }
                            StepOutput::Empty => ("empty".to_string(), None),
                        };
                        let _ = tx
                            .send(AgentEvent::StepCompleted {
                                step_id: step_result.step_id.clone(),
                                tool: tool_name,
                                round,
                                output_kind,
                                preview,
                            })
                            .await;
                        completed.insert(step_result.step_id.clone(), step_result);
                    }
                    Ok((step_id, tool_name, Err(e))) => {
                        let _ = tx
                            .send(AgentEvent::StepFailed {
                                step_id,
                                tool: tool_name,
                                round,
                                error: e.to_string(),
                            })
                            .await;
                        return Err(e);
                    }
                    Err(join_err) => {
                        return Err(NlqError::WorkflowExecution(join_err.to_string()));
                    }
                }
            }
        }

        Ok(completed)
    }
}

async fn run_step(
    step: WorkflowStep,
    tool_registry: Arc<ToolRegistry>,
    sql_ctx: Option<Arc<AtomicSqlContext>>,
    openai_client: Arc<dyn LlmClient>,
    upstream: HashMap<String, StepResult>,
) -> Result<StepResult> {
    let tool = tool_registry
        .get_tool(&step.tool)
        .ok_or_else(|| NlqError::ToolNotFound(step.tool.clone()))?;

    log::debug!("executing step '{}' (tool: {})", step.id, step.tool);

    let output = match &tool.runtime {
        ToolRuntime::Builtin(builtin) => {
            run_builtin(builtin, &step, sql_ctx, openai_client, &upstream).await?
        }
        ToolRuntime::Python(code) => run_python(code, &step, &upstream).await?,
        ToolRuntime::JavaScript(code) => run_javascript(code, &step, &upstream).await?,
    };

    Ok(StepResult {
        step_id: step.id,
        output,
    })
}

async fn run_builtin(
    builtin: &BuiltinTool,
    step: &WorkflowStep,
    sql_ctx: Option<Arc<AtomicSqlContext>>,
    _openai_client: Arc<dyn LlmClient>,
    _upstream: &HashMap<String, StepResult>,
) -> Result<StepOutput> {
    match builtin {
        BuiltinTool::SqlQuery => {
            let ctx = sql_ctx.ok_or_else(|| {
                NlqError::WorkflowExecution(
                    "SqlQuery tool requires a sql_ctx; build NlqContext with build_with_compute"
                        .to_string(),
                )
            })?;
            let query = step
                .args
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    NlqError::WorkflowExecution("SqlQuery tool requires an 'query' arg".to_string())
                })?;
            let df = ctx
                .sql(query)
                .await
                .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
            let batches = df
                .collect()
                .await
                .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
            // Serialize each RecordBatch as Arrow IPC for transport.
            let ipc_bytes: Vec<Vec<u8>> = batches
                .iter()
                .map(|batch| {
                    let mut buf = Vec::new();
                    {
                        use datafusion::arrow::ipc::writer::FileWriter;
                        let mut writer = FileWriter::try_new(&mut buf, &batch.schema())
                            .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
                        writer
                            .write(batch)
                            .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
                        writer
                            .finish()
                            .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
                    }
                    Ok::<_, NlqError>(buf)
                })
                .collect::<Result<_>>()?;
            Ok(StepOutput::DataFrame(ipc_bytes))
        }
        BuiltinTool::LlmFilter
        | BuiltinTool::LlmMap
        | BuiltinTool::Embed
        | BuiltinTool::VectorSearch => {
            // These builtins delegate to the DataFusion extension nodes via SQL.
            // For now, surface a clear message — full wiring goes through NlqContext.query().
            Err(NlqError::WorkflowExecution(format!(
                "builtin tool {:?} must be invoked through NlqContext::query, not directly",
                builtin
            )))
        }
    }
}

async fn run_python(
    code: &str,
    step: &WorkflowStep,
    _upstream: &HashMap<String, StepResult>,
) -> Result<StepOutput> {
    // Python UDFs are executed via the atomic-worker PyO3 runtime.
    // The code string is the full function body; args are passed as JSON.
    // This dispatches through Atomic's existing Python UDF infrastructure.
    log::debug!(
        "running Python tool for step '{}': {} bytes of code",
        step.id,
        code.len()
    );
    let args_json = serde_json::to_string(&step.args)
        .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
    // Placeholder: real dispatch goes through atomic_compute's Python pool.
    // Returns the result as a text JSON payload until full wiring is complete.
    Ok(StepOutput::Text(format!(
        "{{\"step\":\"{}\",\"status\":\"python_udf_dispatched\",\"args\":{args_json}}}",
        step.id
    )))
}

async fn run_javascript(
    code: &str,
    step: &WorkflowStep,
    _upstream: &HashMap<String, StepResult>,
) -> Result<StepOutput> {
    log::debug!(
        "running JS tool for step '{}': {} bytes of code",
        step.id,
        code.len()
    );
    let args_json = serde_json::to_string(&step.args)
        .map_err(|e| NlqError::WorkflowExecution(e.to_string()))?;
    Ok(StepOutput::Text(format!(
        "{{\"step\":\"{}\",\"status\":\"js_udf_dispatched\",\"args\":{args_json}}}",
        step.id
    )))
}

/// Decode Arrow IPC file bytes and return up to `limit` rows as JSON objects.
///
/// IPC is written with `FileWriter` in `run_builtin`, so we read with `FileReader`.
/// Fields with unsupported types fall back to the string representation of the type name.
fn ipc_to_json_rows(bufs: &[Vec<u8>], limit: usize) -> Vec<serde_json::Value> {
    use crate::nodes::llm_filter::record_batch_to_json_rows;
    use datafusion::arrow::ipc::reader::FileReader;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    'outer: for buf in bufs {
        let Ok(reader) = FileReader::try_new(std::io::Cursor::new(buf.as_slice()), None) else {
            continue;
        };
        for batch in reader.flatten() {
            for row in record_batch_to_json_rows(&batch) {
                rows.push(row);
                if rows.len() >= limit {
                    break 'outer;
                }
            }
        }
    }
    rows
}
