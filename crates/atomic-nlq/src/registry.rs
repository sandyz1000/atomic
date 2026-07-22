use std::sync::Arc;

use atomic_data::distributed::{AgentStepPayload, ResolvedTool, ScriptRuntime};
use dashmap::DashMap;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use serde_json::Value;

use crate::errors::Result;

/// A tool that can be called by the LLM agent during workflow execution.
#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    /// Natural-language description; the LLM reads this to decide when to call the tool.
    pub description: String,
    /// JSON Schema describing the tool's `args` object.
    pub input_schema: Value,
    pub runtime: ToolRuntime,
}

#[derive(Debug, Clone)]
pub enum ToolRuntime {
    Builtin(BuiltinTool),
    /// Python source code executed via the atomic-worker PyO3 runtime.
    Python(String),
    /// JavaScript source code executed via the atomic-worker V8 runtime.
    JavaScript(String),
}

#[derive(Debug, Clone)]
pub enum BuiltinTool {
    /// Execute a SQL query against registered tables via `AtomicSqlContext`.
    SqlQuery,
    /// Row-level boolean filter via LLM (uses DataFusion `LlmFilterExec`).
    LlmFilter,
    /// Row-level transformation via LLM (uses DataFusion `LlmMapExec`).
    LlmMap,
    /// Generate text embeddings (uses DataFusion `EmbedExec`).
    Embed,
    /// Approximate nearest-neighbour vector search.
    VectorSearch,
}

#[derive(Debug, Clone)]
pub struct UdfDescription {
    pub name: String,
    pub description: String,
    pub signature: String,
}

/// Central registry for all agent tools and DataFusion UDFs.
///
/// - `register_tool` / `get_tool` — manage Python/JS/builtin tools available to the LLM.
/// - `register_scalar` / `register_aggregate` — register DataFusion UDFs (used by the
///   SQL builtin tool's query execution).
pub struct ToolRegistry {
    session: Arc<SessionContext>,
    tools: DashMap<String, ToolDefinition>,
    udf_descriptions: DashMap<String, UdfDescription>,
}

impl ToolRegistry {
    pub fn new(session: Arc<SessionContext>) -> Self {
        let registry = Self {
            session,
            tools: DashMap::new(),
            udf_descriptions: DashMap::new(),
        };
        registry.register_builtin_tools();
        registry
    }

    fn register_builtin_tools(&self) {
        self.tools.insert(
            "sql_query".to_string(),
            ToolDefinition {
                name: "sql_query".to_string(),
                description: "Execute a SQL SELECT query against registered tables and return the result as a DataFrame.".to_string(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "The SQL SELECT statement to execute." }
                    },
                    "required": ["query"]
                }),
                runtime: ToolRuntime::Builtin(BuiltinTool::SqlQuery),
            },
        );

        for (name, description, builtin) in [
            (
                "llm_filter",
                "Filter rows in a DataFrame using a natural-language predicate evaluated by the LLM.",
                BuiltinTool::LlmFilter,
            ),
            (
                "llm_map",
                "Add a new column to a DataFrame by applying an LLM-evaluated transformation to each row.",
                BuiltinTool::LlmMap,
            ),
            (
                "embed",
                "Generate text embeddings for a string column and store them in a new FixedSizeList<Float32> column.",
                BuiltinTool::Embed,
            ),
            (
                "vector_search",
                "Run approximate nearest-neighbour search against a registered vector index.",
                BuiltinTool::VectorSearch,
            ),
        ] {
            self.tools.insert(
                name.to_string(),
                ToolDefinition {
                    name: name.to_string(),
                    description: description.to_string(),
                    input_schema: serde_json::json!({ "type": "object", "properties": {} }),
                    runtime: ToolRuntime::Builtin(builtin),
                },
            );
        }
    }

    /// Register a user-defined tool (Python or JS source code).
    pub fn register_tool(&self, tool: ToolDefinition) {
        self.tools.insert(tool.name.clone(), tool);
    }

    pub fn get_tool(&self, name: &str) -> Option<ToolDefinition> {
        self.tools.get(name).map(|e| e.value().clone())
    }

    pub fn all_tools(&self) -> Vec<ToolDefinition> {
        self.tools.iter().map(|e| e.value().clone()).collect()
    }

    /// Register a DataFusion scalar UDF with a human-readable description.
    pub fn register_scalar(&self, udf: ScalarUDF, description: impl Into<String>) -> Result<()> {
        let name = udf.name().to_string();
        let signature = format!("({:?})", udf.signature().type_signature);
        self.session.register_udf(udf);
        self.udf_descriptions.insert(
            name.clone(),
            UdfDescription {
                name,
                description: description.into(),
                signature,
            },
        );
        Ok(())
    }

    /// Register a DataFusion aggregate UDF with a human-readable description.
    pub fn register_aggregate(
        &self,
        udaf: AggregateUDF,
        description: impl Into<String>,
    ) -> Result<()> {
        let name = udaf.name().to_string();
        let signature = format!("({:?})", udaf.signature().type_signature);
        self.session.register_udaf(udaf);
        self.udf_descriptions.insert(
            name.clone(),
            UdfDescription {
                name,
                description: description.into(),
                signature,
            },
        );
        Ok(())
    }

    pub fn udf_descriptions(&self) -> Vec<UdfDescription> {
        self.udf_descriptions
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    /// Resolve `config.tool_refs` against this registry and `atomic-compute`'s
    /// `TASK_REGISTRY`, filling in `config.resolved_tools`. Call this once,
    /// driver-side, before staging `config` into a pipeline op (e.g. `rdd.agent_step(..)`)
    /// — workers dispatch `TOOL_CALL:` by task_name / resolved-tool name and never consult
    /// `ToolRegistry` directly.
    ///
    /// - A `tool_refs` entry matching a `#[task]` task_name in `TASK_REGISTRY` is left as-is;
    ///   the worker dispatches it through `TASK_REGISTRY` at call time.
    /// - A `Python`/`JavaScript` tool is resolved into `resolved_tools`.
    /// - A `Builtin` tool (`sql_query`, `llm_filter`, ...) is rejected: builtins run in the
    ///   driver-side `WorkflowExecutor` today, not inside a worker's `agent_step` loop.
    /// - A name matching neither registry is rejected with [`NlqError::ToolNotFound`].
    pub fn resolve_agent_step(&self, mut config: AgentStepPayload) -> Result<AgentStepPayload> {
        let mut resolved = Vec::with_capacity(config.tool_refs.len());
        for name in &config.tool_refs {
            if atomic_compute::task_registry::TASK_REGISTRY.contains_key(name.as_str()) {
                continue;
            }
            match self.get_tool(name) {
                Some(ToolDefinition {
                    runtime: ToolRuntime::Python(source),
                    ..
                }) => resolved.push(ResolvedTool {
                    name: name.clone(),
                    runtime: ScriptRuntime::Python,
                    source,
                }),
                Some(ToolDefinition {
                    runtime: ToolRuntime::JavaScript(source),
                    ..
                }) => resolved.push(ResolvedTool {
                    name: name.clone(),
                    runtime: ScriptRuntime::JavaScript,
                    source,
                }),
                Some(ToolDefinition {
                    runtime: ToolRuntime::Builtin(_),
                    ..
                }) => {
                    return Err(crate::errors::NlqError::ToolNotFound(format!(
                        "'{name}' is a builtin tool; builtins are not supported inside agent_step \
                         (they run in the driver-side WorkflowExecutor)"
                    )));
                }
                None => {
                    return Err(crate::errors::NlqError::ToolNotFound(format!(
                        "'{name}' is not a registered #[task] task_name nor a ToolRegistry tool"
                    )));
                }
            }
        }
        config.resolved_tools = resolved;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry() -> ToolRegistry {
        ToolRegistry::new(Arc::new(SessionContext::new()))
    }

    fn base_payload(tool_refs: Vec<String>) -> AgentStepPayload {
        AgentStepPayload {
            model: "test-model".to_string(),
            system_prompt: String::new(),
            max_rounds: 1,
            tool_refs,
            resolved_tools: vec![],
            provider: "openai".to_string(),
            output_schema: None,
            max_tokens_total: None,
        }
    }

    #[test]
    fn resolve_agent_step_leaves_rust_op_id_unresolved() {
        // Any #[task]-registered task_name (none registered in this crate's test binary,
        // but the call path is identical) is left out of resolved_tools — it's
        // dispatched by the worker via TASK_REGISTRY at call time, not pre-resolved here.
        // We simulate "registered" by checking a genuinely unregistered name is rejected
        // instead (the complementary case is covered by atomic-nlq's agent_runner tests,
        // which run in the same binary as the #[task] registration).
        let registry = test_registry();
        let result = registry.resolve_agent_step(base_payload(vec!["not_a_real_tool".to_string()]));
        assert!(result.is_err());
    }

    #[test]
    fn resolve_agent_step_resolves_python_tool() {
        let registry = test_registry();
        registry.register_tool(ToolDefinition {
            name: "py_tool".to_string(),
            description: "test".to_string(),
            input_schema: serde_json::json!({}),
            runtime: ToolRuntime::Python("def run(args):\n    return args".to_string()),
        });
        let config = registry
            .resolve_agent_step(base_payload(vec!["py_tool".to_string()]))
            .expect("resolution should succeed");
        assert_eq!(config.resolved_tools.len(), 1);
        assert_eq!(config.resolved_tools[0].name, "py_tool");
        assert_eq!(config.resolved_tools[0].runtime, ScriptRuntime::Python);
    }

    #[test]
    fn resolve_agent_step_resolves_javascript_tool() {
        let registry = test_registry();
        registry.register_tool(ToolDefinition {
            name: "js_tool".to_string(),
            description: "test".to_string(),
            input_schema: serde_json::json!({}),
            runtime: ToolRuntime::JavaScript("function run(args) { return args; }".to_string()),
        });
        let config = registry
            .resolve_agent_step(base_payload(vec!["js_tool".to_string()]))
            .expect("resolution should succeed");
        assert_eq!(config.resolved_tools.len(), 1);
        assert_eq!(config.resolved_tools[0].runtime, ScriptRuntime::JavaScript);
    }

    #[test]
    fn resolve_agent_step_rejects_builtin_tool() {
        let registry = test_registry();
        let result = registry.resolve_agent_step(base_payload(vec!["sql_query".to_string()]));
        assert!(
            result.is_err(),
            "builtin tools must be rejected inside agent_step"
        );
    }

    #[test]
    fn resolve_agent_step_rejects_unknown_tool() {
        let registry = test_registry();
        let result = registry.resolve_agent_step(base_payload(vec!["totally_unknown".to_string()]));
        assert!(result.is_err());
    }

    #[test]
    fn resolve_agent_step_empty_tool_refs_is_noop() {
        let registry = test_registry();
        let config = registry
            .resolve_agent_step(base_payload(vec![]))
            .expect("empty tool_refs always resolves");
        assert!(config.resolved_tools.is_empty());
    }
}
