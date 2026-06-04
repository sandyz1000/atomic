use std::sync::Arc;

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
            ("llm_filter", "Filter rows in a DataFrame using a natural-language predicate evaluated by the LLM.", BuiltinTool::LlmFilter),
            ("llm_map", "Add a new column to a DataFrame by applying an LLM-evaluated transformation to each row.", BuiltinTool::LlmMap),
            ("embed", "Generate text embeddings for a string column and store them in a new FixedSizeList<Float32> column.", BuiltinTool::Embed),
            ("vector_search", "Run approximate nearest-neighbour search against a registered vector index.", BuiltinTool::VectorSearch),
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

    /// Return all tool definitions (used to build the LLM system prompt).
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
            UdfDescription { name, description: description.into(), signature },
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
            UdfDescription { name, description: description.into(), signature },
        );
        Ok(())
    }

    pub fn udf_descriptions(&self) -> Vec<UdfDescription> {
        self.udf_descriptions.iter().map(|e| e.value().clone()).collect()
    }
}
