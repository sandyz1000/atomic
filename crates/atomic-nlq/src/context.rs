use std::collections::HashMap;
use std::sync::Arc;

use atomic_sql::context::AtomicSqlContext;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;

use crate::config::{LlmProvider, NlqConfig};
use crate::errors::Result;
use crate::llm::LlmClient;
use crate::llm::anthropic::AnthropicClient;
use crate::openai::OpenAiClient;
use crate::optimizer::llm_batching_rule::LlmBatchingRule;
use crate::physical::extension_planner::{NlqExtensionPlanner, NlqQueryPlanner};
use crate::planner::llm_planner::LlmPlanner;
use crate::registry::{ToolDefinition, ToolRegistry};
use crate::vector::provider::VectorIndexProvider;
use crate::workflow::{AgentLoop, AgentResult, WorkflowExecutor};

/// Primary entry point for natural language queries against Atomic.
///
/// Build with [`NlqContext::build`] or [`NlqContext::build_with_compute`],
/// register tables and tools, then call [`query`].
pub struct NlqContext {
    sql_ctx: Arc<AtomicSqlContext>,
    agent_loop: AgentLoop,
    pub registry: Arc<ToolRegistry>,
    /// Retained NLQ configuration (model, rounds, batching); referenced by future
    /// agent tuning, not read on the current query path.
    #[allow(dead_code)]
    config: Arc<NlqConfig>,
    vector_indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>>,
}

impl NlqContext {
    pub fn build(config: NlqConfig) -> Self {
        Self::build_inner(config, None)
    }

    pub fn build_with_compute(
        config: NlqConfig,
        sc: Arc<atomic_compute::context::Context>,
    ) -> Self {
        Self::build_inner(config, Some(sc))
    }

    fn build_inner(
        config: NlqConfig,
        compute: Option<Arc<atomic_compute::context::Context>>,
    ) -> Self {
        if let Err(e) = config.validate() {
            panic!("NlqConfig is invalid: {e}");
        }
        let config = Arc::new(config);
        let client: Arc<dyn LlmClient> = match config.provider {
            LlmProvider::OpenAi => Arc::new(OpenAiClient::new(
                &config.api_key,
                &config.base_url,
                config.timeout_secs,
                config.max_retries,
            )),
            LlmProvider::Anthropic => Arc::new(AnthropicClient::new(
                &config.api_key,
                &config.base_url,
                config.timeout_secs,
                config.max_retries,
            )),
        };
        let vector_indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>> =
            Arc::new(DashMap::new());

        // Build the DataFusion session with LLM extension nodes (used by builtin tools).
        let extension_planner =
            NlqExtensionPlanner::new(client.clone(), config.clone(), vector_indexes.clone());
        let query_planner = NlqQueryPlanner::new(extension_planner);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(LlmBatchingRule::new(config.llm_batch_size)))
            .with_query_planner(Arc::new(query_planner))
            .build();
        let session = SessionContext::new_with_state(state);
        let registry_session = Arc::new(session.clone());
        let sql_ctx = Arc::new(AtomicSqlContext::from_session(session, compute));

        let registry = Arc::new(ToolRegistry::new(registry_session));
        let planner = Arc::new(LlmPlanner::new(client.clone(), config.clone()));
        let executor = Arc::new(WorkflowExecutor::new(
            Arc::clone(&registry),
            Some(Arc::clone(&sql_ctx)),
            client,
        ));
        let agent_loop = AgentLoop::new(planner, executor, config.clone());

        Self {
            sql_ctx,
            agent_loop,
            registry,
            config,
            vector_indexes,
        }
    }

    /// Register an RDD-backed table (requires `build_with_compute`).
    pub fn register_rdd(
        &self,
        name: &str,
        rdd: atomic_compute::rdd::TypedRdd<datafusion::arrow::record_batch::RecordBatch>,
    ) -> Result<()> {
        self.sql_ctx
            .register_rdd(name, rdd)
            .map_err(|e| crate::errors::NlqError::Internal(e.to_string()))
    }

    /// Register a user-defined tool (Python or JS source code).
    pub fn register_tool(&self, tool: ToolDefinition) {
        self.registry.register_tool(tool);
    }

    /// Register a `VectorIndexProvider` for the `vector_search` builtin tool.
    pub fn register_vector_index(&self, index: Arc<dyn VectorIndexProvider>) {
        self.vector_indexes.insert(index.name().to_string(), index);
    }

    pub fn vector_index(&self, name: &str) -> Option<Arc<dyn VectorIndexProvider>> {
        self.vector_indexes.get(name).map(|e| e.value().clone())
    }

    pub fn sql_ctx(&self) -> &AtomicSqlContext {
        &self.sql_ctx
    }

    /// Dry-run: return the `WorkflowPlan` the LLM would produce for `nl` without executing it.
    ///
    /// Useful for debugging and inspecting which tools / SQL steps would be chosen.
    /// Pretty-print the result with `serde_json::to_string_pretty(&plan)`.
    pub async fn plan(&self, nl: &str) -> Result<crate::workflow::WorkflowPlan> {
        log::info!("NlqContext::plan (dry-run) — {nl:?}");
        let table_schemas = self.list_table_schemas().await?;
        self.agent_loop
            .plan_only(nl, &self.registry, &table_schemas)
            .await
    }

    /// Streaming variant: emits `AgentEvent` through `tx` as the query executes.
    /// Use this from the dashboard SSE endpoint instead of `query()`.
    pub async fn query_streaming(
        &self,
        nl: &str,
        tx: tokio::sync::mpsc::Sender<crate::workflow::AgentEvent>,
    ) -> Result<AgentResult> {
        let table_schemas = self.list_table_schemas().await?;
        self.agent_loop
            .run_streaming(nl, &self.registry, &table_schemas, tx)
            .await
    }

    /// Translate a natural language query into an executed `AgentResult`.
    ///
    /// The agent plans → executes → evaluates in a loop until it is satisfied
    /// with the result or `max_rounds` is reached.
    pub async fn query(&self, nl: &str) -> Result<AgentResult> {
        let t0 = std::time::Instant::now();
        log::info!("NlqContext::query — {nl:?}");

        let table_schemas = self.list_table_schemas().await?;
        let result = self
            .agent_loop
            .run(nl, &self.registry, &table_schemas)
            .await?;

        log::info!(
            "NlqContext::query completed in {}ms ({} rounds, {} steps)",
            t0.elapsed().as_millis(),
            result.rounds,
            result.steps.len()
        );
        Ok(result)
    }

    async fn list_table_schemas(&self) -> Result<HashMap<String, SchemaRef>> {
        let mut result = HashMap::new();
        let session = self.sql_ctx.inner();
        for catalog_name in session.catalog_names() {
            let Some(catalog) = session.catalog(&catalog_name) else {
                continue;
            };
            for schema_name in catalog.schema_names() {
                let Some(schema_prov) = catalog.schema(&schema_name) else {
                    continue;
                };
                for table_name in schema_prov.table_names() {
                    if let Ok(Some(table)) = schema_prov.table(&table_name).await {
                        result.insert(table_name, table.schema());
                    }
                }
            }
        }
        Ok(result)
    }
}
