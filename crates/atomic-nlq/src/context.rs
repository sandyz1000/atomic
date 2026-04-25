use std::collections::HashMap;
use std::sync::Arc;

use atomic_sql::context::AtomicSqlContext;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;

use crate::anthropic::client::AnthropicClient;
use crate::anthropic::embed_client::EmbedClient;
use crate::config::NlqConfig;
use crate::errors::Result;
use crate::ir::json_plan::JsonPlanNode;
use crate::ir::parser::IrParser;
use crate::optimizer::llm_batching_rule::LlmBatchingRule;
use crate::physical::extension_planner::{NlqExtensionPlanner, NlqQueryPlanner};
use crate::planner::llm_planner::LlmPlanner;
use crate::registry::NlqRegistry;
use crate::vector::provider::VectorIndexProvider;

/// The single public entry point for natural language queries.
///
/// Build with [`NlqContext::build`], register tables through the inner
/// `AtomicSqlContext`, then call [`query`].
pub struct NlqContext {
    sql_ctx: Arc<AtomicSqlContext>,
    planner: Arc<LlmPlanner>,
    pub registry: Arc<NlqRegistry>,
    config: Arc<NlqConfig>,
    vector_indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>>,
}

impl NlqContext {
    /// Build an `NlqContext` with a properly wired `SessionContext` that
    /// includes `LlmBatchingRule` and `NlqQueryPlanner`.
    pub fn build(config: NlqConfig) -> Self {
        Self::build_inner(config, None)
    }

    /// Build an `NlqContext` backed by an `atomic-compute` context for RDD tables.
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
        // Fail fast on misconfiguration rather than at first API call.
        if let Err(e) = config.validate() {
            panic!("NlqConfig is invalid: {e}");
        }
        let config = Arc::new(config);
        let client = Arc::new(AnthropicClient::new(
            &config.anthropic_api_key,
            &config.anthropic_base_url,
            config.timeout_secs,
        ));
        let embed_client = Arc::new(EmbedClient::new(
            &config.embed_api_key,
            &config.embed_base_url,
            config.timeout_secs,
        ));
        let vector_indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>> =
            Arc::new(DashMap::new());

        let extension_planner = NlqExtensionPlanner::new(
            client.clone(),
            embed_client,
            config.clone(),
            vector_indexes.clone(),
        );
        let query_planner = NlqQueryPlanner::new(extension_planner);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(LlmBatchingRule::new(config.llm_batch_size)))
            .with_query_planner(Arc::new(query_planner))
            .build();

        let session = SessionContext::new_with_state(state);
        let registry_session = Arc::new(session.clone());
        let sql_ctx = Arc::new(AtomicSqlContext::from_session(session, compute));
        let registry = Arc::new(NlqRegistry::new(registry_session));
        let planner = Arc::new(LlmPlanner::new(client.clone(), config.clone()));

        Self { sql_ctx, planner, registry, config, vector_indexes }
    }

    /// Register an RDD-backed table (requires building with `build_with_compute`).
    pub fn register_rdd(
        &self,
        name: &str,
        rdd: atomic_compute::rdd::TypedRdd<datafusion::arrow::record_batch::RecordBatch>,
    ) -> Result<()> {
        self.sql_ctx.register_rdd(name, rdd).map_err(|e| crate::errors::NlqError::Internal(e.to_string()))
    }

    /// Register a `VectorIndexProvider` for `VectorSearch` plan nodes.
    pub fn register_vector_index(&self, index: Arc<dyn VectorIndexProvider>) {
        self.vector_indexes.insert(index.name().to_string(), index);
    }

    /// Look up a registered vector index by name.
    pub fn vector_index(&self, name: &str) -> Option<Arc<dyn VectorIndexProvider>> {
        self.vector_indexes.get(name).map(|e| e.value().clone())
    }

    /// Access the underlying `AtomicSqlContext` for table registration and SQL.
    pub fn sql_ctx(&self) -> &AtomicSqlContext {
        &self.sql_ctx
    }

    /// Translate a natural language query into a DataFusion `DataFrame` and
    /// execute it.
    pub async fn query(&self, nl: &str) -> Result<atomic_sql::dataframe::DataFrame> {
        let t0 = std::time::Instant::now();
        log::info!("NlqContext::query — {nl:?}");

        // 1. Collect table schemas from the session.
        let table_schemas = self.list_table_schemas().await?;

        // 2. Collect UDF descriptions.
        let udfs = self.registry.udf_descriptions();

        // 3. Collect (name, schema) pairs for the planner prompt.
        let schema_pairs: Vec<(String, SchemaRef)> =
            table_schemas.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        // 4. Call LLM planner → raw JSON plan.
        log::debug!("NlqContext: calling LlmPlanner with {} tables, {} UDFs", schema_pairs.len(), udfs.len());
        let json_value = self.planner.plan(nl, &schema_pairs, &udfs).await?;

        // 5. Deserialize JSON into JsonPlanNode.
        let json_plan: JsonPlanNode = serde_json::from_value(json_value)
            .map_err(|e| crate::errors::NlqError::PlanParse(e.to_string()))?;

        // 6. Parse JSON plan → DataFusion LogicalPlan.
        let session_state = self.sql_ctx.inner().state_ref();
        let ir_parser = IrParser::new(
            session_state,
            table_schemas,
            self.config.default_model.clone(),
            self.config.embed_model.clone(),
            self.config.embed_model_dim,
            self.config.llm_batch_size,
        );
        let logical_plan = ir_parser.parse(&json_plan)?;
        log::debug!("NlqContext: logical plan parsed in {}ms", t0.elapsed().as_millis());

        // 7. Execute (analyzer + optimizer + physical planning run automatically).
        let df = self.sql_ctx.inner().execute_logical_plan(logical_plan).await?;
        log::info!("NlqContext::query completed in {}ms", t0.elapsed().as_millis());
        Ok(atomic_sql::dataframe::DataFrame::from_df(df))
    }

    async fn list_table_schemas(&self) -> Result<HashMap<String, SchemaRef>> {
        let mut result = HashMap::new();
        let session = self.sql_ctx.inner();

        for catalog_name in session.catalog_names() {
            let Some(catalog) = session.catalog(&catalog_name) else { continue };
            for schema_name in catalog.schema_names() {
                let Some(schema_prov) = catalog.schema(&schema_name) else { continue };
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
