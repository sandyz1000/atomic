use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::anthropic::client::AnthropicClient;
use crate::anthropic::embed_client::EmbedClient;
use crate::config::NlqConfig;
use crate::nodes::embed::{EmbedExec, EmbedNode};
use crate::nodes::llm_filter::{LlmFilterExec, LlmFilterNode};
use crate::nodes::llm_map::{LlmMapExec, LlmMapNode};
use crate::nodes::vector_search::{VectorSearchExec, VectorSearchNode};
use crate::vector::provider::VectorIndexProvider;

// ── Extension planner ─────────────────────────────────────────────────────────

pub struct NlqExtensionPlanner {
    client: Arc<AnthropicClient>,
    embed_client: Arc<EmbedClient>,
    config: Arc<NlqConfig>,
    vector_indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>>,
}

impl fmt::Debug for NlqExtensionPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NlqExtensionPlanner").finish_non_exhaustive()
    }
}

impl NlqExtensionPlanner {
    pub fn new(
        client: Arc<AnthropicClient>,
        embed_client: Arc<EmbedClient>,
        config: Arc<NlqConfig>,
        vector_indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>>,
    ) -> Self {
        Self { client, embed_client, config, vector_indexes }
    }
}

#[async_trait]
impl ExtensionPlanner for NlqExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(n) = node.as_any().downcast_ref::<LlmFilterNode>() {
            let exec = LlmFilterExec::new(
                n,
                physical_inputs[0].clone(),
                self.client.clone(),
                self.config.clone(),
            );
            return Ok(Some(Arc::new(exec)));
        }

        if let Some(n) = node.as_any().downcast_ref::<LlmMapNode>() {
            let exec = LlmMapExec::new(
                n,
                physical_inputs[0].clone(),
                self.client.clone(),
                self.config.clone(),
            );
            return Ok(Some(Arc::new(exec)));
        }

        if let Some(n) = node.as_any().downcast_ref::<EmbedNode>() {
            let exec = EmbedExec::new(
                n,
                physical_inputs[0].clone(),
                self.embed_client.clone(),
                self.config.clone(),
            );
            return Ok(Some(Arc::new(exec)));
        }

        if let Some(n) = node.as_any().downcast_ref::<VectorSearchNode>() {
            let exec = VectorSearchExec::new(n, physical_inputs[0].clone(), self.vector_indexes.clone());
            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }
}

// ── QueryPlanner wrapper ──────────────────────────────────────────────────────

pub struct NlqQueryPlanner {
    planner: DefaultPhysicalPlanner,
}

impl fmt::Debug for NlqQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NlqQueryPlanner").finish_non_exhaustive()
    }
}

impl NlqQueryPlanner {
    pub fn new(extension_planner: NlqExtensionPlanner) -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                extension_planner,
            )]),
        }
    }
}

#[async_trait]
impl QueryPlanner for NlqQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.planner.create_physical_plan(logical_plan, session_state).await
    }
}
