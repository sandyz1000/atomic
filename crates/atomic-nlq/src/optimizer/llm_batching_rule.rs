use std::fmt;

use datafusion::common::tree_node::Transformed;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::nodes::llm_filter::LlmFilterNode;
use crate::nodes::llm_map::LlmMapNode;

/// DataFusion optimizer rule that validates and annotates LLM extension nodes
/// with the configured batch_size.
///
/// This runs at logical-plan time before physical planning. Actual row-chunking
/// happens inside `LlmFilterExec` / `LlmMapExec` at execution time.
#[derive(Debug)]
pub struct LlmBatchingRule {
    batch_size: usize,
}

impl LlmBatchingRule {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl OptimizerRule for LlmBatchingRule {
    fn name(&self) -> &str {
        "llm_batching_rule"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match &plan {
            LogicalPlan::Extension(ext) => {
                if let Some(node) = ext.node.as_any().downcast_ref::<LlmFilterNode>() {
                    if node.batch_size == self.batch_size {
                        return Ok(Transformed::no(plan));
                    }
                    let new_node = LlmFilterNode::new(
                        node.prompt.clone(),
                        node.model.clone(),
                        node.input.clone(),
                        self.batch_size,
                    );
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: std::sync::Arc::new(new_node),
                    })));
                }
                if let Some(node) = ext.node.as_any().downcast_ref::<LlmMapNode>() {
                    if node.batch_size == self.batch_size {
                        return Ok(Transformed::no(plan));
                    }
                    // Rebuild with updated batch_size preserving other fields.
                    let mut updated = node.clone();
                    updated.batch_size = self.batch_size;
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: std::sync::Arc::new(updated),
                    })));
                }
                Ok(Transformed::no(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}
