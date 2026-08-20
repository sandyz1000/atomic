pub mod embed;
pub mod llm_filter;
pub mod llm_map;
pub mod vector_search;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{Partitioning, PlanProperties};

/// `PlanProperties` for a row-preserving, single-input streaming node: same output
/// schema and partition count as `input`, emitted incrementally, over bounded data.
/// Shared by every `ExecutionPlan` in this module (`embed`, `llm_filter`, `llm_map`,
/// `vector_search`) — none of them repartition, reorder, or block on the whole input.
pub(crate) fn passthrough_plan_properties(
    schema: SchemaRef,
    num_partitions: usize,
) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(num_partitions),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}
