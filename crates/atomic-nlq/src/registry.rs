use std::sync::Arc;

use dashmap::DashMap;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};

use crate::errors::Result;

/// Human-readable description of a UDF, exposed to the LLM planner.
#[derive(Debug, Clone)]
pub struct UdfDescription {
    pub name: String,
    pub description: String,
    pub signature: String,
}

/// UDF registry that extends DataFusion with per-function descriptions for the
/// LLM system prompt.
pub struct NlqRegistry {
    session: Arc<SessionContext>,
    descriptions: DashMap<String, UdfDescription>,
}

impl NlqRegistry {
    pub fn new(session: Arc<SessionContext>) -> Self {
        Self { session, descriptions: DashMap::new() }
    }

    /// Register a scalar UDF with a human-readable description.
    pub fn register_scalar(
        &self,
        udf: ScalarUDF,
        description: impl Into<String>,
    ) -> Result<()> {
        let name = udf.name().to_string();
        let signature = format!("({:?})", udf.signature().type_signature);
        self.session.register_udf(udf);
        self.descriptions.insert(
            name.clone(),
            UdfDescription { name, description: description.into(), signature },
        );
        Ok(())
    }

    /// Register an aggregate UDF with a human-readable description.
    pub fn register_aggregate(
        &self,
        udaf: AggregateUDF,
        description: impl Into<String>,
    ) -> Result<()> {
        let name = udaf.name().to_string();
        let signature = format!("({:?})", udaf.signature().type_signature);
        self.session.register_udaf(udaf);
        self.descriptions.insert(
            name.clone(),
            UdfDescription { name, description: description.into(), signature },
        );
        Ok(())
    }

    /// Return all UDF descriptions for the LLM system prompt.
    pub fn udf_descriptions(&self) -> Vec<UdfDescription> {
        self.descriptions.iter().map(|e| e.value().clone()).collect()
    }
}
