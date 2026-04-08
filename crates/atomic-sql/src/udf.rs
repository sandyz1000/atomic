use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::prelude::SessionContext;

use crate::errors::Result;

/// Helpers for registering user-defined functions with a [`SessionContext`].
pub struct UdfRegistry<'a> {
    session: &'a SessionContext,
}

impl<'a> UdfRegistry<'a> {
    pub fn new(session: &'a SessionContext) -> Self {
        Self { session }
    }

    /// Register a scalar UDF.
    ///
    /// Build a `ScalarUDF` with [`datafusion::logical_expr::create_udf`] or
    /// [`datafusion::logical_expr::ScalarUDF::from`].
    pub fn register_scalar(&self, udf: ScalarUDF) -> Result<()> {
        self.session.register_udf(udf);
        Ok(())
    }

    /// Register an aggregate UDF.
    pub fn register_aggregate(&self, udaf: AggregateUDF) -> Result<()> {
        self.session.register_udaf(udaf);
        Ok(())
    }
}
