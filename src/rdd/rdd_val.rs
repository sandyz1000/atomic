use crate::rdd::Context;
use ember_data::dependency::Dependency;
use std::sync::Arc;

pub struct RddVals {
    pub id: usize,
    pub dependencies: Vec<Dependency>,
    should_cache: bool,
    /// Strong reference to context - allows RDDs to access context without
    /// needing to pass it through constructors
    pub context: Arc<Context>,
}

impl RddVals {
    pub fn new(sc: Arc<Context>) -> Self {
        RddVals {
            id: sc.new_rdd_id(),
            dependencies: Vec::new(),
            should_cache: false,
            context: sc,
        }
    }

    pub fn get_context(&self) -> Arc<Context> {
        self.context.clone()
    }

    fn cache(mut self) -> Self {
        self.should_cache = true;
        self
    }
}
