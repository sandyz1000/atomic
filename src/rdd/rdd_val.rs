pub(crate) struct RddVals {
    pub id: usize,
    pub dependencies: Vec<Dependency>,
    should_cache: bool,
    pub context: Weak<Context>,
}

impl RddVals {
    pub fn new(sc: Arc<Context>) -> Self {
        RddVals {
            id: sc.new_rdd_id(),
            dependencies: Vec::new(),
            should_cache: false,
            context: Arc::downgrade(&sc),
        }
    }

    fn cache(mut self) -> Self {
        self.should_cache = true;
        self
    }
}
