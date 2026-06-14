use atomic_data::dependency::Dependency;

pub struct RddVals {
    pub id: usize,
    pub dependencies: Vec<Dependency>,
    pub should_cache: bool,
}

impl RddVals {
    pub fn new(id: usize) -> Self {
        RddVals {
            id,
            dependencies: Vec::new(),
            should_cache: false,
        }
    }
}
