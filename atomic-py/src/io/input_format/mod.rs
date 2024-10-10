pub trait InputFormat {
    fn get_splits(&self, ctx: &mut dyn TaskContext, min_num_splits: usize) -> Vec<Box<dyn InputSplit>>;
}