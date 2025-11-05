/// An object that computes a function incrementally by merging in results of type U from multiple
/// tasks. Allows partial evaluation at any point by calling `current_result()`.
pub trait ApproximateEvaluator<U, R> {
    fn merge(&mut self, output_id: usize, task_result: &U);
    
    fn current_result(&self) -> R;
}
