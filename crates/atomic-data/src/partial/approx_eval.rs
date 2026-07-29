/// An object that computes a function incrementally by merging in results of type U from multiple
/// tasks. Allows partial evaluation at any point by calling `current_result()`.
///
/// `U` is the per-partition task result type (e.g. `usize` for a partition count); `R` is the
/// aggregate returned to the caller, often error-bounded (e.g.
/// [`BoundedDouble`](atomic_utils::bounded_double::BoundedDouble) via
/// [`bound`](crate::partial::count_eval::bound)) since `current_result` may be called before
/// every partition has reported in.
///
/// Concrete implementations: [`CountEvaluator`](crate::partial::count_eval::CountEvaluator)
/// (scalar count) and [`GroupedCountEvaluator`](crate::partial::group_count_eval::GroupedCountEvaluator)
/// (count by key). Driven through [`ApproxListener`](crate::partial::approx_action_listener::ApproxListener),
/// which is what `Context::run_approximate_job` builds internally — see `examples/approx_count`
/// for an end-to-end, runnable example (local scheduler only; distributed does not support
/// approximate jobs).
pub trait ApproximateEvaluator<U, R> {
    fn merge(&mut self, output_id: usize, task_result: &U);

    fn current_result(&self) -> R;
}
