


#[async_trait::async_trait]
impl<E, U, R> JobListener for ApproximateActionListener<U, R, E>
where
    E: ApproximateEvaluator<U, R> + Send + Sync,
    R: Clone + Debug + Send + Sync + 'static,
    U: Send + Sync + 'static,
{
    async fn task_succeeded(&self, index: usize, result: &dyn Data) -> Result<()> {
        let result = result.as_any().downcast_ref::<U>().ok_or_else(|| {
            PartialJobError::DowncastFailure(
                "failed converting to generic type param @ ApproximateActionListener",
            )
        })?;
        self.evaluator.lock().await.merge(index, result);
        let current_finished = self.finished_tasks.fetch_add(1, Ordering::SeqCst) + 1;
        if current_finished == self.total_tasks {
            // If we had already returned a PartialResult, set its final value
            if let Some(ref mut value) = *self.result_object.lock().await {
                value.set_final_value(self.evaluator.lock().await.current_result())?;
            }
        }
        Ok(())
    }

    async fn job_failed(&self, err: Error) {
        let mut failure = self.failure.lock().await;
        *failure = Some(err);
    }
}
