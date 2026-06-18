use std::{future::Future, time::Duration};

/// Run `op` up to `max_retries + 1` times with exponential backoff between
/// attempts. On the first success the result is returned immediately. After
/// exhausting all retries the last error is returned.
///
/// Backoff formula: `base_delay_ms * min(2^attempt, 32)` milliseconds, where
/// `attempt` is zero-indexed (first sleep is `base_delay_ms * 1`).
pub(crate) async fn retry_with_backoff<F, Fut, T, E>(
    max_retries: usize,
    base_delay_ms: u64,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut last_err = None;
    for attempt in 0..=max_retries {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                if attempt < max_retries {
                    let delay = Duration::from_millis(base_delay_ms * (1u64 << attempt).min(32));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    Err(last_err.expect("loop body runs at least once"))
}
