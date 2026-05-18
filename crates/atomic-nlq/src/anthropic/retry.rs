use std::sync::Arc;
use std::time::Duration;

use crate::errors::{NlqError, Result};

use super::client::AnthropicClient;
use super::types::{MessagesRequest, MessagesResponse};

/// Cheap pseudo-random jitter (no dep needed): xorshift on current nanoseconds.
fn jitter_millis() -> u64 {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let mut x = seed ^ (seed << 13);
    x ^= x >> 7;
    x ^= x << 17;
    x % 500
}

/// Call `client.messages` with exponential backoff + jitter on 429 / 503 / 529.
pub async fn messages_with_retry(
    client: &Arc<AnthropicClient>,
    req: MessagesRequest,
    max_retries: u32,
) -> Result<MessagesResponse> {
    let mut attempts = 0u32;
    let mut delay = Duration::from_millis(500);

    loop {
        match client.messages(req.clone()).await {
            Ok(resp) => return Ok(resp),
            Err(NlqError::Api { status: status @ (429 | 503 | 529), body }) => {
                attempts += 1;
                if attempts > max_retries {
                    return Err(NlqError::Api { status, body });
                }
                log::warn!(
                    "Anthropic API returned {status} (attempt {attempts}/{max_retries}), retrying in {}ms",
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2 + Duration::from_millis(jitter_millis()))
                    .min(Duration::from_secs(30));
            }
            Err(e) => return Err(e),
        }
    }
}

// MessagesRequest needs to be Clone for the retry loop above.
impl Clone for MessagesRequest {
    fn clone(&self) -> Self {
        use super::types::RequestMessage;
        MessagesRequest {
            model: self.model.clone(),
            max_tokens: self.max_tokens,
            system: self.system.clone(),
            messages: self
                .messages
                .iter()
                .map(|m| RequestMessage { role: m.role.clone(), content: m.content.clone() })
                .collect(),
        }
    }
}
