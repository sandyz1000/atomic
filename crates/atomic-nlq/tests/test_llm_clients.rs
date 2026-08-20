//! HTTP-shape coverage for `AnthropicClient`/`OpenAiClient` against a local `wiremock`
//! server: real request serialization, real response deserialization, real retry
//! timing — no live network call.
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_nlq::llm::LlmClient;
use atomic_nlq::llm::anthropic::AnthropicClient;
use atomic_nlq::openai::OpenAiClient;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

#[tokio::test]
async fn anthropic_parses_text() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .and(header("x-api-key", "test-key"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "content": [{"type": "text", "text": "hello from anthropic"}]
        })))
        .mount(&server)
        .await;

    let client = AnthropicClient::new("test-key", &server.uri(), 5, 0);
    let text = client
        .chat("claude-haiku", "system prompt", "user message", 128)
        .await
        .expect("chat should succeed against the stub");

    assert_eq!(text, "hello from anthropic");
}

#[tokio::test]
async fn anthropic_error_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;

    let client = AnthropicClient::new("test-key", &server.uri(), 5, 0);
    let err = client
        .chat("claude-haiku", "system", "user", 128)
        .await
        .expect_err("400 response must surface as an error");

    assert!(err.to_string().contains("400"), "error was: {err}");
}

/// Returns 529 (overloaded) on the first call, then a valid response — exercises the
/// real exponential-backoff retry loop in `chat_with_retry`, not just a happy-path stub.
struct FlakyThenOk {
    calls: AtomicUsize,
}

impl Respond for FlakyThenOk {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(529)
        } else {
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{"type": "text", "text": "recovered"}]
            }))
        }
    }
}

#[tokio::test]
async fn anthropic_retry_recovers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(FlakyThenOk {
            calls: AtomicUsize::new(0),
        })
        .mount(&server)
        .await;

    let client = AnthropicClient::new("test-key", &server.uri(), 5, 2);
    let text = client
        .chat_with_retry("claude-haiku", "system", "user", 64)
        .await
        .expect("retry should recover after the first 529");

    assert_eq!(text, "recovered");
}

#[tokio::test]
async fn anthropic_retry_exhausts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("still failing"))
        .mount(&server)
        .await;

    // max_retries = 1: two attempts total, bounded backoff (~500ms) so the test stays fast.
    let client = AnthropicClient::new("test-key", &server.uri(), 5, 1);
    let err = client
        .chat_with_retry("claude-haiku", "system", "user", 64)
        .await
        .expect_err("persistent 500s must exhaust retries and fail");

    assert!(err.to_string().contains("500"), "error was: {err}");
}

#[tokio::test]
async fn openai_parses_text() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1_700_000_000,
            "model": "gpt-4o-mini",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello from openai"},
                "finish_reason": "stop",
                "logprobs": null
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        })))
        .mount(&server)
        .await;

    let base_url = format!("{}/v1", server.uri());
    let client = OpenAiClient::new("test-key", &base_url, 5, 0);
    let text = client
        .chat("gpt-4o-mini", "system prompt", "user message", 128)
        .await
        .expect("chat should succeed against the stub");

    assert_eq!(text, "hello from openai");
}
