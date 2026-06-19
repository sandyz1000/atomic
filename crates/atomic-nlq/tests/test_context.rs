use atomic_nlq::{NlqConfig, NlqContext, ToolDefinition, ToolRuntime};

fn test_config() -> NlqConfig {
    NlqConfig {
        api_key: std::env::var("OPENAI_API_KEY").unwrap_or_else(|_| "sk-test".to_string()),
        ..Default::default()
    }
}

#[test]
fn test_config_default_values() {
    let cfg = NlqConfig::default();
    assert_eq!(cfg.model, "gpt-4o");
    assert_eq!(cfg.embed_model, "text-embedding-3-small");
    assert_eq!(cfg.max_rounds, 5);
    assert!(cfg.llm_batch_size > 0);
}

#[test]
fn test_config_empty_key() {
    let cfg = NlqConfig {
        api_key: String::new(),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn test_config_zero_batch() {
    let cfg = NlqConfig {
        api_key: "sk-test".to_string(),
        llm_batch_size: 0,
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn test_config_validation_ok() {
    let cfg = NlqConfig {
        api_key: "sk-test".to_string(),
        ..Default::default()
    };
    assert!(cfg.validate().is_ok());
}

#[tokio::test]
async fn test_nlq_context_builds() {
    let ctx = NlqContext::build(test_config());
    let tools = ctx.registry.all_tools();
    assert!(tools.iter().any(|t| t.name == "sql_query"));
    assert!(tools.iter().any(|t| t.name == "llm_filter"));
    assert!(tools.iter().any(|t| t.name == "llm_map"));
}

#[tokio::test]
async fn test_register_python_tool() {
    let ctx = NlqContext::build(test_config());
    ctx.register_tool(ToolDefinition {
        name: "my_tool".to_string(),
        description: "A test Python tool".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": { "table": { "type": "string" } }
        }),
        runtime: ToolRuntime::Python("def run(args): return []".to_string()),
    });
    let tool = ctx.registry.get_tool("my_tool");
    assert!(tool.is_some());
    assert_eq!(tool.unwrap().name, "my_tool");
}

#[tokio::test]
async fn test_register_js_tool() {
    let ctx = NlqContext::build(test_config());
    ctx.register_tool(ToolDefinition {
        name: "js_tool".to_string(),
        description: "A test JS tool".to_string(),
        input_schema: serde_json::json!({}),
        runtime: ToolRuntime::JavaScript("function run(args) { return []; }".to_string()),
    });
    assert!(ctx.registry.get_tool("js_tool").is_some());
}

#[tokio::test]
async fn test_sql_ctx_accessible() {
    let ctx = NlqContext::build(test_config());
    let _sql = ctx.sql_ctx();
}
