//! Natural-language query layer for the Atomic engine.
//!
//! A user states a question in plain English; an LLM produces a [`WorkflowPlan`]
//! (a dependency graph of tool calls) and [`WorkflowExecutor`](workflow::executor::WorkflowExecutor)
//! runs it on Atomic's SQL and compute layers, iterating until the question is answered.
//!
//! # Entry point
//!
//! ```rust,ignore
//! use atomic_nlq::{NlqContext, NlqConfig};
//!
//! let ctx = NlqContext::build_with_compute(NlqConfig::default(), compute_ctx)?;
//! ctx.register_rdd("orders", orders_rdd)?;
//! let result = ctx.query("find customers who bought luxury items").await?;
//! println!("{}", result.answer);
//! ```
//!
//! # Architecture
//!
//! 1. [`planner`] — [`LlmPlanner`](planner::llm_planner::LlmPlanner) calls the LLM and
//!    produces a [`WorkflowPlan`].
//! 2. [`workflow`] — [`WorkflowExecutor`](workflow::executor::WorkflowExecutor) runs tool
//!    calls in parallel dependency waves; [`AgentLoop`](workflow::agent_loop::AgentLoop)
//!    repeats until the answer is complete.
//! 3. [`registry`] — [`ToolRegistry`] holds built-in tools (`sql_query`) plus user-registered
//!    Python/JS tools.
//! 4. [`nodes`] / [`physical`] / [`optimizer`] — DataFusion extension nodes for per-row LLM
//!    operations (`LlmFilter`, `LlmMap`, `Embed`, `VectorSearch`) batched by [`optimizer::LlmBatchingRule`].
//!
//! Requires `OPENAI_API_KEY` in the environment; tests skip when it is absent.

pub mod agent_runner;
pub mod config;
pub mod context;
pub mod errors;
pub mod llm;
pub mod nodes;
pub mod openai;
pub mod optimizer;
pub mod physical;
pub mod planner;
pub mod registry;
pub mod vector;
pub mod workflow;

pub use config::NlqConfig;
pub use context::NlqContext;
pub use errors::{NlqError, Result};
pub use registry::{ToolDefinition, ToolRegistry, ToolRuntime, UdfDescription};
pub use vector::in_memory::InMemoryVectorIndex;
pub use vector::provider::VectorIndexProvider;
pub use workflow::{
    AgentEvent, AgentEventSender, AgentResult, StepOutput, StepResult, VisualizationSpec,
    WorkflowPlan, WorkflowStep,
};
