//! `atomic-sql` — structured data and SQL query support for the Atomic
//! distributed compute framework.
//!
//! # Architecture
//!
//! `atomic-sql` uses [Apache DataFusion](https://github.com/apache/datafusion)
//! as its SQL query engine.  DataFusion provides SQL parsing, logical planning,
//! a 30+ rule optimizer, and physical execution backed by Apache Arrow.
//!
//! The crate adds:
//! - [`AtomicSqlContext`] — the primary entry point; wraps DataFusion's
//!   `SessionContext`.
//! - [`AtomicTableProvider`] / [`AtomicScanExec`] — bridge pre-loaded Arrow
//!   [`RecordBatch`]es (or data from atomic's `TypedRdd<RecordBatch>`) into
//!   DataFusion's physical plan.
//! - [`DataFrame`] — thin ergonomic wrapper over DataFusion's `DataFrame`.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use atomic_sql::context::AtomicSqlContext;
//!
//! #[tokio::main]
//! async fn main() -> atomic_sql::errors::Result<()> {
//!     let ctx = AtomicSqlContext::new();
//!     ctx.register_parquet("orders", "data/orders.parquet", Default::default()).await?;
//!     let df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1").await?;
//!     df.show().await?;
//!     Ok(())
//! }
//! ```

pub mod column;
pub mod conf;
pub mod context;
pub mod dataframe;
pub mod datasource;
pub mod errors;
pub mod exec_plan;
pub mod rdd_table;
pub mod schema;
pub mod session;
pub mod table;
pub mod udf;

// Re-export the most commonly used types at the crate root.
pub use context::AtomicSqlContext;
pub use dataframe::DataFrame;
pub use errors::{AtomicSqlError, Result};
pub use rdd_table::RddTableProvider;
pub use table::AtomicTableProvider;

// Re-export DataFusion's expression helpers so users don't need a direct
// `datafusion` dependency for common operations.
pub use datafusion::logical_expr::col;
pub use datafusion::logical_expr::lit;
pub use datafusion::prelude::{JoinType, SessionConfig};
