use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionConfig};

use crate::conf::AtomicSqlConfig;
use crate::dataframe::DataFrame;
use crate::errors::Result;
use crate::table::AtomicTableProvider;

/// The primary entry point for `atomic-sql`.
///
/// `AtomicSqlContext` wraps a DataFusion [`SessionContext`] and provides
/// convenience methods for registering tables from various sources and
/// executing SQL queries.
///
/// # Example
///
/// ```rust,no_run
/// use atomic_sql::context::AtomicSqlContext;
///
/// # async fn example() -> atomic_sql::errors::Result<()> {
/// let ctx = AtomicSqlContext::new();
/// ctx.register_parquet("orders", "data/orders.parquet", Default::default()).await?;
/// let df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1").await?;
/// df.show().await?;
/// # Ok(())
/// # }
/// ```
pub struct AtomicSqlContext {
    session: SessionContext,
}

impl AtomicSqlContext {
    /// Create a context with default configuration.
    pub fn new() -> Self {
        Self {
            session: SessionContext::new(),
        }
    }

    /// Create a context from an [`AtomicSqlConfig`].
    pub fn with_config(config: AtomicSqlConfig) -> Self {
        let session_config = SessionConfig::new()
            .with_batch_size(config.batch_size)
            .with_target_partitions(config.default_parallelism)
            .with_parquet_pruning(config.enable_parquet_pruning)
            .with_prefer_existing_sort(false);
        Self {
            session: SessionContext::new_with_config(session_config),
        }
    }

    // ── SQL execution ─────────────────────────────────────────────────────────

    /// Parse and execute a SQL query, returning a lazy [`DataFrame`].
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        let df = self.session.sql(query).await?;
        Ok(DataFrame::new(df))
    }

    // ── Table registration ────────────────────────────────────────────────────

    /// Register a flat list of [`RecordBatch`]es as a single-partition table.
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        let provider = Arc::new(AtomicTableProvider::from_batches(batches)?);
        self.session.register_table(name, provider)?;
        Ok(())
    }

    /// Register pre-partitioned data.  Each inner `Vec<RecordBatch>` is one
    /// DataFusion partition, which maps to one atomic RDD partition.
    pub fn register_partitioned_batches(
        &self,
        name: &str,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Result<()> {
        let provider = Arc::new(AtomicTableProvider::from_partitions(partitions)?);
        self.session.register_table(name, provider)?;
        Ok(())
    }

    /// Register a CSV file or directory as a table.
    pub async fn register_csv(
        &self,
        name: &str,
        path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        self.session.register_csv(name, path, options).await?;
        Ok(())
    }

    /// Register a Parquet file or directory as a table.
    pub async fn register_parquet(
        &self,
        name: &str,
        path: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        self.session.register_parquet(name, path, options).await?;
        Ok(())
    }

    /// Register a newline-delimited JSON file or directory as a table.
    pub async fn register_json(
        &self,
        name: &str,
        path: &str,
        options: JsonReadOptions<'_>,
    ) -> Result<()> {
        self.session.register_json(name, path, options).await?;
        Ok(())
    }

    /// Remove a previously registered table.
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        self.session.deregister_table(name)?;
        Ok(())
    }

    // ── DataFrame builder ─────────────────────────────────────────────────────

    /// Create a [`DataFrame`] directly from pre-loaded batches without
    /// registering the table in the catalog.
    pub fn read_batches(&self, batches: Vec<RecordBatch>) -> Result<DataFrame> {
        let provider = Arc::new(AtomicTableProvider::from_batches(batches)?);
        let df = self.session.read_table(provider)?;
        Ok(DataFrame::new(df))
    }

    // ── Access to the underlying SessionContext ───────────────────────────────

    /// Access the inner DataFusion [`SessionContext`] for advanced use-cases.
    pub fn inner(&self) -> &SessionContext {
        &self.session
    }
}

impl Default for AtomicSqlContext {
    fn default() -> Self {
        Self::new()
    }
}
