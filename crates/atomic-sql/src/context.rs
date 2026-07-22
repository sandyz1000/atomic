use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_compute::rdd::TypedRdd;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionConfig};

use crate::conf::AtomicSqlConfig;
use crate::dataframe::DataFrame;
use crate::errors::{AtomicSqlError, Result};
use crate::rdd_table::RddTableProvider;
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
    /// Optional atomic-compute context for parallel RDD-backed execution.
    sc: Option<Arc<Context>>,
}

/// Data source format for [`AtomicSqlContext::read`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFormat {
    Csv,
    Parquet,
    Json,
    #[cfg(feature = "avro")]
    Avro,
}

impl AtomicSqlContext {
    /// Create a context with default configuration.
    pub fn new() -> Self {
        Self {
            session: SessionContext::new(),
            sc: None,
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
            sc: None,
        }
    }

    /// Create a context from a pre-built [`SessionContext`].
    ///
    /// Used by `atomic-nlq` to inject a custom `SessionState` with its own
    /// optimizer rules and physical planner before handing the session to
    /// `AtomicSqlContext`. Prefer the other constructors unless you need to
    /// customize the `SessionState`.
    pub fn from_session(session: SessionContext, sc: Option<Arc<Context>>) -> Self {
        Self { session, sc }
    }

    /// Create a context backed by an atomic-compute [`Context`] for parallel execution.
    ///
    /// Use this when your data lives in atomic RDDs. SQL queries will materialize
    /// each RDD partition in parallel via atomic's scheduler (local threads or
    /// remote workers) before DataFusion applies its operators.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use atomic_compute::context::Context;
    /// use atomic_sql::context::AtomicSqlContext;
    ///
    /// # async fn example() -> atomic_sql::errors::Result<()> {
    /// // Assumes a Context built with Config::local() or similar.
    /// let sc: Arc<Context> = // ...
    /// #     unimplemented!();
    /// let ctx = AtomicSqlContext::with_compute(sc);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_compute(sc: Arc<Context>) -> Self {
        Self {
            session: SessionContext::new(),
            sc: Some(sc),
        }
    }

    /// Register a [`TypedRdd<RecordBatch>`] as a SQL table.
    ///
    /// Each RDD partition becomes one DataFusion partition. When a query runs,
    /// atomic-compute materializes each partition in parallel (via the scheduler
    /// passed to [`with_compute`]) before DataFusion applies SQL operators.
    ///
    /// Requires this context to have been created with [`with_compute`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use atomic_sql::context::AtomicSqlContext;
    ///
    /// # async fn example(ctx: AtomicSqlContext, rdd: atomic_compute::rdd::TypedRdd<datafusion::arrow::record_batch::RecordBatch>) -> atomic_sql::errors::Result<()> {
    /// ctx.register_rdd("events", rdd)?;
    /// let df = ctx.sql("SELECT user_id, COUNT(*) FROM events GROUP BY 1").await?;
    /// df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_rdd(&self, name: &str, rdd: TypedRdd<RecordBatch>) -> Result<()> {
        let sc = self.sc.as_ref().ok_or_else(|| {
            AtomicSqlError::Internal(
                "register_rdd requires AtomicSqlContext::with_compute(sc)".into(),
            )
        })?;

        // Infer schema by reading the first RecordBatch from partition 0.
        let schema: SchemaRef = sc
            .run_job_with_partitions(
                rdd.inner().clone(),
                |mut iter| iter.next().map(|b| b.schema()),
                [0usize],
            )
            .map_err(|e| AtomicSqlError::Internal(e.to_string()))?
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| AtomicSqlError::Schema("RDD is empty — cannot infer schema".into()))?;

        let provider = Arc::new(RddTableProvider::new(schema, rdd.into_rdd(), sc.clone()));
        self.session.register_table(name, provider)?;
        Ok(())
    }

    /// Parse and execute a SQL query, returning a lazy [`DataFrame`].
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        let df = self.session.sql(query).await?;
        Ok(DataFrame::new(df))
    }

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

    atomic_data::cfg_avro! {
        /// Register an Avro file or directory as a table named `name`. Requires the `avro`
        /// feature (forwards to DataFusion's Avro support).
        pub async fn register_avro(
            &self,
            name: &str,
            path: &str,
            options: datafusion::prelude::AvroReadOptions<'_>,
        ) -> Result<()> {
            self.session.register_avro(name, path, options).await?;
            Ok(())
        }
    }

    /// Remove a previously registered table.
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        self.session.deregister_table(name)?;
        Ok(())
    }

    /// Create a [`DataFrame`] directly from pre-loaded batches without
    /// registering the table in the catalog.
    pub fn read_batches(&self, batches: Vec<RecordBatch>) -> Result<DataFrame> {
        let provider = Arc::new(AtomicTableProvider::from_batches(batches)?);
        let df = self.session.read_table(provider)?;
        Ok(DataFrame::new(df))
    }

    /// Read a registered table by name, returning a lazy [`DataFrame`].
    /// Analogous to Spark's `spark.table(name)`.
    pub async fn table(&self, name: &str) -> Result<DataFrame> {
        let df = self.session.table(name).await?;
        Ok(DataFrame::new(df))
    }

    /// List the names of all registered tables in the default catalog/schema.
    pub fn table_names(&self) -> Result<Vec<String>> {
        let catalog = self
            .session
            .catalog("datafusion")
            .ok_or_else(|| AtomicSqlError::Internal("default catalog not found".into()))?;
        let schema = catalog
            .schema("public")
            .ok_or_else(|| AtomicSqlError::Internal("default schema not found".into()))?;
        Ok(schema.table_names().into_iter().collect())
    }

    /// Directly read a data source by path, returning a lazy [`DataFrame`].
    /// Analogous to `spark.read.format(fmt).load(path)`.
    pub async fn read(&self, format: DataFormat, path: &str) -> Result<DataFrame> {
        let name = format!("__read_{}", path.replace(['/', '.', '-'], "_"));
        match format {
            DataFormat::Csv => {
                self.session
                    .register_csv(&name, path, CsvReadOptions::default())
                    .await?;
            }
            DataFormat::Parquet => {
                self.session
                    .register_parquet(&name, path, ParquetReadOptions::default())
                    .await?;
            }
            DataFormat::Json => {
                self.session
                    .register_json(&name, path, JsonReadOptions::default())
                    .await?;
            }
            #[cfg(feature = "avro")]
            DataFormat::Avro => {
                self.session
                    .register_avro(&name, path, datafusion::prelude::AvroReadOptions::default())
                    .await?;
            }
        }
        self.table(&name).await
    }

    /// Register `df` as a global temporary view named `global_temp.<name>`, replacing any
    /// existing view of that name.
    ///
    /// DataFusion has no cross-session global catalog, so "global" here means the view lives in
    /// a dedicated `global_temp` schema within this context — reachable as `global_temp.<name>`
    /// in SQL. Sharing across separate `AtomicSqlContext`s requires a shared `SessionContext`.
    pub fn create_or_replace_global_temp_view(&self, name: &str, df: DataFrame) -> Result<()> {
        use datafusion::catalog::MemorySchemaProvider;
        use datafusion::sql::TableReference;

        let catalog = self
            .session
            .catalog("datafusion")
            .ok_or_else(|| AtomicSqlError::Internal("default catalog not found".into()))?;
        if catalog.schema("global_temp").is_none() {
            catalog.register_schema("global_temp", Arc::new(MemorySchemaProvider::new()))?;
        }
        self.session.register_table(
            TableReference::partial("global_temp", name),
            df.into_inner().into_view(),
        )?;
        Ok(())
    }

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
