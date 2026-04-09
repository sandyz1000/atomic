use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame as DFDataFrame;
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::prelude::JoinType;

use crate::errors::Result;

/// A lazy, immutable representation of a structured dataset.
///
/// `DataFrame` is a thin wrapper around DataFusion's [`datafusion::dataframe::DataFrame`].
/// All operations are lazy and build a logical plan that is only executed when
/// an action (`collect`, `show`, `count`, etc.) is called.
pub struct DataFrame {
    inner: DFDataFrame,
}

impl DataFrame {
    pub(crate) fn new(inner: DFDataFrame) -> Self {
        Self { inner }
    }

    // ── Transformations (lazy) ────────────────────────────────────────────────

    /// Project columns by expression.
    pub fn select(self, exprs: Vec<Expr>) -> Result<Self> {
        Ok(Self::new(self.inner.select(exprs)?))
    }

    /// Filter rows matching a boolean expression.
    pub fn filter(self, expr: Expr) -> Result<Self> {
        Ok(Self::new(self.inner.filter(expr)?))
    }

    /// Limit the number of rows returned.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self> {
        Ok(Self::new(self.inner.limit(skip, fetch)?))
    }

    /// Sort the DataFrame by one or more expressions.
    pub fn sort(self, exprs: Vec<SortExpr>) -> Result<Self> {
        Ok(Self::new(self.inner.sort(exprs)?))
    }

    /// Join with another DataFrame.
    pub fn join(
        self,
        right: DataFrame,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
    ) -> Result<Self> {
        Ok(Self::new(self.inner.join(
            right.inner,
            join_type,
            left_cols,
            right_cols,
            None,
        )?))
    }

    /// Group by and aggregate.
    pub fn aggregate(self, group_by: Vec<Expr>, aggs: Vec<Expr>) -> Result<Self> {
        Ok(Self::new(self.inner.aggregate(group_by, aggs)?))
    }

    /// Deduplicate rows.
    pub fn distinct(self) -> Result<Self> {
        Ok(Self::new(self.inner.distinct()?))
    }

    /// Keep only the named columns.
    pub fn select_columns(self, columns: &[&str]) -> Result<Self> {
        Ok(Self::new(self.inner.select_columns(columns)?))
    }

    /// Rename a column.
    pub fn with_column_renamed(self, old_name: &str, new_name: &str) -> Result<Self> {
        Ok(Self::new(self.inner.with_column_renamed(old_name, new_name)?))
    }

    /// Add a computed column.
    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self> {
        Ok(Self::new(self.inner.with_column(name, expr)?))
    }

    /// Union with another DataFrame (preserves duplicates).
    pub fn union(self, other: DataFrame) -> Result<Self> {
        Ok(Self::new(self.inner.union(other.inner)?))
    }

    // ── Actions (trigger execution) ───────────────────────────────────────────

    /// Execute the plan and collect all results as a `Vec<RecordBatch>`.
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        Ok(self.inner.collect().await?)
    }

    /// Print a formatted table to stdout (default: 20 rows).
    pub async fn show(self) -> Result<()> {
        self.inner.show().await?;
        Ok(())
    }

    /// Print at most `num_rows` rows to stdout.
    pub async fn show_limit(self, num_rows: usize) -> Result<()> {
        self.inner.show_limit(num_rows).await?;
        Ok(())
    }

    /// Return the number of rows.
    pub async fn count(self) -> Result<usize> {
        Ok(self.inner.count().await?)
    }

    /// Write data to a Parquet file or directory.
    pub async fn write_parquet(
        self,
        path: &str,
        options: datafusion::dataframe::DataFrameWriteOptions,
        writer_options: Option<datafusion::common::config::TableParquetOptions>,
    ) -> Result<()> {
        self.inner.write_parquet(path, options, writer_options).await?;
        Ok(())
    }

    /// Write data to CSV.
    pub async fn write_csv(
        self,
        path: &str,
        options: datafusion::dataframe::DataFrameWriteOptions,
        writer_options: Option<datafusion::common::config::CsvOptions>,
    ) -> Result<()> {
        self.inner.write_csv(path, options, writer_options).await?;
        Ok(())
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    /// Return the logical schema of this DataFrame.
    pub fn schema(&self) -> &datafusion::common::DFSchema {
        self.inner.schema()
    }

    /// Return an `EXPLAIN` plan as a new DataFrame.
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<Self> {
        Ok(Self::new(self.inner.explain(verbose, analyze)?))
    }

    /// Access the inner DataFusion DataFrame for advanced use-cases.
    pub fn into_inner(self) -> DFDataFrame {
        self.inner
    }
}
