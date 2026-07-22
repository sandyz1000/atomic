use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame as DFDataFrame;
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::prelude::{JoinType, col, lit, random};

use crate::errors::{AtomicSqlError, Result};

/// Collect a one-row, one-column aggregate result as an `f64`, returning `None` when the
/// single cell is null (undefined statistic / empty input).
async fn single_f64(df: DFDataFrame) -> Result<Option<f64>> {
    use datafusion::arrow::array::{Array, Float64Array};
    let batches = df.collect().await?;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| AtomicSqlError::Execution("expected f64 aggregate result".into()))?;
        return Ok(if arr.is_valid(0) {
            Some(arr.value(0))
        } else {
            None
        });
    }
    Ok(None)
}

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

    /// Create a `DataFrame` from a DataFusion [`DFDataFrame`].
    ///
    /// Used by external crates (e.g. `atomic-nlq`) that need to wrap a
    /// DataFusion `DataFrame` into this type.
    pub fn from_df(inner: DFDataFrame) -> Self {
        Self { inner }
    }

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
        Ok(Self::new(
            self.inner.with_column_renamed(old_name, new_name)?,
        ))
    }

    /// Add a computed column.
    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self> {
        Ok(Self::new(self.inner.with_column(name, expr)?))
    }

    /// Union with another DataFrame (preserves duplicates).
    pub fn union(self, other: DataFrame) -> Result<Self> {
        Ok(Self::new(self.inner.union(other.inner)?))
    }

    /// Alias for [`filter`](Self::filter).
    pub fn r#where(self, expr: Expr) -> Result<Self> {
        self.filter(expr)
    }

    /// Drop the named columns, keeping the rest.
    pub fn drop(self, columns: &[&str]) -> Result<Self> {
        Ok(Self::new(self.inner.drop_columns(columns)?))
    }

    /// Drop rows that are duplicates over `columns` (keeping one per group). With an empty
    /// slice this is equivalent to [`distinct`](Self::distinct) over all columns.
    pub fn drop_duplicates(self, columns: &[&str]) -> Result<Self> {
        if columns.is_empty() {
            return self.distinct();
        }
        let on_expr: Vec<Expr> = columns.iter().map(|c| col(*c)).collect();
        let select_expr: Vec<Expr> = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| col(f.name()))
            .collect();
        Ok(Self::new(self.inner.distinct_on(
            on_expr,
            select_expr,
            None,
        )?))
    }

    /// Set intersection with another DataFrame (distinct rows present in both).
    pub fn intersect(self, other: DataFrame) -> Result<Self> {
        Ok(Self::new(self.inner.intersect(other.inner)?))
    }

    /// Set difference — rows in `self` not present in `other` (distinct).
    pub fn except(self, other: DataFrame) -> Result<Self> {
        Ok(Self::new(self.inner.except(other.inner)?))
    }

    /// Randomly sample approximately `fraction` of the rows (`0.0..=1.0`), without replacement.
    ///
    /// Implemented as a `random() < fraction` filter, so the returned row count is
    /// probabilistic, not exact.
    pub fn sample(self, fraction: f64) -> Result<Self> {
        self.filter(random().lt(lit(fraction)))
    }

    /// Summary statistics (count, mean, stddev, min, max, and quartiles) for the numeric
    /// and string columns, returned as a new DataFrame.
    pub async fn describe(self) -> Result<Self> {
        Ok(Self::new(self.inner.describe().await?))
    }

    /// Return the first `n` rows as a limited DataFrame.
    pub fn head(self, n: usize) -> Result<Self> {
        self.limit(0, Some(n))
    }

    /// The column names, in schema order.
    pub fn columns(&self) -> Vec<String> {
        self.inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// The `(name, data type)` pairs, in schema order.
    pub fn dtypes(&self) -> Vec<(String, DataType)> {
        self.inner
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    /// Drop rows containing a null in any of `columns` (or in any column when `columns`
    /// is empty). Mirrors `DataFrame.na.drop()`.
    pub fn drop_null(self, columns: &[&str]) -> Result<Self> {
        let names: Vec<String> = if columns.is_empty() {
            self.columns()
        } else {
            columns.iter().map(|c| c.to_string()).collect()
        };
        let Some(pred) = names
            .into_iter()
            .map(|c| col(c).is_not_null())
            .reduce(|a, b| a.and(b))
        else {
            return Ok(self);
        };
        self.filter(pred)
    }

    /// Replace nulls in `column` with `value`. Mirrors `DataFrame.na.fill()`.
    pub fn fill_null(self, column: &str, value: Expr) -> Result<Self> {
        let filled = datafusion::functions::expr_fn::coalesce(vec![col(column), value]);
        self.with_column(column, filled)
    }

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
        self.inner
            .write_parquet(path, options, writer_options)
            .await?;
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

    /// Return the logical schema of this DataFrame.
    pub fn schema(&self) -> &datafusion::common::DFSchema {
        self.inner.schema()
    }

    /// Return an `EXPLAIN` plan as a new DataFrame.
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<Self> {
        Ok(Self::new(self.inner.explain(verbose, analyze)?))
    }

    /// Pearson correlation coefficient between two numeric columns, or `None` when it is
    /// undefined (empty input or zero variance). Mirrors `DataFrame.stat.corr()`.
    pub async fn corr(&self, col1: &str, col2: &str) -> Result<Option<f64>> {
        use datafusion::functions_aggregate::expr_fn::corr;
        let agg = self
            .inner
            .clone()
            .aggregate(vec![], vec![corr(col(col1), col(col2)).alias("v")])?;
        single_f64(agg).await
    }

    /// Sample covariance between two numeric columns, or `None` when undefined. Mirrors
    /// `DataFrame.stat.cov()`.
    pub async fn cov(&self, col1: &str, col2: &str) -> Result<Option<f64>> {
        use datafusion::functions_aggregate::expr_fn::covar_samp;
        let agg = self
            .inner
            .clone()
            .aggregate(vec![], vec![covar_samp(col(col1), col(col2)).alias("v")])?;
        single_f64(agg).await
    }

    /// Contingency table (cross-tabulation) of two columns: one row per distinct `col1`
    /// value, one column per distinct `col2` value, cells holding the pair counts. The first
    /// output column is named `col1_col2` and holds the `col1` values. Mirrors
    /// `DataFrame.stat.crosstab()`.
    pub async fn crosstab(self, col1: &str, col2: &str) -> Result<Self> {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::functions_aggregate::expr_fn::sum;
        use datafusion::logical_expr::{cast, when};

        let c2_str = || cast(col(col2), DataType::Utf8);

        // Distinct `col2` values (as strings) become the output columns.
        let batches = self
            .inner
            .clone()
            .select(vec![c2_str().alias("__c2")])?
            .distinct()?
            .collect()
            .await?;
        let mut values: Vec<String> = Vec::new();
        for batch in &batches {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    AtomicSqlError::Execution("crosstab: expected string column".into())
                })?;
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    values.push(arr.value(i).to_string());
                }
            }
        }
        values.sort();

        let header = format!("{col1}_{col2}");
        let mut aggs = Vec::with_capacity(values.len());
        for v in &values {
            let hit = when(c2_str().eq(lit(v.clone())), lit(1i64))
                .otherwise(lit(0i64))
                .map_err(AtomicSqlError::from)?;
            aggs.push(sum(hit).alias(v.clone()));
        }
        let out = self.inner.aggregate(vec![col(col1).alias(header)], aggs)?;
        Ok(Self::new(out))
    }

    /// Access the inner DataFusion DataFrame for advanced use-cases.
    pub fn into_inner(self) -> DFDataFrame {
        self.inner
    }
}
