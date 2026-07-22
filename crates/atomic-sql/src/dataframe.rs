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
        // Consume `values` so each label is cloned once (for the predicate) and moved into the
        // alias, rather than cloned twice.
        let mut aggs = Vec::with_capacity(values.len());
        for v in values {
            let hit = when(c2_str().eq(lit(v.clone())), lit(1i64))
                .otherwise(lit(0i64))
                .map_err(AtomicSqlError::from)?;
            aggs.push(sum(hit).alias(v));
        }
        let out = self.inner.aggregate(vec![col(col1).alias(header)], aggs)?;
        Ok(Self::new(out))
    }

    /// Group by `group_columns` with a `ROLLUP` grouping set — one aggregate row per prefix of
    /// the columns plus a grand total. Mirrors `DataFrame.rollup(cols).agg(...)`.
    pub fn rollup(self, group_columns: &[&str], aggs: Vec<Expr>) -> Result<Self> {
        let cols: Vec<Expr> = group_columns.iter().map(|c| col(*c)).collect();
        Ok(Self::new(self.inner.aggregate(
            vec![datafusion::logical_expr::rollup(cols)],
            aggs,
        )?))
    }

    /// Group by `group_columns` with a `CUBE` grouping set — one aggregate row per subset of the
    /// columns. Mirrors `DataFrame.cube(cols).agg(...)`.
    pub fn cube(self, group_columns: &[&str], aggs: Vec<Expr>) -> Result<Self> {
        let cols: Vec<Expr> = group_columns.iter().map(|c| col(*c)).collect();
        Ok(Self::new(self.inner.aggregate(
            vec![datafusion::logical_expr::cube(cols)],
            aggs,
        )?))
    }

    /// Pivot table: group by `group_column`, spread the distinct values of `pivot_column` into
    /// their own columns, filling cells with `agg` applied to `value_column` (a two-argument
    /// aggregate constructor such as `sum`/`avg` from `functions_aggregate`).
    ///
    /// DataFusion has no native pivot, so the distinct pivot values are read first (one pass),
    /// then one conditional aggregate is built per value. Mirrors
    /// `DataFrame.groupBy(group).pivot(pivot).agg(...)`.
    pub async fn pivot<A>(
        self,
        group_column: &str,
        pivot_column: &str,
        value_column: &str,
        agg: A,
    ) -> Result<Self>
    where
        A: Fn(Expr) -> Expr,
    {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::logical_expr::{cast, when};

        let pivot_str = || cast(col(pivot_column), DataType::Utf8);

        // Distinct pivot values (as strings) become the output columns.
        let batches = self
            .inner
            .clone()
            .select(vec![pivot_str().alias("__p")])?
            .distinct()?
            .collect()
            .await?;
        let mut values: Vec<String> = Vec::new();
        for batch in &batches {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| AtomicSqlError::Execution("pivot: expected string column".into()))?;
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    values.push(arr.value(i).to_string());
                }
            }
        }
        values.sort();

        // For each pivot value, aggregate `value_column` over only the matching rows. Consume
        // `values` so each label is cloned once (for the predicate) and moved into the alias.
        let mut aggs = Vec::with_capacity(values.len());
        for v in values {
            let masked = when(pivot_str().eq(lit(v.clone())), col(value_column))
                .otherwise(lit(datafusion::scalar::ScalarValue::Null))
                .map_err(AtomicSqlError::from)?;
            aggs.push(agg(masked).alias(v));
        }
        let out = self.inner.aggregate(vec![col(group_column)], aggs)?;
        Ok(Self::new(out))
    }

    /// Materialize this DataFrame and cache the result in memory. Later actions reuse the
    /// cached batches instead of re-running the plan.
    ///
    /// There is no separate `unpersist` — dropping the returned cached DataFrame releases the
    /// memory.
    pub async fn cache(self) -> Result<Self> {
        Ok(Self::new(self.inner.cache().await?))
    }

    /// Split the rows into several DataFrames by `weights`, assigning each row to exactly one
    /// output. Weights are normalised to sum to 1.
    ///
    /// A single random value is drawn per row and the frame is cached so the draw is stable
    /// across the band filters, making the parts disjoint and exhaustive. `seed` is accepted
    /// for API parity but not honoured — DataFusion's `random()` is not seedable.
    pub async fn random_split(self, weights: &[f64], _seed: u64) -> Result<Vec<Self>> {
        let total: f64 = weights.iter().copied().filter(|w| *w > 0.0).sum();
        if total <= 0.0 {
            return Ok(Vec::new());
        }
        // Freeze one random draw per row, then slice by cumulative weight band.
        let tagged = self
            .inner
            .with_column("__split_r", random())?
            .cache()
            .await?;
        let mut bounds = Vec::with_capacity(weights.len() + 1);
        let mut acc = 0.0f64;
        bounds.push(0.0f64);
        for w in weights {
            acc += w.max(0.0) / total;
            bounds.push(acc);
        }
        let mut splits = Vec::with_capacity(weights.len());
        for k in 0..weights.len() {
            let lo = bounds[k];
            let hi = bounds[k + 1];
            let df = tagged
                .clone()
                .filter(
                    col("__split_r")
                        .gt_eq(lit(lo))
                        .and(col("__split_r").lt(lit(hi))),
                )?
                .drop_columns(&["__split_r"])?;
            splits.push(Self::new(df));
        }
        Ok(splits)
    }

    /// Values of `column` that occur in at least `support` (a fraction in `0.0..=1.0`) of the
    /// rows, returned as `(value, count)` rows ordered by descending count.
    ///
    /// Unlike Spark's `stat.freqItems`, which returns a single row of per-column arrays and
    /// uses an approximate sketch, this is an exact count over one column.
    pub async fn freq_items(&self, column: &str, support: f64) -> Result<Self> {
        use datafusion::functions_aggregate::expr_fn::count;
        let total = self.inner.clone().count().await? as f64;
        let threshold = (support * total).ceil() as i64;
        let grouped = self
            .inner
            .clone()
            .aggregate(vec![col(column)], vec![count(col(column)).alias("count")])?
            .filter(col("count").gt_eq(lit(threshold)))?
            .sort(vec![col("count").sort(false, false)])?;
        Ok(Self::new(grouped))
    }

    /// Stratified sample: for each `(value, fraction)` pair, keep approximately `fraction` of
    /// the rows whose `column` equals `value`. Rows whose value is not listed are dropped.
    /// Mirrors `DataFrame.stat.sampleBy()`.
    pub fn sample_by(self, column: &str, fractions: &[(&str, f64)]) -> Result<Self> {
        use datafusion::logical_expr::cast;
        let key = || cast(col(column), DataType::Utf8);
        let mut acc: Option<DFDataFrame> = None;
        for (value, fraction) in fractions {
            let stratum = self
                .inner
                .clone()
                .filter(key().eq(lit(*value)))?
                .filter(random().lt(lit(*fraction)))?;
            acc = Some(match acc {
                Some(a) => a.union(stratum)?,
                None => stratum,
            });
        }
        match acc {
            Some(df) => Ok(Self::new(df)),
            // No strata requested → an empty result with the same schema.
            None => Ok(Self::new(self.inner.filter(lit(false))?)),
        }
    }

    /// Write to Parquet with the output sorted by `sort_columns` (ascending). A convenience
    /// over [`write_parquet`](Self::write_parquet) with a sort applied to the write plan.
    pub async fn write_parquet_sorted(self, path: &str, sort_columns: &[&str]) -> Result<()> {
        use datafusion::dataframe::DataFrameWriteOptions;
        let sort_by: Vec<SortExpr> = sort_columns
            .iter()
            .map(|c| col(*c).sort(true, false))
            .collect();
        let options = DataFrameWriteOptions::new().with_sort_by(sort_by);
        self.inner.write_parquet(path, options, None).await?;
        Ok(())
    }

    /// Rename all columns at once (Spark `toDF`), returning a new DataFrame.
    pub fn to_df(self, col_names: &[&str]) -> Result<Self> {
        let existing: Vec<String> = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        if col_names.len() != existing.len() {
            return Err(AtomicSqlError::Execution(format!(
                "to_df: expected {} column names, got {}",
                existing.len(),
                col_names.len()
            )));
        }
        let mut df = self;
        for (old, new) in existing.iter().zip(col_names) {
            df = df.with_column_renamed(old, new)?;
        }
        Ok(df)
    }

    /// Return the last `n` rows. This is an **action** — it collects and reverses.
    pub async fn tail(self, n: usize) -> Result<Vec<RecordBatch>> {
        let batches = self.inner.collect().await?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        let skip = total.saturating_sub(n);
        let mut out = Vec::new();
        let mut seen = 0usize;
        for b in batches {
            let rows = b.num_rows();
            if seen + rows <= skip {
                seen += rows;
                continue;
            }
            let local_skip = skip.saturating_sub(seen);
            seen += rows;
            out.push(b.slice(local_skip, rows - local_skip));
        }
        Ok(out)
    }

    /// Repartition to `n` partitions using a hash on `cols` (or round-robin when empty).
    pub fn repartition(self, n: usize, cols: &[&str]) -> Result<Self> {
        let exprs: Vec<Expr> = cols.iter().map(|c| col(*c)).collect();
        if exprs.is_empty() {
            return Ok(Self::new(self.inner.repartition(
                datafusion::prelude::Partitioning::RoundRobinBatch(n),
            )?));
        }
        Ok(Self::new(self.inner.repartition(
            datafusion::prelude::Partitioning::Hash(exprs, n),
        )?))
    }

    /// Set the partition count to `n`. DataFusion has no zero-shuffle coalesce, so this applies
    /// a round-robin repartition (a shuffle); use it when the intent is to shrink the partition
    /// count. Mirrors `DataFrame.coalesce(n)`.
    pub fn coalesce(self, n: usize) -> Result<Self> {
        Ok(Self::new(self.inner.repartition(
            datafusion::prelude::Partitioning::RoundRobinBatch(n),
        )?))
    }

    /// Access the inner DataFusion DataFrame for advanced use-cases.
    pub fn into_inner(self) -> DFDataFrame {
        self.inner
    }
}
