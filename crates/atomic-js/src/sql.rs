use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::rdd::JsRdd;

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame as DFDataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::col as df_col;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use atomic_sql::context::AtomicSqlContext;

fn run_sql_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for SQL")
            .block_on(fut),
    }
}

static TMP_VIEW_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_view_name() -> String {
    let n = TMP_VIEW_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("__atomic_tmp_{n}")
}

fn arrow_scalar_to_json(col: &dyn Array, row: usize) -> serde_json::Value {
    if col.is_null(row) {
        return serde_json::Value::Null;
    }
    match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int8 => col
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int16 => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt8 => col
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt16 => col
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| serde_json::json!(a.value(row) as f64))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| serde_json::json!(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Utf8 | DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::String(col.data_type().to_string()),
    }
}

pub(crate) fn batches_to_json_rows(batches: &[RecordBatch]) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let fields = schema.fields();
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in fields.iter().enumerate() {
                let val = arrow_scalar_to_json(batch.column(col_idx).as_ref(), row_idx);
                obj.insert(field.name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }
    rows
}

/// A lazy structured dataset produced by `SqlContext.sql()`.
///
/// Call `collect()`, `count()`, or `show()` to trigger execution.
///
/// @example
/// ```typescript
/// const df = ctx.sql("SELECT id, value FROM t WHERE value > 10");
/// const rows = df.collect();  // Array<Record<string, unknown>>
/// ```
#[napi(js_name = "DataFrame")]
pub struct JsDataFrame {
    inner: DFDataFrame,
    session: Arc<SessionContext>,
}

#[napi]
impl JsDataFrame {
    /// Execute the query and return all rows as an array of objects.
    ///
    /// Each object maps column name → value (number, string, boolean, or null).
    #[napi]
    pub fn collect(&self) -> Result<Vec<serde_json::Value>> {
        let batches = run_sql_async(self.inner.clone().collect())
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(batches_to_json_rows(&batches))
    }

    /// Execute the query and return the result as Arrow IPC stream bytes.
    ///
    /// The returned `Buffer` is an Arrow IPC *stream* — read it on the JS side
    /// with `apache-arrow`'s `tableFromIPC(buffer)`. Mirrors `to_arrow()` in the
    /// Python bindings (which returns a PyArrow Table).
    ///
    /// ```js
    /// import { tableFromIPC } from "apache-arrow";
    /// const table = tableFromIPC(df.toArrow());
    /// ```
    #[napi]
    pub fn to_arrow(&self) -> Result<Buffer> {
        use datafusion::arrow::ipc::writer::StreamWriter;

        let batches = run_sql_async(self.inner.clone().collect())
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let mut buf: Vec<u8> = Vec::new();
        if let Some(first) = batches.first() {
            let schema = first.schema();
            let mut writer = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| Error::from_reason(e.to_string()))?;
            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|e| Error::from_reason(e.to_string()))?;
            }
            writer
                .finish()
                .map_err(|e| Error::from_reason(e.to_string()))?;
        }
        Ok(Buffer::from(buf))
    }

    /// Execute and print a formatted table to stdout (default: 20 rows).
    #[napi]
    pub fn show(&self) -> Result<()> {
        run_sql_async(self.inner.clone().show()).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Execute and print the first `n` rows to stdout.
    #[napi]
    pub fn show_limit(&self, n: u32) -> Result<()> {
        run_sql_async(self.inner.clone().show_limit(n as usize))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Return the total number of rows.
    #[napi]
    pub fn count(&self) -> Result<u32> {
        let n = run_sql_async(self.inner.clone().count())
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(n as u32)
    }

    /// Filter rows using a SQL WHERE-clause expression.
    ///
    /// @param expr - SQL expression string, e.g. `"amount > 100"`.
    ///
    /// @example
    /// ```typescript
    /// const expensive = df.filter("amount > 100");
    /// ```
    #[napi]
    pub fn filter(&self, expr: String) -> Result<JsDataFrame> {
        let view = tmp_view_name();
        let df = self.inner.clone();
        let session = self.session.clone();
        let result_df = run_sql_async(async move {
            session.register_table(&view, df.into_view())?;
            let result = session
                .sql(&format!("SELECT * FROM {view} WHERE {expr}"))
                .await;
            let _ = session.deregister_table(&view);
            result
        })
        .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Keep only the specified columns.
    ///
    /// @param columns - Array of column name strings.
    ///
    /// @example
    /// Equi-join with another DataFrame on `leftOn` = `rightOn`.
    ///
    /// `how` is one of `inner`, `left`, `right`, `full`/`outer`, `semi`, `anti`.
    ///
    /// ```typescript
    /// const joined = orders.join(customers, "inner", ["customer_id"], ["id"]);
    /// ```
    #[napi]
    pub fn join(
        &self,
        other: &JsDataFrame,
        how: String,
        left_on: Vec<String>,
        right_on: Vec<String>,
    ) -> Result<JsDataFrame> {
        use datafusion::prelude::JoinType;
        let jt = match how.to_ascii_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" | "outer" => JoinType::Full,
            "semi" | "leftsemi" => JoinType::LeftSemi,
            "anti" | "leftanti" => JoinType::LeftAnti,
            _ => return Err(Error::from_reason(format!("unknown join type: {how}"))),
        };
        let left: Vec<&str> = left_on.iter().map(String::as_str).collect();
        let right: Vec<&str> = right_on.iter().map(String::as_str).collect();
        let df = self
            .inner
            .clone()
            .join(other.inner.clone(), jt, &left, &right, None)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Group by `groupBy` columns and compute `aggs` (SQL aggregate expressions like
    /// `"SUM(amount) AS total"`). Empty `groupBy` computes global aggregates.
    ///
    /// ```typescript
    /// df.agg(["region"], ["SUM(amount) AS total", "COUNT(*) AS n"])
    /// ```
    #[napi]
    pub fn agg(&self, group_by: Vec<String>, aggs: Vec<String>) -> Result<JsDataFrame> {
        if aggs.is_empty() {
            return Err(Error::from_reason(
                "agg requires at least one aggregate expression",
            ));
        }
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        let select_list = if group_by.is_empty() {
            aggs.join(", ")
        } else {
            format!("{}, {}", group_by.join(", "), aggs.join(", "))
        };
        let group_clause = if group_by.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", group_by.join(", "))
        };
        let result_df = run_sql_async(async move {
            session.register_table(&view, df.into_view())?;
            let r = session
                .sql(&format!("SELECT {select_list} FROM {view}{group_clause}"))
                .await;
            let _ = session.deregister_table(&view);
            r
        })
        .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Rename all columns positionally to `names` (must match the column count).
    #[napi]
    pub fn to_df(&self, names: Vec<String>) -> Result<JsDataFrame> {
        let fields = self.inner.schema().fields();
        if fields.len() != names.len() {
            return Err(Error::from_reason(format!(
                "toDf: expected {} names, got {}",
                fields.len(),
                names.len()
            )));
        }
        let mut df = self.inner.clone();
        for (field, new_name) in fields.iter().zip(&names) {
            df = df
                .with_column_renamed(field.name(), new_name)
                .map_err(|e| Error::from_reason(e.to_string()))?;
        }
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// ```typescript
    /// const slim = df.select(["id", "name"]);
    /// ```
    #[napi]
    pub fn select(&self, columns: Vec<String>) -> Result<JsDataFrame> {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let df = self
            .inner
            .clone()
            .select_columns(&refs)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Limit the result to the first `n` rows.
    #[napi]
    pub fn limit(&self, n: u32) -> Result<JsDataFrame> {
        let df = self
            .inner
            .clone()
            .limit(0, Some(n as usize))
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Sort by a column name.
    ///
    /// @param col - Column name to sort by.
    /// @param ascending - `true` (default) for ascending, `false` for descending.
    #[napi]
    pub fn sort(&self, col: String, ascending: Option<bool>) -> Result<JsDataFrame> {
        let asc = ascending.unwrap_or(true);
        let df = self
            .inner
            .clone()
            .sort(vec![df_col(&col).sort(asc, true)])
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Return a JSON object mapping column name → Arrow type string.
    ///
    /// @example
    /// ```typescript
    /// df.schema()  // → { "id": "Int64", "name": "Utf8", "amount": "Float64" }
    /// ```
    #[napi]
    pub fn schema(&self) -> Result<serde_json::Value> {
        let mut obj = serde_json::Map::new();
        for field in self.inner.schema().fields() {
            obj.insert(
                field.name().clone(),
                serde_json::Value::String(field.data_type().to_string()),
            );
        }
        Ok(serde_json::Value::Object(obj))
    }

    /// Write all rows to a Parquet file or directory.
    ///
    /// @param path - Output directory path.
    ///
    /// @example
    /// ```typescript
    /// await df.writeParquet("/tmp/output/");
    /// ```
    #[napi]
    pub fn write_parquet(&self, path: String) -> Result<()> {
        let df = self.inner.clone();
        run_sql_async(async move { df.write_parquet(&path, Default::default(), None).await })
            .map(|_| ())
            .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))
    }

    /// Write all rows to CSV files in a directory.
    ///
    /// @param path - Output directory path.
    ///
    /// @example
    /// ```typescript
    /// await df.writeCsv("/tmp/output/");
    /// ```
    #[napi]
    pub fn write_csv(&self, path: String) -> Result<()> {
        let df = self.inner.clone();
        run_sql_async(async move { df.write_csv(&path, Default::default(), None).await })
            .map(|_| ())
            .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))
    }

    /// Return the first `n` rows as an array of objects.
    #[napi]
    pub fn head(&self, n: u32) -> Result<Vec<serde_json::Value>> {
        let df = self
            .inner
            .clone()
            .limit(0, Some(n as usize))
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let batches = run_sql_async(df.collect())
            .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))?;
        Ok(batches_to_json_rows(&batches))
    }

    /// Alias for `filter(expr)`.
    #[napi]
    pub fn where_(&self, expr: String) -> Result<JsDataFrame> {
        self.filter(expr)
    }

    /// Return a new DataFrame without the named columns.
    #[napi]
    pub fn drop(&self, cols: Vec<String>) -> Result<JsDataFrame> {
        let all: Vec<String> = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| !cols.contains(n))
            .collect();
        self.select(all)
    }

    /// Remove duplicate rows.
    #[napi]
    pub fn distinct(&self) -> Result<JsDataFrame> {
        let df = self
            .inner
            .clone()
            .distinct()
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Union with another DataFrame (same schema required).
    #[napi]
    pub fn union(&self, other: &JsDataFrame) -> Result<JsDataFrame> {
        let df = self
            .inner
            .clone()
            .union(other.inner.clone())
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Descriptive statistics via SQL DESCRIBE.
    #[napi]
    pub fn describe(&self) -> Result<JsDataFrame> {
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        session
            .register_table(&view, df.into_view())
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let result_df = run_sql_async(async move {
            let r = session.sql(&format!("DESCRIBE {view}")).await;
            let _ = session.deregister_table(&view);
            r
        })
        .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Remove duplicate rows, optionally within the given columns.
    #[napi]
    pub fn drop_duplicates(&self, cols: Option<Vec<String>>) -> Result<JsDataFrame> {
        let result_df = match cols {
            Some(ref columns) if !columns.is_empty() => {
                let view = tmp_view_name();
                let cols_str = columns.join(", ");
                let session = self.session.clone();
                let df = self.inner.clone();
                session
                    .register_table(&view, df.into_view())
                    .map_err(|e| Error::from_reason(e.to_string()))?;
                run_sql_async(async move {
                    let result = session
                        .sql(&format!("SELECT DISTINCT ON ({cols_str}) * FROM {view}"))
                        .await;
                    let _ = session.deregister_table(&view);
                    result
                })
                .map_err(|e: datafusion::error::DataFusionError| {
                    Error::from_reason(e.to_string())
                })?
            }
            _ => self
                .inner
                .clone()
                .distinct()
                .map_err(|e| Error::from_reason(e.to_string()))?,
        };
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// List of column names.
    #[napi]
    pub fn columns(&self) -> Vec<String> {
        self.inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// List of `[column_name, dtype_string]` pairs.
    #[napi]
    pub fn dtypes(&self) -> Vec<Vec<String>> {
        self.inner
            .schema()
            .fields()
            .iter()
            .map(|f| vec![f.name().clone(), f.data_type().to_string()])
            .collect()
    }

    /// Intersect with another DataFrame (same schema), deduplicated (Spark `intersect`).
    #[napi]
    pub fn intersect(&self, other: &JsDataFrame) -> Result<JsDataFrame> {
        // DataFusion's `intersect` keeps duplicates (INTERSECT ALL); Spark's is distinct.
        let df = self
            .inner
            .clone()
            .intersect_distinct(other.inner.clone())
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Set difference — distinct rows in self not in other (Spark `except`).
    #[napi]
    pub fn except(&self, other: &JsDataFrame) -> Result<JsDataFrame> {
        // DataFusion's `except` keeps duplicates (EXCEPT ALL); Spark's is distinct.
        let df = self
            .inner
            .clone()
            .except_distinct(other.inner.clone())
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Random sample at the given fraction (0.0 to 1.0).
    #[napi]
    pub fn sample(&self, fraction: f64) -> Result<JsDataFrame> {
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let result_df = run_sql_async(async move {
            let r = session
                .sql(&format!("SELECT * FROM {view} WHERE random() < {fraction}"))
                .await;
            let _ = session.deregister_table(&view);
            r
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Fill null values in a column with a numeric value.
    #[napi]
    pub fn fill_null(&self, col: String, value: f64) -> Result<JsDataFrame> {
        let view = tmp_view_name();
        let all_cols: Vec<String> = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let sel: Vec<String> = all_cols
            .iter()
            .map(|c| {
                if *c == col {
                    format!("COALESCE({col}, {value}) AS {col}")
                } else {
                    c.clone()
                }
            })
            .collect();
        let cols_str = sel.join(", ");
        let session = self.session.clone();
        let df = self.inner.clone();
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let result_df = run_sql_async(async move {
            let r = session.sql(&format!("SELECT {cols_str} FROM {view}")).await;
            let _ = session.deregister_table(&view);
            r
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Drop rows with nulls in any of the given columns.
    #[napi]
    pub fn drop_null(&self, cols: Vec<String>) -> Result<JsDataFrame> {
        if cols.is_empty() {
            return Ok(JsDataFrame {
                inner: self.inner.clone(),
                session: self.session.clone(),
            });
        }
        let view = tmp_view_name();
        let cond = cols
            .iter()
            .map(|c| format!("{c} IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let session = self.session.clone();
        let df = self.inner.clone();
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let result_df = run_sql_async(async move {
            let r = session
                .sql(&format!("SELECT * FROM {view} WHERE {cond}"))
                .await;
            let _ = session.deregister_table(&view);
            r
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Pearson correlation between two numeric columns.
    #[napi]
    pub fn corr(&self, col1: String, col2: String) -> Result<f64> {
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let batches = run_sql_async(async move {
            session
                .sql(&format!("SELECT CORR({col1}, {col2}) FROM {view}"))
                .await?
                .collect()
                .await
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        if let Some(batch) = batches.first()
            && batch.num_rows() > 0
            && batch.num_columns() > 0
        {
            return Ok(arrow_scalar_to_json(batch.column(0).as_ref(), 0)
                .as_f64()
                .unwrap_or(f64::NAN));
        }
        Ok(f64::NAN)
    }

    /// Population covariance between two numeric columns.
    #[napi]
    pub fn cov(&self, col1: String, col2: String) -> Result<f64> {
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let batches = run_sql_async(async move {
            session
                .sql(&format!("SELECT COVAR_POP({col1}, {col2}) FROM {view}"))
                .await?
                .collect()
                .await
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        if let Some(batch) = batches.first()
            && batch.num_rows() > 0
            && batch.num_columns() > 0
        {
            return Ok(arrow_scalar_to_json(batch.column(0).as_ref(), 0)
                .as_f64()
                .unwrap_or(f64::NAN));
        }
        Ok(f64::NAN)
    }

    /// Cross-tabulation of two columns via SQL.
    #[napi]
    pub fn crosstab(&self, col1: String, col2: String) -> Result<JsDataFrame> {
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let result_df = run_sql_async(async move {
            let r = session
                .sql(&format!(
                    "SELECT {col1}, {col2}, COUNT(*) AS cnt FROM {view} GROUP BY {col1}, {col2}"
                ))
                .await;
            let _ = session.deregister_table(&view);
            r
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: result_df,
            session: self.session.clone(),
        })
    }

    /// Return the last `n` rows.
    #[napi]
    pub fn tail(&self, n: u32) -> Result<Vec<serde_json::Value>> {
        let view = tmp_view_name();
        let session = self.session.clone();
        let df = self.inner.clone();
        let total =
            run_sql_async(df.clone().count()).map_err(|e| Error::from_reason(e.to_string()))?;
        let offset = total.saturating_sub(n as usize);
        if let Err(e) = session.register_table(&view, df.into_view()) {
            return Err(Error::from_reason(e.to_string()));
        }
        let result = run_sql_async(async move {
            let batches = session
                .sql(&format!("SELECT * FROM {view} LIMIT {n} OFFSET {offset}"))
                .await?
                .collect()
                .await?;
            Ok::<_, datafusion::error::DataFusionError>(batches)
        })
        .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(batches_to_json_rows(&result))
    }
}

/// SQL execution context backed by DataFusion.
///
/// Register data sources (CSV, Parquet, JSON) and execute SQL queries.
///
/// @example
/// ```typescript
/// import { SqlContext } from "@atomic-compute/js";
///
/// const ctx = new SqlContext();
/// ctx.registerCsv("orders", "orders.csv");
/// const df = ctx.sql("SELECT id, SUM(amount) FROM orders GROUP BY id");
/// const rows = df.collect();
/// ```
#[napi(js_name = "SqlContext")]
pub struct JsSqlContext {
    inner: Arc<AtomicSqlContext>,
    session: Arc<SessionContext>,
}

#[napi]
impl JsSqlContext {
    /// Create an SQL context.
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        let inner = Arc::new(AtomicSqlContext::new());
        let session = Arc::new(inner.inner().clone());
        Ok(Self { inner, session })
    }

    /// Parse and execute a SQL query. Returns a lazy `DataFrame`.
    ///
    /// The DataFrame is not executed until `collect()`, `show()`, or `count()` is called.
    ///
    /// @param query - SQL query string.
    #[napi]
    pub fn sql(&self, query: String) -> Result<JsDataFrame> {
        let session = self.session.clone();
        let df = run_sql_async(async move { session.sql(&query).await })
            .map_err(|e: datafusion::error::DataFusionError| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// Register a CSV file or directory as a named table.
    ///
    /// @param name - Table name to use in SQL queries.
    /// @param path - Path to the CSV file or directory.
    #[napi]
    pub fn register_csv(&self, name: String, path: String) -> Result<()> {
        let ctx = self.inner.clone();
        run_sql_async(async move {
            ctx.register_csv(
                &name,
                &path,
                datafusion::datasource::file_format::options::CsvReadOptions::default(),
            )
            .await
        })
        .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Register a Parquet file or directory as a named table.
    ///
    /// @param name - Table name to use in SQL queries.
    /// @param path - Path to the Parquet file or directory.
    #[napi]
    pub fn register_parquet(&self, name: String, path: String) -> Result<()> {
        let ctx = self.inner.clone();
        run_sql_async(async move {
            ctx.register_parquet(
                &name,
                &path,
                datafusion::datasource::file_format::options::ParquetReadOptions::default(),
            )
            .await
        })
        .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Register a JSONL file or directory as a named table.
    ///
    /// @param name - Table name to use in SQL queries.
    /// @param path - Path to the JSONL file or directory.
    #[napi]
    pub fn register_json(&self, name: String, path: String) -> Result<()> {
        let ctx = self.inner.clone();
        run_sql_async(async move {
            ctx.register_json(
                &name,
                &path,
                datafusion::datasource::file_format::options::JsonReadOptions::default(),
            )
            .await
        })
        .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Register an `Rdd` as a named SQL table (the RDD→SQL bridge).
    ///
    /// The RDD's rows (JS objects) are materialized into an Arrow table using the
    /// supplied `schema` (column name → Arrow type string), then registered so SQL
    /// can query them. Mirrors `SqlContext.register_rdd` in the Python bindings.
    ///
    /// @param name - Table name to use in SQL queries.
    /// @param rdd - The RDD whose rows become table rows.
    /// @param schema - Map of column name to Arrow type (e.g. `{ id: "int64", val: "float64" }`).
    ///
    /// ```js
    /// const rdd = ctx.parallelize([{ id: 1, val: 2.5 }, { id: 2, val: 3.0 }]);
    /// sqlCtx.registerRdd("data", rdd, { id: "int64", val: "float64" });
    /// const df = sqlCtx.sql("SELECT * FROM data WHERE val > 2.0");
    /// ```
    #[napi]
    pub fn register_rdd(
        &self,
        name: String,
        rdd: &JsRdd,
        schema: HashMap<String, String>,
    ) -> Result<()> {
        let rows = rdd.collect_rows()?;
        let batches = json_rows_to_batches(&rows, &schema)?;
        self.inner
            .register_partitioned_batches(&name, vec![batches])
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Register an Avro file (or directory) as a named table. Requires the `avro` feature;
    /// without it the call returns an error. The method stays napi-visible either way so the
    /// generated registration table is not conditional.
    #[napi]
    pub fn register_avro(&self, name: String, path: String) -> Result<()> {
        #[cfg(feature = "avro")]
        {
            let ctx = self.inner.clone();
            run_sql_async(async move {
                ctx.register_avro(
                    &name,
                    &path,
                    datafusion::prelude::AvroReadOptions::default(),
                )
                .await
            })
            .map_err(|e| Error::from_reason(e.to_string()))
        }
        #[cfg(not(feature = "avro"))]
        {
            let _ = (name, path);
            Err(Error::from_reason(
                "atomic-js was built without the 'avro' feature",
            ))
        }
    }

    /// Remove a previously registered table from the catalog.
    #[napi]
    pub fn deregister_table(&self, name: String) -> Result<()> {
        self.inner
            .deregister_table(&name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Return a registered table as a lazy DataFrame.
    #[napi]
    pub fn table(&self, name: String) -> Result<JsDataFrame> {
        let session = self.session.clone();
        let df = run_sql_async(async move { session.table(&name).await })
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df,
            session: self.session.clone(),
        })
    }

    /// List the names of all registered tables.
    #[napi]
    pub fn table_names(&self) -> Result<Vec<String>> {
        self.inner
            .table_names()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Read a data source by path, returning a lazy DataFrame. `format` is `csv`/`parquet`/`json`.
    #[napi]
    pub fn read(&self, format: String, path: String) -> Result<JsDataFrame> {
        use atomic_sql::context::DataFormat;
        let fmt = match format.to_ascii_lowercase().as_str() {
            "csv" => DataFormat::Csv,
            "parquet" => DataFormat::Parquet,
            "json" => DataFormat::Json,
            #[cfg(feature = "avro")]
            "avro" => DataFormat::Avro,
            _ => return Err(Error::from_reason(format!("unknown format: {format}"))),
        };
        let ctx = self.inner.clone();
        let df = run_sql_async(async move { ctx.read(fmt, &path).await })
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(JsDataFrame {
            inner: df.into_inner(),
            session: self.session.clone(),
        })
    }
}

/// Parse an Arrow type string into a DataFusion `DataType`.
pub(crate) fn parse_arrow_type(s: &str) -> Result<DataType> {
    use datafusion::arrow::datatypes::TimeUnit;
    Ok(match s.to_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" | "int" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" | "double" | "float" => DataType::Float64,
        "bool" | "boolean" => DataType::Boolean,
        "utf8" | "string" | "str" => DataType::Utf8,
        "timestamp_ms" | "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        other => {
            return Err(Error::from_reason(format!(
                "unsupported Arrow type: {other}. Supported: int8/16/32/64, uint8/16/32/64, float32/64, bool, utf8, timestamp_ms"
            )));
        }
    })
}

/// Convert JSON object rows into a single Arrow `RecordBatch` per the schema.
fn json_rows_to_batches(
    rows: &[serde_json::Value],
    schema: &HashMap<String, String>,
) -> Result<Vec<RecordBatch>> {
    use datafusion::arrow::array::{
        ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
        UInt64Builder,
    };
    use datafusion::arrow::datatypes::{Field, Schema};

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let columns: Vec<(String, DataType)> = schema
        .iter()
        .map(|(k, v)| Ok((k.clone(), parse_arrow_type(v)?)))
        .collect::<Result<_>>()?;

    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, dt)| Field::new(name, dt.clone(), true))
        .collect();
    let arrow_schema = Arc::new(Schema::new(fields));

    // Each column reads `row[$col]` from every JSON object row. Defined once here (not per
    // iteration); the column name is a macro parameter since macro_rules is hygienic and
    // resolves free identifiers at this definition site, where the loop variable is not in scope.
    macro_rules! build_int {
        ($builder_ty:ty, $col:expr, $cast:expr) => {{
            let mut b = <$builder_ty>::new();
            for row in rows {
                match row.get($col).and_then(serde_json::Value::as_i64) {
                    Some(v) => b.append_value($cast(v)),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    macro_rules! build_uint {
        ($builder_ty:ty, $col:expr, $cast:expr) => {{
            let mut b = <$builder_ty>::new();
            for row in rows {
                match row.get($col).and_then(serde_json::Value::as_u64) {
                    Some(v) => b.append_value($cast(v)),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let mut col_arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (col_name, col_type) in &columns {
        let array: ArrayRef = match col_type {
            DataType::Int8 => build_int!(Int8Builder, col_name, |v| v as i8),
            DataType::Int16 => build_int!(Int16Builder, col_name, |v| v as i16),
            DataType::Int32 => build_int!(Int32Builder, col_name, |v| v as i32),
            DataType::Int64 => build_int!(Int64Builder, col_name, |v| v),
            DataType::UInt8 => build_uint!(UInt8Builder, col_name, |v| v as u8),
            DataType::UInt16 => build_uint!(UInt16Builder, col_name, |v| v as u16),
            DataType::UInt32 => build_uint!(UInt32Builder, col_name, |v| v as u32),
            DataType::UInt64 => build_uint!(UInt64Builder, col_name, |v| v),
            DataType::Float32 => {
                let mut b = Float32Builder::new();
                for row in rows {
                    match row.get(col_name).and_then(serde_json::Value::as_f64) {
                        Some(v) => b.append_value(v as f32),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for row in rows {
                    match row.get(col_name).and_then(serde_json::Value::as_f64) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for row in rows {
                    match row.get(col_name).and_then(serde_json::Value::as_bool) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = StringBuilder::new();
                for row in rows {
                    match row.get(col_name).and_then(serde_json::Value::as_str) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        col_arrays.push(array);
    }

    let batch = RecordBatch::try_new(arrow_schema, col_arrays)
        .map_err(|e| Error::from_reason(e.to_string()))?;
    Ok(vec![batch])
}
