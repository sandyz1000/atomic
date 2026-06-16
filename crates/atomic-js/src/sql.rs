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

fn batches_to_json_rows(batches: &[RecordBatch]) -> Vec<serde_json::Value> {
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

    /// Remove a previously registered table from the catalog.
    #[napi]
    pub fn deregister_table(&self, name: String) -> Result<()> {
        self.inner
            .deregister_table(&name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }
}

/// Parse an Arrow type string into a DataFusion `DataType`.
fn parse_arrow_type(s: &str) -> Result<DataType> {
    Ok(match s.to_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "bool" | "boolean" => DataType::Boolean,
        "utf8" | "string" | "str" => DataType::Utf8,
        other => {
            return Err(Error::from_reason(format!(
                "unsupported Arrow type: {other}. Supported: int8/16/32/64, uint8/16/32/64, float32/64, bool, utf8"
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

    let mut col_arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (col_name, col_type) in &columns {
        // Each column reads `row[col_name]` from every JSON object row.
        macro_rules! build_int {
            ($builder_ty:ty, $cast:expr) => {{
                let mut b = <$builder_ty>::new();
                for row in rows {
                    match row.get(col_name).and_then(serde_json::Value::as_i64) {
                        Some(v) => b.append_value($cast(v)),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }};
        }
        macro_rules! build_uint {
            ($builder_ty:ty, $cast:expr) => {{
                let mut b = <$builder_ty>::new();
                for row in rows {
                    match row.get(col_name).and_then(serde_json::Value::as_u64) {
                        Some(v) => b.append_value($cast(v)),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }};
        }
        let array: ArrayRef = match col_type {
            DataType::Int8 => build_int!(Int8Builder, |v| v as i8),
            DataType::Int16 => build_int!(Int16Builder, |v| v as i16),
            DataType::Int32 => build_int!(Int32Builder, |v| v as i32),
            DataType::Int64 => build_int!(Int64Builder, |v| v),
            DataType::UInt8 => build_uint!(UInt8Builder, |v| v as u8),
            DataType::UInt16 => build_uint!(UInt16Builder, |v| v as u16),
            DataType::UInt32 => build_uint!(UInt32Builder, |v| v as u32),
            DataType::UInt64 => build_uint!(UInt64Builder, |v| v),
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
