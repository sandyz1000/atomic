use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame as DFDataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::col as df_col;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

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


fn arrow_scalar_to_py(py: Python<'_>, col: &dyn Array, row: usize) -> Py<PyAny> {
    if col.is_null(row) {
        return py.None();
    }
    match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).into_pyobject(py).unwrap().to_owned().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Int8 => col
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| (a.value(row) as i64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Int16 => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| (a.value(row) as i64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| (a.value(row) as i64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::UInt8 => col
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| (a.value(row) as i64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::UInt16 => col
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| (a.value(row) as i64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| (a.value(row) as u64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| (a.value(row) as f64).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        DataType::Utf8 | DataType::LargeUtf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).into_pyobject(py).unwrap().into_any().unbind())
            .unwrap_or_else(|| py.None()),
        _ => {
            // Fallback: format as string
            format!("{:?}", col.data_type())
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind()
        }
    }
}

fn batches_to_py_list(py: Python<'_>, batches: &[RecordBatch]) -> PyResult<Py<PyAny>> {
    let rows = PyList::empty(py);
    for batch in batches {
        let schema = batch.schema();
        let fields = schema.fields();
        for row_idx in 0..batch.num_rows() {
            let row = PyDict::new(py);
            for (col_idx, field) in fields.iter().enumerate() {
                let val = arrow_scalar_to_py(py, batch.column(col_idx).as_ref(), row_idx);
                row.set_item(field.name(), val)?;
            }
            rows.append(row)?;
        }
    }
    Ok(rows.into_any().unbind())
}


/// A lazy structured dataset produced by `SqlContext.sql()` or registered tables.
///
/// Most methods return a new `DataFrame` (lazy); calling `collect()`, `count()`, or
/// `show()` triggers execution.
///
/// # Example
/// ```python
/// df = ctx.sql("SELECT id, value FROM t WHERE value > 10")
/// rows = df.collect()           # → list[dict]
/// df.show()                     # pretty-print to stdout
/// df2 = df.limit(5)             # new lazy DataFrame
/// df3 = df.filter("value < 50") # add a WHERE filter
/// ```
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    inner: DFDataFrame,
    session: Arc<SessionContext>,
}

#[pymethods]
impl PyDataFrame {

    /// Execute the plan and return all rows as a list of dicts.
    ///
    /// Each dict maps column name → Python value (int, float, str, bool, or None).
    pub fn collect(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batches = run_sql_async(self.inner.clone().collect())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        batches_to_py_list(py, &batches)
    }

    /// Execute and print a formatted table to stdout (default: 20 rows).
    pub fn show(&self) -> PyResult<()> {
        run_sql_async(self.inner.clone().show())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Execute and print the first `n` rows to stdout.
    pub fn show_limit(&self, n: usize) -> PyResult<()> {
        run_sql_async(self.inner.clone().show_limit(n))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the total number of rows.
    pub fn count(&self) -> PyResult<usize> {
        run_sql_async(self.inner.clone().count())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }


    /// Filter rows using a SQL WHERE-clause expression (e.g. `"amount > 100"`).
    ///
    /// Internally registers a temp view and executes `SELECT * FROM … WHERE expr`.
    pub fn filter(&self, expr: &str) -> PyResult<Self> {
        let view = tmp_view_name();
        let df = self.inner.clone();
        let session = self.session.clone();
        let expr = expr.to_string();
        let result_df = run_sql_async(async move {
            session.register_table(&view, df.into_view())?;
            let result = session.sql(&format!("SELECT * FROM {view} WHERE {expr}")).await;
            let _ = session.deregister_table(&view);
            result
        })
        .map_err(|e: datafusion::error::DataFusionError| {
            pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
        })?;
        Ok(PyDataFrame { inner: result_df, session: self.session.clone() })
    }

    /// Keep only the specified columns.
    ///
    /// ```python
    /// df.select(["id", "name"])
    /// ```
    pub fn select(&self, columns: Vec<String>) -> PyResult<Self> {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let df = self.inner.clone().select_columns(&refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df, session: self.session.clone() })
    }

    /// Limit the result to the first `n` rows.
    pub fn limit(&self, n: usize) -> PyResult<Self> {
        let df = self.inner.clone().limit(0, Some(n))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df, session: self.session.clone() })
    }

    /// Sort by a column name. `ascending=True` (default) for ascending order.
    pub fn sort(&self, col: &str, ascending: Option<bool>) -> PyResult<Self> {
        let asc = ascending.unwrap_or(true);
        let df = self.inner.clone().sort(vec![df_col(col).sort(asc, true)])
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df, session: self.session.clone() })
    }


    /// Return a dict mapping column name → Arrow type string.
    ///
    /// ```python
    /// df.schema()   # → {"id": "Int64", "name": "Utf8", "amount": "Float64"}
    /// ```
    pub fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for field in self.inner.schema().fields() {
            dict.set_item(field.name(), field.data_type().to_string())?;
        }
        Ok(dict.into())
    }

    /// Write all rows to a Parquet file or directory.
    ///
    /// ```python
    /// df.write_parquet("/tmp/output/")
    /// ```
    pub fn write_parquet(&self, path: &str) -> PyResult<()> {
        let df = self.inner.clone();
        let path = path.to_string();
        run_sql_async(async move {
            df.write_parquet(&path, Default::default(), None).await
        })
        .map(|_| ())
        .map_err(|e: datafusion::error::DataFusionError| {
            pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
        })
    }

    /// Write all rows to CSV files in a directory.
    ///
    /// ```python
    /// df.write_csv("/tmp/output/")
    /// ```
    pub fn write_csv(&self, path: &str) -> PyResult<()> {
        let df = self.inner.clone();
        let path = path.to_string();
        run_sql_async(async move {
            df.write_csv(&path, Default::default(), None).await
        })
        .map(|_| ())
        .map_err(|e: datafusion::error::DataFusionError| {
            pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
        })
    }

    /// Convert the DataFrame to a PyArrow `Table`.
    ///
    /// Collects all rows and returns them as a PyArrow `Table` for use with
    /// pandas, polars, or any other Arrow-compatible Python library.
    ///
    /// ```python
    /// table = df.to_arrow()
    /// df_pandas = table.to_pandas()
    /// ```
    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use datafusion::arrow::ipc::writer::FileWriter;
        use std::io::Cursor;

        let df = self.inner.clone();
        let batches: Vec<RecordBatch> = run_sql_async(async move { df.collect().await })
            .map_err(|e: datafusion::error::DataFusionError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })?;

        if batches.is_empty() {
            let pa = py.import("pyarrow")?;
            return Ok(pa
                .call_method1("table", (pyo3::types::PyList::empty(py),))?
                .into_any()
                .unbind());
        }

        // Serialize to Arrow IPC format and deserialize via pyarrow
        let schema = batches[0].schema();
        let mut buf = Cursor::new(Vec::<u8>::new());
        let mut writer = FileWriter::try_new(&mut buf, &schema)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let ipc_bytes = buf.into_inner();
        let pa_ipc = py.import("pyarrow.ipc")?;
        let py_bytes = pyo3::types::PyBytes::new(py, &ipc_bytes);
        let reader = pa_ipc.call_method1("open_file", (py_bytes,))?;
        Ok(reader.call_method0("read_all")?.into_any().unbind())
    }

    fn __repr__(&self) -> String {
        let fields: Vec<String> = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect();
        format!("DataFrame([{}])", fields.join(", "))
    }
}


/// SQL execution context backed by DataFusion.
///
/// Register data sources (CSV, Parquet, JSON) and execute SQL queries. Works
/// standalone or alongside the RDD `Context` for mixed workloads.
///
/// # Example
/// ```python
/// from atomic_compute import SqlContext
///
/// ctx = SqlContext()
/// ctx.register_csv("orders", "orders.csv")
/// df = ctx.sql("SELECT id, SUM(amount) FROM orders GROUP BY id")
/// rows = df.collect()
/// # [{"id": 1, "amount": 300.0}, {"id": 2, "amount": 150.0}]
/// ```
#[pyclass(name = "SqlContext")]
pub struct PySqlContext {
    inner: Arc<AtomicSqlContext>,
    session: Arc<SessionContext>,
}

#[pymethods]
impl PySqlContext {
    /// Create an SQL context.
    ///
    /// No arguments required. Parallelism defaults to the number of logical CPUs.
    #[new]
    pub fn new() -> PyResult<Self> {
        let inner = Arc::new(AtomicSqlContext::new());
        let session = Arc::new(inner.inner().clone());
        Ok(Self { inner, session })
    }


    /// Parse and execute a SQL query. Returns a lazy `DataFrame`.
    ///
    /// The DataFrame is not executed until an action (`collect`, `show`, `count`)
    /// is called.
    ///
    /// ```python
    /// df = ctx.sql("SELECT * FROM orders WHERE amount > 100")
    /// rows = df.collect()
    /// ```
    pub fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let session = self.session.clone();
        let query = query.to_string();
        let df = run_sql_async(async move { session.sql(&query).await })
            .map_err(|e: datafusion::error::DataFusionError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })?;
        Ok(PyDataFrame { inner: df, session: self.session.clone() })
    }


    /// Register a CSV file (or directory of CSV files) as a named table.
    ///
    /// ```python
    /// ctx.register_csv("orders", "data/orders.csv")
    /// ```
    pub fn register_csv(&self, name: &str, path: &str) -> PyResult<()> {
        let ctx = self.inner.clone();
        let name = name.to_string();
        let path = path.to_string();
        run_sql_async(async move {
            ctx.register_csv(
                &name,
                &path,
                datafusion::datasource::file_format::options::CsvReadOptions::default(),
            )
            .await
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Register a Parquet file (or directory) as a named table.
    ///
    /// ```python
    /// ctx.register_parquet("orders", "data/orders.parquet")
    /// ```
    pub fn register_parquet(&self, name: &str, path: &str) -> PyResult<()> {
        let ctx = self.inner.clone();
        let name = name.to_string();
        let path = path.to_string();
        run_sql_async(async move {
            ctx.register_parquet(
                &name,
                &path,
                datafusion::datasource::file_format::options::ParquetReadOptions::default(),
            )
            .await
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Register a JSONL file (or directory) as a named table.
    ///
    /// ```python
    /// ctx.register_json("events", "data/events.jsonl")
    /// ```
    pub fn register_json(&self, name: &str, path: &str) -> PyResult<()> {
        let ctx = self.inner.clone();
        let name = name.to_string();
        let path = path.to_string();
        run_sql_async(async move {
            ctx.register_json(
                &name,
                &path,
                datafusion::datasource::file_format::options::JsonReadOptions::default(),
            )
            .await
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Remove a previously registered table from the catalog.
    pub fn deregister_table(&self, name: &str) -> PyResult<()> {
        self.inner
            .deregister_table(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Register a Python RDD as a SQL table.
    ///
    /// Collects all elements from the RDD (elements must be `dict`s), converts
    /// them to Arrow batches using the provided schema, and registers the result
    /// as a table that can be queried with `ctx.sql(...)`.
    ///
    /// `schema` is a `dict[str, str]` mapping column names to Arrow type strings:
    /// `"int8"`, `"int16"`, `"int32"`, `"int64"`, `"uint8"`, `"uint16"`,
    /// `"uint32"`, `"uint64"`, `"float32"`, `"float64"`, `"bool"`, `"utf8"`.
    ///
    /// ```python
    /// rdd = ctx.parallelize([{"id": 1, "val": 2.5}, {"id": 2, "val": 3.0}])
    /// sql_ctx.register_rdd("data", rdd, {"id": "int64", "val": "float64"})
    /// df = sql_ctx.sql("SELECT * FROM data WHERE val > 2.0")
    /// ```
    pub fn register_rdd(
        &self,
        py: Python<'_>,
        name: &str,
        rdd: &mut crate::rdd::PyRdd,
        schema: std::collections::HashMap<String, String>,
    ) -> PyResult<()> {
        let rows_obj: Py<PyAny> = rdd.collect(py)?;
        let rows: Vec<Py<PyAny>> = rows_obj.extract::<Vec<Py<PyAny>>>(py)?;
        let batches = python_dicts_to_batches(py, &rows, &schema)?;
        self.inner
            .register_partitioned_batches(name, vec![batches])
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Register a Python callable as a SQL scalar UDF.
    ///
    /// `input_types` is a list of Arrow type strings for the input arguments.
    /// `return_type` is the Arrow type string for the return value.
    ///
    /// ```python
    /// sql_ctx.register_udf("double_int", lambda x: x * 2, ["int64"], "int64")
    /// df = sql_ctx.sql("SELECT double_int(value) FROM table")
    /// ```
    pub fn register_udf(
        &self,
        name: &str,
        func: Py<PyAny>,
        input_types: Vec<String>,
        return_type: String,
    ) -> PyResult<()> {
        use datafusion::arrow::datatypes::DataType as ArrowDataType;
        use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
        use std::sync::Arc;

        let input_dts: Vec<ArrowDataType> = input_types
            .iter()
            .map(|s| parse_arrow_type(s))
            .collect::<PyResult<_>>()?;
        let return_dt = parse_arrow_type(&return_type)?;
        let return_dt_clone = return_dt.clone();
        let name_owned = name.to_string();

        // Wrap the Python callable in a DataFusion ScalarUDF.
        #[derive(Debug)]
        struct PyUdf {
            name: String,
            func: Py<PyAny>,
            signature: Signature,
            return_type: ArrowDataType,
        }

        impl PartialEq for PyUdf {
            fn eq(&self, other: &Self) -> bool {
                self.name == other.name
            }
        }
        impl Eq for PyUdf {}
        impl std::hash::Hash for PyUdf {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.name.hash(state);
            }
        }

        impl ScalarUDFImpl for PyUdf {
            fn as_any(&self) -> &dyn std::any::Any { self }
            fn name(&self) -> &str { &self.name }
            fn signature(&self) -> &Signature { &self.signature }
            fn return_type(&self, _: &[ArrowDataType]) -> datafusion::common::Result<ArrowDataType> {
                Ok(self.return_type.clone())
            }
            fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
                use datafusion::arrow::array::{ArrayRef, Int64Array, Float64Array, StringArray, BooleanArray};
                // For each row, call the Python function and collect results.
                let first_arg = args.args.first()
                    .ok_or_else(|| datafusion::common::DataFusionError::Execution("UDF requires at least one arg".into()))?;
                let len = match first_arg {
                    ColumnarValue::Array(a) => a.len(),
                    ColumnarValue::Scalar(_) => 1,
                };
                let results: Vec<Option<f64>> = Python::attach(|py| {
                    let func = self.func.clone_ref(py);
                    (0..len).map(|i| {
                        let arg_val: Py<PyAny> = match first_arg {
                            ColumnarValue::Array(arr) => {
                                // Extract element i as Python object
                                if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                                    (a.value(i) as f64).into_pyobject(py).ok()?.into_any().unbind()
                                } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                                    a.value(i).into_pyobject(py).ok()?.into_any().unbind()
                                } else {
                                    return None;
                                }
                            }
                            ColumnarValue::Scalar(s) => {
                                s.to_array().ok()?.len();
                                return None;
                            }
                        };
                        let result = func.call1(py, (arg_val,)).ok()?;
                        result.extract::<f64>(py).ok()
                    }).collect()
                });
                let arr: ArrayRef = Arc::new(Float64Array::from(results));
                Ok(ColumnarValue::Array(arr))
            }
        }

        let udf = PyUdf {
            name: name_owned,
            func,
            signature: Signature::exact(input_dts, Volatility::Volatile),
            return_type: return_dt_clone,
        };
        self.session.register_udf(ScalarUDF::new_from_impl(udf));
        Ok(())
    }

    fn __repr__(&self) -> String {
        "SqlContext()".to_string()
    }
}

/// Parse an Arrow type string to a `DataType`.
fn parse_arrow_type(s: &str) -> PyResult<datafusion::arrow::datatypes::DataType> {
    use datafusion::arrow::datatypes::DataType;
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
        other => return Err(pyo3::exceptions::PyValueError::new_err(
            format!("unsupported Arrow type: {other}. Supported: int8/16/32/64, uint8/16/32/64, float32/64, bool, utf8")
        )),
    })
}

/// Convert a list of Python dicts to a single Arrow `RecordBatch`.
fn python_dicts_to_batches(
    py: Python<'_>,
    rows: &[Py<PyAny>],
    schema: &std::collections::HashMap<String, String>,
) -> PyResult<Vec<RecordBatch>> {
    use datafusion::arrow::array::{
        BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder,
        UInt64Builder, UInt8Builder,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let columns: Vec<(String, DataType)> = schema
        .iter()
        .map(|(k, v)| Ok((k.clone(), parse_arrow_type(v)?)))
        .collect::<PyResult<_>>()?;

    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, dt)| Field::new(name, dt.clone(), true))
        .collect();
    let arrow_schema = Arc::new(Schema::new(fields));

    // Build one array per column.
    let mut col_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();
    for (col_name, col_type) in &columns {
        macro_rules! build_numeric {
            ($builder_ty:ty, $extract_ty:ty) => {{
                let mut b = <$builder_ty>::new();
                for row in rows {
                    let dict = row.bind(py).downcast::<pyo3::types::PyDict>()?;
                    match dict.get_item(col_name)? {
                        Some(v) => b.append_option(v.extract::<$extract_ty>().ok()),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish()) as datafusion::arrow::array::ArrayRef
            }};
        }
        let array: datafusion::arrow::array::ArrayRef = match col_type {
            DataType::Int8 => build_numeric!(Int8Builder, i8),
            DataType::Int16 => build_numeric!(Int16Builder, i16),
            DataType::Int32 => build_numeric!(Int32Builder, i32),
            DataType::Int64 => build_numeric!(Int64Builder, i64),
            DataType::UInt8 => build_numeric!(UInt8Builder, u8),
            DataType::UInt16 => build_numeric!(UInt16Builder, u16),
            DataType::UInt32 => build_numeric!(UInt32Builder, u32),
            DataType::UInt64 => build_numeric!(UInt64Builder, u64),
            DataType::Float32 => build_numeric!(Float32Builder, f32),
            DataType::Float64 => build_numeric!(Float64Builder, f64),
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for row in rows {
                    let dict = row.bind(py).downcast::<pyo3::types::PyDict>()?;
                    match dict.get_item(col_name)? {
                        Some(v) => b.append_option(v.extract::<bool>().ok()),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 | _ => {
                let mut b = StringBuilder::new();
                for row in rows {
                    let dict = row.bind(py).downcast::<pyo3::types::PyDict>()?;
                    match dict.get_item(col_name)? {
                        Some(v) => b.append_option(v.extract::<String>().ok()),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        col_arrays.push(array);
    }

    let batch = RecordBatch::try_new(arrow_schema, col_arrays)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(vec![batch])
}
