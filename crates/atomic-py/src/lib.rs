mod context;
mod rdd;
mod sql;

use pyo3::prelude::*;

use context::PyContext;
use rdd::PyRdd;
use sql::{PyDataFrame, PySqlContext};

/// Atomic Python client — Spark-like distributed computing and SQL for Python.
///
/// # RDD quick start
/// ```python
/// import atomic_compute
///
/// ctx = atomic_compute.Context()
/// result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x + 1).collect()
/// # [2, 3, 4, 5]
/// ```
///
/// # SQL quick start
/// ```python
/// from atomic_compute import SqlContext
///
/// ctx = SqlContext()
/// ctx.register_csv("orders", "orders.csv")
/// df = ctx.sql("SELECT id, SUM(amount) FROM orders GROUP BY id")
/// rows = df.collect()   # → list[dict]
/// ```
#[pymodule]
fn atomic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()?;
    m.add_class::<PyRdd>()?;
    m.add_class::<PySqlContext>()?;
    m.add_class::<PyDataFrame>()?;
    Ok(())
}
