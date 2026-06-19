mod context;
mod distributed_vars;
mod graph;
pub(crate) mod parallel;
mod rdd;
mod sql;
mod streaming;

use pyo3::prelude::*;

use context::PyContext;
use distributed_vars::{PyAccumulator, PyBroadcastVar};
use graph::PyGraph;
use parallel::run_partition;
use rdd::{PyRdd, verify_picklable};
use sql::{PyDataFrame, PySqlContext};
use streaming::{PyBatchQueue, PyDStream, PyStreamingContext};

/// Atomic Python client — RDD-style distributed computing and SQL for Python.
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
///
/// # Broadcast + Accumulator
/// ```python
/// bv = ctx.broadcast({"threshold": 10})
/// acc = ctx.accumulator(0)
/// ctx.parallelize([1, 2, 3]).for_each(lambda x: acc.add(x))
/// print(acc.value())  # 6
/// ```
///
/// # Graph
/// ```python
/// g = atomic_compute.Graph([(1, 1.0), (2, 1.0)], [(1, 2, 1.0)])
/// ranks = g.page_rank()
/// ```
#[pymodule(name = "atomic_compute")]
fn atomic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()?;
    m.add_class::<PyRdd>()?;
    m.add_class::<PySqlContext>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyBroadcastVar>()?;
    m.add_class::<PyAccumulator>()?;
    m.add_class::<PyGraph>()?;
    m.add_class::<PyStreamingContext>()?;
    m.add_class::<PyDStream>()?;
    m.add_class::<PyBatchQueue>()?;
    m.add_function(wrap_pyfunction!(verify_picklable, m)?)?;
    m.add_function(wrap_pyfunction!(run_partition, m)?)?;
    Ok(())
}
