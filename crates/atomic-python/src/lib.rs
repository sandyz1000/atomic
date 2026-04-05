mod context;
mod rdd;

use pyo3::prelude::*;

use context::PyContext;
use rdd::PyRdd;

/// Atomic Python client — Spark-like distributed computing for Python.
///
/// # Quick start
/// ```python
/// import atomic
///
/// ctx = atomic.Context()
///
/// result = ctx.parallelize([1, 2, 3, 4]) \
///             .map(lambda x: x + 1) \
///             .collect()
/// # [2, 3, 4, 5]
/// ```
#[pymodule]
fn atomic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()?;
    m.add_class::<PyRdd>()?;
    Ok(())
}
