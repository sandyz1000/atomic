mod context;
mod docker_stub;
mod rdd;

use pyo3::prelude::*;

use context::PyContext;
use docker_stub::PyDockerStub;
use rdd::PyRdd;

/// Atomic Python client — Spark-like distributed computing for Python.
///
/// # Quick start
/// ```python
/// import atomic
///
/// ctx = atomic.Context()
///
/// # Local map/reduce
/// result = ctx.parallelize([1, 2, 3, 4]) \
///             .map(lambda x: x + 1) \
///             .collect()
/// # [2, 3, 4, 5]
///
/// # Word count
/// counts = ctx.parallelize(["hello world", "hello atomic"]) \
///             .flat_map(lambda line: line.split()) \
///             .map(lambda w: (w, 1)) \
///             .reduce_by_key(lambda a, b: a + b) \
///             .collect()
///
/// # Docker task (same pre-built image as Rust driver)
/// stub = atomic.DockerStub.from_manifest("build/manifest.toml", "demo.map.v1")
/// result = ctx.parallelize([1, 2, 3]).map_via(stub)
/// ```
#[pymodule]
fn atomic(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()?;
    m.add_class::<PyRdd>()?;
    m.add_class::<PyDockerStub>()?;
    Ok(())
}
