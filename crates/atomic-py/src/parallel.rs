use std::sync::OnceLock;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

/// Process-level pool singleton. Stored as `Py<PyAny>` (a `ProcessPoolExecutor`).
/// Guarded by a `Mutex` so Context can call `shutdown_pool` safely from any thread.
static POOL: OnceLock<Mutex<Option<Py<PyAny>>>> = OnceLock::new();

fn pool_cell() -> &'static Mutex<Option<Py<PyAny>>> {
    POOL.get_or_init(|| Mutex::new(None))
}

fn get_or_create_pool(py: Python<'_>, n: usize) -> PyResult<Bound<'_, PyAny>> {
    let mut guard = pool_cell().lock();
    if guard.is_none() {
        let cf = py.import("concurrent.futures")?;
        let pool = cf.getattr("ProcessPoolExecutor")?.call1((n,))?.unbind();
        *guard = Some(pool);
    }
    Ok(guard.as_ref().unwrap().bind(py).clone())
}

/// Shut down the pool. Called by `PyContext::stop()`.
pub fn shutdown_pool(py: Python<'_>) {
    let mut guard = pool_cell().lock();
    if let Some(pool) = guard.take() {
        let _ = pool.bind(py).call_method1("shutdown", (false,));
    }
}

/// Worker entry-point executed inside each subprocess.
///
/// Registered in the `atomic_compute` module so it is importable by name —
/// a requirement for `ProcessPoolExecutor` to pickle it across process boundaries.
#[pyfunction(name = "_run_partition")]
pub fn run_partition(
    py: Python<'_>,
    fn_bytes: &[u8],
    items: Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let cp = py.import("cloudpickle")?;
    let fn_obj = cp.call_method1("loads", (PyBytes::new(py, fn_bytes),))?;
    Ok(fn_obj.call1((items,))?.unbind())
}

/// Dispatch a cloudpickled partition lambda across all logical partitions of
/// `elements` using `concurrent.futures.ProcessPoolExecutor`, returning a flat
/// result `Vec`.
///
/// Models PySpark's `local[N]` execution: N separate OS processes, genuine
/// multi-core parallelism, GIL bypassed entirely.
pub fn exec_parallel(
    py: Python<'_>,
    fn_bytes: Vec<u8>,
    elements: &[Py<PyAny>],
    num_partitions: usize,
) -> PyResult<Vec<Py<PyAny>>> {
    if elements.is_empty() {
        return Ok(Vec::new());
    }

    let np = num_partitions.max(1);
    let chunk_size = (elements.len() + np - 1) / np;
    let fn_bytes_obj = PyBytes::new(py, &fn_bytes);

    // Build per-partition lists and repeat fn_bytes for each.
    // Executor.map(fn, iter1, iter2) calls fn(iter1[i], iter2[i]).
    let mut fn_bytes_per_part: Vec<Bound<'_, PyAny>> = Vec::new();
    let mut partition_lists: Vec<Bound<'_, PyAny>> = Vec::new();
    for chunk in elements.chunks(chunk_size.max(1)) {
        let part = PyList::new(py, chunk.iter().map(|e| e.bind(py).clone()))?;
        fn_bytes_per_part.push(fn_bytes_obj.clone().into_any());
        partition_lists.push(part.into_any());
    }
    let actual_np = fn_bytes_per_part.len();
    let pool = get_or_create_pool(py, actual_np)?;

    // Locate `atomic_compute._run_partition` — importable by worker subprocesses.
    let atomic_mod = py.import("atomic_compute")?;
    let run_fn = atomic_mod.getattr("_run_partition")?;

    let py_fn_bytes_list = PyList::new(py, &fn_bytes_per_part)?;
    let py_partitions = PyList::new(py, &partition_lists)?;

    // Blocking call — releases the GIL while waiting for subprocesses.
    let results_iter = pool.call_method1("map", (&run_fn, &py_fn_bytes_list, &py_partitions))?;

    // Flatten iterator[list[Any]] → Vec<Py<PyAny>>
    let mut flat: Vec<Py<PyAny>> = Vec::with_capacity(elements.len());
    for partition_result in results_iter.try_iter()? {
        for item in partition_result?.try_iter()? {
            flat.push(item?.unbind());
        }
    }
    Ok(flat)
}
