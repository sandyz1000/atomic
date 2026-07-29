//! Streaming bindings for Python — Phase 4.4
//!
//! All DStream element types are `Py<PyAny>` (pyo3 0.28 API).
//! The batch loop runs either synchronously (for tests, via `run_one_batch()`)
//! or in a background thread (via `start()`).
//!
//! pyo3 0.28 API notes:
//! - `Py<T>::call1(py, args)` returns `PyResult<Py<PyAny>>` (not `Bound`).
//! - `Bound<T>::unbind()` converts `Bound` → `Py<T>`.
//! - `Py<T>::bind(py)` returns `&Bound<'_, T>`.
//! - `Python::attach(f)` acquires the GIL (replaces `Python::with_gil`).
//! - Do NOT call `.bind(py)` on an already-`Bound` value.
//!
//! Split by responsibility: [`dstream`] is the `DStream` builder API and its internal
//! transform-tree representation; [`engine`] interprets that tree to materialize one
//! batch; [`batch_queue`] is the trivial test-source queue; [`context`] is the
//! top-level orchestrator (`StreamingContext`) that owns output ops and the batch loop.

mod batch_queue;
mod context;
mod dstream;
mod engine;

pub use batch_queue::PyBatchQueue;
pub use context::PyStreamingContext;
pub use dstream::PyDStream;
