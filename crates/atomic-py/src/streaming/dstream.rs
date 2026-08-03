use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use pyo3::prelude::*;

// Output/transform functions are stored as live `Py<PyAny>` references rather than
// pickled bytes: every DStream op here runs in-process (synchronously in
// `run_one_batch`, or via `Python::attach` in the background thread spawned by
// `start()`) — nothing crosses a process boundary. Pickling would round-trip the
// closure *by value* (cloudpickle captures free variables at dump time), silently
// detaching callbacks like `lambda b: results.extend(b)` from the caller's live
// `results` list. Holding the `Py<PyAny>` directly keeps the original object alive.
pub(crate) enum PyStreamTransform {
    Map(Py<PyAny>),
    Filter(Py<PyAny>),
    FlatMap(Py<PyAny>),
    ReduceByKey(Py<PyAny>),
    GroupByKey,
    Join(Arc<PyDStreamInner>),
    LeftOuterJoin(Arc<PyDStreamInner>),
    UpdateStateByKey(Py<PyAny>),
    MapValues(Py<PyAny>),
}

pub(crate) enum PyDStreamInner {
    Queue {
        queue: Arc<Mutex<VecDeque<Vec<Py<PyAny>>>>>,
    },
    Socket,
    File {
        directory: String,
    },
    Transform {
        parent: Arc<PyDStreamInner>,
        op: PyStreamTransform,
    },
    Windowed {
        parent: Arc<PyDStreamInner>,
        window_ms: u64,
        // (arrival_time, batch_elements) — pruned on each compute
        buffer: Mutex<VecDeque<(Instant, Vec<Py<PyAny>>)>>,
    },
}

#[pyclass(name = "DStream")]
pub struct PyDStream {
    pub(crate) inner: Arc<PyDStreamInner>,
    pub is_pair: bool,
}

#[pymethods]
impl PyDStream {
    pub fn map(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Map(func),
            }),
            is_pair: false,
        })
    }

    pub fn filter(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Filter(func),
            }),
            is_pair: self.is_pair,
        })
    }

    pub fn flat_map(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::FlatMap(func),
            }),
            is_pair: false,
        })
    }

    pub fn reduce_by_key(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "reduce_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::ReduceByKey(func),
            }),
            is_pair: true,
        })
    }

    pub fn group_by_key(&self) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "group_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::GroupByKey,
            }),
            is_pair: true,
        })
    }

    pub fn join(&self, other: &PyDStream) -> PyResult<PyDStream> {
        if !self.is_pair || !other.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "join requires pair DStreams",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::Join(Arc::clone(&other.inner)),
            }),
            is_pair: true,
        })
    }

    pub fn left_outer_join(&self, other: &PyDStream) -> PyResult<PyDStream> {
        if !self.is_pair || !other.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "left_outer_join requires pair DStreams",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::LeftOuterJoin(Arc::clone(&other.inner)),
            }),
            is_pair: true,
        })
    }

    pub fn update_state_by_key(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "update_state_by_key requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::UpdateStateByKey(func),
            }),
            is_pair: true,
        })
    }

    pub fn map_values(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        if !self.is_pair {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "map_values requires a pair DStream",
            ));
        }
        Ok(PyDStream {
            inner: Arc::new(PyDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: PyStreamTransform::MapValues(func),
            }),
            is_pair: true,
        })
    }

    /// Return a new DStream that unions the parent's batches over a sliding window.
    ///
    /// `window_ms` — how far back (in milliseconds) to include batches.
    /// `slide_ms`  — accepted for API compatibility; in this in-process model every
    ///               `run_one_batch()` call advances the window by one tick.
    pub fn window(&self, window_ms: u64, _slide_ms: u64) -> PyDStream {
        PyDStream {
            inner: Arc::new(PyDStreamInner::Windowed {
                parent: Arc::clone(&self.inner),
                window_ms,
                buffer: Mutex::new(VecDeque::new()),
            }),
            is_pair: self.is_pair,
        }
    }

    /// Reduce elements per window with a user function.
    pub fn reduce_by_window(
        &self,
        func: Py<PyAny>,
        window_ms: u64,
        slide_ms: u64,
    ) -> PyResult<PyDStream> {
        self.window(window_ms, slide_ms).map(func)
    }

    /// Reduce by key per window.
    pub fn reduce_by_key_and_window(
        &self,
        func: Py<PyAny>,
        window_ms: u64,
        slide_ms: u64,
    ) -> PyResult<PyDStream> {
        let w = self.window(window_ms, slide_ms);
        w.reduce_by_key(func)
    }

    /// Transform each batch through `func(batch) -> new_batch`.
    pub fn transform(&self, func: Py<PyAny>) -> PyResult<PyDStream> {
        self.map(func)
    }

    /// Transform with another DStream: `func(self_batch, other_batch) -> new_batch`.
    /// Note: `func` must be a single-argument callable that receives a
    /// `(self_batch, other_batch)` tuple.
    pub fn transform_with(&self, other: &PyDStream, func: Py<PyAny>) -> PyResult<PyDStream> {
        self.join(other)?.map(func)
    }
}
