//! `atomic-structured` — micro-batch structured streaming for the Atomic engine.
//!
//! A continuous query is a normal SQL/DataFrame query declared once and executed
//! incrementally, once per micro-batch, on top of [`atomic_streaming`]'s batch
//! loop and [`atomic_sql`] (DataFusion). See `notes/structured-streaming-design.md`.
//!
//! ```ignore
//! use atomic_structured::{StreamingDataFrame, OutputMode, Trigger, source::QueueSource, sink::MemorySink};
//!
//! let sink = MemorySink::new();
//! let query = StreamingDataFrame::read_stream(source)
//!     .sql("SELECT user, amount FROM input WHERE amount > 100")
//!     .write_stream()
//!     .output_mode(OutputMode::Append)
//!     .trigger(Trigger::ProcessingTime(Duration::from_secs(1)))
//!     .format(sink.clone())
//!     .start(&ssc)?;
//! query.await_termination()?;
//! ```

pub mod distributed_state;
pub mod errors;
pub mod frame;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod query;
pub mod session_window;
pub mod sink;
pub mod source;
pub mod state;
pub mod stream_join;
pub mod watermark;
pub mod windowed;

pub use errors::{StructuredError, StructuredResult};
pub use frame::{SessionBuilder, StreamWriter, StreamingDataFrame, WindowedBuilder};
pub use query::StreamingQuery;
pub use sink::Sink;
pub use source::StreamSource;
pub use state::{Agg, AggKind};
pub use stream_join::JoinType;

use std::time::Duration;

/// How results are emitted to the sink each batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    /// Emit only new rows (stateless) or finalized windows (after watermark). Default.
    #[default]
    Append,
    /// Emit every key whose value changed this batch. (Stateful — 4b.)
    Update,
    /// Emit the entire result table every batch. (Stateful — 4c.)
    Complete,
}

/// When the engine fires a micro-batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// Fire every `Duration` — reuses the streaming context's batch loop.
    ProcessingTime(Duration),
    /// Process exactly one batch, then terminate.
    Once,
}

impl Default for Trigger {
    fn default() -> Self {
        Trigger::ProcessingTime(Duration::from_secs(1))
    }
}
