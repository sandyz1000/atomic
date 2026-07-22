//! Micro-batch streaming for the Atomic engine.
//!
//! Produces one RDD per batch interval and drives it through a user-defined
//! processing function. Checkpoints are written atomically to a local directory
//! after each batch.
//!
//! # Core types
//!
//! - [`StreamingContext`](context::StreamingContext) — entry point; configure the batch duration here.
//! - [`DStream`](dstream::DStream) — a lazy sequence of RDDs, one per batch.
//! - [`scheduler::JobScheduler`](scheduler::job::JobScheduler) — the internal batch loop driver.
//!
//! # Input sources
//!
//! - [`dstream::input::QueueInputDStream`] — in-memory queue, ideal for testing.
//! - [`dstream::input::SocketInputDStream`] — reads lines from a TCP socket.
//! - [`dstream::input::FileInputDStream`] — watches a local directory for new files.
//! - [`dstream::kafka_direct`] — direct-pull Kafka source (one task per Kafka partition).
//!
//! # Example
//!
//! ```rust,ignore
//! use std::{collections::VecDeque, sync::{Arc, Mutex}, time::Duration};
//! use atomic_compute::context::Context;
//! use atomic_streaming::context::StreamingContext;
//!
//! let ctx = Context::local().unwrap();
//! let ssc = StreamingContext::new(ctx, Duration::from_secs(1));
//! let queue = Arc::new(Mutex::new(VecDeque::new()));
//! let stream = ssc.queue_stream(queue.clone(), true);
//! ssc.foreach_rdd(stream, |rdd, _t| {
//!     println!("batch count: {}", rdd.count().unwrap_or(0));
//! });
//! ssc.start().unwrap();
//! ssc.await_termination().unwrap();
//! ```

pub mod api;
pub mod checkpoint;
pub mod context;
pub mod dstream;
pub mod errors;
pub mod rdd;
pub mod receiver;
pub mod scheduler;
pub mod streaming_support;
pub mod wal;

// cfg_kafka! is defined once in `atomic_data` and shared workspace-wide.
pub use atomic_data::cfg_kafka;
