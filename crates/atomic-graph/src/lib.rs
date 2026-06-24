//! Graph processing on top of the Atomic distributed compute engine.
//!
//! # Core types
//!
//! - [`Graph`] — an in-process directed graph with typed vertex and edge data.
//! - [`pregel::run`] — bulk-synchronous vertex-centric computation.
//!
//! # Built-in algorithms
//!
//! | Module | Algorithm |
//! |---|---|
//! | [`algo::page_rank`] | Fixed-iteration and convergence-based PageRank |
//! | [`algo::connected_component`] | Weakly connected components (Pregel, min-label) |
//! | [`algo::strongly_connected_component`] | SCCs via Tarjan's algorithm |
//! | [`algo::label_propagation`] | Community detection |
//! | [`algo::triangle_count`] | Per-vertex and total triangle count |
//!
//! # Example
//!
//! ```rust,ignore
//! use atomic_graph::{Graph, algo::page_rank};
//! use atomic_graph::topology::Edge;
//!
//! let g = Graph::from_edges(vec![
//!     Edge { src: 0, dst: 1, attr: () },
//!     Edge { src: 1, dst: 2, attr: () },
//! ], ());
//! let ranks = page_rank::run(&g, 20, 0.15);
//! ```

pub mod algo;
pub mod graph;
pub mod pregel;
pub mod topology;
