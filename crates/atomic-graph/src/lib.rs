//! Graph processing on top of the Atomic distributed compute engine.

pub mod algo;
pub mod graph;
pub mod pregel;
pub mod topology;

/// Shuffle-map handlers for the concrete `(K, V)` pair types the built-in algorithms
/// shuffle through `join` / `reduce_by_key`. The engine requires one registration per
/// pair (both in local and distributed mode); they are linked into the binary via
/// `inventory`. The composite tuple shapes come from the triplet joins in
/// [`graph::Graph::triplets`].
mod shuffle_registry {
    // Vertex/message pairs (labels, distances, ranks, counts).
    atomic_compute::register_shuffle_map!(i64, i64);
    atomic_compute::register_shuffle_map!(i64, f64);
    // Triplet-join intermediates with unit edge attribute (CC / LP / SSSP / SCC).
    atomic_compute::register_shuffle_map!(i64, (i64, ()));
    atomic_compute::register_shuffle_map!(i64, (i64, (), i64));
}
