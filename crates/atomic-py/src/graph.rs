use std::collections::HashMap;

use atomic_graph::{
    algo::{
        connected_component, label_propagation, page_rank, shortest_path,
        strongly_connected_component, triangle_count,
    },
    graph::Graph,
    types::Edge,
};
use pyo3::prelude::*;

// ── VertexId alias (atomic_graph uses i64) ────────────────────────────────────
type VId = i64;

/// An in-process directed graph exposed to Python.
///
/// Vertices carry a `float` weight; edges carry a `float` weight used as
/// distance in shortest-path computations.
///
/// # Construction
/// ```python
/// g = atomic_compute.Graph(
///     vertices=[(1, 1.0), (2, 1.0), (3, 1.0)],
///     edges=[(1, 2, 1.0), (2, 3, 2.0)],
/// )
/// print(g.num_vertices(), g.num_edges())
/// ```
#[pyclass(name = "Graph")]
pub struct PyGraph {
    inner: Graph<f64, f64>,
}

#[pymethods]
impl PyGraph {
    /// Build a graph from explicit vertex and edge lists.
    ///
    /// * `vertices` — list of `(vertex_id: int, weight: float)` tuples.
    /// * `edges`    — list of `(src_id: int, dst_id: int, weight: float)` tuples.
    #[new]
    #[pyo3(signature = (vertices, edges))]
    pub fn new(vertices: Vec<(VId, f64)>, edges: Vec<(VId, VId, f64)>) -> Self {
        let edge_list: Vec<Edge<f64>> = edges
            .into_iter()
            .map(|(s, d, w)| Edge {
                src: s,
                dst: d,
                attr: w,
            })
            .collect();
        Self {
            inner: Graph::from_vertices_edges(vertices, edge_list),
        }
    }

    /// Number of vertices in the graph.
    pub fn num_vertices(&self) -> usize {
        self.inner.num_vertices()
    }

    /// Number of edges in the graph.
    pub fn num_edges(&self) -> usize {
        self.inner.num_edges()
    }

    /// Return vertices as a list of `(vertex_id, weight)` tuples.
    pub fn vertices(&self) -> Vec<(VId, f64)> {
        self.inner.vertices().map(|(id, w)| (id, *w)).collect()
    }

    /// Return edges as a list of `(src_id, dst_id, weight)` tuples.
    pub fn edges(&self) -> Vec<(VId, VId, f64)> {
        self.inner
            .edges()
            .map(|e| (e.src, e.dst, e.attr))
            .collect()
    }

    /// Compute PageRank.
    ///
    /// Returns `dict[int, float]` mapping each vertex ID to its rank.
    ///
    /// * `num_iter`   — number of fixed iterations (default: 20).
    /// * `reset_prob` — teleportation probability (default: 0.15, matching GraphX).
    #[pyo3(signature = (num_iter=20, reset_prob=0.15))]
    pub fn page_rank(&self, num_iter: usize, reset_prob: f64) -> HashMap<VId, f64> {
        page_rank::run(&self.inner, num_iter, reset_prob)
    }

    /// Weakly connected components.
    ///
    /// Returns `dict[int, int]` mapping each vertex to the minimum vertex ID
    /// in its component (the component representative).
    pub fn connected_components(&self) -> HashMap<VId, VId> {
        connected_component::run(&self.inner, usize::MAX)
    }

    /// Strongly connected components (Tarjan's algorithm).
    ///
    /// Returns `dict[int, int]` mapping each vertex to the minimum vertex ID
    /// in its SCC.
    pub fn strongly_connected_components(&self) -> HashMap<VId, VId> {
        strongly_connected_component::run(&self.inner, 0)
    }

    /// Label propagation community detection.
    ///
    /// Returns `dict[int, int]` mapping each vertex to its community label
    /// (one of the original vertex IDs).
    ///
    /// * `max_iter` — number of supersteps (default: 10).
    #[pyo3(signature = (max_iter=10))]
    pub fn label_propagation(&self, max_iter: usize) -> HashMap<VId, VId> {
        label_propagation::run(&self.inner, max_iter)
    }

    /// Triangle count per vertex.
    ///
    /// Returns `dict[int, int]` mapping each vertex to the number of triangles
    /// it participates in.
    pub fn triangle_count(&self) -> HashMap<VId, usize> {
        triangle_count::run(&self.inner)
    }

    /// Shortest paths from the given landmark vertices (Dijkstra, edge weights
    /// used as distances).
    ///
    /// Returns `dict[int, dict[int, float]]` — for each vertex, a map from
    /// landmark ID to the shortest distance.  Unreachable landmarks are absent
    /// from the inner dict.
    ///
    /// * `landmarks` — list of destination vertex IDs to compute distances to.
    pub fn shortest_path(&self, landmarks: Vec<VId>) -> HashMap<VId, HashMap<VId, f64>> {
        // shortest_path::run expects Graph<(), f64>; strip vertex attrs.
        let verts: Vec<(VId, ())> = self.inner.vertices().map(|(id, _)| (id, ())).collect();
        let edges: Vec<Edge<f64>> = self
            .inner
            .edges()
            .map(|e| Edge {
                src: e.src,
                dst: e.dst,
                attr: e.attr,
            })
            .collect();
        let unit_graph: Graph<(), f64> = Graph::from_vertices_edges(verts, edges);
        shortest_path::run(&unit_graph, &landmarks)
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Graph(vertices={}, edges={})",
            self.inner.num_vertices(),
            self.inner.num_edges()
        )
    }
}
