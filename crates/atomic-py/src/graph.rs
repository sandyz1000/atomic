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
use pyo3::types::PyList;

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
        self.inner.edges().map(|e| (e.src, e.dst, e.attr)).collect()
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

    /// Run a custom Pregel vertex-centric computation.
    ///
    /// All vertex data, edge data, and messages are `float` values.
    ///
    /// Parameters:
    /// * `initial_msg`    — message sent to every vertex before superstep 0.
    /// * `max_iterations` — maximum number of supersteps.
    /// * `vprog`          — `(vertex_id: int, vertex_data: float, msg: float) -> float`
    ///                      Updates the vertex attribute given an incoming message.
    /// * `send_msg`       — `(src_id, src_data, dst_id, dst_data, edge_data) -> list[(int, float)]`
    ///                      Returns a list of `(target_vertex_id, message)` pairs to send.
    ///                      Use `src_id` or `dst_id` as the target.
    /// * `merge_msg`      — `(msg_a: float, msg_b: float) -> float`
    ///                      Commutative combiner for messages arriving at the same vertex.
    ///
    /// Returns a new `Graph` with updated vertex attributes.
    ///
    /// # Example
    /// ```python
    /// # Propagate minimum vertex ID to all reachable vertices.
    /// result = g.run_pregel(
    ///     initial_msg=float('inf'),
    ///     max_iterations=10,
    ///     vprog=lambda vid, vdata, msg: min(vdata, msg),
    ///     send_msg=lambda si, sd, di, dd, ed: [(di, sd)] if sd < dd else [],
    ///     merge_msg=lambda a, b: min(a, b),
    /// )
    /// ```
    #[pyo3(signature = (initial_msg, max_iterations, vprog, send_msg, merge_msg))]
    pub fn run_pregel(
        &self,
        py: Python<'_>,
        initial_msg: f64,
        max_iterations: usize,
        vprog: Py<PyAny>,
        send_msg: Py<PyAny>,
        merge_msg: Py<PyAny>,
    ) -> PyResult<PyGraph> {
        // Implement the Pregel loop manually so the GIL token stays in scope
        // throughout — Python::with_gil is not available in pyo3 0.22+.
        let graph = &self.inner;

        // Superstep 0: apply vprog with initial_msg to every vertex.
        let mut vdata: HashMap<VId, f64> = graph
            .vertices()
            .map(|(vid, vd)| -> PyResult<(VId, f64)> {
                let new_vd = vprog
                    .call1(py, (vid, *vd, initial_msg))?
                    .extract::<f64>(py)?;
                Ok((vid, new_vd))
            })
            .collect::<PyResult<_>>()?;

        for _ in 0..max_iterations {
            let mut msgs: HashMap<VId, f64> = HashMap::new();

            for edge in graph.edges() {
                let src_vd = vdata.get(&edge.src).copied().unwrap_or(0.0);
                let dst_vd = vdata.get(&edge.dst).copied().unwrap_or(0.0);
                let result = send_msg.call1(
                    py,
                    (edge.src, src_vd, edge.dst, dst_vd, edge.attr),
                )?;
                let pairs: Vec<(i64, f64)> = result
                    .bind(py)
                    .extract::<Vec<(i64, f64)>>()
                    .unwrap_or_default();
                for (target, m) in pairs {
                    if let Some(existing) = msgs.get(&target).copied() {
                        let merged = merge_msg
                            .call1(py, (existing, m))?
                            .extract::<f64>(py)?;
                        msgs.insert(target, merged);
                    } else {
                        msgs.insert(target, m);
                    }
                }
            }

            if msgs.is_empty() {
                break;
            }

            for (vid, msg) in &msgs {
                if let Some(&old_vd) = vdata.get(vid) {
                    let new_vd = vprog
                        .call1(py, (*vid, old_vd, *msg))?
                        .extract::<f64>(py)?;
                    vdata.insert(*vid, new_vd);
                }
            }
        }

        let result = graph.map_vertices(|vid, old_vd| *vdata.get(&vid).unwrap_or(old_vd));
        Ok(PyGraph { inner: result })
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Graph(vertices={}, edges={})",
            self.inner.num_vertices(),
            self.inner.num_edges()
        )
    }
}
