use std::collections::HashMap;
use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_graph::{
    algo::{
        connected_component, label_propagation, page_rank, shortest_path,
        strongly_connected_component, triangle_count,
    },
    graph::Graph,
    topology::Edge,
};
use pyo3::prelude::*;

type VId = i64;

/// A directed graph exposed to Python, backed by Atomic RDDs.
///
/// Vertices carry a `float` weight; edges carry a `float` weight used as the
/// distance in shortest-path computations. Graph algorithms run on the Atomic
/// compute engine.
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
    ctx: Arc<Context>,
    inner: Graph<f64, f64>,
}

impl PyGraph {
    fn rebuild(ctx: Arc<Context>, inner: Graph<f64, f64>) -> Self {
        PyGraph { ctx, inner }
    }
}

#[pymethods]
impl PyGraph {
    /// Build a graph from explicit vertex and edge lists.
    ///
    /// * `vertices` — list of `(vertex_id: int, weight: float)` tuples.
    /// * `edges`    — list of `(src_id: int, dst_id: int, weight: float)` tuples.
    #[new]
    #[pyo3(signature = (vertices, edges))]
    pub fn new(vertices: Vec<(VId, f64)>, edges: Vec<(VId, VId, f64)>) -> PyResult<Self> {
        let ctx = Context::local()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let edge_list: Vec<Edge<f64>> = edges
            .into_iter()
            .map(|(s, d, w)| Edge {
                src: s,
                dst: d,
                attr: w,
            })
            .collect();
        let inner = Graph::from_vertices_edges(ctx.clone(), vertices, edge_list);
        Ok(PyGraph { ctx, inner })
    }

    /// Number of vertices in the graph.
    pub fn num_vertices(&self) -> usize {
        self.inner.num_vertices() as usize
    }

    /// Number of edges in the graph.
    pub fn num_edges(&self) -> usize {
        self.inner.num_edges() as usize
    }

    /// Return vertices as a list of `(vertex_id, weight)` tuples.
    pub fn vertices(&self) -> Vec<(VId, f64)> {
        self.inner.collect_vertices()
    }

    /// Return edges as a list of `(src_id, dst_id, weight)` tuples.
    pub fn edges(&self) -> Vec<(VId, VId, f64)> {
        self.inner
            .collect_edges()
            .into_iter()
            .map(|e| (e.src, e.dst, e.attr))
            .collect()
    }

    /// Compute PageRank.
    ///
    /// Returns `dict[int, float]` mapping each vertex ID to its rank.
    ///
    /// * `num_iter`   — number of fixed iterations (default: 20).
    /// * `reset_prob` — teleportation probability (default: `0.15`).
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

    /// Strongly connected components (distributed color propagation).
    ///
    /// Returns `dict[int, int]` mapping each vertex to the representative of its SCC.
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

    /// Shortest paths from the given landmark vertices (edge weights are distances).
    ///
    /// Returns `dict[int, dict[int, float]]` — for each vertex, a map from landmark
    /// ID to the shortest distance. Unreachable landmarks are absent.
    ///
    /// * `landmarks` — list of source vertex IDs to compute distances from.
    pub fn shortest_path(&self, landmarks: Vec<VId>) -> HashMap<VId, HashMap<VId, f64>> {
        // shortest_path::run expects Graph<(), f64>; strip vertex attributes.
        let verts: Vec<(VId, ())> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(id, _)| (id, ()))
            .collect();
        let edges: Vec<Edge<f64>> = self.inner.collect_edges();
        let unit_graph: Graph<(), f64> = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        shortest_path::run(&unit_graph, &landmarks)
    }

    /// Run a custom Pregel vertex-centric computation.
    ///
    /// Vertex data, edge data, and messages are `float` values. Because Python
    /// callbacks cannot be compiled into engine tasks, this runs the message loop
    /// on the driver over the materialized graph.
    ///
    /// Parameters:
    /// * `initial_msg`    — message sent to every vertex before superstep 0.
    /// * `max_iterations` — maximum number of supersteps.
    /// * `vprog`          — `(vertex_id, vertex_data, msg) -> float`.
    /// * `send_msg`       — `(src_id, src_data, dst_id, dst_data, edge_data) -> list[(int, float)]`.
    /// * `merge_msg`      — `(msg_a, msg_b) -> float`.
    ///
    /// Returns a new `Graph` with updated vertex attributes.
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
        let vertices = self.inner.collect_vertices();
        let edges = self.inner.collect_edges();

        // Superstep 0: apply vprog with initial_msg to every vertex.
        let mut vdata: HashMap<VId, f64> = vertices
            .iter()
            .map(|(vid, vd)| -> PyResult<(VId, f64)> {
                let new_vd = vprog
                    .call1(py, (*vid, *vd, initial_msg))?
                    .extract::<f64>(py)?;
                Ok((*vid, new_vd))
            })
            .collect::<PyResult<_>>()?;

        for _ in 0..max_iterations {
            let mut msgs: HashMap<VId, f64> = HashMap::new();
            for edge in &edges {
                let src_vd = vdata.get(&edge.src).copied().unwrap_or(0.0);
                let dst_vd = vdata.get(&edge.dst).copied().unwrap_or(0.0);
                let result = send_msg.call1(py, (edge.src, src_vd, edge.dst, dst_vd, edge.attr))?;
                let pairs: Vec<(i64, f64)> = result
                    .bind(py)
                    .extract::<Vec<(i64, f64)>>()
                    .unwrap_or_default();
                for (target, m) in pairs {
                    if let Some(existing) = msgs.get(&target).copied() {
                        let merged = merge_msg.call1(py, (existing, m))?.extract::<f64>(py)?;
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
                    let new_vd = vprog.call1(py, (*vid, old_vd, *msg))?.extract::<f64>(py)?;
                    vdata.insert(*vid, new_vd);
                }
            }
        }

        let new_vertices: Vec<(VId, f64)> = vertices
            .iter()
            .map(|(vid, old)| (*vid, vdata.get(vid).copied().unwrap_or(*old)))
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), new_vertices, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Graph(vertices={}, edges={})",
            self.inner.num_vertices(),
            self.inner.num_edges()
        )
    }
}
