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

    /// Return a randomly chosen vertex id, or `None` if the graph is empty.
    pub fn pick_random_vertex(&self) -> Option<VId> {
        self.inner.pick_random_vertex()
    }

    /// Return the edge attribute for `(src, dst)` if the edge exists.
    pub fn find(&self, src: VId, dst: VId) -> Option<f64> {
        self.inner.find(src, dst)
    }

    /// Replace every vertex attribute via `f(vertex_id, current_attr) -> new_attr`.
    pub fn map_vertices(&self, py: Python<'_>, f: Py<PyAny>) -> PyResult<PyGraph> {
        let verts: Vec<(VId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, vd)| {
                let new_vd: f64 = f.call1(py, (vid, vd))?.extract(py)?;
                Ok((vid, new_vd))
            })
            .collect::<PyResult<_>>()?;
        let edges = self.inner.collect_edges();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Replace every edge attribute via `f(src_id, dst_id, current_attr) -> new_attr`.
    pub fn map_edges(&self, py: Python<'_>, f: Py<PyAny>) -> PyResult<PyGraph> {
        let verts = self.inner.collect_vertices();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| {
                let new_attr: f64 = f.call1(py, (e.src, e.dst, e.attr))?.extract(py)?;
                Ok(Edge {
                    src: e.src,
                    dst: e.dst,
                    attr: new_attr,
                })
            })
            .collect::<PyResult<_>>()?;
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Return the subgraph of vertices where `vpred(vid, weight)` is truthy and edges where
    /// `epred(src, dst, attr)` is truthy; edges with a dropped endpoint are also removed.
    pub fn subgraph(
        &self,
        py: Python<'_>,
        vpred: Py<PyAny>,
        epred: Py<PyAny>,
    ) -> PyResult<PyGraph> {
        let mut kept_verts: Vec<(VId, f64)> = Vec::new();
        let mut live: std::collections::HashSet<VId> = std::collections::HashSet::new();
        for (vid, vd) in self.inner.collect_vertices() {
            if vpred.call1(py, (vid, vd))?.is_truthy(py)? {
                kept_verts.push((vid, vd));
                live.insert(vid);
            }
        }
        let mut kept_edges: Vec<Edge<f64>> = Vec::new();
        for e in self.inner.collect_edges() {
            if live.contains(&e.src)
                && live.contains(&e.dst)
                && epred.call1(py, (e.src, e.dst, e.attr))?.is_truthy(py)?
            {
                kept_edges.push(e);
            }
        }
        let inner = Graph::from_vertices_edges(self.ctx.clone(), kept_verts, kept_edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Restrict this graph to the vertices and edges present in `other` (attributes kept from
    /// self). A vertex survives if its id is in `other`; an edge survives if its `(src, dst)` is.
    pub fn mask(&self, other: &PyGraph) -> PyGraph {
        let other_v: std::collections::HashSet<VId> = other
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(v, _)| v)
            .collect();
        let other_e: std::collections::HashSet<(VId, VId)> = other
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| (e.src, e.dst))
            .collect();
        let verts: Vec<(VId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .filter(|(v, _)| other_v.contains(v))
            .collect();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .filter(|e| other_e.contains(&(e.src, e.dst)))
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        PyGraph::rebuild(self.ctx.clone(), inner)
    }

    /// Map each edge's attribute using the full triplet: `f(src, src_weight, dst, dst_weight,
    /// attr) -> new_attr`. Mirrors `mapTriplets`.
    pub fn map_triplets(&self, py: Python<'_>, f: Py<PyAny>) -> PyResult<PyGraph> {
        let vattr: std::collections::HashMap<VId, f64> =
            self.inner.collect_vertices().into_iter().collect();
        let verts = self.inner.collect_vertices();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| {
                let sv = vattr.get(&e.src).copied().unwrap_or(0.0);
                let dv = vattr.get(&e.dst).copied().unwrap_or(0.0);
                let new_attr: f64 = f.call1(py, (e.src, sv, e.dst, dv, e.attr))?.extract(py)?;
                Ok(Edge {
                    src: e.src,
                    dst: e.dst,
                    attr: new_attr,
                })
            })
            .collect::<PyResult<_>>()?;
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Merge parallel edges (same `(src, dst)`) with `f(attr1, attr2) -> attr`. Mirrors
    /// `groupEdges`.
    pub fn group_edges(&self, py: Python<'_>, f: Py<PyAny>) -> PyResult<PyGraph> {
        let verts = self.inner.collect_vertices();
        let mut merged: std::collections::HashMap<(VId, VId), f64> =
            std::collections::HashMap::new();
        let mut order: Vec<(VId, VId)> = Vec::new();
        for e in self.inner.collect_edges() {
            match merged.get(&(e.src, e.dst)) {
                Some(acc) => {
                    let m: f64 = f.call1(py, (*acc, e.attr))?.extract(py)?;
                    merged.insert((e.src, e.dst), m);
                }
                None => {
                    order.push((e.src, e.dst));
                    merged.insert((e.src, e.dst), e.attr);
                }
            }
        }
        let edges: Vec<Edge<f64>> = order
            .into_iter()
            .map(|(src, dst)| Edge {
                src,
                dst,
                attr: merged[&(src, dst)],
            })
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Left-outer-join vertices with `table` (a list of `(vid, value)`), deriving new weights via
    /// `f(vid, weight, value_or_None) -> new_weight`. Edges are unchanged. Mirrors
    /// `outerJoinVertices`.
    pub fn outer_join_vertices(
        &self,
        py: Python<'_>,
        table: Vec<(VId, Py<PyAny>)>,
        f: Py<PyAny>,
    ) -> PyResult<PyGraph> {
        let map: std::collections::HashMap<VId, Py<PyAny>> = table
            .into_iter()
            .map(|(vid, v)| (vid, v.clone_ref(py)))
            .collect();
        let verts: Vec<(VId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, vd)| {
                let value = match map.get(&vid) {
                    Some(v) => v.clone_ref(py),
                    None => py.None(),
                };
                let new_vd: f64 = f.call1(py, (vid, vd, value))?.extract(py)?;
                Ok((vid, new_vd))
            })
            .collect::<PyResult<_>>()?;
        let edges = self.inner.collect_edges();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Inner-join vertices with `table`: only vertices present in `table` are updated via
    /// `f(vid, weight, value) -> new_weight`; others keep their weight. Mirrors `joinVertices`.
    pub fn join_vertices(
        &self,
        py: Python<'_>,
        table: Vec<(VId, Py<PyAny>)>,
        f: Py<PyAny>,
    ) -> PyResult<PyGraph> {
        let map: std::collections::HashMap<VId, Py<PyAny>> = table
            .into_iter()
            .map(|(vid, v)| (vid, v.clone_ref(py)))
            .collect();
        let verts: Vec<(VId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, vd)| match map.get(&vid) {
                Some(v) => {
                    let new_vd: f64 = f.call1(py, (vid, vd, v.clone_ref(py)))?.extract(py)?;
                    Ok((vid, new_vd))
                }
                None => Ok((vid, vd)),
            })
            .collect::<PyResult<_>>()?;
        let edges = self.inner.collect_edges();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(PyGraph::rebuild(self.ctx.clone(), inner))
    }

    /// Return a new graph with all edges reversed.
    pub fn reverse(&self) -> PyGraph {
        let verts = self.inner.collect_vertices();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| Edge {
                src: e.dst,
                dst: e.src,
                attr: e.attr,
            })
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        PyGraph::rebuild(self.ctx.clone(), inner)
    }

    /// Return `(in_degree, out_degree)` for each vertex as a dict `{vid: (in, out)}`.
    pub fn degrees(&self) -> std::collections::HashMap<VId, (usize, usize)> {
        let mut deg: std::collections::HashMap<VId, (usize, usize)> =
            std::collections::HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            deg.entry(vid).or_insert((0, 0));
        }
        for e in self.inner.collect_edges() {
            deg.entry(e.src).or_insert((0, 0)).1 += 1;
            deg.entry(e.dst).or_insert((0, 0)).0 += 1;
        }
        deg
    }

    /// In-degree for each vertex.
    pub fn in_degrees(&self) -> std::collections::HashMap<VId, usize> {
        self.degrees()
            .into_iter()
            .map(|(vid, (ir, _))| (vid, ir))
            .collect()
    }

    /// Out-degree for each vertex.
    pub fn out_degrees(&self) -> std::collections::HashMap<VId, usize> {
        self.degrees()
            .into_iter()
            .map(|(vid, (_, out))| (vid, out))
            .collect()
    }

    /// Collect neighbor IDs for each vertex. `direction` = "in", "out", or "either".
    #[pyo3(signature = (direction="either"))]
    pub fn collect_neighbor_ids(
        &self,
        direction: &str,
    ) -> std::collections::HashMap<VId, Vec<VId>> {
        let mut neigh: std::collections::HashMap<VId, Vec<VId>> = std::collections::HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            neigh.entry(vid).or_default();
        }
        for e in self.inner.collect_edges() {
            match direction {
                "in" => neigh.entry(e.dst).or_default().push(e.src),
                "out" => neigh.entry(e.src).or_default().push(e.dst),
                _ => {
                    neigh.entry(e.src).or_default().push(e.dst);
                    neigh.entry(e.dst).or_default().push(e.src);
                }
            }
        }
        neigh
    }

    /// Collect neighbor `(neighbor_id, edge_attr)` pairs for each vertex.
    #[pyo3(signature = (direction="either"))]
    pub fn collect_neighbors(
        &self,
        direction: &str,
    ) -> std::collections::HashMap<VId, Vec<(VId, f64)>> {
        let mut neigh: std::collections::HashMap<VId, Vec<(VId, f64)>> =
            std::collections::HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            neigh.entry(vid).or_default();
        }
        for e in self.inner.collect_edges() {
            match direction {
                "in" => neigh.entry(e.dst).or_default().push((e.src, e.attr)),
                "out" => neigh.entry(e.src).or_default().push((e.dst, e.attr)),
                _ => {
                    neigh.entry(e.src).or_default().push((e.dst, e.attr));
                    neigh.entry(e.dst).or_default().push((e.src, e.attr));
                }
            }
        }
        neigh
    }

    /// PageRank run until convergence (max mean-diff < `tol`) or `max_iter`.
    #[pyo3(signature = (tol=0.001, reset_prob=0.15, max_iter=100))]
    pub fn page_rank_until_convergence(
        &self,
        tol: f64,
        reset_prob: f64,
        max_iter: usize,
    ) -> std::collections::HashMap<VId, f64> {
        // Initialise all vertices to 1.0 / N
        let n = self.inner.num_vertices() as f64;
        let mut ranks: std::collections::HashMap<VId, f64> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, _)| (vid, 1.0 / n))
            .collect();
        let edges = self.inner.collect_edges();
        // Build reverse adjacency: in-neighbor → (src, edge_weight)
        let mut in_edges: std::collections::HashMap<VId, Vec<(VId, f64)>> =
            std::collections::HashMap::new();
        let mut out_deg: std::collections::HashMap<VId, usize> = std::collections::HashMap::new();
        for e in &edges {
            in_edges.entry(e.dst).or_default().push((e.src, e.attr));
            *out_deg.entry(e.src).or_insert(0) += 1;
        }
        for _iter in 0..max_iter {
            let mut new_ranks: std::collections::HashMap<VId, f64> =
                std::collections::HashMap::new();
            let mut max_diff = 0.0f64;
            for &vid in ranks.keys() {
                let sum: f64 = in_edges
                    .get(&vid)
                    .map(|ins| {
                        ins.iter()
                            .map(|(src, _w)| {
                                let d = *out_deg.get(src).unwrap_or(&1) as f64;
                                ranks.get(src).copied().unwrap_or(0.0) / d
                            })
                            .sum()
                    })
                    .unwrap_or(0.0);
                let new_rank = reset_prob / n + (1.0 - reset_prob) * sum;
                let diff = (new_rank - ranks[&vid]).abs();
                if diff > max_diff {
                    max_diff = diff;
                }
                new_ranks.insert(vid, new_rank);
            }
            ranks = new_ranks;
            if max_diff < tol {
                break;
            }
        }
        ranks
    }

    /// Personalized PageRank from the given source vertices.
    #[pyo3(signature = (sources, num_iter=20, reset_prob=0.15))]
    pub fn personalized_page_rank(
        &self,
        sources: Vec<VId>,
        num_iter: usize,
        reset_prob: f64,
    ) -> std::collections::HashMap<VId, f64> {
        let _n = self.inner.num_vertices() as f64;
        let source_set: std::collections::HashSet<VId> = sources.into_iter().collect();
        let mut ranks: std::collections::HashMap<VId, f64> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, _)| {
                if source_set.contains(&vid) {
                    (vid, 1.0 / source_set.len() as f64)
                } else {
                    (vid, 0.0)
                }
            })
            .collect();
        let edges = self.inner.collect_edges();
        let mut in_edges: std::collections::HashMap<VId, Vec<(VId, f64)>> =
            std::collections::HashMap::new();
        let mut out_deg: std::collections::HashMap<VId, usize> = std::collections::HashMap::new();
        for e in &edges {
            in_edges.entry(e.dst).or_default().push((e.src, e.attr));
            *out_deg.entry(e.src).or_insert(0) += 1;
        }
        for _ in 0..num_iter {
            let mut new_ranks: std::collections::HashMap<VId, f64> =
                std::collections::HashMap::new();
            for &vid in ranks.keys() {
                let sum: f64 = in_edges
                    .get(&vid)
                    .map(|ins| {
                        ins.iter()
                            .map(|(src, _w)| {
                                let d = *out_deg.get(src).unwrap_or(&1) as f64;
                                ranks.get(src).copied().unwrap_or(0.0) / d
                            })
                            .sum()
                    })
                    .unwrap_or(0.0);
                let teleport: f64 = if source_set.contains(&vid) {
                    reset_prob / source_set.len() as f64
                } else {
                    0.0
                };
                new_ranks.insert(vid, teleport + (1.0 - reset_prob) * sum);
            }
            ranks = new_ranks;
        }
        ranks
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Graph(vertices={}, edges={})",
            self.inner.num_vertices(),
            self.inner.num_edges()
        )
    }
}
