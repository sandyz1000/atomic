use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader};

use petgraph::Direction;
use petgraph::stable_graph::{NodeIndex, StableGraph};

use crate::topology::{Edge, EdgeContext, EdgeTriplet, VertexId, VertexMap};

/// An in-process directed graph with vertex data `VD` and edge data `ED`.
///
/// Backed by `petgraph::stable_graph::StableGraph` with a companion
/// `HashMap<VertexId, NodeIndex>` for O(1) vertex lookup by ID.
///
/// Matches the GraphX `Graph[VD, ED]` API where possible, adapted for Rust
/// ownership rules (transformation methods return a new `Graph` rather than
/// mutating in place).
pub struct Graph<VD, ED> {
    pub(crate) inner: StableGraph<(VertexId, VD), ED>,
    // Vertex-id → petgraph node index map; backs the `node_index`/`out_neighbors`
    // traversal helpers (not exercised by every built-in algorithm).
    #[allow(dead_code)]
    pub(crate) id_to_node: HashMap<VertexId, NodeIndex>,
}

impl<VD: Clone, ED: Clone> Graph<VD, ED> {
    /// Build a graph from a vertex list and an edge list.
    ///
    /// Vertices not referenced by any edge will still be present in the graph.
    /// Edges referencing a vertex ID not in `vertices` are silently dropped.
    pub fn from_vertices_edges(vertices: Vec<(VertexId, VD)>, edges: Vec<Edge<ED>>) -> Self {
        let mut inner: StableGraph<(VertexId, VD), ED> = StableGraph::new();
        let mut id_to_node: HashMap<VertexId, NodeIndex> = HashMap::new();

        for (vid, vd) in vertices {
            let idx = inner.add_node((vid, vd));
            id_to_node.insert(vid, idx);
        }

        for e in edges {
            if let (Some(&src_idx), Some(&dst_idx)) =
                (id_to_node.get(&e.src), id_to_node.get(&e.dst))
            {
                inner.add_edge(src_idx, dst_idx, e.attr);
            }
        }

        Graph { inner, id_to_node }
    }

    /// Build a graph from edges only; missing vertices are created with `default_vd`.
    pub fn from_edges(edges: Vec<Edge<ED>>, default_vd: VD) -> Self {
        let mut inner: StableGraph<(VertexId, VD), ED> = StableGraph::new();
        let mut id_to_node: HashMap<VertexId, NodeIndex> = HashMap::new();

        // Two-pass: collect edges, ensure all vertices exist, then add edges.
        let edge_ids: Vec<(VertexId, VertexId, ED)> =
            edges.into_iter().map(|e| (e.src, e.dst, e.attr)).collect();

        for &(src, dst, _) in &edge_ids {
            for id in [src, dst] {
                id_to_node
                    .entry(id)
                    .or_insert_with(|| inner.add_node((id, default_vd.clone())));
            }
        }
        for (src, dst, attr) in edge_ids {
            let src_idx = id_to_node[&src];
            let dst_idx = id_to_node[&dst];
            inner.add_edge(src_idx, dst_idx, attr);
        }

        Graph { inner, id_to_node }
    }
}

impl Graph<(), ()> {
    /// Load a graph from an edge-list file.
    ///
    /// Each non-comment line must contain two whitespace-separated integers
    /// (src_id and dst_id). Lines beginning with `#` are skipped.
    ///
    /// If `canonical_orientation` is `true`, each edge is oriented so that
    /// `src < dst` (useful for undirected graphs stored as directed).
    pub fn from_file(path: &str, canonical_orientation: bool) -> io::Result<Self> {
        let reader = BufReader::new(File::open(path)?);
        let mut edges = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut iter = line.split_whitespace();
            let src: VertexId = iter
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing src"))?
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let dst: VertexId = iter
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing dst"))?
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let (src, dst) = if canonical_orientation && src > dst {
                (dst, src)
            } else {
                (src, dst)
            };
            edges.push(Edge { src, dst, attr: () });
        }

        Ok(Self::from_edges(edges, ()))
    }
}

impl<VD: Clone, ED: Clone> Graph<VD, ED> {
    pub fn num_vertices(&self) -> usize {
        self.inner.node_count()
    }

    pub fn num_edges(&self) -> usize {
        self.inner.edge_count()
    }

    /// Iterate over `(VertexId, &VD)` pairs.
    pub fn vertices(&self) -> impl Iterator<Item = (VertexId, &VD)> {
        self.inner.node_weights().map(|(vid, vd)| (*vid, vd))
    }

    /// Iterate over edges as `Edge<ED>` (cloned attr).
    pub fn edges(&self) -> impl Iterator<Item = Edge<ED>> + '_ {
        self.inner.edge_indices().map(move |ei| {
            let (a, b) = self.inner.edge_endpoints(ei).unwrap();
            let src = self.inner[a].0;
            let dst = self.inner[b].0;
            Edge {
                src,
                dst,
                attr: self.inner[ei].clone(),
            }
        })
    }

    /// Iterate over edge triplets (edge + both vertex attributes).
    pub fn triplets(&self) -> impl Iterator<Item = EdgeTriplet<VD, ED>> + '_ {
        self.inner.edge_indices().map(move |ei| {
            let (a, b) = self.inner.edge_endpoints(ei).unwrap();
            let (src_id, src_attr) = self.inner[a].clone();
            let (dst_id, dst_attr) = self.inner[b].clone();
            EdgeTriplet {
                src_id,
                dst_id,
                src_attr,
                dst_attr,
                attr: self.inner[ei].clone(),
            }
        })
    }

    /// Map from each `VertexId` to its in-degree.
    pub fn in_degrees(&self) -> VertexMap<usize> {
        self.inner
            .node_indices()
            .map(|n| {
                let vid = self.inner[n].0;
                let deg = self
                    .inner
                    .neighbors_directed(n, Direction::Incoming)
                    .count();
                (vid, deg)
            })
            .collect()
    }

    /// Map from each `VertexId` to its out-degree.
    pub fn out_degrees(&self) -> VertexMap<usize> {
        self.inner
            .node_indices()
            .map(|n| {
                let vid = self.inner[n].0;
                let deg = self
                    .inner
                    .neighbors_directed(n, Direction::Outgoing)
                    .count();
                (vid, deg)
            })
            .collect()
    }

    /// Return the `NodeIndex` for a vertex ID, if present.
    #[allow(dead_code)]
    pub(crate) fn node_index(&self, vid: VertexId) -> Option<NodeIndex> {
        self.id_to_node.get(&vid).copied()
    }

    /// Return a `HashSet` of out-neighbor IDs for a given vertex.
    #[allow(dead_code)]
    pub(crate) fn out_neighbors(&self, vid: VertexId) -> HashSet<VertexId> {
        match self.id_to_node.get(&vid) {
            None => HashSet::new(),
            Some(&n) => self
                .inner
                .neighbors_directed(n, Direction::Outgoing)
                .map(|nb| self.inner[nb].0)
                .collect(),
        }
    }
}

impl<VD: Clone, ED: Clone> Graph<VD, ED> {
    /// Apply `f` to each vertex attribute, returning a new graph with the same structure.
    pub fn map_vertices<VD2: Clone, F>(&self, f: F) -> Graph<VD2, ED>
    where
        F: Fn(VertexId, &VD) -> VD2,
    {
        let new_vertices: Vec<(VertexId, VD2)> = self
            .inner
            .node_weights()
            .map(|(vid, vd)| (*vid, f(*vid, vd)))
            .collect();
        let new_edges: Vec<Edge<ED>> = self.edges().collect();
        Graph::from_vertices_edges(new_vertices, new_edges)
    }

    /// Apply `f` to each edge attribute, returning a new graph with the same structure.
    pub fn map_edges<ED2: Clone, F>(&self, f: F) -> Graph<VD, ED2>
    where
        F: Fn(&Edge<ED>) -> ED2,
    {
        let vertices: Vec<(VertexId, VD)> = self
            .inner
            .node_weights()
            .map(|(v, d)| (*v, d.clone()))
            .collect();
        let new_edges: Vec<Edge<ED2>> = self
            .edges()
            .map(|e| {
                let attr2 = f(&e);
                Edge {
                    src: e.src,
                    dst: e.dst,
                    attr: attr2,
                }
            })
            .collect();
        Graph::from_vertices_edges(vertices, new_edges)
    }

    /// Apply `f` to each edge triplet, returning a new graph with new edge attributes.
    pub fn map_triplets<ED2: Clone, F>(&self, f: F) -> Graph<VD, ED2>
    where
        F: Fn(&EdgeTriplet<VD, ED>) -> ED2,
    {
        let vertices: Vec<(VertexId, VD)> = self
            .inner
            .node_weights()
            .map(|(v, d)| (*v, d.clone()))
            .collect();
        let new_edges: Vec<Edge<ED2>> = self
            .triplets()
            .map(|t| {
                let attr2 = f(&t);
                Edge {
                    src: t.src_id,
                    dst: t.dst_id,
                    attr: attr2,
                }
            })
            .collect();
        Graph::from_vertices_edges(vertices, new_edges)
    }

    /// Keep only vertices and edges satisfying the predicates.
    ///
    /// Only edges for which *both* endpoints pass `vpred` are considered; then
    /// `epred` further filters those edges.
    pub fn subgraph<VPred, EPred>(&self, vpred: VPred, epred: EPred) -> Graph<VD, ED>
    where
        VPred: Fn(VertexId, &VD) -> bool,
        EPred: Fn(&Edge<ED>) -> bool,
    {
        let vertices: Vec<(VertexId, VD)> = self
            .inner
            .node_weights()
            .filter(|(vid, vd)| vpred(*vid, vd))
            .map(|(v, d)| (*v, d.clone()))
            .collect();
        let keep_vids: HashSet<VertexId> = vertices.iter().map(|(v, _)| *v).collect();
        let edges: Vec<Edge<ED>> = self
            .edges()
            .filter(|e| keep_vids.contains(&e.src) && keep_vids.contains(&e.dst) && epred(e))
            .collect();
        Graph::from_vertices_edges(vertices, edges)
    }

    /// Return a new graph with all edge directions reversed.
    pub fn reverse(&self) -> Graph<VD, ED> {
        let vertices: Vec<(VertexId, VD)> = self
            .inner
            .node_weights()
            .map(|(v, d)| (*v, d.clone()))
            .collect();
        let edges: Vec<Edge<ED>> = self
            .edges()
            .map(|e| Edge {
                src: e.dst,
                dst: e.src,
                attr: e.attr,
            })
            .collect();
        Graph::from_vertices_edges(vertices, edges)
    }

    /// Merge parallel edges between the same (src, dst) pair using `merge`.
    ///
    /// The graph must be a simple directed multigraph; callers are responsible for
    /// ensuring it was partitioned appropriately before calling this method.
    pub fn group_edges<F>(&self, merge: F) -> Graph<VD, ED>
    where
        F: Fn(ED, ED) -> ED,
    {
        let vertices: Vec<(VertexId, VD)> = self
            .inner
            .node_weights()
            .map(|(v, d)| (*v, d.clone()))
            .collect();

        let mut merged: HashMap<(VertexId, VertexId), ED> = HashMap::new();
        for e in self.edges() {
            merged
                .entry((e.src, e.dst))
                .and_modify(|existing| *existing = merge(existing.clone(), e.attr.clone()))
                .or_insert(e.attr);
        }

        let edges: Vec<Edge<ED>> = merged
            .into_iter()
            .map(|((src, dst), attr)| Edge { src, dst, attr })
            .collect();
        Graph::from_vertices_edges(vertices, edges)
    }

    /// Join vertices with an external map, replacing each vertex attribute.
    ///
    /// Equivalent to GraphX's `outerJoinVertices`.  `map_func` receives
    /// `(vid, old_vd, Option<&U>)` — `None` when the vertex has no entry in `other`.
    pub fn outer_join_vertices<U: Clone, VD2: Clone, F>(
        &self,
        other: &HashMap<VertexId, U>,
        map_func: F,
    ) -> Graph<VD2, ED>
    where
        F: Fn(VertexId, &VD, Option<&U>) -> VD2,
    {
        self.map_vertices(|vid, vd| map_func(vid, vd, other.get(&vid)))
    }

    /// Aggregate messages from edges into per-vertex values.
    ///
    /// `send_msg` is called once per edge triplet; it may call
    /// `ctx.send_to_src()` / `ctx.send_to_dst()` to emit messages.
    /// All messages destined for the same vertex are combined with `merge_msg`.
    ///
    /// Vertices that received no message are absent from the result map.
    pub fn aggregate_messages<A: Clone, SendMsg, MergeMsg>(
        &self,
        send_msg: SendMsg,
        merge_msg: MergeMsg,
    ) -> VertexMap<A>
    where
        SendMsg: Fn(EdgeContext<VD, ED, A>),
        MergeMsg: Fn(A, A) -> A,
    {
        let mut raw: Vec<(VertexId, A)> = Vec::new();

        for t in self.triplets() {
            let ctx = EdgeContext {
                triplet: t,
                msgs: &mut raw,
            };
            send_msg(ctx);
        }

        let mut result: VertexMap<A> = HashMap::new();
        for (vid, msg) in raw {
            result
                .entry(vid)
                .and_modify(|existing| *existing = merge_msg(existing.clone(), msg.clone()))
                .or_insert(msg);
        }
        result
    }

    /// Build a new `Graph` with the same edges but updated vertex data from `new_vd`.
    ///
    /// Vertices absent from `new_vd` retain their current data.
    pub(crate) fn with_updated_vertices(&self, new_vd: &HashMap<VertexId, VD>) -> Graph<VD, ED> {
        let vertices: Vec<(VertexId, VD)> = self
            .inner
            .node_weights()
            .map(|(vid, vd)| (*vid, new_vd.get(vid).cloned().unwrap_or_else(|| vd.clone())))
            .collect();
        let edges: Vec<Edge<ED>> = self.edges().collect();
        Graph::from_vertices_edges(vertices, edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triangle_graph() -> Graph<(), f64> {
        // 0 →(1.0)→ 1 →(2.0)→ 2 →(3.0)→ 0
        Graph::from_edges(
            vec![
                Edge {
                    src: 0,
                    dst: 1,
                    attr: 1.0,
                },
                Edge {
                    src: 1,
                    dst: 2,
                    attr: 2.0,
                },
                Edge {
                    src: 2,
                    dst: 0,
                    attr: 3.0,
                },
            ],
            (),
        )
    }

    #[test]
    fn basic_counts() {
        let g = triangle_graph();
        assert_eq!(g.num_vertices(), 3);
        assert_eq!(g.num_edges(), 3);
    }

    #[test]
    fn triplets_have_correct_attrs() {
        let g = triangle_graph();
        let mut srcs: Vec<VertexId> = g.triplets().map(|t| t.src_id).collect();
        srcs.sort();
        assert_eq!(srcs, vec![0, 1, 2]);
    }

    #[test]
    fn map_vertices_changes_type() {
        let g = triangle_graph();
        let ranked: Graph<f64, f64> = g.map_vertices(|_vid, _| 1.0_f64);
        assert_eq!(ranked.num_vertices(), 3);
    }

    #[test]
    fn reverse_flips_edges() {
        let g = triangle_graph();
        let r = g.reverse();
        // original 0→1, reversed should have 1→0
        let has_1_to_0 = r.edges().any(|e| e.src == 1 && e.dst == 0);
        assert!(has_1_to_0);
    }

    #[test]
    fn out_degrees() {
        let g = triangle_graph();
        let od = g.out_degrees();
        assert_eq!(od[&0], 1);
        assert_eq!(od[&1], 1);
        assert_eq!(od[&2], 1);
    }

    #[test]
    fn aggregate_messages_sums() {
        let g = triangle_graph();
        // send 1.0 to dst of every edge → each vertex gets in-degree as sum
        let result = g.aggregate_messages::<f64, _, _>(
            |mut ctx| {
                ctx.send_to_dst(1.0_f64);
            },
            |a, b| a + b,
        );
        for (_, v) in result {
            assert_eq!(v, 1.0);
        }
    }

    #[test]
    fn empty_graph_has_zero_counts() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![], vec![]);
        assert_eq!(g.num_vertices(), 0);
        assert_eq!(g.num_edges(), 0);
    }

    #[test]
    fn from_vertices_edges_isolated_vertex_is_present() {
        let g: Graph<i32, ()> = Graph::from_vertices_edges(
            vec![(0, 10), (1, 20), (99, 99)],
            vec![Edge {
                src: 0,
                dst: 1,
                attr: (),
            }],
        );
        assert_eq!(g.num_vertices(), 3);
        assert_eq!(g.num_edges(), 1);
        // isolated vertex 99 should be in the graph
        let ids: Vec<_> = g.vertices().map(|(id, _)| id).collect();
        assert!(ids.contains(&99));
    }

    #[test]
    fn from_vertices_edges_edge_referencing_unknown_vertex_dropped() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(
            vec![(0, ()), (1, ())],
            vec![
                Edge {
                    src: 0,
                    dst: 1,
                    attr: (),
                },
                Edge {
                    src: 0,
                    dst: 99,
                    attr: (),
                }, // 99 not in vertex list
            ],
        );
        assert_eq!(g.num_edges(), 1, "edge to unknown vertex should be dropped");
    }

    #[test]
    fn in_degrees() {
        let g = triangle_graph();
        let id = g.in_degrees();
        // Triangle: every vertex has in-degree 1.
        assert_eq!(id[&0], 1);
        assert_eq!(id[&1], 1);
        assert_eq!(id[&2], 1);
    }

    #[test]
    fn map_edges_changes_attributes() {
        let g = triangle_graph();
        let doubled = g.map_edges(|e| e.attr * 2.0);
        let attrs: Vec<f64> = doubled.edges().map(|e| e.attr).collect();
        for a in attrs {
            assert!(a == 2.0 || a == 4.0 || a == 6.0);
        }
    }

    #[test]
    fn subgraph_filter_by_vertex() {
        let g = triangle_graph();
        // Keep only vertices 0 and 1.
        let sub = g.subgraph(|vid, _| vid <= 1, |_| true);
        assert_eq!(sub.num_vertices(), 2);
        // Only edge 0→1 should survive (2 is removed, so 1→2 and 2→0 are gone).
        assert_eq!(sub.num_edges(), 1);
    }

    #[test]
    fn subgraph_filter_by_edge() {
        let g = triangle_graph();
        // Keep only edges with attr < 2.0.
        let sub = g.subgraph(|_, _| true, |e| e.attr < 2.0);
        assert_eq!(sub.num_edges(), 1);
    }

    #[test]
    fn group_edges_merges_parallel_edges() {
        let g: Graph<(), f64> = Graph::from_vertices_edges(
            vec![(0, ()), (1, ())],
            vec![
                Edge {
                    src: 0,
                    dst: 1,
                    attr: 1.0,
                },
                Edge {
                    src: 0,
                    dst: 1,
                    attr: 3.0,
                },
            ],
        );
        let merged = g.group_edges(|a, b| a + b);
        assert_eq!(merged.num_edges(), 1);
        let total: f64 = merged.edges().map(|e| e.attr).sum();
        assert!((total - 4.0).abs() < 1e-9);
    }

    #[test]
    fn outer_join_vertices_updates_matching() {
        let g = triangle_graph();
        let extra: std::collections::HashMap<i64, f64> =
            vec![(0_i64, 100.0), (2, 200.0)].into_iter().collect();
        let joined = g.outer_join_vertices(&extra, |_vid, _old, v| v.cloned().unwrap_or(0.0));
        let vmap: std::collections::HashMap<_, _> =
            joined.vertices().map(|(id, v)| (id, *v)).collect();
        assert!((vmap[&0] - 100.0).abs() < 1e-9);
        assert!((vmap[&1] - 0.0).abs() < 1e-9); // no entry → default 0
        assert!((vmap[&2] - 200.0).abs() < 1e-9);
    }
}
