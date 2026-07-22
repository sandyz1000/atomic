//! Structural graph operators — the shape-changing transforms (reverse, degree
//! counts, subgraph, vertex joins, edge grouping, masking).
//!
//! Every operator is expressed on the existing RDD primitives (`map_partitions`,
//! `join`, `left_outer_join`, `count_values`, `combine_by_key`), so they inherit the
//! same local/distributed execution path as the rest of the graph layer.

use atomic_compute::rdd::TypedRdd;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::topology::{Edge, VertexId};

impl<VD, ED> Graph<VD, ED>
where
    VD: GraphData,
    ED: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED::Archived: GraphDecode<ED>,
{
    fn parallelism(&self) -> usize {
        self.context().default_parallelism().max(1)
    }

    /// Reverse every edge, swapping source and destination. Vertices are unchanged.
    pub fn reverse(&self) -> Graph<VD, ED> {
        let reversed = self.edges.clone().map_partitions(|iter| {
            Box::new(iter.map(|e| Edge {
                src: e.dst,
                dst: e.src,
                attr: e.attr,
            })) as Box<dyn Iterator<Item = Edge<ED>>>
        });
        Graph::from_rdds(self.context().clone(), self.vertices.clone(), reversed)
    }

    /// Out-degree (number of out-edges) for every vertex that has at least one.
    pub fn out_degrees(&self) -> TypedRdd<(VertexId, u64)> {
        self.degree_by(|e| e.src)
    }

    /// In-degree (number of in-edges) for every vertex that has at least one.
    pub fn in_degrees(&self) -> TypedRdd<(VertexId, u64)> {
        self.degree_by(|e| e.dst)
    }

    fn degree_by<F>(&self, key: F) -> TypedRdd<(VertexId, u64)>
    where
        F: Fn(&Edge<ED>) -> VertexId + Clone + Send + Sync + 'static,
    {
        let n = self.parallelism();
        self.edges
            .clone()
            .map_partitions_to_pair(move |_idx, iter| {
                let key = key.clone();
                Box::new(iter.map(move |e| (key(&e), 1u32)))
            })
            .count_values(n)
    }

    /// Total degree (in + out) for every vertex incident to at least one edge.
    pub fn degrees(&self) -> TypedRdd<(VertexId, u64)> {
        let n = self.parallelism();
        self.edges
            .clone()
            .map_partitions_to_pair(|_idx, iter| {
                Box::new(iter.flat_map(|e| [(e.src, 1u32), (e.dst, 1u32)]))
            })
            .count_values(n)
    }

    /// Merge parallel edges (edges sharing the same `(src, dst)` pair) with `merge`,
    /// producing a graph with at most one edge per ordered vertex pair.
    pub fn group_edges<F>(&self, merge: F) -> Graph<VD, ED>
    where
        F: Fn(ED, ED) -> ED + Clone + Send + Sync + 'static,
        (VertexId, VertexId): GraphData,
        ((VertexId, VertexId), ED): GraphData,
    {
        let n = self.parallelism();
        let merged = self
            .edges
            .clone()
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|e| ((e.src, e.dst), e.attr))))
            .combine_by_key(|a| a, merge.clone(), merge, n)
            .map_partitions(|iter| {
                Box::new(iter.map(|((src, dst), attr)| Edge { src, dst, attr }))
                    as Box<dyn Iterator<Item = Edge<ED>>>
            });
        Graph::from_rdds(self.context().clone(), self.vertices.clone(), merged)
    }

    /// Left-outer-join the vertices with an external table, deriving new vertex
    /// attributes. Vertices absent from `other` receive `None`. Edges are unchanged.
    pub fn outer_join_vertices<U, VD2, F>(
        &self,
        other: TypedRdd<(VertexId, U)>,
        f: F,
    ) -> Graph<VD2, ED>
    where
        U: GraphData,
        VD2: GraphData,
        VD2::Archived: GraphDecode<VD2>,
        F: Fn(VertexId, &VD, Option<&U>) -> VD2 + Clone + Send + Sync + 'static,
        (VD, Option<U>): GraphData,
    {
        let joined = self.vertices.clone().left_outer_join(other);
        let mapped = joined.map_partitions_to_pair(move |_idx, iter| {
            let f = f.clone();
            Box::new(iter.map(move |(vid, (vd, ou))| (vid, f(vid, &vd, ou.as_ref()))))
        });
        Graph::from_rdds(self.context().clone(), mapped, self.edges.clone())
    }

    /// Inner-join the vertices with an external table, updating only the vertices
    /// that appear in `other`; all others keep their current attribute.
    pub fn join_vertices<U, F>(&self, other: TypedRdd<(VertexId, U)>, f: F) -> Graph<VD, ED>
    where
        U: GraphData,
        F: Fn(VertexId, &VD, &U) -> VD + Clone + Send + Sync + 'static,
        (VD, Option<U>): GraphData,
    {
        self.outer_join_vertices(other, move |vid, vd, ou| match ou {
            Some(u) => f(vid, vd, u),
            None => vd.clone(),
        })
    }

    /// Restrict this graph to the vertices and edges present in `other`.
    ///
    /// A vertex survives if its id appears in `other`'s vertex set; an edge survives if
    /// its `(src, dst)` pair appears in `other`'s edge set. Attributes come from `self`.
    pub fn mask<VD2, ED2>(&self, other: &Graph<VD2, ED2>) -> Graph<VD, ED>
    where
        VD2: GraphData,
        ED2: GraphData,
        VD2::Archived: GraphDecode<VD2>,
        ED2::Archived: GraphDecode<ED2>,
        (VD, ()): GraphData,
        (VertexId, VertexId): GraphData,
        (ED, ()): GraphData,
        ((VertexId, VertexId), ED): GraphData,
        ((VertexId, VertexId), ()): GraphData,
    {
        let other_v = other
            .vertices
            .clone()
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|(vid, _)| (vid, ()))));
        let masked_v = self
            .vertices
            .clone()
            .join::<()>(other_v)
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|(vid, (vd, ()))| (vid, vd))));

        let other_e = other
            .edges
            .clone()
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|e| ((e.src, e.dst), ()))));
        let masked_e = self
            .edges
            .clone()
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|e| ((e.src, e.dst), e.attr))))
            .join::<()>(other_e)
            .map_partitions(|iter| {
                Box::new(iter.map(|((src, dst), (attr, ()))| Edge { src, dst, attr }))
                    as Box<dyn Iterator<Item = Edge<ED>>>
            });

        Graph::from_rdds(self.context().clone(), masked_v, masked_e)
    }

    /// Return the subgraph induced by a vertex predicate and an edge predicate.
    ///
    /// A vertex is kept when `vpred` holds; an edge is kept when `epred` holds **and**
    /// both of its endpoints survive `vpred` (dangling edges are dropped). Unlike GraphX,
    /// `epred` inspects the [`Edge`] itself, not a full triplet.
    pub fn subgraph<VP, EP>(&self, vpred: VP, epred: EP) -> Graph<VD, ED>
    where
        VP: Fn(VertexId, &VD) -> bool + Clone + Send + Sync + 'static,
        EP: Fn(&Edge<ED>) -> bool + Clone + Send + Sync + 'static,
        (VD, ()): GraphData,
        (Edge<ED>, ()): GraphData,
    {
        let kept_v = self.vertices.clone().map_partitions(move |iter| {
            let vpred = vpred.clone();
            Box::new(iter.filter(move |(vid, vd)| vpred(*vid, vd)))
                as Box<dyn Iterator<Item = (VertexId, VD)>>
        });

        // Surviving-vertex id set, as a `(id, ())` pair RDD for join-based pruning.
        let survivors = kept_v
            .clone()
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|(vid, _)| (vid, ()))));

        let kept_e = self.edges.clone().map_partitions(move |iter| {
            let epred = epred.clone();
            Box::new(iter.filter(move |e| epred(e))) as Box<dyn Iterator<Item = Edge<ED>>>
        });

        // Drop edges whose src or dst did not survive: join on src, then on dst.
        let by_src = kept_e
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|e| (e.src, e))))
            .join::<()>(survivors.clone())
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|(_src, (e, ()))| (e.dst, e))));
        let pruned_e = by_src.join::<()>(survivors).map_partitions(|iter| {
            Box::new(iter.map(|(_dst, (e, ()))| e)) as Box<dyn Iterator<Item = Edge<ED>>>
        });

        Graph::from_rdds(self.context().clone(), kept_v, pruned_e)
    }
}
