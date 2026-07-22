//! Engine-backed graph type.
//!
//! A [`Graph`] is a pair of RDDs — a vertex RDD `(VertexId, VD)` and an edge RDD
//! `Edge<ED>` — held against an Atomic [`Context`]. All graph computation runs as
//! RDD jobs on the compute engine: in local mode on the thread-pool scheduler, in
//! distributed mode dispatched to workers. [`aggregate_messages`](Graph::aggregate_messages)
//! is the primitive every built-in algorithm and [`pregel`](crate::pregel) build on.

use std::collections::BTreeMap;
use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_compute::rdd::TypedRdd;
use atomic_data::distributed::{RkyvWireSerializer, RkyvWireStrategy, RkyvWireValidator};

use crate::topology::{Edge, EdgeTriplet, VertexId};

/// Bound bundle for any value that travels as an RDD element in a graph job:
/// cloneable and debuggable (the engine's `Data`), wire-encodable (`rkyv`) and
/// shuffle-encodable (`bincode`).
///
/// The `rkyv` *decode* side (`Archived: CheckBytes + Deserialize`) is required
/// separately at each `impl`/function via the [`GraphDecode`] alias, because a
/// bound on `Self::Archived` placed on this trait would not propagate to use sites.
pub trait GraphData:
    Clone
    + std::fmt::Debug
    + Send
    + Sync
    + 'static
    + rkyv::Archive
    + for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>
    + bincode::Encode
    + bincode::Decode<()>
{
}

impl<T> GraphData for T where
    T: Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static
        + rkyv::Archive
        + for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>
        + bincode::Encode
        + bincode::Decode<()>
{
}

/// The `rkyv` decode obligation for a graph value type: its archived form can be
/// validated and deserialized back. Spelled as a macro-free helper so the built-in
/// algorithms can repeat it in one line: `where VD: GraphData, VD::Archived: GraphDecode<VD>`.
pub trait GraphDecode<T>:
    for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<T, RkyvWireStrategy>
{
}

impl<T, A> GraphDecode<T> for A where
    A: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
        + rkyv::Deserialize<T, RkyvWireStrategy>
{
}

/// A directed graph with vertex data `VD` and edge data `ED`, backed by RDDs.
///
/// Transformation methods return a new [`Graph`]; the underlying RDDs are lazy, so
/// no computation runs until an action (`collect_vertices`, an algorithm, …) fires.
#[derive(Clone)]
pub struct Graph<VD, ED> {
    ctx: Arc<Context>,
    /// Vertex RDD: one `(id, attribute)` pair per vertex.
    pub vertices: TypedRdd<(VertexId, VD)>,
    /// Edge RDD: one [`Edge`] per directed edge.
    pub edges: TypedRdd<Edge<ED>>,
}

impl<VD, ED> Graph<VD, ED>
where
    VD: GraphData,
    ED: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED::Archived: GraphDecode<ED>,
{
    /// Build a graph directly from a vertex RDD and an edge RDD.
    pub fn from_rdds(
        ctx: Arc<Context>,
        vertices: TypedRdd<(VertexId, VD)>,
        edges: TypedRdd<Edge<ED>>,
    ) -> Self {
        Graph {
            ctx,
            vertices,
            edges,
        }
    }

    /// Build a graph from in-memory vertex and edge lists.
    ///
    /// Edges referencing a vertex id absent from `vertices` are kept; the join-based
    /// algorithms simply find no attribute for that endpoint.
    pub fn from_vertices_edges(
        ctx: Arc<Context>,
        vertices: Vec<(VertexId, VD)>,
        edges: Vec<Edge<ED>>,
    ) -> Self {
        let n = ctx.default_parallelism().max(1);
        let v_rdd = ctx.parallelize_typed(vertices, n);
        let e_rdd = ctx.parallelize_typed(edges, n);
        Graph::from_rdds(ctx, v_rdd, e_rdd)
    }

    /// Build a graph from edges only; every endpoint becomes a vertex carrying `default_vd`.
    pub fn from_edges(ctx: Arc<Context>, edges: Vec<Edge<ED>>, default_vd: VD) -> Self {
        // Derive the unique vertex set from edge endpoints on the driver.
        let mut ids: BTreeMap<VertexId, ()> = BTreeMap::new();
        for e in &edges {
            ids.insert(e.src, ());
            ids.insert(e.dst, ());
        }
        let vertices: Vec<(VertexId, VD)> =
            ids.into_keys().map(|id| (id, default_vd.clone())).collect();
        Graph::from_vertices_edges(ctx, vertices, edges)
    }

    /// The [`Context`] this graph runs on.
    pub fn context(&self) -> &Arc<Context> {
        &self.ctx
    }

    /// Number of vertices.
    pub fn num_vertices(&self) -> u64 {
        self.vertices.count().unwrap_or(0)
    }

    /// Number of edges.
    pub fn num_edges(&self) -> u64 {
        self.edges.count().unwrap_or(0)
    }

    /// Materialize the vertex set to the driver.
    pub fn collect_vertices(&self) -> Vec<(VertexId, VD)> {
        self.vertices.collect().unwrap_or_default()
    }

    /// Materialize the edge set to the driver.
    pub fn collect_edges(&self) -> Vec<Edge<ED>> {
        self.edges.collect().unwrap_or_default()
    }

    /// Return a randomly chosen vertex id, or `None` if the graph is empty.
    ///
    /// Samples one id uniformly. Only the ids cross the wire — vertex attributes are
    /// projected away before the driver-side sample.
    pub fn pick_random_vertex(&self) -> Option<VertexId> {
        let ids = self.vertices.clone().map_partitions(|iter| {
            Box::new(iter.map(|(vid, _)| vid)) as Box<dyn Iterator<Item = VertexId>>
        });
        ids.take_sample(false, 1, rand::random::<u64>())
            .ok()?
            .into_iter()
            .next()
    }

    /// Return the edge attribute for `(src, dst)` if the edge exists.
    ///
    /// In local mode this filters the edge RDD on the driver; in distributed mode
    /// it dispatches the filter to workers.
    pub fn find(&self, src: VertexId, dst: VertexId) -> Option<ED> {
        let filtered = self.edges.clone().map_partitions(move |iter| {
            Box::new(iter.filter(move |e| e.src == src && e.dst == dst))
                as Box<dyn Iterator<Item = Edge<ED>>>
        });
        filtered.take(1).ok()?.into_iter().next().map(|e| e.attr)
    }

    /// Map each edge's attribute using the full edge triplet (source vertex + edge +
    /// destination vertex data), returning a new graph with transformed edge attributes.
    ///
    /// This is the composition `triplets().map(triplet → new_attr)` — a convenience
    /// over the separate [`triplets`](Self::triplets) call.
    pub fn map_triplets<ED2, F>(&self, f: F) -> Graph<VD, ED2>
    where
        ED2: GraphData,
        ED2::Archived: GraphDecode<ED2>,
        F: Fn(&EdgeTriplet<VD, ED>) -> ED2 + Clone + Send + Sync + 'static,
        (VertexId, ED): GraphData,
        (VertexId, ED, VD): GraphData,
        ((VertexId, ED), VD): GraphData,
        ((VertexId, ED, VD), VD): GraphData,
    {
        let new_edges = self.triplets().map_partitions(move |iter| {
            let f = f.clone();
            Box::new(iter.map(move |t| Edge {
                src: t.src_id,
                dst: t.dst_id,
                attr: f(&t),
            })) as Box<dyn Iterator<Item = Edge<ED2>>>
        });
        Graph::from_rdds(self.ctx.clone(), self.vertices.clone(), new_edges)
    }

    /// Replace every vertex attribute via `f`, keeping the edge RDD unchanged.
    ///
    /// `f` runs as a driver-side RDD map (`O(V)`); the distributed work in the
    /// built-in algorithms is the per-edge message pass, not this transform.
    pub fn map_vertices<VD2, F>(&self, f: F) -> Graph<VD2, ED>
    where
        VD2: GraphData,
        VD2::Archived: GraphDecode<VD2>,
        F: Fn(VertexId, &VD) -> VD2 + Clone + Send + Sync + 'static,
    {
        let mapped = self
            .vertices
            .clone()
            .map_partitions_to_pair(move |_idx, iter| {
                let f = f.clone();
                Box::new(iter.map(move |(vid, vd)| (vid, f(vid, &vd))))
            });
        Graph::from_rdds(self.ctx.clone(), mapped, self.edges.clone())
    }

    /// Replace every edge attribute via `f`, keeping the vertex RDD unchanged.
    ///
    /// Algorithms that ignore edge data normalize to `Graph<VD, ()>` with
    /// `map_edges(|_| ())` so the per-edge message task is a concrete type.
    pub fn map_edges<ED2, F>(&self, f: F) -> Graph<VD, ED2>
    where
        ED2: GraphData,
        ED2::Archived: GraphDecode<ED2>,
        F: Fn(&Edge<ED>) -> ED2 + Clone + Send + Sync + 'static,
    {
        let mapped = self.edges.clone().map_partitions(move |iter| {
            let f = f.clone();
            Box::new(iter.map(move |e| {
                let attr = f(&e);
                Edge {
                    src: e.src,
                    dst: e.dst,
                    attr,
                }
            })) as Box<dyn Iterator<Item = Edge<ED2>>>
        });
        Graph::from_rdds(self.ctx.clone(), self.vertices.clone(), mapped)
    }

    /// Build the edge triplets — each edge paired with both endpoint attributes.
    ///
    /// Two shuffle joins attach the source and destination vertex attributes to
    /// each edge. This is the input a message-sending task consumes.
    pub fn triplets(&self) -> TypedRdd<EdgeTriplet<VD, ED>>
    where
        (VertexId, ED): GraphData,
        (VertexId, ED, VD): GraphData,
        ((VertexId, ED), VD): GraphData,
        ((VertexId, ED, VD), VD): GraphData,
    {
        // edges keyed by src: (src, (dst, edge_attr))
        let by_src: TypedRdd<(VertexId, (VertexId, ED))> = self
            .edges
            .clone()
            .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|e| (e.src, (e.dst, e.attr)))));
        // attach src attribute: (src, ((dst, edge_attr), src_attr))
        let with_src = by_src.join::<VD>(self.vertices.clone());
        // re-key by dst: (dst, (src, edge_attr, src_attr))
        let by_dst: TypedRdd<(VertexId, (VertexId, ED, VD))> =
            with_src.map_partitions_to_pair(|_idx, iter| {
                Box::new(iter.map(|(src, ((dst, attr), src_attr))| (dst, (src, attr, src_attr))))
            });
        // attach dst attribute: (dst, ((src, edge_attr, src_attr), dst_attr))
        let with_dst = by_dst.join::<VD>(self.vertices.clone());
        with_dst.map_partitions(|iter| {
            Box::new(
                iter.map(|(dst, ((src, attr, src_attr), dst_attr))| EdgeTriplet {
                    src_id: src,
                    dst_id: dst,
                    src_attr,
                    dst_attr,
                    attr,
                }),
            ) as Box<dyn Iterator<Item = EdgeTriplet<VD, ED>>>
        })
    }

    /// Send a message along every triplet, then combine messages per destination vertex.
    ///
    /// `send` and `merge` are both registered `#[task]`s — `send` maps a triplet to
    /// zero or more `(VertexId, message)` pairs (runs on workers in distributed mode),
    /// `merge` combines messages destined for the same vertex through the shuffle.
    ///
    /// Returns a vertex RDD of the merged message per vertex that received one.
    pub fn aggregate_messages<A, S, M>(&self, send: S, merge: M) -> TypedRdd<(VertexId, A)>
    where
        A: GraphData,
        A::Archived: GraphDecode<A>,
        S: atomic_compute::__macro_support::UnaryTask<EdgeTriplet<VD, ED>, Vec<(VertexId, A)>>,
        M: atomic_compute::__macro_support::BinaryTask<A>,
        (VertexId, ED): GraphData,
        (VertexId, ED, VD): GraphData,
        ((VertexId, ED), VD): GraphData,
        ((VertexId, ED, VD), VD): GraphData,
    {
        self.triplets()
            .flat_map_task::<(VertexId, A), S>(send)
            .reduce_by_key_task(merge)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weighted(src: VertexId, dst: VertexId, w: i64) -> Edge<i64> {
        Edge { src, dst, attr: w }
    }

    // 0 → 1 (weight 5), 1 → 2 (weight 7); vertex attrs are labels.
    fn sample() -> Graph<i64, i64> {
        let ctx = Context::local().unwrap();
        Graph::from_vertices_edges(
            ctx,
            vec![(0, 100), (1, 200), (2, 300)],
            vec![weighted(0, 1, 5), weighted(1, 2, 7)],
        )
    }

    #[test]
    fn test_find_hit() {
        assert_eq!(sample().find(1, 2), Some(7));
    }

    #[test]
    fn test_find_miss() {
        assert_eq!(sample().find(2, 0), None);
    }

    #[test]
    fn test_pick_vertex() {
        let vid = sample().pick_random_vertex().unwrap();
        assert!([0, 1, 2].contains(&vid));
    }

    #[test]
    fn test_pick_empty() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_vertices_edges(ctx, vec![], vec![]);
        assert_eq!(g.pick_random_vertex(), None);
    }

    #[test]
    fn test_map_triplets() {
        // New edge attr = src_label + dst_label + weight.
        let g = sample().map_triplets(|t| t.src_attr + t.dst_attr + t.attr);
        let mut edges = g.collect_edges();
        edges.sort_by_key(|e| e.src);
        assert_eq!(edges[0].attr, 100 + 200 + 5);
        assert_eq!(edges[1].attr, 200 + 300 + 7);
    }
}
