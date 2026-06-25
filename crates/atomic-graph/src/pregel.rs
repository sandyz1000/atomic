//! Distributed Pregel — bulk-synchronous vertex-centric computation on the engine.
//!
//! Each superstep runs as RDD jobs: [`Graph::aggregate_messages`](crate::graph::Graph::aggregate_messages)
//! sends and combines messages (join + per-edge task + shuffle), then a left-outer
//! join applies the vertex program. The caller supplies the message (`send`), combine
//! (`merge`), and vertex (`vprog`) functions; `send` and `vprog` are registered
//! `#[task]`s so the per-element work dispatches to workers.

use atomic_compute::__macro_support::{BinaryTask, UnaryTask};

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::topology::{EdgeTriplet, VertexId};

type VertexVal<VD, A> = (VD, Option<A>);
type VertexPair<VD, A> = (VertexId, VertexVal<VD, A>);
/// Run up to `max_iterations` Pregel supersteps over `graph`.
///
/// Vertex state must be initialized by the caller before the loop (set the starting
/// label, rank, or distance). Each superstep:
///
/// 1. `send` maps every edge triplet to `(vertex, message)` pairs.
/// 2. `merge` combines messages destined for the same vertex (a shuffle).
/// 3. `vprog` folds each vertex's merged message into its new attribute; vertices
///    with no message keep their current attribute.
///
/// The loop stops early when a superstep produces no messages. Vertex and message
/// sets are materialized each superstep to bound lineage growth.
///
/// `send` and `vprog` are registered `#[task]`s (`Copy` zero-sized markers); `merge`
/// is a commutative-associative combiner.
#[allow(clippy::type_complexity)]
pub fn run<VD, ED, A, SendT, MergeF, VProgT>(
    graph: &Graph<VD, ED>,
    max_iterations: usize,
    send: SendT,
    merge: MergeF,
    vprog: VProgT,
) -> Graph<VD, ED>
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
    A: GraphData,
    A::Archived: GraphDecode<A>,
    Option<A>: GraphData,
    <Option<A> as rkyv::Archive>::Archived: GraphDecode<Option<A>>,
    SendT: UnaryTask<EdgeTriplet<VD, ED>, Vec<(VertexId, A)>> + Copy,
    VProgT: UnaryTask<VertexPair<VD, A>, (VertexId, VD)> + Copy,
    MergeF: BinaryTask<A> + Copy,
{
    let ctx = graph.context().clone();
    let n = ctx.default_parallelism().max(1);
    let edges = graph.edges.clone();
    let mut vertices = graph.vertices.clone();

    for _ in 0..max_iterations {
        let g = Graph::from_rdds(ctx.clone(), vertices.clone(), edges.clone());
        let msgs = g.aggregate_messages::<A, SendT, MergeF>(send, merge);

        // Materialize messages: detect convergence and bound lineage.
        let msg_vec = msgs.collect().unwrap_or_default();
        if msg_vec.is_empty() {
            break;
        }
        let msgs_rdd = ctx.parallelize_typed(msg_vec, n);

        // Apply the vertex program: (vid, (old_attr, Option<msg>)) -> (vid, new_attr).
        let joined = vertices.clone().left_outer_join::<A>(msgs_rdd);
        let new_vertices = joined.map_task::<(VertexId, VD), VProgT>(vprog);

        // Materialize vertices so the next superstep starts from a fresh collection.
        let vv = new_vertices.collect().unwrap_or_default();
        vertices = ctx.parallelize_typed(vv, n);
    }

    Graph::from_rdds(ctx, vertices, edges)
}
