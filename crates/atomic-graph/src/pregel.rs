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

/// A convergence test comparing the vertex state before and after a superstep; returns
/// `true` to stop iterating. Used by [`run_until_convergence`].
type Converged<'a, VD> = &'a dyn Fn(&[(VertexId, VD)], &[(VertexId, VD)]) -> bool;

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
    run_loop::<VD, ED, A, SendT, MergeF, VProgT>(graph, max_iterations, send, merge, vprog, None)
}

/// Run Pregel supersteps until `converged` reports the vertex state has settled, or
/// `max_iterations` is reached (whichever comes first).
///
/// Identical to [`run`] except that after each superstep `converged(previous, current)`
/// is evaluated against the vertex sets before and after the step; returning `true` stops
/// the loop. A superstep that produces no messages still stops the loop as in [`run`].
pub fn run_until_convergence<VD, ED, A, SendT, MergeF, VProgT>(
    graph: &Graph<VD, ED>,
    max_iterations: usize,
    send: SendT,
    merge: MergeF,
    vprog: VProgT,
    converged: Converged<'_, VD>,
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
    run_loop::<VD, ED, A, SendT, MergeF, VProgT>(
        graph,
        max_iterations,
        send,
        merge,
        vprog,
        Some(converged),
    )
}

/// The shared superstep loop behind [`run`] and [`run_until_convergence`]. With
/// `converged` set, the previous and current vertex sets are compared after each step.
fn run_loop<VD, ED, A, SendT, MergeF, VProgT>(
    graph: &Graph<VD, ED>,
    max_iterations: usize,
    send: SendT,
    merge: MergeF,
    vprog: VProgT,
    converged: Option<Converged<'_, VD>>,
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
    // Cap fan-out at the vertex count so small graphs don't over-shuffle.
    let n = (ctx.default_parallelism() as u64)
        .min(graph.num_vertices())
        .max(1) as usize;
    let edges = graph.edges.clone();
    let mut vertices = graph.vertices.clone();
    // The prior superstep's vertex set, kept only when a convergence test is active.
    let mut prev: Option<Vec<(VertexId, VD)>> = None;

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
        let vv = joined
            .map_task::<(VertexId, VD), VProgT>(vprog)
            .collect()
            .unwrap_or_default();

        let stop = match (converged, &prev) {
            (Some(test), Some(before)) => test(before, &vv),
            _ => false,
        };

        // Materialize vertices so the next superstep starts from a fresh collection.
        if converged.is_some() {
            vertices = ctx.parallelize_typed(vv.clone(), n);
            prev = Some(vv);
        } else {
            vertices = ctx.parallelize_typed(vv, n);
        }

        if stop {
            break;
        }
    }

    Graph::from_rdds(ctx, vertices, edges)
}
