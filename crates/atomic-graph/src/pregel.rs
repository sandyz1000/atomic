use std::collections::HashMap;

use crate::graph::Graph;
use crate::topology::{EdgeContext, EdgeDirection, VertexId};

/// Run the Pregel vertex-centric computation model on `graph`.
///
/// Execution proceeds as follows:
///
/// 1. Apply `vprog(vid, vd, initial_msg)` to every vertex to obtain the
///    starting vertex state.
/// 2. Repeat up to `max_iterations` supersteps:
///    a. Call `send_msg` on every edge triplet, emitting messages via `ctx.send_to_src()` / `ctx.send_to_dst()`.
///    b. Merge all messages destined for the same vertex with `merge_msg`.
///    c. For each vertex that received a message, apply `vprog(vid, old_vd, msg)` to update its attribute.
///    d. If no messages were generated, terminate early.
/// 3. Return the final graph.
///
/// # Type parameters
///
/// * `VD` — vertex data type (must be `Clone`).
/// * `ED` — edge data type (must be `Clone`).
/// * `A`  — message type (must be `Clone`).
///
/// # Arguments
///
/// * `graph`            — input graph (borrowed; the result is a new [`Graph`]).
/// * `initial_msg`      — message sent to every vertex before superstep 0.
/// * `max_iterations`   — maximum number of supersteps.
/// * `active_direction` — which direction of edges to consider when sending messages.
///   [`EdgeDirection::Either`] considers all edges.
/// * `vprog`            — vertex program: `(vid, vd, msg) -> new_vd`.
/// * `send_msg`         — edge function: receives an [`EdgeContext`] and emits messages.
/// * `merge_msg`        — commutative associative combiner for messages.
pub fn run<VD, ED, A, VProg, SendMsg, MergeMsg>(
    graph: &Graph<VD, ED>,
    initial_msg: A,
    max_iterations: usize,
    _active_direction: EdgeDirection,
    vprog: VProg,
    send_msg: SendMsg,
    merge_msg: MergeMsg,
) -> Graph<VD, ED>
where
    VD: Clone,
    ED: Clone,
    A: Clone,
    VProg: Fn(VertexId, &VD, A) -> VD,
    SendMsg: Fn(EdgeContext<VD, ED, A>),
    MergeMsg: Fn(A, A) -> A,
{
    // Superstep 0: apply vprog with initial_msg to every vertex.
    let mut vdata: HashMap<VertexId, VD> = graph
        .vertices()
        .map(|(vid, vd)| (vid, vprog(vid, vd, initial_msg.clone())))
        .collect();

    let mut current: Graph<VD, ED> = graph.with_updated_vertices(&vdata);

    for _ in 0..max_iterations {
        // Aggregate messages.
        let msgs = current.aggregate_messages::<A, _, _>(&send_msg, &merge_msg);

        if msgs.is_empty() {
            break;
        }

        // Apply vprog to vertices that received a message.
        for (vid, msg) in &msgs {
            if let Some(old_vd) = vdata.get(vid) {
                let new_vd = vprog(*vid, old_vd, msg.clone());
                vdata.insert(*vid, new_vd);
            }
        }

        current = graph.with_updated_vertices(&vdata);
    }

    current
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::topology::Edge;

    #[test]
    fn pregel_min_id_propagation() {
        // Build a chain 0 → 1 → 2 with vertex attr = own ID.
        let g: Graph<VertexId, ()> = Graph::from_edges(
            vec![
                Edge {
                    src: 0,
                    dst: 1,
                    attr: (),
                },
                Edge {
                    src: 1,
                    dst: 2,
                    attr: (),
                },
            ],
            0_i64,
        )
        .map_vertices(|vid, _| vid);

        // Pregel: propagate minimum label. After convergence every vertex
        // should hold label 0 (the global minimum).
        let result = run(
            &g,
            VertexId::MAX,
            10,
            EdgeDirection::Either,
            |_vid, vd, msg: VertexId| (*vd).min(msg),
            |mut ctx| {
                let src_label = ctx.triplet.src_attr;
                let dst_label = ctx.triplet.dst_attr;
                if src_label < dst_label {
                    ctx.send_to_dst(src_label);
                } else if dst_label < src_label {
                    ctx.send_to_src(dst_label);
                }
            },
            |a: VertexId, b: VertexId| a.min(b),
        );

        for (_, label) in result.vertices() {
            assert_eq!(*label, 0);
        }
    }

    #[test]
    fn single_vertex_receives_initial_msg_only() {
        // Graph with one isolated vertex. No edges → send_msg is never called → terminates immediately.
        let g: Graph<VertexId, ()> = Graph::from_vertices_edges(vec![(0, 0)], vec![]);
        let result = run(
            &g,
            VertexId::MAX,
            10,
            EdgeDirection::Either,
            |_vid, _vd, msg: VertexId| msg,
            |_ctx| {},
            |a, b| a.min(b),
        );
        // vprog was called with initial_msg = VertexId::MAX, so all vertices hold MAX.
        assert_eq!(*result.vertices().next().unwrap().1, VertexId::MAX);
    }

    #[test]
    fn max_iterations_respected() {
        // Dense clique that would converge in many iterations.
        let g: Graph<VertexId, ()> = Graph::from_edges(
            vec![
                Edge {
                    src: 0,
                    dst: 1,
                    attr: (),
                },
                Edge {
                    src: 1,
                    dst: 0,
                    attr: (),
                },
            ],
            100_i64,
        )
        .map_vertices(|vid, _| vid);

        let _calls = 0usize;
        let result = run(
            &g,
            VertexId::MAX,
            2, // max 2 iterations
            EdgeDirection::Either,
            |_vid, vd, msg: VertexId| (*vd).min(msg),
            |mut ctx| {
                let s = ctx.triplet.src_attr;
                let d = ctx.triplet.dst_attr;
                if s < d {
                    ctx.send_to_dst(s);
                }
                if d < s {
                    ctx.send_to_src(d);
                }
            },
            |a, b| a.min(b),
        );
        // After at most 2 supersteps the computation must have stopped.
        assert_eq!(result.num_vertices(), 2);
    }

    #[test]
    fn converges_early_when_no_messages_sent() {
        // A path that converges in exactly 1 superstep.
        let g: Graph<VertexId, ()> = Graph::from_edges(
            vec![Edge {
                src: 0,
                dst: 1,
                attr: (),
            }],
            VertexId::MAX,
        )
        .map_vertices(|vid, _| vid);

        let result = run(
            &g,
            VertexId::MAX,
            100,
            EdgeDirection::Either,
            |_vid, vd, msg: VertexId| (*vd).min(msg),
            |mut ctx| {
                // After superstep 0: both hold their own ID (0 and 1).
                // 0 < 1 → send 0 to dst once. After merge, vertex 1 holds 0.
                // Superstep 1: 0 = 0 and 1 now holds 0 → no messages → terminates.
                if ctx.triplet.src_attr < ctx.triplet.dst_attr {
                    ctx.send_to_dst(ctx.triplet.src_attr);
                }
            },
            |a, b| a.min(b),
        );
        for (_, label) in result.vertices() {
            assert_eq!(*label, 0, "all vertices should converge to min label 0");
        }
    }
}
