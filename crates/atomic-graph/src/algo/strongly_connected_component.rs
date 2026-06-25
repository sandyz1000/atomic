//! Strongly connected components via distributed color propagation.
//!
//! This is the coloring algorithm (Orzan / Fleischer-style), run entirely on the
//! engine's Pregel rather than a single-machine pass. Each round:
//!
//! 1. **Forward coloring** — every active vertex takes the maximum vertex id that
//!    can reach it (forward propagation, fixpoint). The vertex equal to its own
//!    color is a root.
//! 2. **Backward reachability** — within each color, mark the vertices that can
//!    reach their color's root following edges. Those vertices form the root's SCC.
//! 3. Finalize the marked vertices (root = their color) and remove them; the rest
//!    advance to the next round.
//!
//! The round loop is coordinated on the driver; the two propagations per round are
//! distributed Pregel jobs.

use std::collections::{HashMap, HashSet};

use atomic_compute::task;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::pregel;
use crate::topology::{Edge, EdgeTriplet, VertexId, VertexMap};

// Backward-pass pair shapes (VD = (color, reach), A = bool). The forward-pass and
// vertex/label shapes are registered in the crate root.
atomic_compute::register_shuffle_map!(i64, (i64, bool));
atomic_compute::register_shuffle_map!(i64, bool);
atomic_compute::register_shuffle_map!(i64, (i64, (), (i64, bool)));

/// Forward: propagate the larger color along the edge direction (src → dst).
#[task]
fn sc_fwd_send(t: EdgeTriplet<VertexId, ()>) -> Vec<(VertexId, VertexId)> {
    if t.src_attr > t.dst_attr {
        vec![(t.dst_id, t.src_attr)]
    } else {
        Vec::new()
    }
}

/// Forward: keep the maximum color seen.
#[task]
fn sc_fwd_vprog(input: (VertexId, (VertexId, Option<VertexId>))) -> (VertexId, VertexId) {
    let (vid, (color, msg)) = input;
    (vid, color.max(msg.unwrap_or(color)))
}

/// Backward: if the destination can reach its root and shares the source's color,
/// the source can reach the root too. Propagates against edge direction.
#[task]
fn sc_back_send(t: EdgeTriplet<(VertexId, bool), ()>) -> Vec<(VertexId, bool)> {
    let (src_color, src_reach) = t.src_attr;
    let (dst_color, dst_reach) = t.dst_attr;
    if dst_reach && src_color == dst_color && !src_reach {
        vec![(t.src_id, true)]
    } else {
        Vec::new()
    }
}

/// Backward: a vertex reaches its root once any successor in its color does.
#[task]
fn sc_back_vprog(
    input: (VertexId, ((VertexId, bool), Option<bool>)),
) -> (VertexId, (VertexId, bool)) {
    let (vid, ((color, reach), msg)) = input;
    (vid, (color, reach || msg.unwrap_or(false)))
}

/// Forward merge: keep the larger color.
#[task]
fn sc_fwd_merge(a: VertexId, b: VertexId) -> VertexId {
    a.max(b)
}

/// Backward merge: reachable if either is.
#[task]
fn sc_back_merge(a: bool, b: bool) -> bool {
    a || b
}

fn unit_edges(pairs: &[(VertexId, VertexId)]) -> Vec<Edge<()>> {
    pairs
        .iter()
        .map(|(s, d)| Edge {
            src: *s,
            dst: *d,
            attr: (),
        })
        .collect()
}

/// Compute strongly connected components.
///
/// Each vertex is labeled with the representative (lowest id) of its SCC. The
/// `_num_iter` parameter is accepted for API compatibility; the algorithm runs its
/// own driver-coordinated rounds to completion.
pub fn run<VD, ED>(graph: &Graph<VD, ED>, _num_iter: usize) -> VertexMap<VertexId>
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
{
    let ctx = graph.context().clone();

    let mut active_v: Vec<VertexId> = graph
        .collect_vertices()
        .into_iter()
        .map(|(v, _)| v)
        .collect();
    let mut active_e: Vec<(VertexId, VertexId)> = graph
        .collect_edges()
        .into_iter()
        .filter(|e| e.src != e.dst)
        .map(|e| (e.src, e.dst))
        .collect();

    // SCC root per vertex; every vertex is at least its own singleton SCC.
    let mut result: VertexMap<VertexId> = HashMap::new();

    while !active_v.is_empty() {
        let bound = active_v.len() + 1;

        // 1. Forward coloring: color[v] = max id reaching v.
        let color_graph: Graph<VertexId, ()> = Graph::from_vertices_edges(
            ctx.clone(),
            active_v.iter().map(|v| (*v, *v)).collect(),
            unit_edges(&active_e),
        );
        let colored = pregel::run::<VertexId, (), VertexId, _, _, _>(
            &color_graph,
            bound,
            ScFwdSend,
            ScFwdMerge,
            ScFwdVprog,
        );
        let color_map: HashMap<VertexId, VertexId> =
            colored.collect_vertices().into_iter().collect();

        // 2. Backward reachability to each color's root, within the color.
        let reach_graph: Graph<(VertexId, bool), ()> = Graph::from_vertices_edges(
            ctx.clone(),
            active_v
                .iter()
                .map(|v| {
                    let c = *color_map.get(v).unwrap_or(v);
                    (*v, (c, *v == c))
                })
                .collect(),
            unit_edges(&active_e),
        );
        let reached = pregel::run::<(VertexId, bool), (), bool, _, _, _>(
            &reach_graph,
            bound,
            ScBackSend,
            ScBackMerge,
            ScBackVprog,
        );

        // 3. Finalize reached vertices; the rest go to the next round.
        let mut next_v: Vec<VertexId> = Vec::new();
        for (vid, (color, reach)) in reached.collect_vertices() {
            if reach {
                result.insert(vid, color);
            } else {
                next_v.push(vid);
            }
        }

        if next_v.len() == active_v.len() {
            // No vertex was finalized — assign the remainder as singletons to
            // guarantee termination (cannot happen for well-formed input).
            for v in next_v {
                result.entry(v).or_insert(v);
            }
            break;
        }

        let keep: HashSet<VertexId> = next_v.iter().copied().collect();
        active_e.retain(|(s, d)| keep.contains(s) && keep.contains(d));
        active_v = next_v;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_compute::context::Context;

    fn edge(src: VertexId, dst: VertexId) -> Edge<()> {
        Edge { src, dst, attr: () }
    }

    #[test]
    fn simple_cycle_is_one_scc() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 2), edge(2, 0)], ());
        let labels = run(&g, 0);
        let l0 = labels[&0];
        assert_eq!(labels[&1], l0);
        assert_eq!(labels[&2], l0);
    }

    #[test]
    fn dag_has_singleton_sccs() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 2)], ());
        let labels = run(&g, 0);
        let unique: HashSet<_> = labels.values().copied().collect();
        assert_eq!(unique.len(), 3, "three singleton SCCs expected: {labels:?}");
    }

    #[test]
    fn two_cycles_connected() {
        let ctx = Context::local().unwrap();
        // Cycle A: 0->1->2->0, cycle B: 3->4->3, bridge 2->3.
        let g: Graph<(), ()> = Graph::from_edges(
            ctx,
            vec![
                edge(0, 1),
                edge(1, 2),
                edge(2, 0),
                edge(2, 3),
                edge(3, 4),
                edge(4, 3),
            ],
            (),
        );
        let labels = run(&g, 0);
        assert_eq!(labels[&0], labels[&1]);
        assert_eq!(labels[&1], labels[&2]);
        assert_eq!(labels[&3], labels[&4]);
        assert_ne!(labels[&0], labels[&3]);
    }

    #[test]
    fn self_loop_vertex_is_own_scc() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_vertices_edges(ctx, vec![(0, ())], vec![edge(0, 0)]);
        let labels = run(&g, 0);
        assert!(labels.contains_key(&0));
    }
}
