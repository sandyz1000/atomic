//! PageRank on the engine, via distributed Pregel.
//!
//! PageRank's scalar parameters (`reset_prob`, vertex count `n`) cannot ride on a
//! `#[task]` (tasks are stateless), so they are baked into the per-vertex state
//! [`PrVertex`]: `base = reset/n` and `damping = 1 - reset` are precomputed on the
//! driver and carried in every vertex. Edge weights `1/out_degree(src)` are baked
//! into the edge attribute. The vertex task then computes `base + damping * sum`.

use std::collections::HashMap;

use atomic_compute::task;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::pregel;
use crate::topology::{Edge, EdgeTriplet, VertexId, VertexMap};

/// Per-vertex PageRank state: the current `rank`, plus the precomputed `base`
/// (`reset/n`) and `damping` (`1 - reset`) constants.
#[derive(
    Clone,
    Debug,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct PrVertex {
    pub rank: f64,
    pub base: f64,
    pub damping: f64,
}

// Shuffle handlers for PageRank's concrete pair shapes (VD = PrVertex, ED = f64).
atomic_compute::register_shuffle_map!(i64, PrVertex);
atomic_compute::register_shuffle_map!(i64, (i64, f64));
atomic_compute::register_shuffle_map!(i64, (i64, f64, PrVertex));

/// Send `src_rank * edge_weight` to the destination of every edge.
#[task]
fn pr_send(t: EdgeTriplet<PrVertex, f64>) -> Vec<(VertexId, f64)> {
    vec![(t.dst_id, t.src_attr.rank * t.attr)]
}

/// Update a vertex's rank from the summed incoming contribution.
#[task]
fn pr_vprog(input: (VertexId, (PrVertex, Option<f64>))) -> (VertexId, PrVertex) {
    let (vid, (mut pv, msg)) = input;
    let sum = msg.unwrap_or(0.0);
    pv.rank = pv.base + pv.damping * sum;
    (vid, pv)
}

/// Sum two rank contributions.
#[task]
fn pr_merge(a: f64, b: f64) -> f64 {
    a + b
}

/// Sum two out-degree counts.
#[task]
fn deg_add(a: i64, b: i64) -> i64 {
    a + b
}

/// Compute PageRank for `num_iter` iterations.
///
/// Edge attributes on the input are ignored; only topology matters. Returns a
/// [`VertexMap`] of vertex id to PageRank score. `reset_prob` is the teleportation
/// probability (a common default is `0.15`).
pub fn run<VD, ED>(graph: &Graph<VD, ED>, num_iter: usize, reset_prob: f64) -> VertexMap<f64>
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
{
    let n = graph.num_vertices();
    if n == 0 {
        return HashMap::new();
    }
    let nf = n as f64;

    // Out-degree per source, computed on the engine.
    let out_deg: HashMap<VertexId, i64> = graph
        .edges
        .clone()
        .map_partitions_to_pair(|_idx, iter| Box::new(iter.map(|e| (e.src, 1i64))))
        .reduce_by_key_task(DegAdd)
        .collect()
        .unwrap_or_default()
        .into_iter()
        .collect();

    // Weighted graph: vertex = PrVertex(rank = 1/n), edge attr = 1/out_degree(src).
    let verts: Vec<(VertexId, PrVertex)> = graph
        .collect_vertices()
        .into_iter()
        .map(|(vid, _)| {
            (
                vid,
                PrVertex {
                    rank: 1.0 / nf,
                    base: reset_prob / nf,
                    damping: 1.0 - reset_prob,
                },
            )
        })
        .collect();
    let edges: Vec<Edge<f64>> = graph
        .collect_edges()
        .into_iter()
        .map(|e| {
            let deg = (*out_deg.get(&e.src).unwrap_or(&1)).max(1) as f64;
            Edge {
                src: e.src,
                dst: e.dst,
                attr: 1.0 / deg,
            }
        })
        .collect();

    let wg = Graph::from_vertices_edges(graph.context().clone(), verts, edges);
    let result =
        pregel::run::<PrVertex, f64, f64, _, _, _>(&wg, num_iter, PrSend, PrMerge, PrVprog);
    result
        .collect_vertices()
        .into_iter()
        .map(|(vid, pv)| (vid, pv.rank))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Edge;
    use atomic_compute::context::Context;

    fn edge(src: VertexId, dst: VertexId) -> Edge<()> {
        Edge { src, dst, attr: () }
    }

    fn star_graph(ctx: std::sync::Arc<Context>) -> Graph<(), ()> {
        // Spokes 1,2,3 all point at hub 0.
        Graph::from_edges(ctx, vec![edge(1, 0), edge(2, 0), edge(3, 0)], ())
    }

    #[test]
    fn hub_has_highest_rank() {
        let ctx = Context::local().unwrap();
        let ranks = run(&star_graph(ctx), 10, 0.15);
        let hub = ranks[&0];
        for (&vid, &r) in &ranks {
            if vid != 0 {
                assert!(hub > r, "hub rank {hub} should exceed spoke rank {r}");
            }
        }
    }

    #[test]
    fn empty_graph_returns_empty_ranks() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_vertices_edges(ctx, vec![], vec![]);
        let ranks = run(&g, 10, 0.15);
        assert!(ranks.is_empty());
    }

    #[test]
    fn two_node_cycle_equal_ranks() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 0)], ());
        let ranks = run(&g, 20, 0.15);
        assert!((ranks[&0] - ranks[&1]).abs() < 0.01);
    }
}
