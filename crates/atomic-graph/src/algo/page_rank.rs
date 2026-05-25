use std::collections::HashMap;

use crate::graph::Graph;
use crate::types::{Edge, VertexId, VertexMap};

/// Fixed-iteration PageRank (matches GraphX's `PageRank.run`).
///
/// Uses `aggregate_messages` directly (no Pregel) for `num_iter` iterations.
///
/// # Parameters
/// * `graph`      — the graph (edge attr is ignored; only topology matters).
/// * `num_iter`   — number of iterations.
/// * `reset_prob` — teleportation probability (GraphX default: 0.15).
///
/// Returns a `VertexMap` mapping each vertex ID to its PageRank score.
pub fn run<VD: Clone, ED: Clone>(
    graph: &Graph<VD, ED>,
    num_iter: usize,
    reset_prob: f64,
) -> VertexMap<f64> {
    let n = graph.num_vertices() as f64;
    if n == 0.0 {
        return HashMap::new();
    }

    let out_degrees = graph.out_degrees();

    // Start: uniform rank = 1.0 / n; edge weight = 1 / out_degree[src].
    let initial_rank = 1.0 / n;
    // Build a weighted graph: vertex attr = rank, edge attr = 1/out_degree[src].
    let weighted: Graph<f64, f64> = graph.from_edge_weights(&out_degrees, initial_rank);

    let mut current = weighted;

    for _ in 0..num_iter {
        let msgs = current.aggregate_messages::<f64, _, _>(
            |mut ctx| {
                // send (src_rank * edge_weight) to dst
                let contribution = ctx.triplet.src_attr * ctx.triplet.attr;
                ctx.send_to_dst(contribution);
            },
            |a, b| a + b,
        );

        // Update rank: reset_prob/n + (1-reset_prob) * sum_contributions
        let new_ranks: HashMap<VertexId, f64> = current
            .vertices()
            .map(|(vid, _old_rank)| {
                let msg = msgs.get(&vid).copied().unwrap_or(0.0);
                (vid, reset_prob / n + (1.0 - reset_prob) * msg)
            })
            .collect();

        current = graph.from_edge_weights(&out_degrees, initial_rank);
        current = current.outer_join_vertices(&new_ranks, |_vid, _default, rank_opt| {
            rank_opt.copied().unwrap_or(initial_rank)
        });
    }

    current.vertices().map(|(vid, rank)| (vid, *rank)).collect()
}

/// Convergence-based PageRank (matches GraphX's `PageRank.runUntilConvergence`).
///
/// Runs fixed-iteration PageRank and stops early when the maximum rank change
/// across all vertices falls below `tol`. Capped at `max_iter` to prevent
/// infinite loops on graphs that oscillate (e.g., star graphs with dangling nodes).
///
/// # Parameters
/// * `graph`      — the graph.
/// * `tol`        — convergence tolerance (e.g., `0.001`).
/// * `reset_prob` — teleportation probability (GraphX default: 0.15).
/// * `max_iter`   — hard cap on iterations (GraphX uses `Int.MaxValue`; use
///                  a finite value like `100` for safety).
pub fn run_until_convergence<VD: Clone, ED: Clone>(
    graph: &Graph<VD, ED>,
    tol: f64,
    reset_prob: f64,
    max_iter: usize,
) -> VertexMap<f64> {
    let n = graph.num_vertices() as f64;
    if n == 0.0 {
        return HashMap::new();
    }

    let out_degrees = graph.out_degrees();
    let initial_rank = 1.0 / n;

    let weighted: Graph<f64, f64> = graph.from_edge_weights(&out_degrees, initial_rank);
    let mut current = weighted;

    for _ in 0..max_iter {
        let msgs = current.aggregate_messages::<f64, _, _>(
            |mut ctx| {
                let contribution = ctx.triplet.src_attr * ctx.triplet.attr;
                ctx.send_to_dst(contribution);
            },
            |a, b| a + b,
        );

        let mut max_delta: f64 = 0.0;
        let new_ranks: HashMap<VertexId, f64> = current
            .vertices()
            .map(|(vid, old_rank)| {
                let msg = msgs.get(&vid).copied().unwrap_or(0.0);
                let new_rank = reset_prob / n + (1.0 - reset_prob) * msg;
                max_delta = max_delta.max((new_rank - old_rank).abs());
                (vid, new_rank)
            })
            .collect();

        current = graph.from_edge_weights(&out_degrees, initial_rank);
        current = current.outer_join_vertices(&new_ranks, |_vid, _default, rank_opt| {
            rank_opt.copied().unwrap_or(initial_rank)
        });

        if max_delta < tol {
            break;
        }
    }

    current.vertices().map(|(vid, rank)| (vid, *rank)).collect()
}

// ── Internal helper ───────────────────────────────────────────────────────────

impl<VD: Clone, ED: Clone> Graph<VD, ED> {
    /// Build a graph with f64 vertex attr = `initial_rank` and f64 edge attr
    /// = `1 / out_degree[src]`.  Used internally by PageRank.
    fn from_edge_weights(
        &self,
        out_degrees: &HashMap<VertexId, usize>,
        initial_rank: f64,
    ) -> Graph<f64, f64> {
        let vertices: Vec<(VertexId, f64)> =
            self.vertices().map(|(vid, _)| (vid, initial_rank)).collect();
        let edges: Vec<Edge<f64>> = self
            .edges()
            .map(|e| {
                let deg = *out_degrees.get(&e.src).unwrap_or(&1);
                let w = 1.0 / deg.max(1) as f64;
                Edge { src: e.src, dst: e.dst, attr: w }
            })
            .collect();
        Graph::from_vertices_edges(vertices, edges)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::types::Edge;

    fn star_graph() -> Graph<(), ()> {
        // Hub = 0, spokes = 1,2,3 all pointing at hub.
        Graph::from_edges(
            vec![
                Edge { src: 1, dst: 0, attr: () },
                Edge { src: 2, dst: 0, attr: () },
                Edge { src: 3, dst: 0, attr: () },
            ],
            (),
        )
    }

    #[test]
    fn hub_has_highest_rank() {
        let ranks = run(&star_graph(), 10, 0.15);
        let hub = ranks[&0];
        for (&vid, &r) in &ranks {
            if vid != 0 {
                assert!(hub > r, "hub rank {hub} should exceed spoke rank {r}");
            }
        }
    }

    #[test]
    fn convergence_variant_hub_has_highest_rank() {
        let ranks = run_until_convergence(&star_graph(), 0.001, 0.15, 50);
        let hub = ranks[&0];
        for (&vid, &r) in &ranks {
            if vid != 0 {
                assert!(hub > r, "hub rank {hub} should exceed spoke rank {r}");
            }
        }
    }

    #[test]
    fn rank_sum_roughly_one() {
        // Use a cycle (every vertex has out_degree=1, no dangling nodes)
        // so rank doesn't sink at dangling nodes.
        let cycle = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 2, dst: 3, attr: () },
                Edge { src: 3, dst: 0, attr: () },
            ],
            (),
        );
        let ranks = run(&cycle, 20, 0.15);
        let total: f64 = ranks.values().sum();
        // In a cycle all vertices share equal rank; sum ≈ 1.0.
        assert!((total - 1.0).abs() < 0.1, "rank sum {total} far from 1.0");
    }

    #[test]
    fn two_node_cycle_equal_ranks() {
        let g: Graph<(), ()> = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 0, attr: () },
            ],
            (),
        );
        let ranks = run(&g, 20, 0.15);
        let r0 = ranks[&0];
        let r1 = ranks[&1];
        assert!(
            (r0 - r1).abs() < 0.01,
            "symmetric cycle: both nodes should have equal rank, got {r0} vs {r1}"
        );
    }

    #[test]
    fn sink_node_has_lower_rank_than_hub() {
        // Hub (0) is pointed to by spokes (1,2,3) but points nowhere → dangling node.
        // Hub collects all rank but redistributes via teleportation only.
        // In a star-to-hub the hub should still rank highest.
        let ranks = run(&star_graph(), 20, 0.15);
        let hub = ranks[&0];
        for (&vid, &r) in &ranks {
            if vid != 0 {
                assert!(hub > r, "hub {hub} should exceed spoke {r} for vid={vid}");
            }
        }
    }

    #[test]
    fn empty_graph_returns_empty_ranks() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![], vec![]);
        let ranks = run(&g, 10, 0.15);
        assert!(ranks.is_empty());
    }
}
