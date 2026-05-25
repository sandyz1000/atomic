use std::collections::{HashMap, HashSet};

use crate::graph::Graph;
use crate::types::{VertexId, VertexMap};

/// Count the number of triangles each vertex participates in.
///
/// Uses the neighbor-set intersection approach matching GraphX's
/// `TriangleCount.run`:
///
/// 1. Treat the graph as undirected by collecting both-direction neighbors.
/// 2. For each edge (u, v), count `|N(u) ∩ N(v)|` where N is the undirected
///    neighbor set.
/// 3. Each triangle is counted twice (once per direction of the edge), so the
///    final per-vertex count is the sum of intersections divided by 2.
///
/// Returns `VertexMap<usize>` — the triangle count for each vertex.
pub fn run<VD: Clone, ED: Clone>(graph: &Graph<VD, ED>) -> VertexMap<usize> {
    // Build undirected neighbor sets (include both directions).
    let mut nbr_sets: HashMap<VertexId, HashSet<VertexId>> = graph
        .vertices()
        .map(|(vid, _)| (vid, HashSet::new()))
        .collect();

    for e in graph.edges() {
        if e.src != e.dst {
            nbr_sets.entry(e.src).or_default().insert(e.dst);
            nbr_sets.entry(e.dst).or_default().insert(e.src);
        }
    }

    // For each directed edge (u→v), count common neighbors.
    let mut raw_counts: HashMap<VertexId, usize> =
        graph.vertices().map(|(vid, _)| (vid, 0)).collect();

    for e in graph.edges() {
        let u = e.src;
        let v = e.dst;
        if u == v {
            continue;
        }
        // Intersection size.
        let count = match (nbr_sets.get(&u), nbr_sets.get(&v)) {
            (Some(nu), Some(nv)) => nu.intersection(nv).count(),
            _ => 0,
        };
        *raw_counts.entry(u).or_insert(0) += count;
        *raw_counts.entry(v).or_insert(0) += count;
    }

    // Each triangle is counted twice (once per edge direction between u and v).
    raw_counts.into_iter().map(|(vid, c)| (vid, c / 2)).collect()
}

/// Count the total number of triangles in the graph (each triangle counted once).
pub fn total<VD: Clone, ED: Clone>(graph: &Graph<VD, ED>) -> usize {
    // Sum per-vertex counts; each triangle contributes 3 vertex counts.
    run(graph).values().sum::<usize>() / 3
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::types::Edge;

    /// K4: complete graph on 4 vertices — each vertex in 3 triangles, 4 total.
    fn k4() -> Graph<(), ()> {
        Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 0, dst: 2, attr: () },
                Edge { src: 0, dst: 3, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 1, dst: 3, attr: () },
                Edge { src: 2, dst: 3, attr: () },
            ],
            (),
        )
    }

    #[test]
    fn k4_total_triangles() {
        assert_eq!(total(&k4()), 4, "K4 has 4 triangles");
    }

    #[test]
    fn k4_per_vertex_count() {
        let counts = run(&k4());
        for (_, c) in counts {
            assert_eq!(c, 3, "each K4 vertex participates in 3 triangles");
        }
    }

    #[test]
    fn triangle_graph() {
        let g = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 2, dst: 0, attr: () },
            ],
            (),
        );
        assert_eq!(total(&g), 1, "single triangle");
        let counts = run(&g);
        for (_, c) in counts {
            assert_eq!(c, 1);
        }
    }

    #[test]
    fn no_triangles_in_path() {
        let g = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
            ],
            (),
        );
        assert_eq!(total(&g), 0);
    }

    #[test]
    fn empty_graph_has_zero_triangles() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![], vec![]);
        assert_eq!(total(&g), 0);
    }

    #[test]
    fn single_vertex_has_zero_triangles() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![(0, ())], vec![]);
        assert_eq!(total(&g), 0);
        let counts = run(&g);
        assert_eq!(counts[&0], 0);
    }

    #[test]
    fn two_vertices_no_triangle() {
        let g: Graph<(), ()> = Graph::from_edges(
            vec![Edge { src: 0, dst: 1, attr: () }],
            (),
        );
        assert_eq!(total(&g), 0);
    }
}
