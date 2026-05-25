use std::collections::HashMap;

use petgraph::algo::tarjan_scc;

use crate::graph::Graph;
use crate::types::{VertexId, VertexMap};

/// Compute Strongly Connected Components using Tarjan's algorithm.
///
/// Each vertex is labeled with the minimum vertex ID in its SCC — matching
/// the GraphX `StronglyConnectedComponents.run` semantics.
///
/// Uses petgraph's built-in `tarjan_scc` under the hood (correct and efficient)
/// rather than a Pregel-based approximation. The `num_iter` parameter is kept
/// for API compatibility with GraphX but is not used (Tarjan is a single pass).
///
/// Returns `VertexMap<VertexId>` mapping each vertex to the representative
/// (lowest ID) of its SCC.
pub fn run<VD: Clone, ED: Clone>(
    graph: &Graph<VD, ED>,
    _num_iter: usize,
) -> VertexMap<VertexId> {
    // Run Tarjan's SCC on the petgraph inner representation.
    // tarjan_scc returns a Vec<Vec<NodeIndex>> — each inner Vec is one SCC.
    let sccs = tarjan_scc(&graph.inner);

    let mut labels: VertexMap<VertexId> = HashMap::new();

    for component in sccs {
        // Compute the minimum VertexId within this SCC.
        let min_id = component
            .iter()
            .map(|&n| graph.inner[n].0)
            .min()
            .unwrap_or(0);

        // Assign the minimum ID as the representative label for all vertices.
        for n in component {
            let vid = graph.inner[n].0;
            labels.insert(vid, min_id);
        }
    }

    labels
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::types::Edge;

    #[test]
    fn simple_cycle_is_one_scc() {
        // 0 → 1 → 2 → 0 — all three form one SCC.
        let g: Graph<(), ()> = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 2, dst: 0, attr: () },
            ],
            (),
        );
        let labels = run(&g, 5);
        let lbl0 = labels[&0];
        assert_eq!(labels[&1], lbl0, "vertices 0,1,2 should be in same SCC");
        assert_eq!(labels[&2], lbl0, "vertices 0,1,2 should be in same SCC");
    }

    #[test]
    fn dag_has_singleton_sccs() {
        // 0 → 1 → 2  (no back-edges → three singleton SCCs)
        let g: Graph<(), ()> = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
            ],
            (),
        );
        let labels = run(&g, 5);
        // All labels must be distinct in a DAG.
        let unique: std::collections::HashSet<_> = labels.values().collect();
        assert_eq!(unique.len(), 3, "three singleton SCCs expected, got {:?}", labels);
    }

    #[test]
    fn two_cycles_connected() {
        // Cycle A: 0→1→2→0, Cycle B: 3→4→3, bridge: 2→3
        let g: Graph<(), ()> = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 2, dst: 0, attr: () },
                Edge { src: 2, dst: 3, attr: () },
                Edge { src: 3, dst: 4, attr: () },
                Edge { src: 4, dst: 3, attr: () },
            ],
            (),
        );
        let labels = run(&g, 5);
        // Cycle A vertices (0,1,2) should share one label.
        assert_eq!(labels[&0], labels[&1]);
        assert_eq!(labels[&1], labels[&2]);
        // Cycle B vertices (3,4) should share a different label.
        assert_eq!(labels[&3], labels[&4]);
        // The two SCCs should have different labels.
        assert_ne!(labels[&0], labels[&3]);
    }

    #[test]
    fn single_vertex_is_own_scc() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![(5, ())], vec![]);
        let labels = run(&g, 10);
        assert_eq!(labels.len(), 1);
    }

    #[test]
    fn self_loop_vertex_is_own_scc() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(
            vec![(0, ())],
            vec![Edge { src: 0, dst: 0, attr: () }],
        );
        let labels = run(&g, 5);
        assert!(labels.contains_key(&0));
    }
}
