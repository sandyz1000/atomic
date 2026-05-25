use std::collections::HashMap;

use crate::graph::Graph;
use crate::pregel;
use crate::types::{EdgeContext, EdgeDirection, VertexId, VertexMap};

/// Community detection via label propagation (matching GraphX's `LabelPropagation.run`).
///
/// Each vertex starts with its own vertex ID as its community label.
/// In each Pregel superstep:
///
/// 1. Every vertex sends its current label to all neighbors.
/// 2. Each vertex collects all received labels, tallies their frequencies,
///    and adopts the **most frequent** label.  Ties are broken by the
///    **smallest** label value (deterministic, matches GraphX behavior).
///
/// The algorithm runs for exactly `max_steps` supersteps (static; no
/// convergence detection — GraphX's implementation is also non-convergent).
///
/// # Parameters
/// * `graph`     — input graph (vertex and edge attributes are ignored).
/// * `max_steps` — number of supersteps.
///
/// Returns `VertexMap<VertexId>` mapping each vertex to its community label.
pub fn run<VD: Clone, ED: Clone>(
    graph: &Graph<VD, ED>,
    max_steps: usize,
) -> VertexMap<VertexId> {
    // Vertex attr = current community label (initially own ID).
    let g: Graph<VertexId, ED> = graph.map_vertices(|vid, _| vid);

    // Message type: a frequency map of labels received.
    type LabelFreq = HashMap<VertexId, usize>;

    let result = pregel::run(
        &g,
        // initial_msg: empty frequency map (no pre-superstep 0 info)
        HashMap::new() as LabelFreq,
        max_steps,
        EdgeDirection::Either,
        // vprog: pick the most frequent label (min on ties).
        |_vid, current_label: &VertexId, msg: LabelFreq| {
            if msg.is_empty() {
                return *current_label;
            }
            // Find the max frequency, then the min label among those.
            let max_freq = *msg.values().max().unwrap();
            msg.into_iter()
                .filter(|(_, freq)| *freq == max_freq)
                .map(|(label, _)| label)
                .min()
                .unwrap_or(*current_label)
        },
        // send_msg: each vertex sends its label to all neighbors as a
        // single-entry frequency map.
        |mut ctx: EdgeContext<VertexId, ED, LabelFreq>| {
            let mut m = HashMap::new();
            m.insert(ctx.triplet.src_attr, 1_usize);
            ctx.send_to_dst(m.clone());

            let mut m2 = HashMap::new();
            m2.insert(ctx.triplet.dst_attr, 1_usize);
            ctx.send_to_src(m2);
        },
        // merge_msg: combine two frequency maps (sum counts per label).
        |a: LabelFreq, b: LabelFreq| {
            let mut merged = a;
            for (label, freq) in b {
                *merged.entry(label).or_insert(0) += freq;
            }
            merged
        },
    );

    result.vertices().map(|(vid, label)| (vid, *label)).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::types::Edge;

    /// Two cliques (0-1-2 and 10-11-12) connected by a bridge 2→10.
    fn two_cliques_bridge() -> Graph<(), ()> {
        Graph::from_edges(
            vec![
                // Clique A
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 2, dst: 0, attr: () },
                // Bridge
                Edge { src: 2, dst: 10, attr: () },
                // Clique B
                Edge { src: 10, dst: 11, attr: () },
                Edge { src: 11, dst: 12, attr: () },
                Edge { src: 12, dst: 10, attr: () },
            ],
            (),
        )
    }

    #[test]
    fn each_vertex_gets_a_label() {
        let g = two_cliques_bridge();
        let labels = run(&g, 5);
        assert_eq!(labels.len(), g.num_vertices());
    }

    #[test]
    fn isolated_vertex_keeps_own_label() {
        let g = Graph::from_vertices_edges(
            vec![(0, ()), (1, ()), (99, ())],
            vec![Edge { src: 0, dst: 1, attr: () }],
        );
        let labels = run(&g, 5);
        assert_eq!(labels[&99], 99, "isolated vertex keeps its own label");
    }

    #[test]
    fn all_vertices_receive_a_label() {
        let g = two_cliques_bridge();
        let labels = run(&g, 20);
        assert_eq!(labels.len(), g.num_vertices(), "every vertex must have a label");
    }

    #[test]
    fn single_vertex_gets_own_label() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![(42, ())], vec![]);
        let labels = run(&g, 5);
        assert_eq!(labels[&42], 42);
    }

    #[test]
    fn all_labels_are_valid_vertex_ids() {
        // Labels can only ever be original vertex IDs — verify that invariant.
        let g: Graph<(), ()> = Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: () },
                Edge { src: 1, dst: 2, attr: () },
                Edge { src: 2, dst: 0, attr: () },
            ],
            (),
        );
        let valid_ids: std::collections::HashSet<VertexId> =
            g.vertices().map(|(v, _)| v).collect();
        let labels = run(&g, 10);
        for (_, &label) in &labels {
            assert!(
                valid_ids.contains(&label),
                "label {label} is not an original vertex ID"
            );
        }
    }
}
