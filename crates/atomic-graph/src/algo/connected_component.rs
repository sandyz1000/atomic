use crate::graph::Graph;
use crate::pregel;
use crate::topology::{EdgeDirection, VertexId, VertexMap};

/// Compute the connected component for each vertex using Pregel.
///
/// Each vertex is labeled with the minimum vertex ID in its (weakly) connected
/// component — identical to GraphX's `ConnectedComponents.run`.
///
/// # Parameters
/// * `graph`         — input graph.
/// * `max_iterations`— maximum number of Pregel supersteps (use `usize::MAX`
///   for full convergence).
///
/// Returns a `VertexMap<VertexId>` mapping each vertex to the representative
/// (lowest ID) of its component.
pub fn run<VD: Clone, ED: Clone>(
    graph: &Graph<VD, ED>,
    max_iterations: usize,
) -> VertexMap<VertexId> {
    // Vertex attr = own ID initially; Pregel propagates the minimum.
    let g: Graph<VertexId, ED> = graph.map_vertices(|vid, _| vid);

    let result = pregel::run(
        &g,
        VertexId::MAX,
        max_iterations,
        EdgeDirection::Either,
        // vprog: keep the smaller of current label and incoming message.
        |_vid, vd: &VertexId, msg: VertexId| (*vd).min(msg),
        // send_msg: send src/dst label across the edge if it is smaller than
        // the other endpoint's current label.
        |mut ctx| {
            let src_label = ctx.triplet.src_attr;
            let dst_label = ctx.triplet.dst_attr;
            if src_label < dst_label {
                ctx.send_to_dst(src_label);
            } else if dst_label < src_label {
                ctx.send_to_src(dst_label);
            }
        },
        // merge_msg: keep the minimum.
        |a, b| a.min(b),
    );

    result
        .vertices()
        .map(|(vid, label)| (vid, *label))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::topology::Edge;

    #[test]
    fn two_disconnected_components() {
        // Component A: 0 — 1 — 2
        // Component B: 10 — 11
        let g: Graph<(), ()> = Graph::from_edges(
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
                Edge {
                    src: 10,
                    dst: 11,
                    attr: (),
                },
            ],
            (),
        );

        let labels = run(&g, 10);

        // All vertices in component A should carry label 0.
        assert_eq!(labels[&0], 0);
        assert_eq!(labels[&1], 0);
        assert_eq!(labels[&2], 0);

        // All vertices in component B should carry label 10.
        assert_eq!(labels[&10], 10);
        assert_eq!(labels[&11], 10);

        // The two components must have different labels.
        assert_ne!(labels[&0], labels[&10]);
    }

    #[test]
    fn single_component_cycle() {
        let g: Graph<(), ()> = Graph::from_edges(
            vec![
                Edge {
                    src: 3,
                    dst: 4,
                    attr: (),
                },
                Edge {
                    src: 4,
                    dst: 5,
                    attr: (),
                },
                Edge {
                    src: 5,
                    dst: 3,
                    attr: (),
                },
            ],
            (),
        );
        let labels = run(&g, 10);
        assert_eq!(labels[&3], 3);
        assert_eq!(labels[&4], 3);
        assert_eq!(labels[&5], 3);
    }

    #[test]
    fn single_vertex_is_its_own_component() {
        let g: Graph<(), ()> = Graph::from_vertices_edges(vec![(7, ())], vec![]);
        let labels = run(&g, 10);
        assert_eq!(labels[&7], 7);
    }

    #[test]
    fn isolated_vertices_each_in_own_component() {
        let g: Graph<(), ()> =
            Graph::from_vertices_edges(vec![(10, ()), (20, ()), (30, ())], vec![]);
        let labels = run(&g, 10);
        assert_eq!(labels[&10], 10);
        assert_eq!(labels[&20], 20);
        assert_eq!(labels[&30], 30);
    }
}
