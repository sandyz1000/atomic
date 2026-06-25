//! Weakly connected components via distributed Pregel.

use atomic_compute::task;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::pregel;
use crate::topology::{EdgeTriplet, VertexId, VertexMap};

/// Propagate the smaller endpoint label across each edge (both directions).
#[task]
fn cc_send(t: EdgeTriplet<VertexId, ()>) -> Vec<(VertexId, VertexId)> {
    if t.src_attr < t.dst_attr {
        vec![(t.dst_id, t.src_attr)]
    } else if t.dst_attr < t.src_attr {
        vec![(t.src_id, t.dst_attr)]
    } else {
        Vec::new()
    }
}

/// Adopt the minimum of the current label and the incoming message.
#[task]
fn cc_vprog(input: (VertexId, (VertexId, Option<VertexId>))) -> (VertexId, VertexId) {
    let (vid, (label, msg)) = input;
    let new_label = match msg {
        Some(m) => label.min(m),
        None => label,
    };
    (vid, new_label)
}

/// Combine two labels by keeping the smaller.
#[task]
fn cc_merge(a: VertexId, b: VertexId) -> VertexId {
    a.min(b)
}

/// Compute the weakly connected component of each vertex.
///
/// Every vertex is labeled with the minimum vertex id reachable from it when edges
/// are treated as undirected. Runs up to `max_iterations` Pregel supersteps.
///
/// Returns a [`VertexMap`] mapping each vertex to the representative (lowest id) of
/// its component.
pub fn run<VD, ED>(graph: &Graph<VD, ED>, max_iterations: usize) -> VertexMap<VertexId>
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
{
    // Normalize to a graph whose vertex attribute is its own id and whose edges
    // carry no attribute, so the message task operates on concrete types.
    let g = graph.map_vertices(|vid, _| vid).map_edges(|_| ());
    let result = pregel::run::<VertexId, (), VertexId, _, _, _>(
        &g,
        max_iterations,
        CcSend,
        CcMerge,
        CcVprog,
    );
    result.collect_vertices().into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Edge;
    use atomic_compute::context::Context;

    fn edge(src: VertexId, dst: VertexId) -> Edge<()> {
        Edge { src, dst, attr: () }
    }

    #[test]
    fn two_disconnected_components() {
        let ctx = Context::local().unwrap();
        // Component A: 0-1-2, Component B: 10-11
        let g: Graph<(), ()> =
            Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 2), edge(10, 11)], ());
        let labels = run(&g, 10);
        assert_eq!(labels[&0], 0);
        assert_eq!(labels[&1], 0);
        assert_eq!(labels[&2], 0);
        assert_eq!(labels[&10], 10);
        assert_eq!(labels[&11], 10);
        assert_ne!(labels[&0], labels[&10]);
    }

    #[test]
    fn single_component_cycle() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(3, 4), edge(4, 5), edge(5, 3)], ());
        let labels = run(&g, 10);
        assert_eq!(labels[&3], 3);
        assert_eq!(labels[&4], 3);
        assert_eq!(labels[&5], 3);
    }

    #[test]
    fn isolated_vertices_each_in_own_component() {
        let ctx = Context::local().unwrap();
        let g: Graph<i64, ()> =
            Graph::from_vertices_edges(ctx, vec![(10, 0), (20, 0), (30, 0)], vec![]);
        let labels = run(&g, 10);
        assert_eq!(labels[&10], 10);
        assert_eq!(labels[&20], 20);
        assert_eq!(labels[&30], 30);
    }
}
