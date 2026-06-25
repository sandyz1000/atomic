//! Community detection by label propagation, on distributed Pregel.
//!
//! Every vertex starts labeled with its own id. Each superstep, vertices broadcast
//! their label to neighbors and adopt the most frequent label received (ties broken
//! by the smallest label). Message frequencies travel as `Vec<(label, count)>`.

use std::collections::BTreeMap;

use atomic_compute::task;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::pregel;
use crate::topology::{EdgeTriplet, VertexId, VertexMap};

/// Label-frequency message: `(label, count)` entries.
type LabelFreq = Vec<(VertexId, usize)>;

atomic_compute::register_shuffle_map!(i64, LabelFreq);

/// Combine two label-frequency maps by summing counts per label.
#[task]
fn lp_merge(a: LabelFreq, b: LabelFreq) -> LabelFreq {
    let mut m: BTreeMap<VertexId, usize> = BTreeMap::new();
    for (l, c) in a.into_iter().chain(b) {
        *m.entry(l).or_insert(0) += c;
    }
    m.into_iter().collect()
}

/// Send each endpoint's label to the other end of the edge (undirected).
#[task]
fn lp_send(t: EdgeTriplet<VertexId, ()>) -> Vec<(VertexId, LabelFreq)> {
    vec![
        (t.dst_id, vec![(t.src_attr, 1usize)]),
        (t.src_id, vec![(t.dst_attr, 1usize)]),
    ]
}

/// Adopt the most frequent received label; ties broken by the smallest label.
#[task]
fn lp_vprog(input: (VertexId, (VertexId, Option<LabelFreq>))) -> (VertexId, VertexId) {
    let (vid, (cur, msg)) = input;
    let Some(freq) = msg else {
        return (vid, cur);
    };
    let mut counts: BTreeMap<VertexId, usize> = BTreeMap::new();
    for (label, c) in freq {
        *counts.entry(label).or_insert(0) += c;
    }
    // BTreeMap iterates labels ascending, so the first max-count wins the tie.
    let mut best_label = cur;
    let mut best_count = 0usize;
    for (label, c) in counts {
        if c > best_count {
            best_count = c;
            best_label = label;
        }
    }
    (vid, best_label)
}

/// Detect communities via label propagation for `max_steps` supersteps.
///
/// Returns a [`VertexMap`] from vertex id to its community label. The algorithm is
/// static (no convergence test), matching the conventional formulation.
pub fn run<VD, ED>(graph: &Graph<VD, ED>, max_steps: usize) -> VertexMap<VertexId>
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
{
    let g = graph.map_vertices(|vid, _| vid).map_edges(|_| ());
    let result =
        pregel::run::<VertexId, (), LabelFreq, _, _, _>(&g, max_steps, LpSend, LpMerge, LpVprog);
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
    fn each_vertex_gets_a_valid_label() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 2), edge(2, 0)], ());
        let n = g.num_vertices() as usize;
        let labels = run(&g, 10);
        assert_eq!(labels.len(), n);
        let ids: std::collections::HashSet<VertexId> =
            g.collect_vertices().into_iter().map(|(v, _)| v).collect();
        for &l in labels.values() {
            assert!(ids.contains(&l), "label {l} is not an original vertex id");
        }
    }

    #[test]
    fn isolated_vertex_keeps_own_label() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> =
            Graph::from_vertices_edges(ctx, vec![(0, ()), (1, ()), (99, ())], vec![edge(0, 1)]);
        let labels = run(&g, 5);
        assert_eq!(labels[&99], 99);
    }
}
