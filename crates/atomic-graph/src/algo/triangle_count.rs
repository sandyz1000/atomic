//! Triangle counting via neighbor-set intersection on the engine.
//!
//! Undirected neighbor sets are built per vertex and carried as the vertex
//! attribute. A per-edge task then counts the common neighbors of the two
//! endpoints; summing per vertex and halving (each triangle is seen from both
//! endpoints of its shared edge) gives the per-vertex triangle count.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use atomic_compute::task;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::topology::{Edge, EdgeTriplet, VertexId, VertexMap};

/// Sorted neighbor id list carried as a vertex attribute.
type Neighbors = Vec<VertexId>;

atomic_compute::register_shuffle_map!(i64, Neighbors);
atomic_compute::register_shuffle_map!(i64, (i64, (), Neighbors));
atomic_compute::register_shuffle_map!(i64, usize);

/// Count common neighbors of the two endpoints (sorted-list intersection) and
/// credit both endpoints.
#[task]
fn tc_send(t: EdgeTriplet<Neighbors, ()>) -> Vec<(VertexId, usize)> {
    let (mut i, mut j) = (0usize, 0usize);
    let (a, b) = (&t.src_attr, &t.dst_attr);
    let mut count = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                count += 1;
                i += 1;
                j += 1;
            }
        }
    }
    vec![(t.src_id, count), (t.dst_id, count)]
}

/// Sum two triangle counts.
#[task]
fn tc_merge(a: usize, b: usize) -> usize {
    a + b
}

/// Count the triangles each vertex participates in.
///
/// Returns a [`VertexMap`] from vertex id to triangle count.
pub fn run<VD, ED>(graph: &Graph<VD, ED>) -> VertexMap<usize>
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
{
    let raw_edges = graph.collect_edges();

    // Undirected neighbor sets.
    let mut nbr: BTreeMap<VertexId, BTreeSet<VertexId>> = BTreeMap::new();
    for (vid, _) in graph.collect_vertices() {
        nbr.entry(vid).or_default();
    }
    for e in &raw_edges {
        if e.src != e.dst {
            nbr.entry(e.src).or_default().insert(e.dst);
            nbr.entry(e.dst).or_default().insert(e.src);
        }
    }

    let verts: Vec<(VertexId, Neighbors)> = nbr
        .iter()
        .map(|(vid, set)| (*vid, set.iter().copied().collect()))
        .collect();
    let edges: Vec<Edge<()>> = raw_edges
        .iter()
        .filter(|e| e.src != e.dst)
        .map(|e| Edge {
            src: e.src,
            dst: e.dst,
            attr: (),
        })
        .collect();

    let g: Graph<Neighbors, ()> =
        Graph::from_vertices_edges(graph.context().clone(), verts.clone(), edges);
    let raw: HashMap<VertexId, usize> = g
        .aggregate_messages::<usize, _, _>(TcSend, TcMerge)
        .collect()
        .unwrap_or_default()
        .into_iter()
        .collect();

    // Each triangle is counted twice per vertex (once per direction of the shared edge).
    verts
        .into_iter()
        .map(|(vid, _)| (vid, raw.get(&vid).copied().unwrap_or(0) / 2))
        .collect()
}

/// Count the total number of triangles in the graph (each triangle counted once).
pub fn total<VD, ED>(graph: &Graph<VD, ED>) -> usize
where
    VD: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED: GraphData,
    ED::Archived: GraphDecode<ED>,
{
    run(graph).values().sum::<usize>() / 3
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Edge;
    use atomic_compute::context::Context;

    fn edge(src: VertexId, dst: VertexId) -> Edge<()> {
        Edge { src, dst, attr: () }
    }

    fn k4(ctx: std::sync::Arc<Context>) -> Graph<(), ()> {
        Graph::from_edges(
            ctx,
            vec![
                edge(0, 1),
                edge(0, 2),
                edge(0, 3),
                edge(1, 2),
                edge(1, 3),
                edge(2, 3),
            ],
            (),
        )
    }

    #[test]
    fn k4_total_and_per_vertex() {
        let ctx = Context::local().unwrap();
        let g = k4(ctx);
        assert_eq!(total(&g), 4, "K4 has 4 triangles");
        for (_, c) in run(&g) {
            assert_eq!(c, 3, "each K4 vertex is in 3 triangles");
        }
    }

    #[test]
    fn single_triangle() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 2), edge(2, 0)], ());
        assert_eq!(total(&g), 1);
    }

    #[test]
    fn path_has_no_triangle() {
        let ctx = Context::local().unwrap();
        let g: Graph<(), ()> = Graph::from_edges(ctx, vec![edge(0, 1), edge(1, 2)], ());
        assert_eq!(total(&g), 0);
    }
}
