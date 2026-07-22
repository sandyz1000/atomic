//! Neighbourhood operators — per-vertex neighbour ids and decorated neighbour
//! (id, attribute) lists.
//!
//! Both operators use the existing RDD primitives (`map_partitions_to_pair`,
//! `combine_by_key`); they inherit the same local/distributed execution path
//! as the rest of the graph layer.

use atomic_compute::rdd::TypedRdd;

use crate::graph::{Graph, GraphData, GraphDecode};
use crate::topology::{EdgeDirection, VertexId};

impl<VD, ED> Graph<VD, ED>
where
    VD: GraphData,
    ED: GraphData,
    VD::Archived: GraphDecode<VD>,
    ED::Archived: GraphDecode<ED>,
{
    /// For every vertex, collect the ids of its neighbours according to `direction`.
    ///
    /// Returns a pair RDD `(VertexId, Vec<VertexId>)` where each vertex maps to the
    /// (possibly empty) list of neighbour ids. Vertices with no neighbours in the
    /// chosen direction are absent from the result unless they appear as a source.
    ///
    /// `Either` and `Both` both collect neighbours across in- and out-edges (the union);
    /// unlike GraphX this method does not reject `Both`.
    pub fn collect_neighbor_ids(
        &self,
        direction: EdgeDirection,
    ) -> TypedRdd<(VertexId, Vec<VertexId>)> {
        let n = self.context().default_parallelism().max(1);
        self.edges
            .clone()
            .map_partitions_to_pair(move |_idx, iter| {
                Box::new(iter.flat_map(move |e| match direction {
                    EdgeDirection::Out => vec![(e.src, e.dst)],
                    EdgeDirection::In => vec![(e.dst, e.src)],
                    EdgeDirection::Either | EdgeDirection::Both => {
                        vec![(e.src, e.dst), (e.dst, e.src)]
                    }
                }))
            })
            .combine_by_key(
                |id| vec![id],
                |mut acc, id| {
                    acc.push(id);
                    acc
                },
                |mut a, mut b| {
                    a.append(&mut b);
                    a
                },
                n,
            )
    }

    /// For every vertex, collect its neighbouring vertices together with their
    /// attributes, according to `direction`.
    ///
    /// Returns a pair RDD `(VertexId, Vec<(VertexId, VD)>)` where each vertex
    /// maps to the list of `(neighbor_id, neighbor_attr)` pairs.
    pub fn collect_neighbors(
        &self,
        direction: EdgeDirection,
    ) -> TypedRdd<(VertexId, Vec<(VertexId, VD)>)>
    where
        ((VertexId, ()), VD): GraphData,
        (VertexId, ()): GraphData,
    {
        let neighbor_ids = self.collect_neighbor_ids(direction);

        // Flatten to (neighbor_id, (source_id, ())) so we can join with vertices
        // to get the neighbor's attribute, then re-group by source.
        let n = self.context().default_parallelism().max(1);
        let flattened = neighbor_ids.map_partitions(move |iter| {
            Box::new(iter.flat_map(|(src, ids)| ids.into_iter().map(move |nid| (nid, (src, ())))))
                as Box<dyn Iterator<Item = (VertexId, (VertexId, ()))>>
        });

        let joined = flattened.join::<VD>(self.vertices.clone());

        // Re-group: (neighbor_id, ((source_id, ()), neighbor_attr)) → by source
        let regrouped = joined.map_partitions_to_pair(move |_idx, iter| {
            Box::new(iter.map(|(nid, ((src, ()), nattr))| (src, (nid, nattr))))
        });

        regrouped.combine_by_key(
            |(nid, nattr)| vec![(nid, nattr)],
            |mut acc, (nid, nattr)| {
                acc.push((nid, nattr));
                acc
            },
            |mut a, mut b| {
                a.append(&mut b);
                a
            },
            n,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use atomic_compute::context::Context;

    use crate::graph::Graph;
    use crate::topology::{Edge, EdgeDirection, VertexId};

    fn edge(src: VertexId, dst: VertexId) -> Edge<()> {
        Edge { src, dst, attr: () }
    }

    // 0 → 1, 0 → 2, 1 → 2.
    fn sample_graph() -> Graph<i64, ()> {
        let ctx = Context::local().unwrap();
        Graph::from_vertices_edges(
            ctx,
            vec![(0, 10), (1, 11), (2, 12)],
            vec![edge(0, 1), edge(0, 2), edge(1, 2)],
        )
    }

    fn ids_map(pairs: Vec<(VertexId, Vec<VertexId>)>) -> HashMap<VertexId, Vec<VertexId>> {
        pairs
            .into_iter()
            .map(|(v, mut ns)| {
                ns.sort();
                (v, ns)
            })
            .collect()
    }

    #[test]
    fn test_out_neighbors() {
        let got = ids_map(
            sample_graph()
                .collect_neighbor_ids(EdgeDirection::Out)
                .collect()
                .unwrap(),
        );
        assert_eq!(got[&0], vec![1, 2]);
        assert_eq!(got[&1], vec![2]);
    }

    #[test]
    fn test_in_neighbors() {
        let got = ids_map(
            sample_graph()
                .collect_neighbor_ids(EdgeDirection::In)
                .collect()
                .unwrap(),
        );
        assert_eq!(got[&2], vec![0, 1]);
        assert_eq!(got[&1], vec![0]);
    }

    #[test]
    fn test_neighbors_attrs() {
        let pairs = sample_graph()
            .collect_neighbors(EdgeDirection::Out)
            .collect()
            .unwrap();
        let got: HashMap<VertexId, Vec<(VertexId, i64)>> = pairs
            .into_iter()
            .map(|(v, mut ns)| {
                ns.sort();
                (v, ns)
            })
            .collect();
        // Vertex 0's out-neighbours carry their attributes (1→11, 2→12).
        assert_eq!(got[&0], vec![(1, 11), (2, 12)]);
    }
}
