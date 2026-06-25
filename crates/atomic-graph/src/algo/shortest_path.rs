//! Multi-source shortest paths (to a set of landmarks) via distributed Pregel.
//!
//! Each vertex carries its current distance to every landmark as a `Vec<(landmark,
//! distance)>` (an association list rather than a `HashMap`, so the state encodes
//! cleanly on the wire). Distances start at `0` for landmark vertices; every
//! superstep relaxes edges and keeps the minimum.

use std::collections::{BTreeMap, HashMap};

use atomic_compute::task;

use crate::graph::Graph;
use crate::pregel;
use crate::topology::{EdgeTriplet, VertexId, VertexMap};

/// Per-vertex distance vector: `(landmark, distance)` entries.
type SpVec = Vec<(VertexId, f64)>;

// Shuffle handlers for shortest-path pair shapes (VD = SpVec, ED = f64).
atomic_compute::register_shuffle_map!(i64, SpVec);
atomic_compute::register_shuffle_map!(i64, (i64, f64, SpVec));

fn merge_into(map: &mut BTreeMap<VertexId, f64>, entries: SpVec) {
    for (lm, d) in entries {
        let slot = map.entry(lm).or_insert(f64::INFINITY);
        if d < *slot {
            *slot = d;
        }
    }
}

/// Combine two distance vectors, keeping the minimum per landmark.
#[task]
fn sp_merge(a: SpVec, b: SpVec) -> SpVec {
    let mut map: BTreeMap<VertexId, f64> = a.into_iter().collect();
    merge_into(&mut map, b);
    map.into_iter().collect()
}

/// Relax every edge: propose `src distance + weight` for each landmark when it
/// improves the destination's current distance.
#[task]
fn sp_send(t: EdgeTriplet<SpVec, f64>) -> Vec<(VertexId, SpVec)> {
    let dst: BTreeMap<VertexId, f64> = t.dst_attr.iter().copied().collect();
    let mut out: SpVec = Vec::new();
    for (lm, d) in &t.src_attr {
        let nd = d + t.attr;
        let better = dst.get(lm).map(|c| nd < *c).unwrap_or(true);
        if better {
            out.push((*lm, nd));
        }
    }
    if out.is_empty() {
        Vec::new()
    } else {
        vec![(t.dst_id, out)]
    }
}

/// Fold the incoming distances into the vertex's distance vector, keeping minimums.
#[task]
fn sp_vprog(input: (VertexId, (SpVec, Option<SpVec>))) -> (VertexId, SpVec) {
    let (vid, (cur, msg)) = input;
    let mut map: BTreeMap<VertexId, f64> = cur.into_iter().collect();
    if let Some(m) = msg {
        merge_into(&mut map, m);
    }
    (vid, map.into_iter().collect())
}

/// Compute the shortest distance from every vertex to each landmark.
///
/// Edge attributes are the edge weights. Returns a [`VertexMap`] from vertex id to
/// a `landmark -> distance` map; landmarks unreachable from a vertex are absent.
pub fn run(graph: &Graph<(), f64>, landmarks: &[VertexId]) -> VertexMap<HashMap<VertexId, f64>> {
    if landmarks.is_empty() {
        return HashMap::new();
    }
    let lm: Vec<VertexId> = landmarks.to_vec();
    // Initial state: landmark vertices start at distance 0 to themselves.
    let init = graph.map_vertices(move |vid, _| {
        if lm.contains(&vid) {
            vec![(vid, 0.0_f64)]
        } else {
            Vec::new()
        }
    });
    let max_iter = graph.num_vertices() as usize + 1;
    let result =
        pregel::run::<SpVec, f64, SpVec, _, _, _>(&init, max_iter, SpSend, SpMerge, SpVprog);
    result
        .collect_vertices()
        .into_iter()
        .map(|(vid, v)| (vid, v.into_iter().collect::<HashMap<VertexId, f64>>()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Edge;
    use atomic_compute::context::Context;

    fn wedge(src: VertexId, dst: VertexId, w: f64) -> Edge<f64> {
        Edge { src, dst, attr: w }
    }

    #[test]
    fn line_graph_distances() {
        let ctx = Context::local().unwrap();
        // 1 -> 2 -> 3 -> 4, unit weights.
        let g: Graph<(), f64> = Graph::from_edges(
            ctx,
            vec![wedge(1, 2, 1.0), wedge(2, 3, 1.0), wedge(3, 4, 1.0)],
            (),
        );
        let dist = run(&g, &[1]);
        assert_eq!(dist[&1][&1], 0.0);
        assert_eq!(dist[&2][&1], 1.0);
        assert_eq!(dist[&3][&1], 2.0);
        assert_eq!(dist[&4][&1], 3.0);
    }

    #[test]
    fn unreachable_landmark_absent() {
        let ctx = Context::local().unwrap();
        // 1 -> 2 ; 3 isolated-ish (no path from 1 to 3).
        let g: Graph<(), f64> = Graph::from_vertices_edges(
            ctx,
            vec![(1, ()), (2, ()), (3, ())],
            vec![wedge(1, 2, 1.0)],
        );
        let dist = run(&g, &[1]);
        assert_eq!(dist[&2][&1], 1.0);
        assert!(dist.get(&3).map(|m| m.get(&1).is_none()).unwrap_or(true));
    }
}
