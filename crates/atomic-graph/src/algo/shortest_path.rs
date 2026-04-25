use std::collections::HashMap;

use crate::graph::Graph;
use crate::pregel;
use crate::types::{EdgeDirection, VertexId, VertexMap};

/// Distance map: landmark vertex ID → distance from this vertex.
type SpMap = HashMap<VertexId, f64>;

/// Compute shortest-path distances from every vertex to each landmark.
///
/// Matches GraphX's `ShortestPaths.run` API: returns a per-vertex map of
/// distances to each specified landmark.  Edge weights are taken from the
/// edge attribute `ED = f64`; use `1.0` for unweighted graphs.
///
/// # Parameters
/// * `graph`     — directed graph with `f64` edge attributes as distances.
/// * `landmarks` — slice of destination vertex IDs.
///
/// Returns `VertexMap<SpMap>` — for each vertex, a map from landmark ID to
/// the shortest distance.  Unreachable landmarks are absent from the inner map.
pub fn run(graph: &Graph<(), f64>, landmarks: &[VertexId]) -> VertexMap<SpMap> {
    if landmarks.is_empty() || graph.num_vertices() == 0 {
        return graph.vertices().map(|(vid, _)| (vid, HashMap::new())).collect();
    }

    // Vertex attr = SpMap (distances to landmarks known so far).
    // Landmark vertices start knowing distance 0 to themselves.
    let sp_graph: Graph<SpMap, f64> = graph.map_vertices(|vid, _| {
        if landmarks.contains(&vid) {
            let mut m = HashMap::new();
            m.insert(vid, 0.0);
            m
        } else {
            HashMap::new()
        }
    });

    let result = pregel::run(
        &sp_graph,
        // initial_msg: empty map — no new information yet
        HashMap::new(),
        graph.num_vertices() + 1,
        EdgeDirection::In,
        // vprog: merge incoming distance map with current map (keep min per landmark)
        |_vid, vd: &SpMap, msg: SpMap| add_maps(vd, &msg),
        // send_msg: send (src distances incremented by edge weight) to src vertex
        // (we propagate distances backward: dst knows distances, src learns from dst)
        |mut ctx| {
            let new_dists = increment_map(&ctx.triplet.dst_attr, ctx.triplet.attr);
            // Only send if src would learn something new.
            if add_maps(&ctx.triplet.src_attr, &new_dists) != ctx.triplet.src_attr {
                ctx.send_to_src(new_dists);
            }
        },
        // merge_msg: combine two incoming distance maps (keep min per landmark)
        |a, b| add_maps(&a, &b),
    );

    result.vertices().map(|(vid, sp)| (vid, sp.clone())).collect()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn increment_map(m: &SpMap, weight: f64) -> SpMap {
    m.iter().map(|(&k, &d)| (k, d + weight)).collect()
}

fn add_maps(m1: &SpMap, m2: &SpMap) -> SpMap {
    let mut result = m1.clone();
    for (&k, &d) in m2 {
        result.entry(k).and_modify(|existing| {
            if d < *existing {
                *existing = d;
            }
        }).or_insert(d);
    }
    result
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::types::Edge;

    fn weighted_triangle() -> Graph<(), f64> {
        // 0 →(1.0)→ 1 →(2.0)→ 2 →(3.0)→ 0
        Graph::from_edges(
            vec![
                Edge { src: 0, dst: 1, attr: 1.0 },
                Edge { src: 1, dst: 2, attr: 2.0 },
                Edge { src: 2, dst: 0, attr: 3.0 },
            ],
            (),
        )
    }

    #[test]
    fn landmark_distance_to_self_is_zero() {
        let g = weighted_triangle();
        let result = run(&g, &[0]);
        assert_eq!(result[&0][&0], 0.0, "landmark distance to self must be 0");
    }

    #[test]
    fn distances_are_correct() {
        // Landmark = vertex 2.
        // 0 → 1 → 2: distance = 1.0 + 2.0 = 3.0
        // 1 → 2:     distance = 2.0
        // 2 → 2:     distance = 0.0
        let g = weighted_triangle();
        let result = run(&g, &[2]);
        assert_eq!(result[&2][&2], 0.0);
        assert!((result[&1][&2] - 2.0).abs() < 1e-9);
        assert!((result[&0][&2] - 3.0).abs() < 1e-9);
    }

    #[test]
    fn multiple_landmarks() {
        let g = weighted_triangle();
        let result = run(&g, &[0, 2]);
        // vertex 1 should have distances to both landmarks
        assert!(result[&1].contains_key(&0) || result[&1].contains_key(&2));
    }
}
