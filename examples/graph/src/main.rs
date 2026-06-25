//! Graph processing example — `atomic-graph` on the Atomic engine.
//!
//! Build a directed graph, then run three built-in algorithms over it — PageRank,
//! Connected Components, and Triangle Count. Each runs as RDD jobs on the compute
//! engine via the Pregel / aggregate-messages primitives.
//!
//! The graph (vertex ids are `i64`):
//! ```text
//!   1 → 2 → 3 → 1      (a 3-cycle: 1,2,3 all reachable from each other)
//!   3 → 4              (4 hangs off the cycle)
//!   5 → 6              (a separate component)
//! ```
//!
//! # Running locally
//!
//! ```bash
//! cargo run -p graph
//! ```
use std::collections::HashMap;

use atomic_compute::context::Context;
use atomic_graph::algo::{connected_component, page_rank, triangle_count};
use atomic_graph::graph::Graph;
use atomic_graph::topology::{Edge, VertexId};

/// Edge with no attribute.
fn edge(src: VertexId, dst: VertexId) -> Edge<()> {
    Edge { src, dst, attr: () }
}

/// Print a `VertexId -> value` map sorted by vertex id.
fn print_by_vertex<V: std::fmt::Display>(map: &HashMap<VertexId, V>) {
    let mut rows: Vec<_> = map.iter().collect();
    rows.sort_by_key(|(vid, _)| **vid);
    for (vid, value) in rows {
        println!("  vertex {vid:>2}: {value}");
    }
}

fn main() {
    let ctx = Context::local().expect("failed to build local context");

    // Build a directed graph from an edge list; vertex attribute is `()`.
    let edges = vec![edge(1, 2), edge(2, 3), edge(3, 1), edge(3, 4), edge(5, 6)];
    let g: Graph<(), ()> = Graph::from_edges(ctx, edges, ());

    println!(
        "Graph: {} vertices, {} edges\n",
        g.num_vertices(),
        g.num_edges()
    );

    // --- PageRank: 20 iterations, reset probability 0.15 ---
    println!("=== PageRank (20 iters, reset=0.15) ===");
    let ranks = page_rank::run(&g, 20, 0.15);
    let mut ranked: Vec<_> = ranks.iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap().then(a.0.cmp(b.0)));
    for (vid, rank) in ranked {
        println!("  vertex {vid:>2}: {rank:.4}");
    }

    // --- Connected Components: vertices in the same component share a label ---
    println!("\n=== Connected Components (component id = min vertex id) ===");
    let components = connected_component::run(&g, 20);
    print_by_vertex(&components);

    // --- Triangle Count: the 1→2→3→1 cycle forms one triangle ---
    println!("\n=== Triangle Count (per vertex) ===");
    let triangles = triangle_count::run(&g);
    print_by_vertex(&triangles);
}
