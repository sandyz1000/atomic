extern crate petgraph;

use petgraph::{Graph, EdgeDirection};
use petgraph::prelude::*;
use petgraph::algo::dijkstra;
use std::collections::HashSet;

fn run<VD: Clone, ED>(graph: Graph<VD, ED, Undirected>) -> Graph<usize, ED, Undirected> {
    // Transform the edge data something cheap to shuffle and then canonicalize
    let canonical_graph = graph.map(|_, _, _| true)
        .filter_map(|_, edge| edge)
        .convert_to();
    
    // Get the triangle counts
    let counters = run_pre_canonicalized(&canonical_graph);
    
    // Join them both with the original graph
    graph.map(|node, _, _| counters[node.index()])
}

fn run_pre_canonicalized<VD: Clone, ED>(graph: &Graph<VD, ED, Undirected>) -> Vec<usize> {
    // Construct set representations of the neighborhoods
    let nbr_sets: Vec<HashSet<NodeIndex>> = graph.node_indices()
        .map(|vid| {
            let nbrs: HashSet<_> = graph.neighbors(vid).filter(|&nbr| nbr != vid).collect();
            nbrs
        })
        .collect();

    // Join the sets with the graph
    let set_graph: Graph<HashSet<NodeIndex>, ED, _> = graph.map(|node, _, _| nbr_sets[node.index()].clone());

    // Edge function computes intersection of smaller vertex with larger vertex
    fn edge_func(e: Edge<NodeIndex>, ctx: &mut EdgeContext<HashSet<NodeIndex>, ED, usize>) {
        let (small_set, large_set) = if ctx.source().index() < ctx.target().index() {
            (&ctx.source()[..], &ctx.target()[..])
        } else {
            (&ctx.target()[..], &ctx.source()[..])
        };

        let counter = small_set.iter()
            .filter(|&&vid| vid != ctx.source() && vid != ctx.target() && large_set.contains(&vid))
            .count();

        ctx.send_to_source(counter);
        ctx.send_to_target(counter);
    }

    // Compute the intersection along edges
    let counters = petgraph::algo::map_reduce(graph, edge_func, |x, y| x + y);

    // Merge counters with the graph and divide by two since each triangle is counted twice
    counters.into_iter().map(|(_, counter)| {
        // This algorithm double counts each triangle, so the final count should be even
        assert!(counter % 2 == 0, "Triangle count resulted in an invalid number of triangles.");
        counter / 2
    }).collect()
}


#[test]
fn test_triangle_count() {
    // Example usage
    let mut graph = Graph::<&str, &str, Undirected>::new_undirected();
    let node_a = graph.add_node("A");
    let node_b = graph.add_node("B");
    let node_c = graph.add_node("C");

    graph.add_edge(node_a, node_b, "AB");
    graph.add_edge(node_b, node_c, "BC");
    graph.add_edge(node_c, node_a, "CA");

    let result = run(graph);
    println!("{:?}", result);
}
