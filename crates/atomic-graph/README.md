# atomic-graph

GraphX-style graph processing for the Atomic distributed compute framework.

`atomic-graph` provides an in-process directed graph type (`Graph<VD, ED>`), a
Pregel bulk-synchronous message-passing engine, and six ready-to-use graph
algorithms. All computation runs on the driver in a single process — there is no
distributed graph partitioning at this time.

> **Note:** `atomic-graph` is a **Rust-only** API. It is not exposed through the
> Python (`atomic-compute`) or TypeScript (`@atomic-compute/js`) bindings.

---

## Quick start

```rust
use atomic_graph::{
    graph::Graph,
    pregel,
    types::{Edge, EdgeDirection},
    algo::page_rank,
};

fn main() {
    let vertices = vec![(1, ()), (2, ()), (3, ()), (4, ())];
    let edges = vec![
        Edge { src: 1, dst: 2, attr: () },
        Edge { src: 2, dst: 3, attr: () },
        Edge { src: 3, dst: 1, attr: () },
        Edge { src: 4, dst: 1, attr: () },
    ];

    let graph = Graph::from_vertices_edges(vertices, edges);

    // PageRank — 20 iterations, reset prob 0.15
    let ranks = page_rank::run(&graph, 20, 0.15);
    for (vid, rank) in &ranks {
        println!("vertex {vid}: {rank:.4}");
    }
}
```

---

## `Graph<VD, ED>`

An in-process directed graph backed by `petgraph::StableGraph`.

- `VD` — vertex data type (arbitrary; must implement `Clone`)
- `ED` — edge data type (arbitrary; must implement `Clone`)

### Construction

```rust
// From explicit vertex and edge lists
let g = Graph::from_vertices_edges(vertices, edges);

// From edges only (missing vertices get `default_vd`)
let g = Graph::from_edges(edges, ());

// From an edge-list file (vertex data = (), edge data = ())
let g = Graph::from_edge_list_file("data/graph.txt")?;
```

### Introspection

| Method | Returns | Description |
| --- | --- | --- |
| `num_vertices()` | `usize` | Total vertex count |
| `num_edges()` | `usize` | Total edge count |
| `vertices()` | `impl Iterator<(VertexId, &VD)>` | Iterate over all vertices |
| `edges()` | `impl Iterator<Edge<ED>>` | Iterate over all edges |
| `triplets()` | `impl Iterator<EdgeTriplet<VD, ED>>` | Iterate over (src, edge, dst) triplets |
| `in_degrees()` | `VertexMap<usize>` | In-degree per vertex |
| `out_degrees()` | `VertexMap<usize>` | Out-degree per vertex |

### Transformations

All transformations return a **new** `Graph` — the original is not mutated.

| Method | Description |
| --- | --- |
| `map_vertices(f)` | Apply `f(vid, vd) -> VD2` to every vertex |
| `map_edges(f)` | Apply `f(&edge) -> ED2` to every edge |
| `map_triplets(f)` | Apply `f(&triplet) -> ED2` using src/dst vertex data |
| `subgraph(vpred, epred)` | Keep only vertices / edges satisfying the predicates |
| `reverse()` | Reverse all edge directions |
| `aggregate_messages(send, merge, default)` | Collect per-vertex aggregated messages |

---

## Pregel engine

```rust
use atomic_graph::{pregel, types::EdgeDirection};

let result = pregel::run(
    &graph,
    initial_msg,        // sent to every vertex before superstep 0
    max_iterations,     // hard cap on superstep count
    EdgeDirection::Either,
    |vid, vd, msg| { /* vertex program: return new vertex data */ },
    |ctx| { /* send_msg: call ctx.send_to_src() / ctx.send_to_dst() */ },
    |a, b| { /* merge_msg: commutative associative combiner */ },
);
```

Terminates early when no messages are generated in a superstep.

---

## Built-in algorithms

All algorithms are standalone functions in `atomic_graph::algo::*`.

| Algorithm | Module | Input graph type | Output |
| --- | --- | --- | --- |
| PageRank | `algo::page_rank` | `Graph<VD, ED>` | `VertexMap<f64>` |
| Shortest path (SSSP) | `algo::shortest_path` | `Graph<(), f64>` (edge = weight) | `VertexMap<SpMap>` |
| Strongly connected components | `algo::strongly_connected_component` | `Graph<VD, ED>` | `VertexMap<VertexId>` (component label) |
| Label propagation (community) | `algo::label_propagation` | `Graph<VD, ED>` | `VertexMap<VertexId>` (community label) |
| Triangle count | `algo::triangle_count` | `Graph<VD, ED>` | `VertexMap<usize>` |
| Connected components | `algo::connected_component` | `Graph<VD, ED>` | `VertexMap<VertexId>` (component label) |

### Examples

```rust
use atomic_graph::algo::{page_rank, shortest_path, connected_component};

// PageRank
let ranks = page_rank::run(&graph, 20, 0.15);

// Single-source shortest path (Dijkstra via Pregel)
// Edge attr must be f64 weights; pass landmark vertex IDs
let distances = shortest_path::run(&weighted_graph, &[source_vid]);

// Connected components (union-find via Pregel)
let components = connected_component::run(&graph);
let num_components = components.values().collect::<std::collections::HashSet<_>>().len();
```

---

## Adding to your workspace

```toml
[dependencies]
atomic-graph = { path = "../atomic-graph" }
```

---

## Running tests

```bash
cargo test -p atomic-graph
```
