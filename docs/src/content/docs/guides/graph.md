---
title: Graph Processing
description: Build graphs, run Pregel programs, and call built-in algorithms.
---

`atomic-graph` processes directed graphs with typed vertex and edge data. A
`Graph<VD, ED>` pairs a vertex set with an edge set. The Pregel engine runs
vertex-centric message-passing programs, and several common algorithms are
built on top of it.

## Building a graph

```rust
use atomic_graph::graph::Graph;
use atomic_graph::topology::Edge;

let g: Graph<i64, ()> = Graph::from_edges(
    vec![
        Edge { src: 0, dst: 1, attr: () },
        Edge { src: 1, dst: 2, attr: () },
    ],
    0,                // default vertex attribute
).map_vertices(|vid, _| vid);
```

`from_vertices_edges` builds from explicit vertex and edge lists;
`from_edges` creates missing vertices with a default attribute.

## Built-in algorithms

| Algorithm | Module | Method |
|---|---|---|
| PageRank | `algo::page_rank` | Fixed-iteration and convergence-based |
| Connected components | `algo::connected_component` | Weakly connected, min-label via Pregel |
| Strongly connected components | `algo::strongly_connected_component` | Tarjan's algorithm |
| Label propagation | `algo::label_propagation` | Community detection |
| Triangle count | `algo::triangle_count` | Per-vertex and total |
| Shortest path | `algo::shortest_path` | Single-source via Pregel |

```rust
use atomic_graph::algo::page_rank;

let ranks = page_rank::run(&g, 20, 0.15);   // 20 iterations, 0.15 reset probability
```

## Pregel

`pregel::run` runs supersteps until no messages remain or a maximum is reached.
Each superstep applies a vertex program to vertices that received messages,
sends messages along edges, and merges messages destined for the same vertex.

```rust
use atomic_graph::pregel;
use atomic_graph::topology::EdgeDirection;

let result = pregel::run(
    &g,
    i64::MAX,                                  // initial message
    10,                                        // max supersteps
    EdgeDirection::Either,
    |_vid, vd, msg| (*vd).min(msg),            // vertex program
    |mut ctx| {                                // send messages
        if ctx.triplet.src_attr < ctx.triplet.dst_attr {
            ctx.send_to_dst(ctx.triplet.src_attr);
        }
    },
    |a, b| a.min(b),                           // merge messages
);
```

## Python and TypeScript

```python
import atomic_compute

g = atomic_compute.Graph(
    vertices=[(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)],
    edges=[(1, 2, 1.0), (2, 3, 1.0), (3, 4, 1.0), (4, 1, 1.0)],
)

print(g.page_rank())              # rank per vertex
print(g.connected_components())   # component representative per vertex
print(g.shortest_path([1]))       # distances from source vertex 1
```

The same API is available in TypeScript as `new Graph(vertices, edges)` with
camelCase methods. Custom Pregel programs are exposed through `run_pregel`
(Python) and `runPregelF64` (TypeScript), with `f64` messages.
