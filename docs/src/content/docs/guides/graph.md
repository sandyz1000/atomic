---
title: Graph Processing
description: Build graphs, run Pregel programs, and call built-in algorithms.
---

`atomic-graph` processes directed graphs with typed vertex and edge data. A
`Graph<VD, ED>` pairs a vertex RDD `(VertexId, VD)` with an edge RDD `Edge<ED>`,
held against an Atomic [`Context`](/guides/configuration/). Every algorithm runs
as RDD jobs on the compute engine — in local mode on the thread pool, in
distributed mode dispatched to workers. The Pregel engine and
`aggregate_messages` are the primitives the built-in algorithms build on.

## Building a graph

```rust
use atomic_compute::context::Context;
use atomic_graph::graph::Graph;
use atomic_graph::topology::Edge;

let ctx = Context::local()?;
let g: Graph<i64, ()> = Graph::from_edges(
    ctx,
    vec![
        Edge { src: 0, dst: 1, attr: () },
        Edge { src: 1, dst: 2, attr: () },
    ],
    0,                // default vertex attribute
).map_vertices(|vid, _| vid);
```

`from_vertices_edges` builds from explicit vertex and edge lists; `from_edges`
creates missing vertices with a default attribute; `from_rdds` wraps existing
vertex and edge RDDs.

## Built-in algorithms

All algorithms run on the engine. PageRank, connected components, label
propagation, and shortest path are Pregel programs; triangle count uses
neighbor-set joins; strongly connected components uses distributed color
propagation (driver-coordinated rounds, two Pregel passes each).

| Algorithm | Module | Method |
|---|---|---|
| PageRank | `algo::page_rank` | Fixed-iteration, via `aggregate_messages` |
| Connected components | `algo::connected_component` | Weakly connected, min-label Pregel |
| Strongly connected components | `algo::strongly_connected_component` | Distributed color propagation |
| Label propagation | `algo::label_propagation` | Community detection (Pregel) |
| Triangle count | `algo::triangle_count` | Per-vertex and total (neighbor-set joins) |
| Shortest path | `algo::shortest_path` | Multi-source to landmarks (Pregel) |

```rust
use atomic_graph::algo::page_rank;

let ranks = page_rank::run(&g, 20, 0.15);   // 20 iterations, 0.15 reset probability
```

## Pregel

`pregel::run` runs supersteps until no messages remain or a maximum is reached.
Each superstep sends messages along edges, merges messages destined for the same
vertex (a shuffle), and applies a vertex program.

Because distributed work dispatches by compile-time identifier, the message
(`send`) and vertex (`vprog`) functions are registered `#[task]`s, not closures.
The combine (`merge`) is an ordinary closure run on the reduce side. A `#[task]`
takes no captured state, so per-run parameters are baked into the vertex or edge
data on the driver before the loop (see how `page_rank` precomputes its constants
into a `PrVertex`).

```rust
use atomic_compute::task;
use atomic_graph::pregel;
use atomic_graph::topology::{EdgeTriplet, VertexId};

// Propagate the smaller endpoint label across each edge.
#[task]
fn min_send(t: EdgeTriplet<VertexId, ()>) -> Vec<(VertexId, VertexId)> {
    if t.src_attr < t.dst_attr {
        vec![(t.dst_id, t.src_attr)]
    } else {
        Vec::new()
    }
}

#[task]
fn min_vprog(input: (VertexId, (VertexId, Option<VertexId>))) -> (VertexId, VertexId) {
    let (vid, (label, msg)) = input;
    (vid, msg.map_or(label, |m| label.min(m)))
}

let result = pregel::run::<VertexId, (), VertexId, _, _, _>(
    &g,
    10,                          // max supersteps
    MinSend,                     // message task (generated from `min_send`)
    |a, b| a.min(b),             // merge (closure)
    MinVprog,                    // vertex task (generated from `min_vprog`)
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
