use atomic_graph::{
    algo::{
        connected_component, label_propagation, page_rank, shortest_path,
        strongly_connected_component, triangle_count,
    },
    graph::Graph,
    topology::Edge,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::collections::HashMap;

// VertexId in atomic-graph is i64.
type VertexId = i64;

/// JS `send_msg` callback for Pregel: `(srcId, dstId, edgeAttr, srcRank, dstRank)`
/// returns a list of `[targetId, message]` pairs.
type PregelSendFn<'a> = Function<'a, FnArgs<(f64, f64, f64, f64, f64)>, Vec<Vec<f64>>>;

#[napi(js_name = "Graph")]
pub struct JsGraph {
    inner: Graph<f64, f64>,
}

#[napi]
impl JsGraph {
    /// Create a graph from explicit vertex and edge lists.
    ///
    /// @param vertices - Array of [vertexId, weight] tuples
    /// @param edges    - Array of [srcId, dstId, weight] tuples
    #[napi(constructor)]
    pub fn new(
        vertices: Vec<Vec<serde_json::Value>>,
        edges: Vec<Vec<serde_json::Value>>,
    ) -> Result<Self> {
        let verts: Vec<(VertexId, f64)> = vertices
            .iter()
            .map(|v| {
                let id = v
                    .first()
                    .and_then(|x| x.as_i64())
                    .ok_or_else(|| napi::Error::from_reason("vertex id must be integer"))?;
                let w = v
                    .get(1)
                    .and_then(|x| x.as_f64())
                    .ok_or_else(|| napi::Error::from_reason("vertex weight must be float"))?;
                Ok((id, w))
            })
            .collect::<Result<_>>()?;

        let edge_list: Vec<Edge<f64>> = edges
            .iter()
            .map(|e| {
                let src = e
                    .first()
                    .and_then(|x| x.as_i64())
                    .ok_or_else(|| napi::Error::from_reason("edge src must be integer"))?;
                let dst = e
                    .get(1)
                    .and_then(|x| x.as_i64())
                    .ok_or_else(|| napi::Error::from_reason("edge dst must be integer"))?;
                let w = e
                    .get(2)
                    .and_then(|x| x.as_f64())
                    .ok_or_else(|| napi::Error::from_reason("edge weight must be float"))?;
                Ok(Edge { src, dst, attr: w })
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            inner: Graph::from_vertices_edges(verts, edge_list),
        })
    }

    /// Return the number of vertices.
    #[napi]
    pub fn num_vertices(&self) -> u32 {
        self.inner.num_vertices() as u32
    }

    /// Return the number of edges.
    #[napi]
    pub fn num_edges(&self) -> u32 {
        self.inner.num_edges() as u32
    }

    /// Run PageRank. Returns Record<string, number> (keys are vertex ID strings).
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn page_rank(&self, num_iter: u32, reset_prob: f64) -> serde_json::Value {
        let result = page_rank::run(&self.inner, num_iter as usize, reset_prob);
        i64map_f64_to_json(result)
    }

    /// Connected components (weakly). Returns Record<string, number>.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn connected_components(&self) -> serde_json::Value {
        let result = connected_component::run(&self.inner, usize::MAX);
        i64_to_i64_map_to_json(result)
    }

    /// Strongly connected components (Tarjan's). Returns Record<string, number>.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn strongly_connected_components(&self) -> serde_json::Value {
        let result = strongly_connected_component::run(&self.inner, 0);
        i64_to_i64_map_to_json(result)
    }

    /// Label propagation community detection. Returns Record<string, number>.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn label_propagation(&self, max_iter: u32) -> serde_json::Value {
        let result = label_propagation::run(&self.inner, max_iter as usize);
        i64_to_i64_map_to_json(result)
    }

    /// Triangle count per vertex. Returns Record<string, number>.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn triangle_count(&self) -> serde_json::Value {
        let result = triangle_count::run(&self.inner);
        // result is HashMap<VertexId, usize>
        let m: serde_json::Map<String, serde_json::Value> = result
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::Number(v.into())))
            .collect();
        serde_json::Value::Object(m)
    }

    /// Shortest path distances from landmarks.
    /// Returns Record<string, Record<string, number>>.
    #[napi(ts_return_type = "Record<string, Record<string, number>>")]
    pub fn shortest_path(&self, landmarks: Vec<i64>) -> Result<serde_json::Value> {
        // shortest_path::run requires Graph<(), f64>; build one from our f64/f64 graph.
        let verts: Vec<(VertexId, ())> = self.inner.vertices().map(|(id, _)| (id, ())).collect();
        let edges: Vec<Edge<f64>> = self
            .inner
            .edges()
            .map(|e| Edge {
                src: e.src,
                dst: e.dst,
                attr: e.attr,
            })
            .collect();
        let unit_graph: Graph<(), f64> = Graph::from_vertices_edges(verts, edges);

        let result = shortest_path::run(&unit_graph, &landmarks);

        let outer: serde_json::Map<String, serde_json::Value> = result
            .into_iter()
            .map(|(k, inner)| {
                let inner_map: serde_json::Map<String, serde_json::Value> = inner
                    .into_iter()
                    .map(|(ik, v)| {
                        (
                            ik.to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(v)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        )
                    })
                    .collect();
                (k.to_string(), serde_json::Value::Object(inner_map))
            })
            .collect();

        Ok(serde_json::Value::Object(outer))
    }

    /// Run a custom Pregel vertex-centric computation.
    ///
    /// All vertex data, edge data, and messages are `number` (f64) values.
    ///
    /// @param initialMsg    - Message sent to every vertex before superstep 0.
    /// @param maxIterations - Maximum number of supersteps.
    /// @param vprog         - `(vertexId: number, vertexData: number, msg: number) => number`
    ///                        Updates vertex attribute given an incoming message.
    /// @param sendMsg       - `(srcId, srcData, dstId, dstData, edgeData) => Array<[number, number]>`
    ///                        Returns `[targetVertexId, message]` pairs to send.
    ///                        Use `srcId` or `dstId` as the target.
    /// @param mergeMsg      - `(msgA: number, msgB: number) => number`
    ///                        Commutative combiner for messages to the same vertex.
    ///
    /// Returns a new `Graph` with updated vertex attributes.
    ///
    /// @example
    /// ```javascript
    /// // Propagate minimum vertex ID to all reachable vertices.
    /// const result = g.runPregelF64(
    ///   Infinity, 10,
    ///   (vid, vd, msg) => Math.min(vd, msg),
    ///   (si, sd, di, dd, ed) => sd < dd ? [[di, sd]] : [],
    ///   (a, b) => Math.min(a, b),
    /// );
    /// ```
    #[napi]
    pub fn run_pregel_f64(
        &self,
        initial_msg: f64,
        max_iterations: u32,
        vprog: Function<FnArgs<(f64, f64, f64)>, f64>,
        send_msg: PregelSendFn<'_>,
        merge_msg: Function<FnArgs<(f64, f64)>, f64>,
    ) -> Result<JsGraph> {
        // Implement the Pregel loop directly so JS functions stay on the current
        // thread and no Send/Sync bounds are needed.
        let graph = &self.inner;

        // Superstep 0: apply vprog with initial_msg to every vertex.
        let mut vdata: HashMap<VertexId, f64> = graph
            .vertices()
            .map(|(vid, vd)| {
                let new_vd = vprog.call(FnArgs::from((vid as f64, *vd, initial_msg)))?;
                Ok((vid, new_vd))
            })
            .collect::<Result<_>>()?;

        for _ in 0..max_iterations as usize {
            // Collect messages using original graph structure + current vdata.
            let mut msgs: HashMap<VertexId, f64> = HashMap::new();

            for edge in graph.edges() {
                let src_vd = vdata.get(&edge.src).copied().unwrap_or(0.0);
                let dst_vd = vdata.get(&edge.dst).copied().unwrap_or(0.0);
                let pairs = send_msg.call(FnArgs::from((
                    edge.src as f64,
                    src_vd,
                    edge.dst as f64,
                    dst_vd,
                    edge.attr,
                )))?;
                for pair in pairs {
                    if pair.len() >= 2 {
                        let target = pair[0] as i64;
                        let m = pair[1];
                        if let Some(existing) = msgs.get(&target).copied() {
                            msgs.insert(target, merge_msg.call(FnArgs::from((existing, m)))?);
                        } else {
                            msgs.insert(target, m);
                        }
                    }
                }
            }

            if msgs.is_empty() {
                break;
            }

            // Apply vprog to vertices that received a message.
            for (vid, msg) in &msgs {
                if let Some(&old_vd) = vdata.get(vid) {
                    let new_vd = vprog.call(FnArgs::from((*vid as f64, old_vd, *msg)))?;
                    vdata.insert(*vid, new_vd);
                }
            }
        }

        // Rebuild graph with final vertex data.
        let result = graph.map_vertices(|vid, old_vd| *vdata.get(&vid).unwrap_or(old_vd));
        Ok(JsGraph { inner: result })
    }
}

fn i64map_f64_to_json(map: HashMap<VertexId, f64>) -> serde_json::Value {
    let m: serde_json::Map<String, serde_json::Value> = map
        .into_iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)),
                ),
            )
        })
        .collect();
    serde_json::Value::Object(m)
}

fn i64_to_i64_map_to_json(map: HashMap<VertexId, VertexId>) -> serde_json::Value {
    let m: serde_json::Map<String, serde_json::Value> = map
        .into_iter()
        .map(|(k, v)| (k.to_string(), serde_json::Value::Number(v.into())))
        .collect();
    serde_json::Value::Object(m)
}
