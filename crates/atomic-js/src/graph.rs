use atomic_compute::context::Context;
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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// VertexId in atomic-graph is i64.
type VertexId = i64;

/// JS `send_msg` callback for Pregel: `(srcId, dstId, edgeAttr, srcRank, dstRank)`
/// returns a list of `[targetId, message]` pairs.
type PregelSendFn<'a> = Function<'a, FnArgs<(f64, f64, f64, f64, f64)>, Vec<Vec<f64>>>;

/// JS `map_triplets` callback: `(srcId, srcAttr, dstId, dstAttr, edgeAttr)` returns the new
/// edge attribute.
type TripletMapFn<'a> = Function<'a, FnArgs<(f64, f64, f64, f64, f64)>, f64>;

#[napi(js_name = "Graph")]
pub struct JsGraph {
    ctx: Arc<Context>,
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

        let ctx = Context::local().map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let inner = Graph::from_vertices_edges(ctx.clone(), verts, edge_list);
        Ok(Self { ctx, inner })
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

    /// Return all vertices as an array of `[vertexId, data]` pairs.
    #[napi(ts_return_type = "Array<[number, number]>")]
    pub fn vertices(&self) -> serde_json::Value {
        let arr: Vec<serde_json::Value> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(id, data)| serde_json::json!([id, data]))
            .collect();
        serde_json::Value::Array(arr)
    }

    /// Return all edges as an array of `[srcId, dstId, weight]` triples.
    #[napi(ts_return_type = "Array<[number, number, number]>")]
    pub fn edges(&self) -> serde_json::Value {
        let arr: Vec<serde_json::Value> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| serde_json::json!([e.src, e.dst, e.attr]))
            .collect();
        serde_json::Value::Array(arr)
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
        let verts: Vec<(VertexId, ())> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(id, _)| (id, ()))
            .collect();
        let edges: Vec<Edge<f64>> = self.inner.collect_edges();
        let unit_graph: Graph<(), f64> = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);

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
        // Run the Pregel loop on the driver so JS functions stay on the current
        // thread and no Send/Sync bounds are needed. Python/JS closures cannot be
        // compiled into engine tasks, so custom Pregel materializes the graph.
        let vertices = self.inner.collect_vertices();
        let edges = self.inner.collect_edges();

        // Superstep 0: apply vprog with initial_msg to every vertex.
        let mut vdata: HashMap<VertexId, f64> = vertices
            .iter()
            .map(|(vid, vd)| {
                let new_vd = vprog.call(FnArgs::from((*vid as f64, *vd, initial_msg)))?;
                Ok((*vid, new_vd))
            })
            .collect::<Result<_>>()?;

        for _ in 0..max_iterations as usize {
            // Collect messages using original graph structure + current vdata.
            let mut msgs: HashMap<VertexId, f64> = HashMap::new();

            for edge in &edges {
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
        let new_vertices: Vec<(VertexId, f64)> = vertices
            .iter()
            .map(|(vid, old)| (*vid, vdata.get(vid).copied().unwrap_or(*old)))
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), new_vertices, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Randomly chosen vertex id, or `null` if the graph is empty.
    #[napi]
    pub fn pick_random_vertex(&self) -> Option<i64> {
        self.inner.pick_random_vertex()
    }

    /// Edge attribute for `(src, dst)` if the edge exists.
    #[napi]
    pub fn find(&self, src: i64, dst: i64) -> Option<f64> {
        self.inner.find(src, dst)
    }

    /// Map vertex attributes: `f(vertexId, currentAttr) => newAttr`.
    #[napi]
    pub fn map_vertices(&self, f: Function<FnArgs<(f64, f64)>, f64>) -> Result<JsGraph> {
        let verts: Vec<(VertexId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, vd)| {
                let new_vd = f.call(FnArgs::from((vid as f64, vd)))?;
                Ok((vid, new_vd))
            })
            .collect::<Result<_>>()?;
        let edges = self.inner.collect_edges();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Map edge attributes: `f(srcId, dstId, currentAttr) => newAttr`.
    #[napi]
    pub fn map_edges(&self, f: Function<FnArgs<(f64, f64, f64)>, f64>) -> Result<JsGraph> {
        let verts = self.inner.collect_vertices();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| {
                let new_attr = f.call(FnArgs::from((e.src as f64, e.dst as f64, e.attr)))?;
                Ok(Edge {
                    src: e.src,
                    dst: e.dst,
                    attr: new_attr,
                })
            })
            .collect::<Result<_>>()?;
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Return the subgraph of vertices where `vpred(vertexId, attr)` is true and edges where
    /// `epred(srcId, dstId, attr)` is true; edges with a dropped endpoint are also removed.
    #[napi]
    pub fn subgraph(
        &self,
        vpred: Function<FnArgs<(f64, f64)>, bool>,
        epred: Function<FnArgs<(f64, f64, f64)>, bool>,
    ) -> Result<JsGraph> {
        let mut kept_verts: Vec<(VertexId, f64)> = Vec::new();
        let mut live: HashSet<VertexId> = HashSet::new();
        for (vid, vd) in self.inner.collect_vertices() {
            if vpred.call(FnArgs::from((vid as f64, vd)))? {
                kept_verts.push((vid, vd));
                live.insert(vid);
            }
        }
        let mut kept_edges: Vec<Edge<f64>> = Vec::new();
        for e in self.inner.collect_edges() {
            if live.contains(&e.src)
                && live.contains(&e.dst)
                && epred.call(FnArgs::from((e.src as f64, e.dst as f64, e.attr)))?
            {
                kept_edges.push(e);
            }
        }
        let inner = Graph::from_vertices_edges(self.ctx.clone(), kept_verts, kept_edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Restrict this graph to the vertices and edges present in `other` (attributes kept from
    /// self). A vertex survives if its id is in `other`; an edge survives if its `(src, dst)` is.
    #[napi]
    pub fn mask(&self, other: &JsGraph) -> JsGraph {
        let other_v: HashSet<VertexId> = other
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(v, _)| v)
            .collect();
        let other_e: HashSet<(VertexId, VertexId)> = other
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| (e.src, e.dst))
            .collect();
        let verts: Vec<(VertexId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .filter(|(v, _)| other_v.contains(v))
            .collect();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .filter(|e| other_e.contains(&(e.src, e.dst)))
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        JsGraph {
            ctx: self.ctx.clone(),
            inner,
        }
    }

    /// Map each edge's attribute using the full triplet:
    /// `f(srcId, srcAttr, dstId, dstAttr, edgeAttr) => newAttr`.
    #[napi]
    pub fn map_triplets(&self, f: TripletMapFn) -> Result<JsGraph> {
        let vattr: HashMap<VertexId, f64> = self.inner.collect_vertices().into_iter().collect();
        let verts = self.inner.collect_vertices();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| {
                let sv = vattr.get(&e.src).copied().unwrap_or(0.0);
                let dv = vattr.get(&e.dst).copied().unwrap_or(0.0);
                let new_attr =
                    f.call(FnArgs::from((e.src as f64, sv, e.dst as f64, dv, e.attr)))?;
                Ok(Edge {
                    src: e.src,
                    dst: e.dst,
                    attr: new_attr,
                })
            })
            .collect::<Result<_>>()?;
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Merge parallel edges (same `(src, dst)`) with `f(attr1, attr2) => attr`.
    #[napi]
    pub fn group_edges(&self, f: Function<FnArgs<(f64, f64)>, f64>) -> Result<JsGraph> {
        let verts = self.inner.collect_vertices();
        let mut merged: HashMap<(VertexId, VertexId), f64> = HashMap::new();
        let mut order: Vec<(VertexId, VertexId)> = Vec::new();
        for e in self.inner.collect_edges() {
            match merged.get(&(e.src, e.dst)) {
                Some(acc) => {
                    let m = f.call(FnArgs::from((*acc, e.attr)))?;
                    merged.insert((e.src, e.dst), m);
                }
                None => {
                    order.push((e.src, e.dst));
                    merged.insert((e.src, e.dst), e.attr);
                }
            }
        }
        let edges: Vec<Edge<f64>> = order
            .into_iter()
            .map(|(src, dst)| Edge {
                src,
                dst,
                attr: merged[&(src, dst)],
            })
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Left-outer-join vertices with `table` (`[vertexId, value]` pairs), deriving new attributes
    /// via `f(vertexId, attr, valueOrNull) => newAttr`. Edges are unchanged.
    #[napi]
    pub fn outer_join_vertices(
        &self,
        table: Vec<(f64, f64)>,
        f: Function<FnArgs<(f64, f64, Option<f64>)>, f64>,
    ) -> Result<JsGraph> {
        let map: HashMap<VertexId, f64> =
            table.into_iter().map(|(vid, v)| (vid as i64, v)).collect();
        let verts: Vec<(VertexId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, vd)| {
                let new_vd = f.call(FnArgs::from((vid as f64, vd, map.get(&vid).copied())))?;
                Ok((vid, new_vd))
            })
            .collect::<Result<_>>()?;
        let edges = self.inner.collect_edges();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Inner-join vertices with `table`: only vertices present in `table` are updated via
    /// `f(vertexId, attr, value) => newAttr`; others keep their attribute.
    #[napi]
    pub fn join_vertices(
        &self,
        table: Vec<(f64, f64)>,
        f: Function<FnArgs<(f64, f64, f64)>, f64>,
    ) -> Result<JsGraph> {
        let map: HashMap<VertexId, f64> =
            table.into_iter().map(|(vid, v)| (vid as i64, v)).collect();
        let verts: Vec<(VertexId, f64)> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, vd)| match map.get(&vid) {
                Some(v) => {
                    let new_vd = f.call(FnArgs::from((vid as f64, vd, *v)))?;
                    Ok((vid, new_vd))
                }
                None => Ok((vid, vd)),
            })
            .collect::<Result<_>>()?;
        let edges = self.inner.collect_edges();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        Ok(JsGraph {
            ctx: self.ctx.clone(),
            inner,
        })
    }

    /// Return a new graph with all edges reversed.
    #[napi]
    pub fn reverse(&self) -> JsGraph {
        let verts = self.inner.collect_vertices();
        let edges: Vec<Edge<f64>> = self
            .inner
            .collect_edges()
            .into_iter()
            .map(|e| Edge {
                src: e.dst,
                dst: e.src,
                attr: e.attr,
            })
            .collect();
        let inner = Graph::from_vertices_edges(self.ctx.clone(), verts, edges);
        JsGraph {
            ctx: self.ctx.clone(),
            inner,
        }
    }

    /// `(inDegree, outDegree)` per vertex as `Record<string, [number, number]>`.
    #[napi(ts_return_type = "Record<string, [number, number]>")]
    pub fn degrees(&self) -> serde_json::Value {
        let mut deg: HashMap<VertexId, (usize, usize)> = HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            deg.entry(vid).or_insert((0, 0));
        }
        for e in self.inner.collect_edges() {
            deg.entry(e.src).or_insert((0, 0)).1 += 1;
            deg.entry(e.dst).or_insert((0, 0)).0 += 1;
        }
        let m: serde_json::Map<String, serde_json::Value> = deg
            .into_iter()
            .map(|(k, (ir, out))| (k.to_string(), serde_json::json!([ir, out])))
            .collect();
        serde_json::Value::Object(m)
    }

    /// In-degree per vertex.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn in_degrees(&self) -> serde_json::Value {
        let mut deg: HashMap<VertexId, usize> = HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            deg.entry(vid).or_insert(0);
        }
        for e in self.inner.collect_edges() {
            *deg.entry(e.dst).or_insert(0) += 1;
        }
        let m: serde_json::Map<String, serde_json::Value> = deg
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
            .collect();
        serde_json::Value::Object(m)
    }

    /// Out-degree per vertex.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn out_degrees(&self) -> serde_json::Value {
        let mut deg: HashMap<VertexId, usize> = HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            deg.entry(vid).or_insert(0);
        }
        for e in self.inner.collect_edges() {
            *deg.entry(e.src).or_insert(0) += 1;
        }
        let m: serde_json::Map<String, serde_json::Value> = deg
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
            .collect();
        serde_json::Value::Object(m)
    }

    /// Neighbor IDs per vertex. `direction` = "in", "out", or "either" (default).
    #[napi(ts_return_type = "Record<string, number[]>")]
    pub fn collect_neighbor_ids(&self, direction: Option<String>) -> serde_json::Value {
        let dir = direction.unwrap_or_else(|| "either".to_string());
        let mut neigh: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            neigh.entry(vid).or_default();
        }
        for e in self.inner.collect_edges() {
            match dir.as_str() {
                "in" => neigh.entry(e.dst).or_default().push(e.src),
                "out" => neigh.entry(e.src).or_default().push(e.dst),
                _ => {
                    neigh.entry(e.src).or_default().push(e.dst);
                    neigh.entry(e.dst).or_default().push(e.src);
                }
            }
        }
        let m: serde_json::Map<String, serde_json::Value> = neigh
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
            .collect();
        serde_json::Value::Object(m)
    }

    /// Neighbor `(neighborId, edgeAttr)` pairs per vertex.
    #[napi(ts_return_type = "Record<string, [number, number][]>")]
    pub fn collect_neighbors(&self, direction: Option<String>) -> serde_json::Value {
        let dir = direction.unwrap_or_else(|| "either".to_string());
        let mut neigh: HashMap<VertexId, Vec<(VertexId, f64)>> = HashMap::new();
        for (vid, _) in self.inner.collect_vertices() {
            neigh.entry(vid).or_default();
        }
        for e in self.inner.collect_edges() {
            match dir.as_str() {
                "in" => neigh.entry(e.dst).or_default().push((e.src, e.attr)),
                "out" => neigh.entry(e.src).or_default().push((e.dst, e.attr)),
                _ => {
                    neigh.entry(e.src).or_default().push((e.dst, e.attr));
                    neigh.entry(e.dst).or_default().push((e.src, e.attr));
                }
            }
        }
        let m: serde_json::Map<String, serde_json::Value> = neigh
            .into_iter()
            .map(|(k, v)| {
                let pairs: Vec<serde_json::Value> = v
                    .into_iter()
                    .map(|(nid, attr)| serde_json::json!([nid, attr]))
                    .collect();
                (k.to_string(), serde_json::Value::Array(pairs))
            })
            .collect();
        serde_json::Value::Object(m)
    }

    /// PageRank until mean-diff < `tol` or `maxIter`.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn page_rank_until_convergence(
        &self,
        tol: f64,
        reset_prob: f64,
        max_iter: u32,
    ) -> serde_json::Value {
        let n = self.inner.num_vertices() as f64;
        let mut ranks: HashMap<VertexId, f64> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, _)| (vid, 1.0 / n))
            .collect();
        let edges = self.inner.collect_edges();
        let mut in_edges: HashMap<VertexId, Vec<(VertexId, f64)>> = HashMap::new();
        let mut out_deg: HashMap<VertexId, usize> = HashMap::new();
        for e in &edges {
            in_edges.entry(e.dst).or_default().push((e.src, e.attr));
            *out_deg.entry(e.src).or_insert(0) += 1;
        }
        for _ in 0..max_iter {
            let mut new_ranks: HashMap<VertexId, f64> = HashMap::new();
            let mut max_diff = 0.0f64;
            for &vid in ranks.keys() {
                let sum: f64 = in_edges
                    .get(&vid)
                    .map(|ins| {
                        ins.iter()
                            .map(|(src, _w)| {
                                let d = *out_deg.get(src).unwrap_or(&1) as f64;
                                ranks.get(src).copied().unwrap_or(0.0) / d
                            })
                            .sum()
                    })
                    .unwrap_or(0.0);
                let new_rank = reset_prob / n + (1.0 - reset_prob) * sum;
                let diff = (new_rank - ranks[&vid]).abs();
                if diff > max_diff {
                    max_diff = diff;
                }
                new_ranks.insert(vid, new_rank);
            }
            ranks = new_ranks;
            if max_diff < tol {
                break;
            }
        }
        i64map_f64_to_json(ranks)
    }

    /// Personalized PageRank from source vertices.
    #[napi(ts_return_type = "Record<string, number>")]
    pub fn personalized_page_rank(
        &self,
        sources: Vec<i64>,
        num_iter: u32,
        reset_prob: f64,
    ) -> serde_json::Value {
        let source_set: std::collections::HashSet<VertexId> = sources.into_iter().collect();
        let mut ranks: HashMap<VertexId, f64> = self
            .inner
            .collect_vertices()
            .into_iter()
            .map(|(vid, _)| {
                if source_set.contains(&vid) {
                    (vid, 1.0 / source_set.len() as f64)
                } else {
                    (vid, 0.0)
                }
            })
            .collect();
        let edges = self.inner.collect_edges();
        let mut in_edges: HashMap<VertexId, Vec<(VertexId, f64)>> = HashMap::new();
        let mut out_deg: HashMap<VertexId, usize> = HashMap::new();
        for e in &edges {
            in_edges.entry(e.dst).or_default().push((e.src, e.attr));
            *out_deg.entry(e.src).or_insert(0) += 1;
        }
        for _ in 0..num_iter {
            let mut new_ranks: HashMap<VertexId, f64> = HashMap::new();
            for &vid in ranks.keys() {
                let sum: f64 = in_edges
                    .get(&vid)
                    .map(|ins| {
                        ins.iter()
                            .map(|(src, _w)| {
                                let d = *out_deg.get(src).unwrap_or(&1) as f64;
                                ranks.get(src).copied().unwrap_or(0.0) / d
                            })
                            .sum()
                    })
                    .unwrap_or(0.0);
                let teleport: f64 = if source_set.contains(&vid) {
                    reset_prob / source_set.len() as f64
                } else {
                    0.0
                };
                new_ranks.insert(vid, teleport + (1.0 - reset_prob) * sum);
            }
            ranks = new_ranks;
        }
        i64map_f64_to_json(ranks)
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
