//! Core graph value types shared across the engine-backed graph layer.
//!
//! Vertex and edge attributes travel as RDD elements, so every type here derives
//! the wire (`rkyv`) and shuffle (`bincode`) encodings the compute engine requires,
//! alongside `serde` for the language bindings.

use std::collections::HashMap;

/// Vertex identifier — a 64-bit signed integer uniquely naming a vertex in the graph.
pub type VertexId = i64;

/// A directed edge with an associated attribute.
#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Edge<ED> {
    pub src: VertexId,
    pub dst: VertexId,
    pub attr: ED,
}

/// An edge together with the attribute data of both endpoint vertices.
///
/// This is the element a message-sending task receives in [`aggregate_messages`].
/// The task inspects the endpoints and returns the messages to deliver.
///
/// [`aggregate_messages`]: crate::graph::Graph::aggregate_messages
#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct EdgeTriplet<VD, ED> {
    pub src_id: VertexId,
    pub dst_id: VertexId,
    pub src_attr: VD,
    pub dst_attr: VD,
    pub attr: ED,
}

/// Which edges are considered when restricting message passing in Pregel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeDirection {
    /// Consider only in-edges of a vertex.
    In,
    /// Consider only out-edges of a vertex.
    Out,
    /// Consider either in- or out-edges.
    Either,
    /// Consider both in- and out-edges.
    Both,
}

/// A per-vertex map — the standard return type for the built-in algorithms.
pub type VertexMap<A> = HashMap<VertexId, A>;
