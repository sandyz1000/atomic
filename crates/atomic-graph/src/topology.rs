use std::collections::HashMap;

/// Vertex identifier — i64 matches GraphX's VertexId = Long.
pub type VertexId = i64;

/// A directed edge with associated attribute.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Edge<ED> {
    pub src: VertexId,
    pub dst: VertexId,
    pub attr: ED,
}

/// An edge together with the attribute data of both endpoint vertices.
#[derive(Clone, Debug)]
pub struct EdgeTriplet<VD, ED> {
    pub src_id: VertexId,
    pub dst_id: VertexId,
    pub src_attr: VD,
    pub dst_attr: VD,
    pub attr: ED,
}

/// Which edges are considered when restricting message passing.
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

/// Passed to the `send_msg` closure in Pregel and `aggregate_messages`.
///
/// The closure can send zero or more messages to the src or dst vertex of
/// the current edge.
pub struct EdgeContext<'a, VD, ED, A> {
    pub triplet: EdgeTriplet<VD, ED>,
    pub(crate) msgs: &'a mut Vec<(VertexId, A)>,
}

impl<'a, VD: Clone, ED: Clone, A> EdgeContext<'a, VD, ED, A> {
    /// Send a message to the source vertex of this edge.
    pub fn send_to_src(&mut self, msg: A) {
        self.msgs.push((self.triplet.src_id, msg));
    }

    /// Send a message to the destination vertex of this edge.
    pub fn send_to_dst(&mut self, msg: A) {
        self.msgs.push((self.triplet.dst_id, msg));
    }
}

/// A per-vertex map — the standard return type for most algorithms.
pub type VertexMap<A> = HashMap<VertexId, A>;
