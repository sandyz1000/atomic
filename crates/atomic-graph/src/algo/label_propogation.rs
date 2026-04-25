use std::collections::HashMap;
use crate::pregel::*;


#[derive(Debug)]
pub struct LabelPropagation;

impl LabelPropagation {
    /// Run static Label Propagation for detecting communities in networks.
    ///
    /// Each node in the network is initially assigned to its own community. At every superstep, nodes
    /// send their community affiliation to all neighbors and update their state to the mode community
    /// affiliation of incoming messages.
    ///
    /// LPA is a standard community detection algorithm for graphs. It is very inexpensive
    /// computationally, although (1) convergence is not guaranteed and (2) one can end up with
    /// trivial solutions (all nodes are identified into a single community).
    ///
    /// # Arguments
    ///
    /// * `graph` - The graph for which to compute the community affiliation
    /// * `max_steps` - The number of supersteps of LPA to be performed. Because this is a static
    /// implementation, the algorithm will run for exactly this many supersteps.
    ///
    /// # Returns
    ///
    /// A graph with vertex attributes containing the label of community affiliation
    pub fn run<VD, ED>(graph: Graph<VD, ED>, max_steps: i32) -> Graph<VertexId, ED>
        where ED: std::fmt::Debug + std::clone::Clone,
              VD: std::fmt::Debug + std::clone::Clone {
        let msg = format!("Maximum of steps must be greater than 0, but got {}", max_steps);
        assert!(max_steps > 0, msg);

        let lpa_graph = graph.map_vertices(|(vid, _)| vid);
        let send_message = |e: EdgeTriplet<VertexId, ED>| {
            let src_id = e.src_id();
            let dst_id = e.dst_id();
            let mut map = HashMap::new();
            map.insert(dst_id, 1i64);
            map.insert(src_id, 1i64);
            vec![(src_id, map), (dst_id, map)]
        };
        let merge_message = |count1: HashMap<VertexId, i64>, count2: HashMap<VertexId, i64>| {
            let mut map = HashMap::new();
            for i in count1.keys().chain(count2.keys()) {
                let count1_val = *count1.get(i).unwrap_or(&0i64);
                let count2_val = *count2.get(i).unwrap_or(&0i64);
                map.insert(*i, count1_val + count2_val);
            }
            map
        };
        let vertex_program = |vid: VertexId, attr: VertexId, message: HashMap<VertexId, i64>| {
            if message.is_empty() { attr } else { *message.iter().max_by_key(|&(_, count)| count).unwrap().0 }
        };
        let initial_message = HashMap::new();
        pregel(lpa_graph, initial_message, max_steps, vertex_program, send_message, merge_message)
    }
}
