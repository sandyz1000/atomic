use std::{
    net::SocketAddrV4,
    path::PathBuf,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
};

pub struct TaskContext {
    pub stage_id: usize,
    pub split_id: usize,
    pub attempt_id: usize,
}

impl TaskContext {
    pub fn new(stage_id: usize, split_id: usize, attempt_id: usize) -> Self {
        TaskContext {
            stage_id,
            split_id,
            attempt_id,
        }
    }
}

// #[derive(Debug, Clone)]
// pub struct RddContext {
//     pub next_rdd_id: Arc<AtomicUsize>,
//     pub next_shuffle_id: Arc<AtomicUsize>,
//     pub address_map: Vec<SocketAddrV4>,
//     pub distributed_driver: bool,
//     /// this context/session temp work dir  
//     pub work_dir: PathBuf,
//     pub shuffle_fetcher: Arc<crate::shuffle::fetcher::ShuffleFetcher>,
// }


// impl RddContext {
//     pub fn new_rdd_id(self: &Arc<Self>) -> usize {
//         self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
//     }
// }
