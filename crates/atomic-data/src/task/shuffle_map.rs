use std::fmt::Display;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::data::Data;
use crate::dependency::Dependency;
use crate::rdd::RddBase;
use crate::task::{Task, TaskBase, TaskMeta};

#[derive(Clone)]
pub struct ShuffleMapTask {
    pub meta: TaskMeta,
    pub rdd: Arc<dyn RddBase>,
    pub dep: Arc<Dependency>,
}

impl ShuffleMapTask {
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn RddBase>,
        dep: Arc<Dependency>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
    ) -> Self {
        let pinned = rdd.is_pinned();
        ShuffleMapTask {
            meta: TaskMeta::new(task_id, run_id, stage_id, partition, locs, pinned),
            rdd,
            dep,
        }
    }
}

impl Display for ShuffleMapTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleMapTask({:?}, {:?})",
            self.meta.stage_id, self.meta.partition
        )
    }
}

impl TaskBase for ShuffleMapTask {
    fn meta(&self) -> &TaskMeta {
        &self.meta
    }
}

impl Task for ShuffleMapTask {
    fn run(&self, _id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        if let Some(shuffle_dep) = self.dep.get_shuffle_dep() {
            // New signature: do_shuffle_task now only takes partition
            // The ShuffleDependency internally has the typed RDD
            let result = shuffle_dep.do_shuffle_task(self.meta.partition);
            return Ok(Box::new(result));
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ShuffleMapTask requires a shuffle dependency",
        )))
    }
}
