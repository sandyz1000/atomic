use std::fmt::Display;
use std::net::Ipv4Addr;
use std::sync::Arc;

use ember_data::context::{Task, TaskBase};
use ember_data::data::Data;
use ember_data::dependency::Dependency;
use ember_data::rdd::RddBase;

#[derive(Clone)]
pub(crate) struct ShuffleMapTask {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pub rdd: Arc<dyn RddBase>,
    pinned: bool,
    pub dep: Arc<Dependency>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
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
        ShuffleMapTask {
            task_id,
            run_id,
            stage_id,
            pinned: rdd.is_pinned(),
            rdd,
            dep,
            partition,
            locs,
        }
    }
}

impl Display for ShuffleMapTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleMapTask({:?}, {:?})",
            self.stage_id, self.partition
        )
    }
}

impl TaskBase for ShuffleMapTask {
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }

    fn is_pinned(&self) -> bool {
        self.pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }

    fn generation(&self) -> Option<i64> {
        // Some(env::Env::get().map_output_tracker.get_generation())
        None
    }
}

impl Task for ShuffleMapTask {
    fn run(&self, _id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        if let Some(shuffle_dep) = self.dep.get_shuffle_deps() {
            // New signature: do_shuffle_task now only takes partition
            // The ShuffleDependency internally has the typed RDD
            let result = shuffle_dep.do_shuffle_task(self.partition);
            return Ok(Box::new(result));
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ShuffleMapTask requires a shuffle dependency",
        )))
    }
}
