use std::fmt::Display;
use std::net::Ipv4Addr;
use std::sync::Arc;
use crate::env;
use crate::rdd::rdd::RddBase;
use crate::scheduler::{Task, TaskBase};
use crate::ser_data::AnyData;
use crate::split::Split;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ShuffleMapTask<RDD, S> {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pub rdd: Arc<RDD>,
    pinned: bool,
    pub dep: Arc<S>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
}

impl<RDB: RddBase, S: Split> ShuffleMapTask<RDB, S>  {
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<RDB>,
        dep: Arc<S>,
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

impl<RDB, S>  Display for ShuffleMapTask<RDB, S> 
where 
    S: Split,
    RDB: RddBase,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleMapTask({:?}, {:?})",
            self.stage_id, self.partition
        )
    }
}

impl<RDB, S> TaskBase for ShuffleMapTask<RDB, S> 
where 
    S: Split,
    RDB: RddBase,
{
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
        Some(env::Env::get().map_output_tracker.get_generation())
    }
}

impl<RDB, S> Task for ShuffleMapTask<RDB, S> 
where 
    S: Split,
    RDB: RddBase,
{
    fn run(&self, _id: usize) -> Box<impl AnyData> {
        Box::new(self.dep.do_shuffle_task(self.rdd.clone(), self.partition))
    }
}
