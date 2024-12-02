use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::dependency::{NarrowDependencyTrait, ShuffleDependencyTrait};
use crate::env;
use crate::partitioner::Partitioner;
use crate::rdd::rdd::RddBase;
use crate::scheduler::{Task, TaskBase};
use crate::ser_data::AnyData;
use crate::shuffle::*;
use crate::split::Split;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ShuffleMapTask<RDB, S, P, ND, SD> {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pub rdd: Arc<RDB>,
    pinned: bool,
    pub dep: Arc<S>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    _marker_p: PhantomData<P>,
    _marker_nd: PhantomData<ND>,
    _marker_sd: PhantomData<SD>,
}

impl<RDB, S, P, ND, SD> ShuffleMapTask<RDB, S, P, ND, SD>  
where 
    S: Split,
    RDB: RddBase<ND, SD, S, P>,
    P: Partitioner,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait

{
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
            _marker_p: PhantomData,
            _marker_nd: PhantomData,
            _marker_sd: PhantomData,
        }
    }
}

impl<RDB, S, P, ND, SD>  Display for ShuffleMapTask<RDB, S, P, ND, SD> 
where 
    S: Split,
    RDB: RddBase<ND, SD, S, P>,
    P: Partitioner,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleMapTask({:?}, {:?})",
            self.stage_id, self.partition
        )
    }
}

impl<RDB, S, P, ND, SD> TaskBase for ShuffleMapTask<RDB, S, P, ND, SD> 
where 
    S: Split,
    RDB: RddBase<ND, SD, S, P>,
    P: Partitioner,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static
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

impl<RDB, S, P, ND, SD> Task for ShuffleMapTask<RDB, S, P, ND, SD> 
where 
    S: Split,
    RDB: RddBase<ND, SD, S, P>,
    P: Partitioner,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static
{
    fn run(&self, _id: usize) -> Box<impl AnyData> {
        Box::new(self.dep.do_shuffle_task(self.rdd.clone(), self.partition))
    }
}
