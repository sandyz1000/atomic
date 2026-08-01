//! One module per integration scenario. Each owns its `#[task]`s, shuffle/
//! partitioner registrations, and a `run_driver` that prints machine-readable
//! JSON to stdout.
//!
//! All scenario modules are compiled into the binary regardless of which one
//! runs, so the task registry (and `REGISTRY_FINGERPRINT`) is identical
//! between the worker process and any driver invocation — worker mode never
//! needs to know the scenario.

use atomic_compute::context::Context;
use std::error::Error;
use std::sync::Arc;

mod cache_locality;
mod combine_aggregate_by_key;
mod combine_reduce_by_key;
mod distributed_state;
mod fault_tolerance;
mod map_fold;
mod multi_stage;
mod named_partitioner;
mod shuffle_wordcount;
mod sort_by_task;
mod staged_shuffle_terminal_action;
mod unstaged_shuffle;

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "snake_case")]
pub enum Scenario {
    MapFold,
    ShuffleWordcount,
    MultiStage,
    FaultTolerance,
    CacheLocality,
    NamedPartitioner,
    SortByTask,
    UnstagedShuffle,
    StagedShuffleTerminalAction,
    CombineReduceByKey,
    CombineAggregateByKey,
    DistributedState,
}

pub fn run(scenario: Scenario, ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    match scenario {
        Scenario::MapFold => map_fold::run_driver(ctx),
        Scenario::ShuffleWordcount => shuffle_wordcount::run_driver(ctx),
        Scenario::MultiStage => multi_stage::run_driver(ctx),
        Scenario::FaultTolerance => fault_tolerance::run_driver(ctx),
        Scenario::CacheLocality => cache_locality::run_driver(ctx),
        Scenario::NamedPartitioner => named_partitioner::run_driver(ctx),
        Scenario::SortByTask => sort_by_task::run_driver(ctx),
        Scenario::UnstagedShuffle => unstaged_shuffle::run_driver(ctx),
        Scenario::StagedShuffleTerminalAction => staged_shuffle_terminal_action::run_driver(ctx),
        Scenario::CombineReduceByKey => combine_reduce_by_key::run_driver(ctx),
        Scenario::CombineAggregateByKey => combine_aggregate_by_key::run_driver(ctx),
        Scenario::DistributedState => distributed_state::run_driver(ctx),
    }
}
