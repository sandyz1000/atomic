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
mod fault_tolerance;
mod map_fold;
mod multi_stage;
mod named_partitioner;
mod shuffle_wordcount;
mod sort_by_task;

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
    }
}
