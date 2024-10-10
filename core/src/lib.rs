// #![feature(
//     arbitrary_self_types,
//     coerce_unsized,
//     core_intrinsics,
//     fn_traits,
//     never_type,
//     specialization,
//     unboxed_closures,
//     unsize,
//     binary_heap_into_iter_sorted
// )]
#![allow(dead_code, where_clauses_object_safety, deprecated)]
#![allow(clippy::single_component_path_imports)]

// TODO: Fix output path
mod serialized_data_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/serialized_data_capnp.rs"));
}

mod aggregator;
mod cache;
mod cache_tracker;
mod context;
mod dependency;
mod env;
mod executor;
pub mod io;
mod map_output_tracker;
mod partial;
pub mod partitioner;
pub mod rdd;
mod scheduler;

mod shuffle;
mod split;
// pub mod serializers;
mod error;
pub mod fs;
mod hosts;
mod utils;
pub mod serializable_traits;
// Import global external types and macros:
// pub use serde_closure::Fn;

// Re-exports:
pub use context::Context;
pub use error::*;
pub use io::LocalFsReaderConfig;
pub use partial::BoundedDouble;
pub use rdd::rdd::{PairRdd, Rdd};


// fn test_closure() {
//     use serde_closure::{traits::Fn, Fn};

//     let one = 1;
//     let func = Fn!(|x: i32| x + one);
//     let plus_one = std::sync::Arc::new(Fn!(|x: i32| x + one));

//     assert_eq!(2, plus_one.call((1,))); // this works on stable and nightly
//     // assert_eq!(2, plus_one(1));      // this only works on nightly
//     println!("{:#?}", plus_one);

//     // prints:
//     // Fn<main::{{closure}} at main.rs:6:15> {
//     //     one: 1,
//     //     source: "| x : i32 | x + one",
//     // }
// }
