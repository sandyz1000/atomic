pub mod parallel_collection_rdd;
pub mod cartesian_rdd;
pub mod co_grouped_rdd;
pub mod coalesced_rdd;
pub mod flatmapper_rdd;
pub mod mapper_rdd;
pub mod pair_rdd;
pub mod partitionwise_sampled_rdd;
pub mod shuffled_rdd;
pub mod map_partitions_rdd;
pub mod zip_rdd;
pub mod union_rdd;
pub mod rdd;
pub mod hdfs_read_rdd;
pub mod local_fs_read_rdd;


// Data passing through RDD needs to satisfy the following traits.
// Debug is only added here for debugging convenience during development stage but is not necessary.
// Sync is also not necessary I think. Have to look into it.
pub trait Data:
    Clone + std::any::Any + Send + Sync + std::fmt::Debug + serde::ser::Serialize
    + serde::de::DeserializeOwned + 'static
{
}

impl<T> Data for T
where
    T: Clone
    + std::any::Any
    + Send
    + Sync
    + std::fmt::Debug
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static,
{
}


// pub trait AnyData:
//     dyn_clone::DynClone + any::Any + Send + Sync + fmt::Debug + Serialize + Deserialize + 'static
// {
//     fn as_any(&self) -> &dyn any::Any;
//     /// Convert to a `&mut std::any::Any`.
//     fn as_any_mut(&mut self) -> &mut dyn any::Any;
//     /// Convert to a `std::boxed::Box<dyn std::any::Any>`.
//     fn into_any(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any>;
//     /// Convert to a `std::boxed::Box<dyn std::any::Any + Send>`.
//     fn into_any_send(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send>;
//     /// Convert to a `std::boxed::Box<dyn std::any::Any + Sync>`.
//     fn into_any_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Sync>;
//     /// Convert to a `std::boxed::Box<dyn std::any::Any + Send + Sync>`.
//     fn into_any_send_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send + Sync>;
// }

// dyn_clone::clone_trait_object!(AnyData);

// // Automatically implementing the Data trait for all types which implements the required traits
// impl<
//         T: dyn_clone::DynClone
//             + std::any::Any
//             + Send
//             + Sync
//             + std::fmt::Debug
//             // + Serialize
//             // + Deserialize
//             + 'static,
//     > AnyData for T
// {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }

//     fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
//         self
//     }
    
//     fn into_any(self: std::boxed::Box<Self>) -> std::boxed::Box<dyn std::any::Any> {
//         self
//     }
    
//     fn into_any_send(self: std::boxed::Box<Self>) -> std::boxed::Box<dyn std::any::Any + Send> {
//         self
//     }
    
//     fn into_any_sync(self: std::boxed::Box<Self>) -> std::boxed::Box<dyn std::any::Any + Sync> {
//         self
//     }
    
//     fn into_any_send_sync(self: std::boxed::Box<Self>) -> std::boxed::Box<dyn std::any::Any + Send + Sync> {
//         self
//     }
// }
