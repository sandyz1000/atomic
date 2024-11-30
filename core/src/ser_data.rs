// use erased_serde::{Serialize, Serializer};


// Data passing through RDD needs to satisfy the following traits.
// Debug is only added here for debugging convenience during development stage but is not necessary.
// Sync is also not necessary I think. Have to look into it.
pub trait Data:
    Clone
    + std::any::Any
    + Send
    + Sync
    + std::fmt::Debug
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static
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

// DynClone is used for cloning trait object
pub trait AnyData: dyn_clone::DynClone + std::any::Any + Send + Sync + std::fmt::Debug + 'static
{
    fn as_any(&self) -> &dyn std::any::Any;
    /// Convert to a `&mut std::any::Any`.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    /// Convert to a `std::boxed::Box<dyn std::any::Any>`.
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send>`.
    fn into_any_send(self: Box<Self>) -> Box<dyn std::any::Any + Send>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Sync>`.
    fn into_any_sync(self: Box<Self>) -> Box<dyn std::any::Any + Sync>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send + Sync>`.
    fn into_any_send_sync(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync>;
}

dyn_clone::clone_trait_object!(AnyData);

// Automatically implementing the Data trait for all types which implements the required traits
impl<
        T: dyn_clone::DynClone
            + std::any::Any
            + Send
            + Sync
            + std::fmt::Debug
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    > AnyData for T
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
    fn into_any_send(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
    fn into_any_sync(self: Box<Self>) -> Box<dyn std::any::Any + Sync> {
        self
    }
    fn into_any_send_sync(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync> {
        self
    }
}


pub trait SerFunc<Args>:
    serde_closure::traits::Fn<Args>
    + Send
    + Sync
    + Clone
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static
where
    Args: Sized
{
}

impl<Args, T> SerFunc<Args> for T
where
    T: serde_closure::traits::Fn<Args>
        + Send
        + Sync
        + Clone
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static,
    Args: Sized
{
}
