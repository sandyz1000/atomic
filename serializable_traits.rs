use std::{
    any,
    borrow::{Borrow, BorrowMut},
    error, fmt, marker,
    ops::{self, Deref, DerefMut},
};
// Data passing through RDD needs to satisfy the following traits.
// Debug is only added here for debugging convenience during development stage but is not necessary.
// Sync is also not necessary I think. Have to look into it.
pub trait Data:
    Clone
    + any::Any
    + Send
    + Sync
    + fmt::Debug
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static
{
}

impl<T> Data for T
where
    T: Clone
    + any::Any
    + Send
    + Sync
    + fmt::Debug
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static,
{
}

// DynClone is used for cloning trait object
pub trait AnyData: dyn_clone::DynClone + any::Any + Send + Sync + fmt::Debug + 'static
{
    fn as_any(&self) -> &dyn any::Any;
    /// Convert to a `&mut std::any::Any`.
    fn as_any_mut(&mut self) -> &mut dyn any::Any;
    /// Convert to a `std::boxed::Box<dyn std::any::Any>`.
    fn into_any(self: Box<Self>) -> Box<dyn any::Any>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send>`.
    fn into_any_send(self: Box<Self>) -> Box<dyn any::Any + Send>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Sync>`.
    fn into_any_sync(self: Box<Self>) -> Box<dyn any::Any + Sync>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send + Sync>`.
    fn into_any_send_sync(self: Box<Self>) -> Box<dyn any::Any + Send + Sync>;
}

dyn_clone::clone_trait_object!(AnyData);

// Automatically implementing the Data trait for all types which implements the required traits
impl<
        T: dyn_clone::DynClone
            + any::Any
            + Send
            + Sync
            + fmt::Debug
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    > AnyData for T
{
    fn as_any(&self) -> &dyn any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn any::Any> {
        self
    }
    fn into_any_send(self: Box<Self>) -> Box<dyn any::Any + Send> {
        self
    }
    fn into_any_sync(self: Box<Self>) -> Box<dyn any::Any + Sync> {
        self
    }
    fn into_any_send_sync(self: Box<Self>) -> Box<dyn any::Any + Send + Sync> {
        self
    }
}

impl serde::ser::Serialize for Box<dyn AnyData + 'static> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        erased_serde::serialize(&self, serializer)
    }
}

impl<'de> serde::de::Deserialize<'de> for Box<dyn AnyData + 'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Box<dyn AnyData + 'static>>::deserialize(deserializer)
            // .map(|x| x.0)
    }
}

impl serde::ser::Serialize for Box<dyn AnyData + Send + 'static> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        erased_serde::serialize(&self, serializer)
    }
}

impl<'de> serde::de::Deserialize<'de> for Box<dyn AnyData + Send + 'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Box<dyn AnyData + Send + 'static>>::deserialize(deserializer)
            // .map(|x| x.0)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AtomicBox<T: ?Sized>(Box<T>);

impl<T> AtomicBox<T> {
    // Create a new Box wrapper
    pub fn new(t: T) -> Self {
        Self(Box::new(t))
    }
}

impl<T: ?Sized> AtomicBox<T> {
    // Convert to a regular `std::Boxed::Box<T>`. Coherence rules prevent currently prevent `impl Into<std::boxed::Box<T>> for Box<T>`.
    pub fn into_box(self) -> Box<T> {
        self.0
    }
}

impl AtomicBox<dyn AnyData> {
    // Convert into a `std::boxed::Box<dyn std::any::Any>`.
    pub fn into_any(self) -> Box<dyn any::Any> {
        self.0.into_any()
    }
}

// impl<T: ?Sized + marker::Unsize<U>, U: ?Sized> ops::CoerceUnsized<AtomicBox<U>> for Box<T> {}

impl<T: ?Sized> Deref for AtomicBox<T> {
    type Target = Box<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> DerefMut for AtomicBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: ?Sized> AsRef<Box<T>> for AtomicBox<T> {
    fn as_ref(&self) -> &Box<T> {
        &self.0
    }
}

impl<T: ?Sized> AsMut<Box<T>> for AtomicBox<T> {
    fn as_mut(&mut self) -> &mut Box<T> {
        &mut self.0
    }
}

impl<T: ?Sized> AsRef<T> for AtomicBox<T> {
    fn as_ref(&self) -> &T {
        &*self.0
    }
}

impl<T: ?Sized> AsMut<T> for AtomicBox<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}

impl<T: ?Sized> Borrow<T> for AtomicBox<T> {
    fn borrow(&self) -> &T {
        &*self.0
    }
}

impl<T: ?Sized> BorrowMut<T> for AtomicBox<T> {
    fn borrow_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}

impl<T: ?Sized> From<Box<T>> for AtomicBox<T> {
    fn from(t: Box<T>) -> Self {
        Self(t)
    }
}

impl<T> From<T> for AtomicBox<T> {
    fn from(t: T) -> Self {
        Self(Box::new(t))
    }
}

impl<T: error::Error> error::Error for Box<T> {
    fn description(&self) -> &str {
        error::Error::description(&**self)
    }
    #[allow(deprecated)]
    fn cause(&self) -> Option<&dyn error::Error> {
        error::Error::cause(&**self)
    }
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        error::Error::source(&**self)
    }
}

impl<T: fmt::Debug + ?Sized> fmt::Debug for AtomicBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl<T: fmt::Display + ?Sized> fmt::Display for AtomicBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl<A, F: ?Sized> ops::FnOnce<A> for AtomicBox<F>
where
    F: FnOnce<A>,
{
    type Output = F::Output;
    fn call_once(self, args: A) -> Self::Output {
        self.0.call_once(args)
    }
}

impl<A, F: ?Sized> ops::FnMut<A> for AtomicBox<F>
where
    F: FnMut<A>,
{
    fn call_mut(&mut self, args: A) -> Self::Output {
        self.0.call_mut(args)
    }
}

impl<A, F: ?Sized> ops::Fn<A> for AtomicBox<F>
where
    F: Func<A>,
{
    fn call(&self, args: A) -> Self::Output {
        self.0.call(args)
    }
}


impl<T: ?Sized + 'static> serde::ser::Serialize for AtomicBox<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        erased_serde::serialize(&self.0, serializer)
    }
}

impl<'de, T: ?Sized + 'static> serde::de::Deserialize<'de> for AtomicBox<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        erased_serde::deserialize(deserializer).map(Self)
    }
}

pub trait SerFunc<Args>:
    Fn(Args) -> ()
    + Send
    + Sync
    + Clone
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    // + erased_serde::Serialize
    + 'static
where
    Args: Sized
{
}

impl<Args, T> SerFunc<Args> for T
where
    T: Fn(Args) -> () 
        + Send
        + Sync
        + Clone
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        // + erased_serde::Serialize
        + 'static,
    Args: Sized
{
}

pub trait Func<Args>:
    Fn() -> () + Send + Sync + 'static + dyn_clone::DynClone
{
}

impl<T: ?Sized, Args> Func<Args> for T
where
    T: ops::Fn(Args) -> () + Send + Sync + 'static + dyn_clone::DynClone,
    // Args: std::marker::Tuple,
{
}

impl<Args: 'static, Output: 'static> std::clone::Clone
    for Box<dyn Func<Args, Output = Output>>
{
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl<'a, Args, Output> AsRef<Self> for dyn Func<Args, Output = Output> + 'a {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<Args: 'static, Output: 'static> serde::ser::Serialize for dyn Func<Args, Output = Output> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        erased_serde::serialize(self, serializer)
    }
}

impl<'de, Args: 'static, Output: 'static> serde::de::Deserialize<'de>
    for Box<dyn Func<Args, Output = Output> + 'static>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Box<dyn Func<Args, Output = Output> + 'static>>::deserialize(deserializer).map(|x| x.0)
    }
}
