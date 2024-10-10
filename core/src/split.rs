use downcast_rs::{impl_downcast, DowncastSync};


pub(crate) struct SplitStruct {
    index: usize,
}

// pub trait Split<'de>: DowncastSync + dyn_clone::DynClone + serde::Serialize + serde::de::Deserialize<'de> {
//     fn get_index(&self) -> usize;
// }

pub trait Split: DowncastSync + dyn_clone::DynClone {
    fn get_index(&self) -> usize;
}

// TODO: Fix this
impl_downcast!(Split);
dyn_clone::clone_trait_object!(Split);
