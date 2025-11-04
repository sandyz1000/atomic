use dyn_clone::DynClone;

pub struct SplitStruct {
    index: usize,
}

pub trait Split: DynClone {
    fn get_index(&self) -> usize;
}

dyn_clone::clone_trait_object!(Split);
