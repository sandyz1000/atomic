pub struct InputSplitStruct ();

pub trait InputSplit {
    fn get_length(&self) -> usize;

    fn get_locations(&self) -> Vec<String>;
}