use std::{any::Any, io::Read, net::Ipv4Addr, path::PathBuf, sync::Arc};

use dyn_clone::DynClone;

use crate::{
    data::Data,
    error::BaseError,
    rdd::{Rdd, RddBase},
};

pub struct SplitStruct {
    pub index: usize,
}

pub trait Split: DynClone {
    fn get_index(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

dyn_clone::clone_trait_object!(Split);

#[derive(Debug, Clone, Copy)]
pub struct PrefLoc(u32);

impl Into<Ipv4Addr> for PrefLoc {
    fn into(self) -> Ipv4Addr {
        Ipv4Addr::from(self.0)
    }
}

impl From<Ipv4Addr> for PrefLoc {
    fn from(other: Ipv4Addr) -> PrefLoc {
        PrefLoc(other.into())
    }
}

/// Class that captures a coalesced RDD by essentially keeping track of parent partitions.
#[derive(Clone)]
pub struct CoalescedRddSplit {
    index: usize,
    rdd: Arc<dyn RddBase>,
    pub parent_indices: Vec<usize>,
    preferred_location: Option<PrefLoc>,
}

impl CoalescedRddSplit {
    pub fn new(
        index: usize,
        preferred_location: Option<PrefLoc>,
        rdd: Arc<dyn RddBase>,
        parent_indices: Vec<usize>,
    ) -> Self {
        CoalescedRddSplit {
            index,
            preferred_location,
            rdd,
            parent_indices,
        }
    }

    /// Computes the fraction of the parents partitions containing preferred_location within
    /// their preferred_locs.
    ///
    /// Returns locality of this coalesced partition between 0 and 1.
    pub fn local_fraction(&self) -> f64 {
        if self.parent_indices.is_empty() {
            0.0
        } else {
            let mut loc = 0u32;
            let pl: Ipv4Addr = self.preferred_location.unwrap().into();
            for p in self.rdd.splits() {
                let parent_pref_locs = self.rdd.preferred_locations(p);
                if parent_pref_locs.contains(&pl) {
                    loc += 1;
                }
            }
            loc as f64 / self.parent_indices.len() as f64
        }
    }

    pub fn downcasting(split: Box<dyn Split>) -> Result<CoalescedRddSplit, BaseError> {
        // Use the as_any method to downcast
        split
            .as_any()
            .downcast_ref::<CoalescedRddSplit>()
            .cloned()
            .ok_or_else(|| BaseError::DowncastFailure("CoalescedRddSplit".to_string()))
    }
}

impl Split for CoalescedRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone)]
pub struct ShuffledRddSplit {
    index: usize,
}

impl ShuffledRddSplit {
    pub fn new(index: usize) -> Self {
        ShuffledRddSplit { index }
    }
}

impl Split for ShuffledRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone)]
pub struct CartesianSplit {
    pub idx: usize,
    pub s1_idx: usize,
    pub s2_idx: usize,
    pub s1: Box<dyn Split>,
    pub s2: Box<dyn Split>,
}

impl Split for CartesianSplit {
    fn get_index(&self) -> usize {
        self.idx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone)]
pub enum CoGroupSplitDep {
    NarrowCoGroupSplitDep {
        rdd: Arc<dyn RddBase>,
        split: Box<dyn Split>,
    },
    ShuffleCoGroupSplitDep {
        shuffle_id: usize,
    },
}

#[derive(Clone)]
pub struct CoGroupSplit {
    pub index: usize,
    pub deps: Vec<CoGroupSplitDep>,
}

impl CoGroupSplit {
    pub fn new(index: usize, deps: Vec<CoGroupSplitDep>) -> Self {
        CoGroupSplit { index, deps }
    }
}

impl std::hash::Hasher for CoGroupSplit {
    fn finish(&self) -> u64 {
        self.index as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for i in bytes {
            self.write_u8(*i);
        }
    }
}

impl Split for CoGroupSplit {
    fn get_index(&self) -> usize {
        self.index
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct BytesReader {
    files: Vec<PathBuf>,
    idx: usize,
    host: Ipv4Addr,
}

impl Split for BytesReader {
    fn get_index(&self) -> usize {
        self.idx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Iterator for BytesReader {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(path) = self.files.pop() {
            let file = std::fs::File::open(path).unwrap();
            let mut content = vec![];
            let mut reader = std::io::BufReader::new(file);
            reader.read_to_end(&mut content).unwrap();
            Some(content)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileReader {
    pub files: Vec<PathBuf>,
    pub idx: usize,
    pub host: Ipv4Addr,
}

impl Split for FileReader {
    fn get_index(&self) -> usize {
        self.idx
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Iterator for FileReader {
    type Item = PathBuf;
    fn next(&mut self) -> Option<Self::Item> {
        self.files.pop()
    }
}

#[derive(Clone)]
pub struct ZippedPartitionsSplit {
    pub fst_idx: usize,
    pub sec_idx: usize,
    pub idx: usize,
    pub fst_split: Box<dyn Split>,
    pub sec_split: Box<dyn Split>,
}

impl Split for ZippedPartitionsSplit {
    fn get_index(&self) -> usize {
        self.idx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone)]
pub struct ParallelCollectionSplit<T> {
    pub rdd_id: i64,
    pub index: usize,
    pub values: Arc<Vec<T>>,
}

impl<T> Split for ParallelCollectionSplit<T>
where
    T: Data + Clone,
{
    fn get_index(&self) -> usize {
        self.index
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<T> ParallelCollectionSplit<T>
where
    T: Data + Clone,
{
    pub fn new(rdd_id: i64, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            index,
            values,
        }
    }

    // Lot of unnecessary cloning is there. Have to refactor for better performance
    pub fn iterator(&self) -> Box<dyn Iterator<Item = T>> {
        let data = self.values.clone();
        let len = data.len();
        Box::new((0..len).map(move |i| data[i].clone()))
    }
}

#[derive(Clone)]
pub struct UnionSplit<T: 'static> {
    /// index of the partition
    pub idx: usize,
    /// the parent RDD this partition refers to
    pub rdd: Arc<dyn Rdd<Item = T>>,
    /// index of the parent RDD this partition refers to
    pub parent_rdd_index: usize,
    /// index of the partition within the parent RDD this partition refers to
    pub parent_rdd_split_index: usize,
}

impl<T: Data> UnionSplit<T> {
    pub fn parent_partition(&self) -> Box<dyn Split> {
        self.rdd.splits()[self.parent_rdd_split_index].clone()
    }
}

impl<T> Split for UnionSplit<T>
where
    T: Data + Clone,
{
    fn get_index(&self) -> usize {
        self.idx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone)]
pub struct PartitionerAwareUnionSplit {
    pub idx: usize,
}

impl PartitionerAwareUnionSplit {
    pub fn parents<'a, T: Data>(
        &'a self,
        rdds: &'a [Arc<dyn Rdd<Item = T>>],
    ) -> impl Iterator<Item = Box<dyn Split>> + 'a {
        rdds.iter().map(move |rdd| rdd.splits()[self.idx].clone())
    }
}

impl Split for PartitionerAwareUnionSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
