use std::{any::Any, net::Ipv4Addr, sync::Arc};

use dyn_clone::DynClone;

use crate::{error::BaseError, rdd::RddBase};

pub struct SplitStruct {
    index: usize,
}

pub trait Split: DynClone {
    fn get_index(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

dyn_clone::clone_trait_object!(Split);

#[derive(Debug, Clone, Copy)]
struct PrefLoc(u32);

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
    fn new(
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
    fn local_fraction(&self) -> f64 {
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
