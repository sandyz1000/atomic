use crate::rdd::rdd_val::RddVals;
use crate::rdd::*;
use crate::rdd::{Rdd, RddBase};
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::split::{CoalescedRddSplit, PrefLoc, Split};
use parking_lot::Mutex;
use rand::RngExt;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as SyncOrd};

/// Represents a coalesced RDD that has fewer partitions than its parent RDD
///
/// This type uses the PartitionCoalescer type to find a good partitioning of the parent RDD
/// so that each new partition has roughly the same number of parent partitions and that
/// the preferred location of each new partition overlaps with as many preferred locations of its
/// parent partitions
#[derive(Clone)]
pub struct CoalescedRdd<T>
where
    T: Data,
{
    vals: Arc<RddVals>,
    parent: Arc<dyn Rdd<Item = T>>,
    max_partitions: usize,
}

impl<T: Data> CoalescedRdd<T> {
    /// ## Arguments
    ///
    /// max_partitions: number of desired partitions in the coalesced RDD
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, max_partitions: usize) -> Self {
        let vals = RddVals::new(id);
        CoalescedRdd {
            vals: Arc::new(vals),
            parent: prev,
            max_partitions,
        }
    }
}

impl<T: Data> RddBase for CoalescedRdd<T> {
    fn splits(&self) -> Vec<Box<dyn Split>> {
        let partition_coalescer = DefaultPartitionCoalescer::default();
        partition_coalescer
            .coalesce(self.max_partitions, self.parent.get_rdd_base())
            .into_iter()
            .enumerate()
            .map(|(i, pg)| {
                let ids: Vec<_> = pg.partitions.iter().map(|p| p.get_index()).collect();
                Box::new(CoalescedRddSplit::new(
                    i,
                    pg.preferred_location,
                    self.parent.get_rdd_base(),
                    ids,
                )) as Box<dyn Split>
            })
            .collect()
    }

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::CoalescedSplitDep {
            rdd: self.get_rdd_base(),
            prev: self.parent.get_rdd_base(),
        }]
    }

    /// Returns the preferred machine for the partition. If split is of type CoalescedRddSplit,
    /// then the preferred machine will be one which most parent splits prefer too.
    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        // The preferred location is computed during split creation in splits()
        // We need to look it up from the parent partitions
        if let Ok(coalesced_split) = CoalescedRddSplit::downcasting(split) {
            // Get the preferred locations from parent splits
            let mut location_counts: HashMap<Ipv4Addr, usize> = HashMap::new();
            for parent_idx in &coalesced_split.parent_indices {
                if let Some(parent_split) = self.parent.splits().get(*parent_idx) {
                    for loc in self
                        .parent
                        .get_rdd_base()
                        .preferred_locations(parent_split.clone())
                    {
                        *location_counts.entry(loc).or_insert(0) += 1;
                    }
                }
            }
            // Return the most common location
            location_counts
                .into_iter()
                .max_by_key(|(_, count)| *count)
                .map(|(loc, _)| vec![loc])
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
}

impl<T: Data> Rdd for CoalescedRdd<T> {
    type Item = T;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(CoalescedRdd {
            vals: self.vals.clone(),
            parent: self.parent.clone(),
            max_partitions: self.max_partitions,
        })
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(CoalescedRdd {
            vals: self.vals.clone(),
            parent: self.parent.clone(),
            max_partitions: self.max_partitions,
        }) as Arc<dyn RddBase>
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let split = CoalescedRddSplit::downcasting(split)?;
        let mut iter = Vec::new();
        for (_, p) in self
            .parent
            .splits()
            .into_iter()
            .enumerate()
            .filter(|(i, _)| split.parent_indices.contains(i))
        {
            let it = self.parent.iterator(p)?;
            iter.push(it);
        }
        Ok(Box::new(iter.into_iter().flatten()) as Box<dyn Iterator<Item = Self::Item>>)
    }
}

type SplitIdx = usize;

/// A PartitionCoalescer defines how to coalesce the partitions of a given RDD.
pub trait PartitionCoalescer: Send + Sync {
    // FIXME: Decide upon this really requiring any of those trait bounds.
    // The implementation in Scala embeeds the coalescer into the rdd itself, so on initial
    // transliteration this was added. But in reality the only moment this being called
    // is upon task computation in the driver at the main thread in a completely synchronous and
    // single-threaded environment under the splits subroutine.
    // With the current implementation all those required traits could be dropped entirely.

    /// Coalesce the partitions of the given RDD.
    ///
    /// ## Arguments
    ///
    /// * max_partitions: the maximum number of partitions to have after coalescing
    /// * parent: the parent RDD whose partitions to coalesce
    ///
    /// ## Return
    ///
    /// A vec of `PartitionGroup`s, where each element is itself a vector of
    /// `Partition`s and represents a partition after coalescing is performed.
    fn coalesce(self, max_partitions: usize, parent: Arc<dyn RddBase>) -> Vec<PartitionGroup>;
}

pub struct PartitionGroup {
    id: usize,
    /// preferred location for the partition group
    preferred_location: Option<PrefLoc>,
    partitions: Vec<Box<dyn Split>>,
}

impl PartitionGroup {
    fn new(preferred_location: Option<Ipv4Addr>, id: usize) -> Self {
        PartitionGroup {
            id,
            preferred_location: preferred_location.map(|pl| pl.into()),
            partitions: vec![],
        }
    }

    fn num_partitions(&self) -> usize {
        self.partitions.len()
    }
}

impl PartialEq for PartitionGroup {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PartitionGroup {}

impl PartialOrd for PartitionGroup {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionGroup {
    fn cmp(&self, other: &Self) -> Ordering {
        self.num_partitions().cmp(&other.num_partitions())
    }
}

// Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
// this RDD computes one or more of the parent ones. It will produce exactly `max_partitions` if the
// parent had more than max_partitions, or fewer if the parent had fewer.
//
// This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
// or to avoid having a large number of small tasks when processing a directory with many files.
//
// If there is no locality information (no preferred_locations) in the parent, then the coalescing
// is very simple: chunk parents that are close in the array in chunks.
// If there is locality information, it proceeds to pack them with the following four goals:
//
// (1) Balance the groups so they roughly have the same number of parent partitions
// (2) Achieve locality per partition, i.e. find one machine which most parent partitions prefer
// (3) Be efficient, i.e. O(n) algorithm for n parent partitions (problem is likely NP-hard)
// (4) Balance preferred machines, i.e. avoid as much as possible picking the same preferred machine
//
// Furthermore, it is assumed that the parent RDD may have many partitions, e.g. 100 000.
// We assume the final number of desired partitions is small, e.g. less than 1000.
//
// The algorithm tries to assign unique preferred machines to each partition. If the number of
// desired partitions is greater than the number of preferred machines (can happen), it needs to
// start picking duplicate preferred machines. This is determined using coupon collector estimation
// (2n log(n)). The load balancing is done using power-of-two randomized bins-balls with one twist:
// it tries to also achieve locality. This is done by allowing a slack (balanceSlack, where
// 1.0 is all locality, 0 is all balance) between two bins. If two bins are within the slack
// in terms of balance, the algorithm will assign partitions according to locality.

#[derive(Clone)]
struct PartitionLocations {
    /// contains all the partitions from the previous RDD that don't have preferred locations
    parts_without_locs: Vec<Box<dyn Split>>,
    /// contains all the partitions from the previous RDD that have preferred locations
    parts_with_locs: Vec<(Ipv4Addr, Box<dyn Split>)>,
}

impl PartitionLocations {
    fn new(prev: Arc<dyn RddBase>) -> Self {
        // Gets all the preferred locations of the previous RDD and splits them into partitions
        // with preferred locations and ones without
        let mut tmp_parts_with_loc: Vec<(Box<dyn Split>, Vec<Ipv4Addr>)> = Vec::new();
        let mut parts_without_locs = vec![];
        let mut parts_with_locs = vec![];

        // first get the locations for each partition, only do this once since it can be expensive
        prev.splits().into_iter().for_each(|p| {
            let locs = Self::current_pref_locs(p.clone(), &*prev);
            if !locs.is_empty() {
                tmp_parts_with_loc.push((p, locs));
            } else {
                parts_without_locs.push(p);
            }
        });
        // convert it into an array of host to partition
        for x in 0..=2 {
            for (part, locs) in tmp_parts_with_loc.iter() {
                if locs.len() > x {
                    parts_with_locs.push((locs[x], part.clone()))
                }
            }
        }

        PartitionLocations {
            parts_without_locs,
            parts_with_locs,
        }
    }

    /// Gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones).
    fn current_pref_locs(part: Box<dyn Split>, prev: &dyn RddBase) -> Vec<Ipv4Addr> {
        // TODO: this is inefficient and likely to happen in more places,
        //we should add a preferred_locs method that takes split by ref (&dyn Split) not by value
        prev.preferred_locations(part)
    }
}

/// A group of `Partition`s
struct PSyncGroup(Mutex<PartitionGroup>);

impl std::convert::From<PartitionGroup> for PSyncGroup {
    fn from(origin: PartitionGroup) -> Self {
        PSyncGroup(Mutex::new(origin))
    }
}

impl std::ops::Deref for PSyncGroup {
    type Target = Mutex<PartitionGroup>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
struct DefaultPartitionCoalescer {
    balance_slack: f64,
    /// each element of group arr represents one coalesced partition
    group_arr: Vec<Arc<PSyncGroup>>,
    /// hash used to check whether some machine is already in group_arr
    group_hash: HashMap<Ipv4Addr, Vec<Arc<PSyncGroup>>>,
    /// hash used for the first max_partitions (to avoid duplicates)
    initial_hash: HashSet<SplitIdx>,
    /// true if no preferred_locations exists for parent RDD
    no_locality: bool,
}

impl Default for DefaultPartitionCoalescer {
    fn default() -> Self {
        DefaultPartitionCoalescer {
            balance_slack: 0.10,
            group_arr: Vec::new(),
            group_hash: HashMap::new(),
            initial_hash: HashSet::new(),
            no_locality: true,
        }
    }
}

impl DefaultPartitionCoalescer {
    fn new(balance_slack: Option<f64>) -> Self {
        if let Some(slack) = balance_slack {
            DefaultPartitionCoalescer {
                balance_slack: slack,
                group_arr: Vec::new(),
                group_hash: HashMap::new(),
                initial_hash: HashSet::new(),
                no_locality: true,
            }
        } else {
            Self::default()
        }
    }

    fn add_part_to_pgroup(&mut self, part: Box<dyn Split>, pgroup: &mut PartitionGroup) -> bool {
        if !self.initial_hash.contains(&part.get_index()) {
            self.initial_hash.insert(part.get_index()); // needed to avoid assigning partitions to multiple buckets
            pgroup.partitions.push(part); // already assign this element
            true
        } else {
            false
        }
    }

    /// Gets the least element of the list associated with key in group_hash
    /// The returned PartitionGroup is the least loaded of all groups that represent the machine "key"
    fn get_least_group_hash(&self, key: Ipv4Addr) -> Option<Arc<PSyncGroup>> {
        let mut current_min: Option<Arc<PSyncGroup>> = None;
        if let Some(group) = self.group_hash.get(&key) {
            for g in group.as_slice() {
                if let Some(ref cmin) = current_min
                    && *cmin.lock() > *g.lock() {
                        current_min = Some((*g).clone());
                    }
            }
        }
        current_min
    }

    /// Initializes target_len partition groups. If there are preferred locations, each group
    /// is assigned a preferred location. This uses coupon collector to estimate how many
    /// preferred locations it must rotate through until it has seen most of the preferred
    /// locations (2 * n log(n))
    ///
    /// ## Arguments
    ///
    /// * target_len - the number of desired partition groups
    #[allow(clippy::map_entry)]
    fn setup_groups(&mut self, target_len: usize, partition_locs: &mut PartitionLocations) {
        let mut rng = atomic_utils::random::get_default_rng();
        let part_cnt = AtomicUsize::new(0);

        // deal with empty case, just create target_len partition groups with no preferred location
        if partition_locs.parts_with_locs.is_empty() {
            for _ in 1..=target_len {
                self.group_arr
                    .push(Arc::new(PSyncGroup(Mutex::new(PartitionGroup::new(
                        None,
                        part_cnt.fetch_add(1, SyncOrd::SeqCst),
                    )))))
            }
            return;
        }

        self.no_locality = false;

        // number of iterations needed to be certain that we've seen most preferred locations
        let expected_coupons_2 = {
            let target_len = target_len as f64;
            2 * (target_len.ln() * target_len + target_len + 0.5f64) as u64
        };

        let mut num_created = 0;
        let mut tries = 0;

        // rotate through until either target_len unique/distinct preferred locations have been created
        // OR (we have went through either all partitions OR we've rotated expected_coupons_2 - in
        // which case we have likely seen all preferred locations)
        let num_parts_to_look_at =
            expected_coupons_2.min(partition_locs.parts_with_locs.len() as u64);
        while num_created < target_len as u64 && tries < num_parts_to_look_at {
            let (nxt_replica, nxt_part) = &partition_locs.parts_with_locs[tries as usize];
            tries += 1;

            if !self.group_hash.contains_key(nxt_replica) {
                let mut pgroup =
                    PartitionGroup::new(Some(*nxt_replica), part_cnt.fetch_add(1, SyncOrd::SeqCst));
                self.add_part_to_pgroup(dyn_clone::clone_box(&**nxt_part), &mut pgroup);
                self.group_hash
                    .insert(*nxt_replica, vec![Arc::new(PSyncGroup(Mutex::new(pgroup)))]); // list in case we have multiple
                num_created += 1;
            }
        }

        // if we don't have enough partition groups, create duplicates
        while num_created < target_len as u64 {
            // Copy the preferred location from a random input partition.
            // This helps in avoiding skew when the input partitions are clustered by preferred location.
            let (nxt_replica, nxt_part) = &partition_locs.parts_with_locs
                [rng.random_range(0..partition_locs.parts_with_locs.len()) as usize];
            let pgroup = Arc::new(PSyncGroup(Mutex::new(PartitionGroup::new(
                Some(*nxt_replica),
                part_cnt.fetch_add(1, SyncOrd::SeqCst),
            ))));
            self.add_part_to_pgroup(
                dyn_clone::clone_box(&**nxt_part),
                &mut pgroup.lock(),
            );
            self.group_hash
                .entry(*nxt_replica)
                .or_default()
                .push(pgroup.clone());
            self.group_arr.push(pgroup);
            num_created += 1;
        }
    }

    /// Takes a parent RDD partition and decides which of the partition groups to put it in
    /// Takes locality into account, but also uses power of 2 choices to load balance
    /// It strikes a balance between the two using the balance_slack variable
    ///
    /// ## Arguments
    ///
    /// * p: partition (ball to be thrown)
    /// * balance_slack: determines the trade-off between load-balancing the partitions sizes and
    ///   their locality. e.g., balance_slack=0.10 means that it allows up to 10%
    ///   imbalance in favor of locality
    fn pick_bin(
        &mut self,
        p: Box<dyn Split>,
        prev: &dyn RddBase,
        balance_slack: f64,
    ) -> Arc<PSyncGroup> {
        let mut rnd = atomic_utils::random::get_default_rng();
        let slack = balance_slack * prev.number_of_splits() as f64;

        // least loaded pref_locs
        let pref: Vec<_> = PartitionLocations::current_pref_locs(p, prev)
            .into_iter()
            .map(|i| self.get_least_group_hash(i))
            .collect();

        let pref_part = if pref.is_empty() {
            None
        } else {
            let mut min: Option<Arc<PSyncGroup>> = None;
            for pl in pref.into_iter().flatten() {
                if let Some(ref pl_min) = min {
                    if *pl.lock() < *pl_min.lock() {
                        min = Some(pl)
                    }
                } else {
                    min = Some(pl);
                }
            }
            min
            // pref.iter().enumerate().map(|i| &*(***i).lock()).min()
        };

        let r1 = rnd.random_range(0..self.group_arr.len());
        let r2 = rnd.random_range(0..self.group_arr.len());

        let min_power_of_two = {
            if self.group_arr[r1].lock().num_partitions()
                < self.group_arr[r2].lock().num_partitions()
            {
                self.group_arr[r1].clone()
            } else {
                self.group_arr[r2].clone()
            }
        };

        if let Some(pref_part_actual) = pref_part {
            // more imbalance than the slack allows
            if min_power_of_two.lock().num_partitions() + slack as usize
                <= pref_part_actual.lock().num_partitions()
            {
                min_power_of_two // prefer balance over locality
            } else {
                pref_part_actual // prefer locality over balance
            }
        } else {
            // if no preferred locations, just use basic power of two
            min_power_of_two
        }
    }

    fn throw_balls(
        &mut self,
        max_partitions: usize,
        prev: Arc<dyn RddBase>,
        balance_slack: f64,
        mut partition_locs: PartitionLocations,
    ) {
        if self.no_locality {
            // no preferred_locations in parent RDD, no randomization needed
            if max_partitions > self.group_arr.len() {
                // just return prev.partitions
                for (i, p) in prev.splits().into_iter().enumerate() {
                    self.group_arr[i].lock().partitions.push(p);
                }
            } else {
                // no locality available, then simply split partitions based on positions in array
                let prev_splits = prev.splits();
                let chunk_size =
                    (prev_splits.len() as f64 / max_partitions as f64).floor() as usize;
                let mut chunk = 0;
                for (i, e) in prev_splits.into_iter().enumerate() {
                    if i % chunk_size == 0 && chunk + 1 < max_partitions && i != 0 {
                        chunk += 1;
                    }
                    self.group_arr[chunk].lock().partitions.push(e);
                }
            }
        } else {
            // It is possible to have unionRDD where one rdd has preferred locations and another rdd
            // that doesn't. To make sure we end up with the requested number of partitions,
            // make sure to put a partition in every group.

            // if we don't have a partition assigned to every group first try to fill them
            // with the partitions with preferred locations
            let mut part_iter = partition_locs.parts_with_locs.drain(..).peekable();
            for pg in self
                .group_arr
                .iter()
                .filter(|pg| pg.lock().num_partitions() == 0)
            {
                while part_iter.peek().is_some() && pg.lock().num_partitions() == 0 {
                    let (_, nxt_part) = part_iter.next().unwrap();
                    if !self.initial_hash.contains(&nxt_part.get_index()) {
                        self.initial_hash.insert(nxt_part.get_index());
                        pg.lock().partitions.push(nxt_part);
                    }
                }
            }

            // if we didn't get one partitions per group from partitions with preferred locations
            // use partitions without preferred locations
            let mut part_no_loc_iter = partition_locs.parts_without_locs.drain(..).peekable();
            for pg in self
                .group_arr
                .iter()
                .filter(|pg| pg.lock().num_partitions() == 0)
            {
                while part_no_loc_iter.peek().is_some() && pg.lock().num_partitions() == 0 {
                    let nxt_part = part_no_loc_iter.next().unwrap();
                    if !self.initial_hash.contains(&nxt_part.get_index()) {
                        self.initial_hash.insert(nxt_part.get_index());
                        pg.lock().partitions.push(nxt_part);
                    }
                }
            }

            // finally pick bin for the rest
            for p in prev.splits().into_iter() {
                if !self.initial_hash.contains(&p.get_index()) {
                    // throw every partition into group
                    self.pick_bin(p.clone(), &*prev, balance_slack)
                        .lock()
                        .partitions
                        .push(p);
                }
            }
        }
    }

    fn get_partitions(self) -> Vec<PartitionGroup> {
        self.group_arr
            .into_iter()
            .filter(|pg| pg.lock().num_partitions() > 0)
            .map(|pg: Arc<PSyncGroup>| {
                let pg: PSyncGroup = Arc::try_unwrap(pg)
                    .map_err(|_| "Not unique reference.")
                    .unwrap();
                pg.0.into_inner()
            })
            .collect()
    }
}

impl PartitionCoalescer for DefaultPartitionCoalescer {
    /// Runs the packing algorithm and returns an array of InnerPGroups that if possible are
    /// load balanced and grouped by locality
    fn coalesce(mut self, max_partitions: usize, prev: Arc<dyn RddBase>) -> Vec<PartitionGroup> {
        let mut partition_locs = PartitionLocations::new(prev.clone());
        // setup the groups (bins)
        let target_len = prev.number_of_splits().min(max_partitions);
        self.setup_groups(target_len, &mut partition_locs);
        // assign partitions (balls) to each group (bins)
        self.throw_balls(
            max_partitions,
            prev.clone(),
            self.balance_slack,
            partition_locs,
        );
        self.get_partitions()
    }
}
