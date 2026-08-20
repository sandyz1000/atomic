use atomic_data::dependency::ShuffleDependency;
use atomic_data::rdd::RddBase;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;

/// Stage in the DAG scheduler
#[derive(Clone)]
pub struct Stage {
    pub id: usize,
    pub num_partitions: usize,
    pub shuffle_dependency: Option<Arc<ShuffleDependency>>,
    pub is_shuffle_map: bool,
    pub rdd: Arc<dyn RddBase>,
    pub parents: Vec<Stage>,
    pub output_locs: Vec<Vec<String>>,
    pub num_available_outputs: usize,
}

impl PartialOrd for Stage {
    fn partial_cmp(&self, other: &Stage) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Stage {
    fn eq(&self, other: &Stage) -> bool {
        self.id == other.id
    }
}

impl Eq for Stage {}

impl Ord for Stage {
    fn cmp(&self, other: &Stage) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Stage {}", self.id)
    }
}
impl Stage {
    pub fn get_rdd(&self) -> Arc<dyn RddBase> {
        self.rdd.clone()
    }

    pub fn new(
        id: usize,
        rdd: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<ShuffleDependency>>,
        parents: Vec<Stage>,
    ) -> Self {
        // A staged shuffle's `rdd` (`get_rdd_base()`) is a 1-partition placeholder — the real
        // map-side partition count lives on the dependency itself. See
        // `ShuffleDependency::num_map_partitions`'s doc for why this can't just be
        // `rdd.number_of_splits()` unconditionally.
        let num_partitions = shuffle_dependency
            .as_ref()
            .map(|dep| dep.num_map_partitions())
            .unwrap_or_else(|| rdd.number_of_splits());
        Stage {
            id,
            num_partitions,
            is_shuffle_map: shuffle_dependency.is_some(),
            shuffle_dependency,
            parents,
            rdd,
            output_locs: vec![Vec::new(); num_partitions],
            num_available_outputs: 0,
        }
    }

    pub fn is_available(&self) -> bool {
        if self.parents.is_empty() && !self.is_shuffle_map {
            true
        } else {
            log::debug!(
                "num available outputs {}, and num partitions {}, in is available method in stage",
                self.num_available_outputs,
                self.num_partitions
            );
            self.num_available_outputs == self.num_partitions
        }
    }

    pub fn add_output_loc(&mut self, partition: usize, host: String) {
        log::debug!(
            "adding loc for partition inside stage {} @{}",
            partition,
            host
        );
        if self.output_locs[partition].is_empty() {
            self.num_available_outputs += 1;
        }
        self.output_locs[partition].push(host);
    }

    pub fn remove_output_loc(&mut self, partition: usize, host: &str) {
        let locs = &mut self.output_locs[partition];
        let was_nonempty = !locs.is_empty();
        locs.retain(|x| x != host);
        if was_nonempty && locs.is_empty() {
            self.num_available_outputs -= 1;
        }
    }
}
