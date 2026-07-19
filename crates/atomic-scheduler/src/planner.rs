//! DAG planning: turning an RDD lineage into a graph of [`Stage`]s.
//!
//! [`StagePlanner`] holds the backend-agnostic planning logic — building stages, walking
//! dependencies to find parent and missing-parent stages, and computing preferred
//! locations. Execution (task submission, the completion-event loop) lives in
//! [`NativeScheduler`](crate::base::NativeScheduler), which builds on this trait.

use std::collections::{BTreeSet, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;

use atomic_data::dependency::{Dependency, ErasedShuffleDependency};
use atomic_data::rdd::RddBase;

use crate::base::SchedulerState;
use crate::error::LibResult;
use crate::stage::Stage;

/// Backend-agnostic DAG planning over the shared [`SchedulerState`].
///
/// Implementors provide [`state`](Self::state) and [`get_shuffle_map_stage`](Self::get_shuffle_map_stage);
/// everything else (stage construction, parent-stage discovery, preferred locations) is a
/// default method shared by every scheduler.
#[async_trait::async_trait]
pub trait StagePlanner: Send + Sync {
    /// The shared scheduler state (stage cache, map-output tracker, id counters).
    fn state(&self) -> SchedulerState;

    /// Return (creating if needed) the shuffle-map stage for a shuffle dependency.
    /// Backend-specific: the distributed scheduler caches completed shuffles, the local
    /// scheduler pre-populates output locations from an earlier distributed run.
    async fn get_shuffle_map_stage(&self, shuf: Arc<ErasedShuffleDependency>) -> LibResult<Stage>;

    /// Create a new stage for `rdd_base`, registering its shuffle dependency (if any) with
    /// the map-output tracker and discovering its parent stages.
    async fn new_stage(
        &self,
        rdd_base: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<ErasedShuffleDependency>>,
    ) -> LibResult<Stage> {
        log::debug!("creating new stage");
        let m = self.state();
        if let Some(dep) = shuffle_dependency.clone() {
            log::debug!("shuffle dependency exists, registering to map output tracker");
            m.register_shuffle(dep.get_shuffle_id(), rdd_base.number_of_splits());
            log::debug!("new stage tracker after");
        }
        let id = m.get_next_stage_id();
        log::debug!("new stage id #{}", id);
        let stage = Stage::new(
            id,
            rdd_base.clone(),
            shuffle_dependency,
            self.get_parent_stages(rdd_base).await?,
        );
        m.insert_into_stage_cache(id, stage.clone());
        log::debug!("returning new stage #{}", id);
        Ok(stage)
    }

    /// Recursively collect the not-yet-available shuffle-map stages upstream of `rdd`.
    async fn visit_missing_parent<'s>(
        &'s self,
        missing: &'s mut BTreeSet<Stage>,
        visited: &'s mut HashSet<usize>,
        rdd: Arc<dyn RddBase>,
    ) -> LibResult<()> {
        log::debug!(
            "missing stages: {:?}",
            missing.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        log::debug!("visited rdd ids: {:?}", visited);
        let rdd_id = rdd.get_rdd_id();
        if !visited.contains(&rdd_id) {
            visited.insert(rdd_id);
            for _ in 0..rdd.number_of_splits() {
                let locs = self.state().get_cache_locs(rdd.clone());
                log::debug!("cache locs: {:?}", locs);
                if locs.is_none() {
                    for dep in rdd.get_dependencies() {
                        log::debug!("for dep in missing stages ");
                        match dep {
                            Dependency::Shuffle(shuf_dep) => {
                                let stage = self.get_shuffle_map_stage(shuf_dep.clone()).await?;
                                log::debug!("shuffle stage #{} in missing stages", stage.id);
                                if !stage.is_available() {
                                    log::debug!(
                                        "inserting shuffle stage #{} in missing stages",
                                        stage.id
                                    );
                                    missing.insert(stage);
                                }
                            }
                            Dependency::OneToOne { rdd_base }
                            | Dependency::Range { rdd_base, .. }
                            | Dependency::CoalescedSplitDep {
                                rdd: rdd_base,
                                prev: _,
                            } => {
                                log::debug!("narrow stage in missing stages");
                                self.visit_missing_parent(missing, visited, rdd_base)
                                    .await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Recursively collect the direct parent shuffle-map stages of `rdd`.
    async fn visit_for_parent_stages<'s>(
        &'s self,
        parents: &'s mut BTreeSet<Stage>,
        visited: &'s mut HashSet<usize>,
        rdd: Arc<dyn RddBase>,
    ) -> LibResult<()> {
        log::debug!(
            "parent stages: {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        log::debug!("visited rdd ids: {:?}", visited);
        let rdd_id = rdd.get_rdd_id();
        if !visited.contains(&rdd_id) {
            visited.insert(rdd_id);
            for dep in rdd.get_dependencies() {
                match dep {
                    Dependency::Shuffle(shuf_dep) => {
                        parents.insert(self.get_shuffle_map_stage(shuf_dep.clone()).await?);
                    }
                    Dependency::OneToOne { rdd_base }
                    | Dependency::Range { rdd_base, .. }
                    | Dependency::CoalescedSplitDep {
                        rdd: rdd_base,
                        prev: _,
                    } => {
                        self.visit_for_parent_stages(parents, visited, rdd_base)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The parent stages of `rdd`, in stage-id order.
    async fn get_parent_stages(&self, rdd: Arc<dyn RddBase>) -> LibResult<Vec<Stage>> {
        log::debug!("inside get parent stages");
        let mut parents: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: HashSet<usize> = HashSet::new();
        self.visit_for_parent_stages(&mut parents, &mut visited, rdd.clone())
            .await?;
        log::debug!(
            "parent stages: {:?}",
            parents.iter().map(|x| x.id).collect::<Vec<_>>()
        );
        Ok(parents.into_iter().collect())
    }

    /// The not-yet-available parent stages of `stage` — the stages that must complete
    /// before `stage` can run.
    async fn get_missing_parent_stages(&self, stage: Stage) -> LibResult<Vec<Stage>> {
        log::debug!("getting missing parent stages");
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: HashSet<usize> = HashSet::new();
        self.visit_missing_parent(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }

    /// Preferred worker IPs for `partition` of `rdd`: cached locations first, then the
    /// RDD's own preferences, then recursively its narrow parents'.
    fn get_preferred_locs(&self, rdd: Arc<dyn RddBase>, partition: usize) -> Vec<Ipv4Addr> {
        if let Some(cached) = self.state().get_cache_locs(rdd.clone())
            && let Some(cached) = cached.get(partition)
        {
            return cached.clone();
        }
        let rdd_prefs = rdd.preferred_locations(rdd.splits()[partition].clone());
        if !rdd.is_pinned() {
            if !rdd_prefs.is_empty() {
                return rdd_prefs;
            }
            for dep in rdd.get_dependencies().iter() {
                match dep {
                    Dependency::OneToOne { .. }
                    | Dependency::Range { .. }
                    | Dependency::CoalescedSplitDep { .. } => {
                        for in_part in dep.get_parents(partition) {
                            let locs = self.get_preferred_locs(dep.get_rdd_base(), in_part);
                            if !locs.is_empty() {
                                return locs;
                            }
                        }
                    }
                    Dependency::Shuffle(_) => {
                        // Shuffle dependencies don't have preferred locations
                    }
                }
            }
            Vec::new()
        } else {
            // when pinned, exactly one preferred location is required for a partition
            assert!(rdd_prefs.len() == 1);
            rdd_prefs
        }
    }
}
