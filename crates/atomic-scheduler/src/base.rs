use crate::dag::CompletionEvent;
use crate::dag::FetchFailedVals;
use crate::error::{LibResult, SchedulerError};
use crate::job::JobTracker;
use crate::listener::JobListener;
use crate::stage::Stage;
use atomic_data::data::Data;
use atomic_data::dependency::{Dependency, ShuffleDependencyBox};
use atomic_data::rdd::RddBase;
use atomic_data::shuffle::MapOutputTracker;
use atomic_data::task::TaskOption;
use atomic_data::task::result::ResultTask;
use atomic_data::task::shuffle_map::ShuffleMapTask;
use atomic_data::task_context::TaskContext;
use dashmap::DashMap;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub type EventQueue = Arc<DashMap<usize, VecDeque<CompletionEvent>>>;

// pub type RddFunc<T, U> =
//     Arc<dyn Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static>;

/// Functionality of the library built-in schedulers
#[async_trait::async_trait]
pub trait NativeScheduler: Send + Sync {
    fn get_mutators(&self) -> Mutators;

    /// Fast path for execution. Runs the DD in the driver main thread if possible.
    fn local_execution<T: Data, U: Data, F, L>(
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<Option<Vec<U>>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync,
        L: JobListener,
    {
        if jt.final_stage.parents.is_empty() && (jt.num_output_parts == 1) {
            let split = (jt.final_rdd.splits()[jt.output_parts[0]]).clone();
            let task_context = TaskContext::new(jt.final_stage.id, jt.output_parts[0], 0);
            let iter = jt
                .final_rdd
                .iterator(split)
                .map_err(|_| SchedulerError::Other)?;
            Ok(Some(vec![(jt.func)((task_context, iter))]))
        } else {
            Ok(None)
        }
    }

    async fn new_stage(
        &self,
        rdd_base: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<ShuffleDependencyBox>>,
    ) -> LibResult<Stage> {
        log::debug!("creating new stage");
        // TODO: Cache tracker - for LocalScheduler, cache is managed locally
        // For now, we skip cache registration in base scheduler
        let m = self.get_mutators();
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
            // TODO: CacheTracker register
            for _ in 0..rdd.number_of_splits() {
                let locs = self.get_mutators().get_cache_locs(rdd.clone());
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
            // TODO: Cache tracker - for LocalScheduler, cache is managed locally
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

    async fn on_event_failure<T: Data, U: Data, F, L>(
        &self,
        jt: Arc<JobTracker<F, U, T, L>>,
        failed_vals: FetchFailedVals,
        stage_id: usize,
    ) where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync,
        L: JobListener,
    {
        let FetchFailedVals {
            server_uri,
            shuffle_id,
            map_id,
            ..
        } = failed_vals;

        // TODO: mapoutput tracker needs to be finished for this
        // let failed_stage = self.id_to_stage.lock().get(&stage_id).?.clone();
        let m = self.get_mutators();
        let failed_stage = m.fetch_from_stage_cache(stage_id);
        jt.running.lock().await.remove(&failed_stage);
        jt.failed.lock().await.insert(failed_stage);
        // TODO: logging
        m.remove_output_loc_from_stage(shuffle_id, map_id, &server_uri);
        m.unregister_map_output(shuffle_id, map_id, server_uri);
        jt.failed
            .lock()
            .await
            .insert(m.fetch_from_shuffle_to_cache(shuffle_id));
    }

    async fn on_event_success<T: Data, U: Data + Clone, F, L>(
        &self,
        mut completed_event: CompletionEvent,
        results: &mut Vec<Option<U>>,
        num_finished: &mut usize,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<()>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        L: JobListener,
    {
        // TODO: logging
        // TODO: add to Accumulator

        match &completed_event.task {
            TaskOption::ResultTask(rt) => {
                let result = completed_event
                    .result
                    .take()
                    .ok_or(SchedulerError::Other)?;

                jt.listener.task_succeeded(rt.output_id, &*result).await?;

                let typed_result = result
                    .as_any()
                    .downcast_ref::<U>()
                    .ok_or_else(|| {
                        SchedulerError::DowncastFailure(
                            "Failed to downcast result to expected type U".to_string(),
                        )
                    })?
                    .clone();

                results[rt.output_id] = Some(typed_result);
                jt.finished.lock().await[rt.output_id] = true;
                *num_finished += 1;
            }
            TaskOption::ShuffleMapTask(smt) => {
                let shuffle_server_uri = completed_event
                    .result
                    .take()
                    .ok_or(SchedulerError::Other)?
                    .as_any()
                    .downcast_ref::<String>()
                    .ok_or_else(|| SchedulerError::DowncastFailure("String".to_string()))?
                    .clone();
                log::debug!(
                    "completed shuffle task server uri: {:?}",
                    shuffle_server_uri
                );
                let m = self.get_mutators();
                m.add_output_loc_to_stage(smt.stage_id, smt.partition, shuffle_server_uri);

                let stage = m.fetch_from_stage_cache(smt.stage_id);
                log::debug!(
                    "pending stages: {:?}",
                    jt.pending_tasks
                        .lock()
                        .await
                        .iter()
                        .map(|(x, y)| (x.id, y.iter().map(|k| k.get_task_id()).collect::<Vec<_>>()))
                        .collect::<Vec<_>>()
                );
                log::debug!(
                    "pending tasks: {:?}",
                    jt.pending_tasks
                        .lock()
                        .await
                        .get(&stage)
                        .ok_or(SchedulerError::Other)?
                        .iter()
                        .map(|x| x.get_task_id())
                        .collect::<Vec<_>>()
                );
                log::debug!(
                    "running stages: {:?}",
                    jt.running
                        .lock()
                        .await
                        .iter()
                        .map(|x| x.id)
                        .collect::<Vec<_>>()
                );
                log::debug!(
                    "waiting stages: {:?}",
                    jt.waiting
                        .lock()
                        .await
                        .iter()
                        .map(|x| x.id)
                        .collect::<Vec<_>>()
                );

                if jt.running.lock().await.contains(&stage)
                    && jt
                        .pending_tasks
                        .lock()
                        .await
                        .get(&stage)
                        .ok_or(SchedulerError::Other)?
                        .is_empty()
                {
                    log::debug!("started registering map outputs");
                    // TODO: logging
                    jt.running.lock().await.remove(&stage);
                    if let Some(dep) = stage.shuffle_dependency {
                        log::debug!(
                            "stage output locs before register mapoutput tracker: {:?}",
                            stage.output_locs
                        );
                        let locs = stage
                            .output_locs
                            .iter()
                            .map(|x| x.first().map(|s| s.to_owned()))
                            .collect();
                        let shuffle_id = dep.get_shuffle_id();
                        log::debug!("locs for shuffle id #{}: {:?}", shuffle_id, locs);
                        m.register_map_outputs(shuffle_id, locs);
                        log::debug!("finished registering map outputs");

                        // ── Adaptive coalescing ───────────────────────────────
                        // After the map stage completes, compute the optimal number
                        // of reduce partitions based on actual bucket byte sizes.
                        if m.coalesce_threshold_bytes > 0 {
                            m.compute_coalescing(shuffle_id, stage.num_partitions);
                        }
                    }
                    // TODO: Cache
                    self.update_cache_locs().await?;
                    let mut newly_runnable = Vec::new();
                    let waiting_stages: Vec<_> = jt.waiting.lock().await.iter().cloned().collect();
                    for stage in waiting_stages {
                        let missing_stages = self.get_missing_parent_stages(stage.clone()).await?;
                        log::debug!(
                            "waiting stage parent stages for stage #{} are: {:?}",
                            stage.id,
                            missing_stages.iter().map(|x| x.id).collect::<Vec<_>>()
                        );
                        if missing_stages.is_empty() {
                            newly_runnable.push(stage.clone())
                        }
                    }
                    for stage in &newly_runnable {
                        jt.waiting.lock().await.remove(stage);
                    }
                    for stage in &newly_runnable {
                        jt.running.lock().await.insert(stage.clone());
                    }
                    for stage in newly_runnable {
                        self.submit_missing_tasks(stage, jt.clone()).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn submit_stage<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<()>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        L: JobListener,
    {
        log::debug!("submitting stage #{}", stage.id);
        if !jt.waiting.lock().await.contains(&stage) && !jt.running.lock().await.contains(&stage) {
            let missing = self.get_missing_parent_stages(stage.clone()).await?;
            log::debug!(
                "while submitting stage #{}, missing stages: {:?}",
                stage.id,
                missing.iter().map(|x| x.id).collect::<Vec<_>>()
            );
            if missing.is_empty() {
                self.submit_missing_tasks(stage.clone(), jt.clone()).await?;
                jt.running.lock().await.insert(stage);
            } else {
                for parent in missing {
                    self.submit_stage(parent, jt.clone()).await?;
                }
                jt.waiting.lock().await.insert(stage);
            }
        }
        Ok(())
    }

    async fn submit_missing_tasks<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<()>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        L: JobListener,
    {
        let m = self.get_mutators();
        let mut pending_tasks = jt.pending_tasks.lock().await;
        let my_pending = pending_tasks
            .entry(stage.clone())
            .or_insert_with(BTreeSet::new);
        if stage == jt.final_stage {
            log::debug!("final stage #{}", stage.id);
            for (id_in_job, (id, part)) in jt
                .output_parts
                .iter()
                .enumerate()
                .take(jt.num_output_parts)
                .enumerate()
            {
                let locs = self.get_preferred_locs(jt.final_rdd.get_rdd_base(), *part);
                let result_task = ResultTask::new(
                    m.get_next_task_id(),
                    jt.run_id,
                    jt.final_stage.id,
                    jt.final_rdd.clone(),
                    jt.func.clone(),
                    *part,
                    locs.clone(),
                    id,
                );
                let task_option = TaskOption::ResultTask(result_task.into());
                let executor = self.next_executor_server(&task_option);
                my_pending.insert(task_option.clone());
                self.submit_task::<T, U, F>(task_option, id_in_job, executor)
            }
        } else {
            for p in 0..stage.num_partitions {
                log::debug!("shuffle stage #{}", stage.id);
                if stage.output_locs[p].is_empty() {
                    let locs = self.get_preferred_locs(stage.get_rdd(), p);
                    log::debug!("creating task for stage #{} partition #{}", stage.id, p);
                    let shuffle_dep = Arc::new(Dependency::Shuffle(
                        stage
                            .shuffle_dependency
                            .clone()
                            .ok_or(SchedulerError::Other)?,
                    ));
                    let shuffle_map_task = ShuffleMapTask::new(
                        m.get_next_task_id(),
                        jt.run_id,
                        stage.id,
                        stage.rdd.clone(),
                        shuffle_dep,
                        p,
                        locs,
                    );
                    log::debug!(
                        "creating task for stage #{}, partition #{} and shuffle id #{:?}",
                        stage.id,
                        p,
                        shuffle_map_task.dep.get_shuffle_id()
                    );
                    let task_option = TaskOption::ShuffleMapTask(shuffle_map_task);
                    let executor = self.next_executor_server(&task_option);
                    my_pending.insert(task_option.clone());
                    self.submit_task::<T, U, F>(task_option, p, executor);
                }
            }
        }
        Ok(())
    }

    fn wait_for_event(&self, run_id: usize, timeout: u64) -> Option<CompletionEvent> {
        // TODO: make use of async to wait for events
        let end = Instant::now() + Duration::from_millis(timeout);
        let mutators = self.get_mutators();
        let event_queue = mutators.get_event_queue();
        while event_queue.get(&run_id)?.is_empty() {
            if Instant::now() > end {
                return None;
            }
            thread::sleep(Duration::from_millis(10));
        }
        event_queue.get_mut(&run_id)?.pop_front()
    }

    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        id_in_job: usize,
        target_executor: SocketAddrV4,
    ) where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U;

    // mutators:
    // fn add_output_loc_to_stage(&self, stage_id: usize, partition: usize, host: String);
    // fn insert_into_stage_cache(&self, id: usize, stage: Stage);
    // /// refreshes cache locations
    // fn register_shuffle(&self, shuffle_id: usize, num_maps: usize);
    // fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>);
    // fn remove_output_loc_from_stage(&self, shuffle_id: usize, map_id: usize, server_uri: &str);
    async fn update_cache_locs(&self) -> LibResult<()>;
    // fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String);

    // // getters:
    // fn fetch_from_stage_cache(&self, id: usize) -> Stage;
    // fn fetch_from_shuffle_to_cache(&self, id: usize) -> Stage;
    // fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>>;
    // fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>>;
    async fn get_missing_parent_stages<'a>(&'a self, stage: Stage) -> LibResult<Vec<Stage>>;
    // fn get_next_job_id(&self) -> usize;
    // fn get_next_stage_id(&self) -> usize;
    // fn get_next_task_id(&self) -> usize;
    fn next_executor_server(&self, task: &TaskOption) -> SocketAddrV4;

    fn get_preferred_locs(&self, rdd: Arc<dyn RddBase>, partition: usize) -> Vec<Ipv4Addr> {
        // TODO: have to implement this completely
        if let Some(cached) = self.get_mutators().get_cache_locs(rdd.clone())
            && let Some(cached) = cached.get(partition) {
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
            // when pinned, is required that there is exactly one preferred location
            // for a given partition
            assert!(rdd_prefs.len() == 1);
            rdd_prefs
        }
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<ShuffleDependencyBox>) -> LibResult<Stage>;
}

#[derive(Clone, Default)]
pub struct Mutators {
    pub stage_cache: Arc<DashMap<usize, Stage>>,
    pub map_output_tracker: Option<Arc<MapOutputTracker>>,
    pub shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    pub event_queues: EventQueue,
    /// Per-RDD cached partition locations. Cleared by `update_cache_locs` when
    /// shuffle stages complete or fetch failures are detected.
    pub cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    /// Monotonically increasing counter used to allocate unique job IDs.
    pub next_job_id: Arc<AtomicUsize>,
    /// Monotonically increasing counter used to allocate unique task IDs.
    pub next_task_id: Arc<AtomicUsize>,
    /// Monotonically increasing counter used to allocate unique stage IDs.
    pub next_stage_id: Arc<AtomicUsize>,
    /// Adaptive coalescing threshold in bytes (0 = disabled).
    /// Copied from `Config::coalesce_shuffle_threshold_bytes` at context init.
    pub coalesce_threshold_bytes: u64,
}

impl Mutators {
    pub fn new() -> Self {
        Self {
            stage_cache: Arc::new(DashMap::new()),
            map_output_tracker: atomic_data::env::get_map_output_tracker(),
            shuffle_to_map_stage: Arc::new(DashMap::new()),
            event_queues: Arc::new(DashMap::new()),
            cache_locs: Arc::new(DashMap::new()),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            coalesce_threshold_bytes: 0,
        }
    }

    pub fn with_coalesce_threshold(mut self, bytes: u64) -> Self {
        self.coalesce_threshold_bytes = bytes;
        self
    }

    #[inline]
    pub fn add_output_loc_to_stage(&self, stage_id: usize, partition: usize, host: String) {
        self.stage_cache
            .get_mut(&stage_id)
            .unwrap()
            .add_output_loc(partition, host);
    }

    #[inline]
    pub fn insert_into_stage_cache(&self, id: usize, stage: Stage) {
        self.stage_cache.insert(id, stage.clone());
    }

    #[inline]
    pub fn fetch_from_stage_cache(&self, id: usize) -> Stage {
        self.stage_cache.get(&id).unwrap().clone()
    }

    #[inline]
    pub fn fetch_from_shuffle_to_cache(&self, id: usize) -> Stage {
        self.shuffle_to_map_stage.get(&id).unwrap().clone()
    }

    #[inline]
    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        if let Some(tracker) = &self.map_output_tracker {
            tracker.unregister_map_output(shuffle_id, map_id, server_uri)
        }
    }

    #[inline]
    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        if let Some(tracker) = &self.map_output_tracker {
            tracker.register_shuffle(shuffle_id, num_maps)
        }
    }

    #[inline]
    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        if let Some(tracker) = &self.map_output_tracker {
            tracker.register_map_outputs(shuffle_id, locs)
        }
    }

    /// Compute the optimal coalesced reduce partition count for a completed shuffle-map
    /// stage and store it in the `MapOutputTracker`.
    ///
    /// Uses the global `SHUFFLE_CACHE` to measure the total bytes written per reduce
    /// partition (summed across all map tasks). Merges adjacent small partitions until
    /// each coalesced partition holds at least `coalesce_threshold_bytes / original_n`
    /// average bytes.
    pub fn compute_coalescing(&self, shuffle_id: usize, num_map_partitions: usize) {
        let tracker = match &self.map_output_tracker {
            Some(t) => t.clone(),
            None => return,
        };
        let cache = match atomic_data::env::get_shuffle_cache() {
            Some(c) => c,
            None => return,
        };

        // server_uris[shuffle_id].len() gives the original number of reduce partitions.
        let num_reduce_partitions = tracker
            .server_uris
            .get(&shuffle_id)
            .map(|v| v.len())
            .unwrap_or(0);
        if num_reduce_partitions <= 1 {
            return; // nothing to coalesce
        }

        // Compute total bytes for each reduce partition across all map tasks.
        let bucket_bytes: Vec<u64> = (0..num_reduce_partitions)
            .map(|reduce_id| {
                cache.bytes_for_reduce_partition(shuffle_id, num_map_partitions, reduce_id)
            })
            .collect();

        let total_bytes: u64 = bucket_bytes.iter().sum();
        if total_bytes == 0 {
            return; // empty shuffle — no coalescing needed
        }

        // Target: each coalesced partition should hold at least `threshold / original_n` bytes.
        // Greedily merge adjacent partitions until each meets the target.
        let target_bytes_per_partition =
            (self.coalesce_threshold_bytes / num_reduce_partitions as u64).max(1);

        let mut coalesced_count = 0usize;
        let mut running = 0u64;
        for &bytes in &bucket_bytes {
            running += bytes;
            if running >= target_bytes_per_partition {
                coalesced_count += 1;
                running = 0;
            }
        }
        // Any remaining bytes form the last coalesced partition.
        if running > 0 {
            coalesced_count += 1;
        }

        let coalesced_count = coalesced_count.max(1).min(num_reduce_partitions);
        if coalesced_count < num_reduce_partitions {
            log::info!(
                "adaptive coalescing: shuffle #{shuffle_id} coalesced {num_reduce_partitions} → \
                 {coalesced_count} partitions ({total_bytes} bytes total)"
            );
            tracker.set_coalesced_partitions(shuffle_id, coalesced_count);
        }
    }

    #[inline]
    pub fn remove_output_loc_from_stage(&self, shuffle_id: usize, map_id: usize, server_uri: &str) {
        self.shuffle_to_map_stage
            .get_mut(&shuffle_id)
            .unwrap()
            .remove_output_loc(map_id, server_uri);
    }

    #[inline]
    pub fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>> {
        let locs_opt = self.cache_locs.get(&rdd.get_rdd_id());
        locs_opt.map(|l| l.clone())
    }

    #[inline]
    pub fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>> {
        &self.event_queues
    }

    #[inline]
    pub fn get_next_job_id(&self) -> usize {
        self.next_job_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    pub fn get_next_stage_id(&self) -> usize {
        self.next_stage_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    pub fn get_next_task_id(&self) -> usize {
        self.next_task_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns true if the distributed scheduler already completed a shuffle
    /// for `shuffle_id` — i.e., every map-partition slot in the tracker has a URI.
    pub fn is_shuffle_complete(&self, shuffle_id: usize) -> bool {
        if let Some(tracker) = &self.map_output_tracker {
            tracker
                .server_uris
                .get(&shuffle_id)
                .map_or(false, |arr| !arr.is_empty() && arr.iter().all(|s| s.is_some()))
        } else {
            false
        }
    }

    /// Returns the per-partition URIs for a completed shuffle (in partition order).
    pub fn get_shuffle_server_uris(&self, shuffle_id: usize) -> Vec<String> {
        if let Some(tracker) = &self.map_output_tracker {
            tracker
                .server_uris
                .get(&shuffle_id)
                .map(|arr| arr.iter().filter_map(|s| s.clone()).collect())
                .unwrap_or_default()
        } else {
            vec![]
        }
    }
}
