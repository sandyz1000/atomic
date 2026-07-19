use atomic_data::accumulator::MergeFn;
use atomic_scheduler::{
    ActiveShuffleStage as SchedulerActiveShuffleStage, LocalScheduler, Schedulers,
};
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use crate::env::{Config, DeploymentMode};
use crate::error::{ComputeError, ComputeResult};
mod broadcast;
mod io;
mod job_runner;
mod pipeline_executor;
mod scope;
mod worker;

pub use worker::start_worker;

/// Process-global shuffle-id counter. Shuffle cache and `MAP_OUTPUT_TRACKER` are
/// process-wide singletons, so ids must be unique across every `Context` in the process.
static NEXT_SHUFFLE_ID: AtomicUsize = AtomicUsize::new(0);

/// Driver-side accumulator registry: `accumulator_id → (current_bytes, merge_fn)`.
type AccumulatorStore = Arc<dashmap::DashMap<usize, (Vec<u8>, Arc<MergeFn>)>>;

pub struct Context {
    /// Runtime configuration — built at the entry point and passed in.
    pub(crate) config: Arc<Config>,
    /// Routes native `#[task]` jobs: local or distributed TCP dispatch.
    pub(crate) scheduler: Schedulers,
    /// Always-local scheduler used for closure-based driver operations.
    /// Closures cannot be sent to remote workers, so all `collect()`, `count()`,
    /// `fold()`, etc. execute here regardless of deployment mode.
    pub(crate) driver_scheduler: Arc<LocalScheduler>,
    pub(crate) next_rdd_id: Arc<AtomicUsize>,
    pub(crate) address_map: Vec<SocketAddrV4>,
    pub(crate) distributed_driver: bool,
    pub(crate) work_dir: PathBuf,
    /// Driver-side broadcast variable store: `broadcast_id → rkyv-encoded bytes`.
    pub(crate) broadcast_store: Arc<dashmap::DashMap<usize, Vec<u8>>>,
    /// Driver-side accumulator store: `accumulator_id → (current_bytes, merge_fn)`.
    pub(crate) accumulator_store: AccumulatorStore,
    /// Provisions dedicated workers for [`Context::with_workers`]. `None` in local
    /// mode (and whenever no allocator is configured).
    pub(crate) allocator: Option<Arc<dyn atomic_scheduler::WorkerAllocator>>,
    /// True for the transient per-allocation view built by [`Context::with_workers`].
    /// Its `Drop` must not run driver cleanup — it shares `work_dir`/`address_map`
    /// with the parent context, which still owns them.
    pub(crate) scoped: bool,
    /// Distributed shuffle stages seen by `run_pending_shuffle_stages`, keyed by
    /// shuffle id. The fetch-failure recovery hook replays a single lost map
    /// partition from the stored dependency + ops. Entries live as long as the
    /// context (like the `MapOutputTracker`'s per-shuffle state).
    pub(crate) active_shuffle_stages: ActiveShuffleStages,
}

/// Distributed shuffle-map stage metadata retained for reduce-side fetch-failure
/// recovery (`shuffle_id → dep + ops`).
pub(crate) type ActiveShuffleStage = SchedulerActiveShuffleStage;

pub(crate) type ActiveShuffleStages = Arc<dashmap::DashMap<usize, ActiveShuffleStage>>;

impl Drop for Context {
    fn drop(&mut self) {
        if self.scoped {
            return;
        }
        #[cfg(debug_assertions)]
        if self.distributed_driver {
            log::info!("inside context drop in master");
        }
        Context::driver_clean_up_directives(&self.work_dir, &self.address_map);
        // The shuffle infrastructure is a process-global singleton. A single Context
        // drop must NOT clear it — that would break concurrent Contexts. Use
        // `shutdown()` / `stop()` for explicit teardown.
    }
}

impl Context {
    /// Create a context using an explicit [`Config`] built at the program entry point.
    pub fn new_with_config(config: Config) -> ComputeResult<Arc<Self>> {
        config.validate()?;
        match config.mode {
            DeploymentMode::Distributed => {
                let ctx = Context::init_distributed_driver(config)?;
                ctx.set_cleanup_process();
                Ok(ctx)
            }
            DeploymentMode::Local => Context::init_local_scheduler(config),
        }
    }

    /// Create a context from environment variables.
    ///
    /// Prefer [`Context::new_with_config`] for new Rust programs; this exists for
    /// Python/JS bindings and legacy code where explicit config is not practical.
    pub fn new() -> ComputeResult<Arc<Self>> {
        let mut config = Config::from_env()?;
        if config.mode == DeploymentMode::Distributed
            && config.workers.is_empty()
            && let Ok(hosts) = crate::hosts::Hosts::get()
        {
            config.workers = hosts
                .slaves
                .iter()
                .filter_map(|s| {
                    let hp = s.split('@').nth(1)?;
                    hp.parse().ok()
                })
                .collect();
        }
        Context::new_with_config(config)
    }

    /// Create a local-only context.
    pub fn local() -> ComputeResult<Arc<Self>> {
        Context::new_with_config(Config::local())
    }

    pub fn is_distributed(&self) -> bool {
        self.distributed_driver
    }

    /// Shut down the compute context, releasing shuffle infrastructure.
    pub fn shutdown(&self) {
        atomic_data::env::clear_shuffle_infra();
    }

    /// Gracefully stop this context, draining in-flight tasks then signaling workers.
    pub fn stop(&self) {
        if let Schedulers::Distributed(sched) = &self.scheduler {
            let timeout = Duration::from_millis(self.config.drain_timeout_ms);
            if !sched.drain(timeout, Duration::from_millis(50)) {
                log::warn!(
                    "Context::stop: {} task(s) still in-flight after {:?} drain timeout; proceeding",
                    sched.total_inflight(),
                    timeout,
                );
            }
        }
        if !self.config.workers.is_empty() {
            Context::drop_executors(&self.config.workers);
        }
        atomic_data::env::clear_shuffle_infra();
    }

    /// Cancel a running distributed job by its `run_id`. No-op in local mode.
    pub fn cancel_job(&self, run_id: usize) -> ComputeResult<()> {
        match &self.scheduler {
            Schedulers::Distributed(sched) => Ok(sched.cancel_job(run_id)?),
            Schedulers::Local(_) => Ok(()),
        }
    }

    /// Drop a distributed RDD's cache locations so the next job recomputes. No-op in local mode.
    pub fn invalidate_distributed_cache(&self, rdd_id: usize) {
        if let Schedulers::Distributed(sched) = &self.scheduler {
            sched.invalidate_rdd_cache(rdd_id);
        }
    }

    pub fn new_rdd_id(&self) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_shuffle_id(&self) -> usize {
        NEXT_SHUFFLE_ID.fetch_add(1, Ordering::SeqCst)
    }

    /// Default number of output partitions for wide transformations.
    pub fn default_parallelism(&self) -> usize {
        num_cpus::get().clamp(1, 64)
    }
}

pub fn save<R: atomic_data::data::Data>(
    ctx: atomic_data::task_context::TaskContext,
    iter: Box<dyn Iterator<Item = R>>,
    path: String,
) -> ComputeResult<()> {
    use std::io::Write;
    std::fs::create_dir_all(&path).map_err(ComputeError::OutputWrite)?;
    let id = ctx.split_id;
    let file_path = std::path::Path::new(&path).join(format!("part-{}", id));
    let f = std::fs::File::create(file_path).map_err(ComputeError::OutputWrite)?;
    let mut f = std::io::BufWriter::new(f);
    for item in iter {
        let line = format!("{:?}", item);
        f.write_all(line.as_bytes())
            .map_err(ComputeError::OutputWrite)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Context;

    #[test]
    fn local_creates() {
        assert!(Context::local().is_ok());
    }

    #[test]
    fn local_not_distributed() {
        let ctx = Context::local().unwrap();
        assert!(!ctx.is_distributed());
    }
}
