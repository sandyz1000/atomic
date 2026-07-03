//! Per-job dedicated worker allocation: [`Context::with_workers`].
//!
//! A job declares a [`ResourceProfile`]; the context's allocator provisions matching
//! workers, the job runs pinned to exactly those workers, and they are released when
//! the closure returns. In local mode (or when no allocator is configured) this is a
//! transparent pass-through that runs the closure against the current context.

use std::sync::Arc;

use atomic_scheduler::{DistributedScheduler, ResourceProfile, Schedulers};

use crate::error::{ComputeError, ComputeResult};

use super::Context;

impl Context {
    /// Run `f` on workers dedicated to this job and sized to `profile`.
    ///
    /// Allocates workers via the configured allocator, registers any that are not
    /// already in the pool, scopes task placement to exactly the allocated set, runs
    /// `f`, then releases the workers it provisioned. Pre-existing pool workers handed
    /// out by the [`StaticAllocator`](atomic_scheduler::StaticAllocator) are never
    /// torn down — only workers this call newly registered are removed afterward.
    ///
    /// In local mode, or when no allocator is configured, `f` runs against `self`
    /// unchanged.
    ///
    /// `f` runs on a blocking thread (`spawn_blocking`): the RDD actions inside it
    /// (`collect`, `count`, …) are synchronous and drive the scheduler's own runtime,
    /// so they must not run on this `async fn`'s executor thread. `f` and its result
    /// are therefore `Send + 'static`.
    pub async fn with_workers<R, F>(
        self: &Arc<Self>,
        profile: ResourceProfile,
        f: F,
    ) -> ComputeResult<R>
    where
        F: FnOnce(Arc<Context>) -> ComputeResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let (Schedulers::Distributed(parent_sched), Some(allocator)) =
            (&self.scheduler, &self.allocator)
        else {
            return run_blocking(Arc::clone(self), f).await;
        };

        let alloc_id = uuid::Uuid::new_v4().to_string();
        let endpoints = allocator
            .allocate(&alloc_id, &profile)
            .await
            .map_err(|e| ComputeError::WorkerHandshake(e.to_string()))?;
        if endpoints.is_empty() {
            return Err(ComputeError::WorkerHandshake(
                "allocator returned no workers".to_string(),
            ));
        }

        // Register endpoints not already in the pool (e.g. freshly created pods), and
        // track which we added so teardown removes only those — never the static pool.
        let mut newly_registered = Vec::new();
        for &endpoint in &endpoints {
            if parent_sched.is_worker_registered(&endpoint) {
                continue;
            }
            match Context::probe_worker(endpoint) {
                Ok(capabilities) => {
                    parent_sched.register_worker(endpoint, capabilities);
                    newly_registered.push(endpoint);
                }
                Err(e) => {
                    teardown(parent_sched, &newly_registered, allocator, &alloc_id).await;
                    return Err(e);
                }
            }
        }

        let scoped_sched = Arc::new(parent_sched.scoped_to(endpoints));
        let scoped_ctx = Arc::new(self.scoped_view(scoped_sched));

        let result = run_blocking(scoped_ctx, f).await;

        teardown(parent_sched, &newly_registered, allocator, &alloc_id).await;
        result
    }

    /// Clone this context but route jobs through `scheduler` (a scoped view). Shares
    /// every other resource by `Arc`; marked `scoped` so its `Drop` does not run
    /// driver cleanup on the shared `work_dir`/`address_map`.
    fn scoped_view(&self, scheduler: Arc<DistributedScheduler>) -> Context {
        Context {
            config: Arc::clone(&self.config),
            scheduler: Schedulers::Distributed(scheduler),
            driver_scheduler: Arc::clone(&self.driver_scheduler),
            next_rdd_id: Arc::clone(&self.next_rdd_id),
            address_map: self.address_map.clone(),
            distributed_driver: self.distributed_driver,
            work_dir: self.work_dir.clone(),
            broadcast_store: Arc::clone(&self.broadcast_store),
            accumulator_store: Arc::clone(&self.accumulator_store),
            allocator: self.allocator.clone(),
            scoped: true,
            active_shuffle_stages: Arc::clone(&self.active_shuffle_stages),
        }
    }
}

/// Run a synchronous job closure off the async executor (the RDD actions inside it
/// drive the scheduler's own runtime and must not block this thread).
async fn run_blocking<R, F>(ctx: Arc<Context>, f: F) -> ComputeResult<R>
where
    F: FnOnce(Arc<Context>) -> ComputeResult<R> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || f(ctx))
        .await
        .map_err(|e| ComputeError::WorkerHandshake(format!("with_workers job panicked: {e}")))?
}

/// Remove the workers this allocation registered and release the allocation.
async fn teardown(
    sched: &DistributedScheduler,
    newly_registered: &[std::net::SocketAddrV4],
    allocator: &Arc<dyn atomic_scheduler::WorkerAllocator>,
    alloc_id: &str,
) {
    for &endpoint in newly_registered {
        sched.remove_worker(endpoint);
    }
    if let Err(e) = allocator.release(alloc_id).await {
        log::warn!("worker release for alloc {alloc_id} failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_passthrough() {
        let ctx = Context::local().expect("local context");
        let distributed = ctx
            .with_workers(ResourceProfile::new(2), |sc| Ok(sc.is_distributed()))
            .await
            .expect("with_workers should run the closure");
        assert!(!distributed);
    }
}
