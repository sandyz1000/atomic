use crate::context::StreamingContext;
use crate::errors::{StreamingError, StreamingResult};
use crate::utils::timer::{next_tick_ms, now_ms};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// JobSet — groups jobs for a single batch time
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct JobSet {
    pub batch_time_ms: u64,
    pub submission_time_ms: u64,
    pub jobs_completed: usize,
    pub jobs_failed: usize,
}

impl JobSet {
    pub fn new(batch_time_ms: u64) -> Self {
        let now = now_ms();
        JobSet {
            batch_time_ms,
            submission_time_ms: now,
            jobs_completed: 0,
            jobs_failed: 0,
        }
    }

    pub fn scheduling_delay_ms(&self) -> u64 {
        self.submission_time_ms.saturating_sub(self.batch_time_ms)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JobScheduler — the batch loop
// ─────────────────────────────────────────────────────────────────────────────

/// Drives the streaming batch loop.
///
/// On each tick (every `batch_duration`), calls `DStreamGraph::generate_jobs()`
/// and runs each job synchronously on the current thread.
pub struct JobScheduler {
    ssc: Arc<StreamingContext>,
    stop_flag: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl JobScheduler {
    pub fn new(ssc: Arc<StreamingContext>) -> Arc<Self> {
        Arc::new(JobScheduler {
            ssc,
            stop_flag: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
        })
    }

    /// Start the batch loop thread.
    pub fn start(self: &Arc<Self>) -> StreamingResult<()> {
        let ssc = self.ssc.clone();
        let stop = self.stop_flag.clone();

        let handle = thread::Builder::new()
            .name("streaming-batch-loop".into())
            .spawn(move || {
                Self::run_batch_loop(ssc, stop);
            })
            .map_err(|e| StreamingError::Internal(e.to_string()))?;

        *self.handle.lock() = Some(handle);
        Ok(())
    }

    /// Stop the batch loop and wait for it to finish.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.lock().take() {
            let _ = h.join();
        }
    }

    fn run_batch_loop(ssc: Arc<StreamingContext>, stop: Arc<AtomicBool>) {
        let batch_ms = ssc.batch_duration.as_millis() as u64;

        // Align to the next batch boundary and start the graph at the previous one
        let zero_time_ms = next_tick_ms(now_ms(), batch_ms) - batch_ms;
        ssc.graph.lock().start(zero_time_ms);

        log::info!(
            "Streaming batch loop started (batch={}ms, zero_time={}ms)",
            batch_ms,
            zero_time_ms
        );

        loop {
            // Sleep until the next batch boundary
            let next = next_tick_ms(now_ms(), batch_ms);
            let now = now_ms();
            if next > now {
                thread::sleep(Duration::from_millis(next - now));
            }

            if stop.load(Ordering::SeqCst) {
                break;
            }

            let batch_time_ms = next_tick_ms(now_ms().saturating_sub(1), batch_ms);
            log::debug!("Generating jobs for batch time {}ms", batch_time_ms);

            let jobs = ssc.graph.lock().generate_jobs(batch_time_ms);
            let num_jobs = jobs.len();

            for job in jobs {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                if let Err(e) = job.run() {
                    log::error!(
                        "Streaming job (batch={}ms) failed: {}",
                        batch_time_ms,
                        e
                    );
                }
            }

            log::debug!("Completed {} jobs for batch time {}ms", num_jobs, batch_time_ms);

            // Optional checkpoint (if checkpoint_dir is set)
            // TODO Phase 5: write checkpoint here when checkpoint_duration aligns
        }

        log::info!("Streaming batch loop stopped");
    }
}
