use crate::error::{LibResult, SchedulerError};
use atomic_data::data::Data;
use parking_lot::{Mutex, RwLock};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

/// Interface used to listen for job completion or failure events after submitting a job to the
/// DAGScheduler. The listener is notified each time a task succeeds, as well as if the whole
/// job fails (and no further taskSucceeded events will happen).
#[async_trait::async_trait]
pub trait JobListener: Send + Sync {
    async fn task_succeeded(&self, _index: usize, _result: &dyn Data) -> LibResult<()> {
        Ok(())
    }
    async fn job_failed(&self, err: SchedulerError) {
        log::debug!("job failed with error: {}", err);
    }
}

/// A listener which produces no action whatsoever.
pub struct NoOpListener;

impl JobListener for NoOpListener {}

pub trait ListenerEvent: Send + Sync {
    ///Whether output this event to the event log.
    fn log_event(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy)]
pub struct StageInfo {}

pub struct JobStartListener {
    pub job_id: usize,
    pub time: Instant,
    pub stage_infos: Vec<StageInfo>,
}
impl ListenerEvent for JobStartListener {}

pub struct JobEndListener {
    pub job_id: usize,
    pub time: Instant,
    pub job_result: bool,
}
impl ListenerEvent for JobEndListener {}

trait AsyncEventQueue: Send + Sync {
    fn post(&mut self, event: Arc<dyn ListenerEvent>);
    fn start(&mut self);
    fn stop(&mut self);
}

type QueueBuffer = Option<Arc<Mutex<Vec<Arc<dyn ListenerEvent>>>>>;

/// Asynchronously passes SparkListenerEvents to registered SparkListeners.
///
/// Until `start()` is called, all posted events are only buffered. Only after this listener bus
/// has started will events be actually propagated to all attached listeners. This listener bus
/// is stopped when `stop()` is called, and it will drop further events after stopping.
#[derive(Clone)]
pub struct LiveListenerBus {
    /// Indicate if `start()` is called
    started: Arc<AtomicBool>,
    /// Indicate if `stop()` is called
    stopped: Arc<AtomicBool>,
    queued_events: QueueBuffer,
    queues: Arc<RwLock<Vec<Box<dyn AsyncEventQueue>>>>,
}

impl Default for LiveListenerBus {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveListenerBus {
    pub fn new() -> Self {
        LiveListenerBus {
            started: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            queued_events: Some(Arc::new(Mutex::new(vec![]))),
            queues: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Post an event to all queues.
    pub fn post(&self, event: Box<dyn ListenerEvent>) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }

        //TODO: self.metrics.num_events_posted.inc()

        match self.queued_events {
            None => {
                // If the event buffer is null, it means the bus has been started and we can avoid
                // synchronization and post events directly to the queues. This should be the most
                // common case during the life of the bus.
                self.post_to_queues(event);
            }
            Some(ref queue) => {
                // Otherwise, need to synchronize to check whether the bus is started, to make sure the thread
                // calling start() picks up the new event.
                if !self.started.load(Ordering::SeqCst) {
                    queue.lock().push(Arc::from(event));
                } else {
                    // If the bus was already started when the check above was made, just post directly to the queues.
                    self.post_to_queues(event);
                }
            }
        }
    }

    fn post_to_queues(&self, event: Box<dyn ListenerEvent>) {
        let event: Arc<dyn ListenerEvent> = Arc::from(event);
        for queue in &mut *self.queues.write() {
            queue.post(event.clone());
        }
    }

    /// Start sending events to attached listeners.
    ///
    /// This first sends out all buffered events posted before this listener bus has started, then
    /// listens for any additional events asynchronously while the listener bus is still running.
    /// This should only be called once.
    pub fn start(&mut self) -> LibResult<()> {
        if self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SchedulerError::Other);
        }

        let mut queues = self.queues.write();
        {
            let queued_events = self
                .queued_events
                .as_ref()
                .ok_or(SchedulerError::Other)?
                .lock();
            for queue in queues.iter_mut() {
                queue.start();
                queued_events
                    .iter()
                    .for_each(|event| queue.post(event.clone()));
            }
        }
        self.queued_events = None;
        // TODO: metricsSystem.registerSource(metrics)
        Ok(())
    }

    /// Stop the listener bus. It will wait until the queued events have been processed, but drop the
    /// new events after stopping.
    pub fn stop(&mut self) -> LibResult<()> {
        if !self.started.load(Ordering::SeqCst) {
            return Err(SchedulerError::Other);
        }

        if self
            .stopped
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Ok(());
        }

        let mut queues = self.queues.write();
        for queue in queues.iter_mut() {
            queue.stop();
        }
        queues.clear();
        Ok(())
    }
}
