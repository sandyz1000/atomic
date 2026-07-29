use crate::error::{LibResult, SchedulerError};
use atomic_data::data::Data;
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

/// Interface used to listen for job completion or failure events after submitting a job to the
/// scheduler. The listener is notified each time a task succeeds, as well as if the whole
/// job fails (and no further task-succeeded events will happen).
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
    /// Downcast support for `BusListener`s that need the concrete event type.
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Copy)]
pub struct StageInfo {}

pub struct JobStartListener {
    pub job_id: usize,
    pub time: Instant,
    pub stage_infos: Vec<StageInfo>,
}
impl ListenerEvent for JobStartListener {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct JobEndListener {
    pub job_id: usize,
    pub time: Instant,
    pub job_result: bool,
}
impl ListenerEvent for JobEndListener {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Receives every event posted to a `LiveListenerBus` after registration via
/// [`LiveListenerBus::add_listener`].
pub trait BusListener: Send + Sync {
    fn on_event(&self, event: &dyn ListenerEvent);
}

trait AsyncEventQueue: Send + Sync {
    fn post(&mut self, event: Arc<dyn ListenerEvent>);
    fn start(&mut self);
    fn stop(&mut self);
}

/// Bridges a `BusListener` into the bus's internal `AsyncEventQueue` slots.
struct ListenerQueue {
    listener: Arc<dyn BusListener>,
}

impl AsyncEventQueue for ListenerQueue {
    fn post(&mut self, event: Arc<dyn ListenerEvent>) {
        self.listener.on_event(event.as_ref());
    }
    fn start(&mut self) {}
    fn stop(&mut self) {}
}

type QueueBuffer = Option<Arc<Mutex<Vec<Arc<dyn ListenerEvent>>>>>;

/// Asynchronously passes listener events to registered listeners.
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

    /// Register a listener to receive every event posted to this bus from now on.
    ///
    /// A listener added before `start()` also receives every event buffered before
    /// start (replayed in post order, once `start()` runs). A listener added after
    /// `start()` only sees events posted from that point forward — already-delivered
    /// pre-start events are gone from the buffer by then.
    pub fn add_listener(&self, listener: Arc<dyn BusListener>) {
        let mut queue: Box<dyn AsyncEventQueue> = Box::new(ListenerQueue { listener });
        if self.started.load(Ordering::SeqCst) {
            queue.start();
        }
        self.queues.write().push(queue);
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

#[cfg(test)]
mod tests {
    use super::*;

    struct RecordingListener {
        job_ids: Mutex<Vec<usize>>,
    }

    impl BusListener for RecordingListener {
        fn on_event(&self, event: &dyn ListenerEvent) {
            if let Some(e) = event.as_any().downcast_ref::<JobStartListener>() {
                self.job_ids.lock().push(e.job_id);
            } else if let Some(e) = event.as_any().downcast_ref::<JobEndListener>() {
                self.job_ids.lock().push(e.job_id);
            }
        }
    }

    #[test]
    fn add_listener_receives_events_posted_after_start() {
        let mut bus = LiveListenerBus::new();
        bus.start().unwrap();

        let listener = Arc::new(RecordingListener {
            job_ids: Mutex::new(vec![]),
        });
        bus.add_listener(listener.clone());

        bus.post(Box::new(JobStartListener {
            job_id: 7,
            time: Instant::now(),
            stage_infos: vec![],
        }));
        bus.post(Box::new(JobEndListener {
            job_id: 7,
            time: Instant::now(),
            job_result: true,
        }));

        assert_eq!(*listener.job_ids.lock(), vec![7, 7]);
    }

    #[test]
    fn add_listener_before_start_replays_buffered_events() {
        let mut bus = LiveListenerBus::new();

        // Posted before start(): buffered on the bus, not yet delivered anywhere.
        bus.post(Box::new(JobStartListener {
            job_id: 3,
            time: Instant::now(),
            stage_infos: vec![],
        }));

        let listener = Arc::new(RecordingListener {
            job_ids: Mutex::new(vec![]),
        });
        bus.add_listener(listener.clone());

        assert!(listener.job_ids.lock().is_empty(), "not delivered yet");

        bus.start().unwrap();

        assert_eq!(*listener.job_ids.lock(), vec![3]);
    }
}
