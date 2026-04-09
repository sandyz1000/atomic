use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

pub mod fileutils;
pub mod timer;

/// A simple single-threaded event loop that processes events from a queue.
pub struct EventLoop<E: Send + 'static> {
    name: String,
    queue: Arc<Mutex<VecDeque<E>>>,
    stopped: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl<E: Send + 'static> EventLoop<E> {
    pub fn new(name: impl Into<String>) -> Self {
        EventLoop {
            name: name.into(),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            stopped: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Start the event loop. `on_receive` is called for each event.
    pub fn start<F>(&mut self, on_receive: F)
    where
        F: Fn(E) + Send + 'static,
    {
        assert!(
            !self.stopped.load(Ordering::SeqCst),
            "{} has already been stopped",
            self.name
        );
        let queue = self.queue.clone();
        let stopped = self.stopped.clone();
        let handle = thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || {
                loop {
                    let event = queue.lock().pop_front();
                    if let Some(e) = event {
                        on_receive(e);
                    } else if stopped.load(Ordering::SeqCst) {
                        break;
                    } else {
                        thread::yield_now();
                    }
                }
            })
            .expect("failed to spawn event loop thread");
        self.handle = Some(handle);
    }

    /// Post an event to the queue.
    pub fn post(&self, event: E) {
        if !self.stopped.load(Ordering::SeqCst) {
            self.queue.lock().push_back(event);
        }
    }

    /// Stop the event loop and wait for the thread to finish.
    pub fn stop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    pub fn is_active(&self) -> bool {
        !self.stopped.load(Ordering::SeqCst)
    }
}
