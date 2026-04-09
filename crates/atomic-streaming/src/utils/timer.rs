use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A timer that fires a callback at fixed intervals.
pub struct RecurringTimer {
    period: Duration,
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RecurringTimer {
    pub fn new(period: Duration) -> Self {
        RecurringTimer {
            period,
            stop: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
        }
    }

    /// Start the timer, calling `on_tick(time_ms)` at each interval boundary.
    pub fn start<F>(&self, on_tick: F)
    where
        F: Fn(u64) + Send + 'static,
    {
        let period_ms = self.period.as_millis() as u64;
        let stop = self.stop.clone();

        let handle = thread::Builder::new()
            .name("recurring-timer".into())
            .spawn(move || {
                // Align to the next period boundary
                let mut next_ms = next_tick_ms(now_ms(), period_ms);
                loop {
                    let now = now_ms();
                    if next_ms > now {
                        thread::sleep(Duration::from_millis(next_ms - now));
                    }
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    on_tick(next_ms);
                    next_ms += period_ms;
                }
            })
            .expect("failed to spawn recurring timer thread");

        *self.handle.lock() = Some(handle);
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.lock().take() {
            let _ = h.join();
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn next_tick_ms(now: u64, period_ms: u64) -> u64 {
    ((now / period_ms) + 1) * period_ms
}
