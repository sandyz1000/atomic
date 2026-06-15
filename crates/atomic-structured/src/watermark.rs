//! Event-time watermark tracking.
//!
//! The watermark is `max(event_time seen) − delay`, advanced monotonically across
//! batches. A window whose end is `<= watermark` is considered final: its result
//! can be emitted (Append) and its state evicted, and rows for it are late.

/// Tracks a single monotonic event-time watermark for a query.
#[derive(Debug, Clone, Default)]
pub struct WatermarkTracker {
    /// Allowed lateness, in milliseconds.
    delay_ms: u64,
    /// Current watermark (epoch ms). `None` until the first event is seen.
    watermark_ms: Option<u64>,
}

impl WatermarkTracker {
    pub fn new(delay_ms: u64) -> Self {
        WatermarkTracker {
            delay_ms,
            watermark_ms: None,
        }
    }

    /// Fold the maximum event time observed in a batch into the watermark.
    /// The watermark only ever moves forward.
    pub fn observe_max(&mut self, max_event_ms: u64) {
        let candidate = max_event_ms.saturating_sub(self.delay_ms);
        self.watermark_ms = Some(match self.watermark_ms {
            Some(w) => w.max(candidate),
            None => candidate,
        });
    }

    /// Current watermark (epoch ms), or `None` before any event.
    pub fn current(&self) -> Option<u64> {
        self.watermark_ms
    }

    /// Whether a window ending at `window_end_ms` is final (watermark passed it).
    pub fn is_final(&self, window_end_ms: u64) -> bool {
        matches!(self.watermark_ms, Some(w) if window_end_ms <= w)
    }

    /// Restore a watermark value (recovery from checkpoint).
    pub fn restore(&mut self, watermark_ms: Option<u64>) {
        self.watermark_ms = watermark_ms;
    }
}
