use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn next_tick_ms(now: u64, period_ms: u64) -> u64 {
    ((now / period_ms) + 1) * period_ms
}
