//! Bugs B2, B3, B4: `MapOutputTracker` panics on invalid state instead of returning `Err`.
//!
//! All three bugs are unchecked `.unwrap()` / index operations on `DashMap` entries.
//!
//! Affected lines in `crates/atomic-data/src/shuffle/map_output.rs`:
//!
//!   B2 (line 302): `self.server_uris.get_mut(&shuffle_id).unwrap()[map_id]`
//!     Panics when `register_shuffle()` was never called for `shuffle_id`.
//!
//!   B3 (line 317): `arr.get(map_id).unwrap()`
//!     Panics when `map_id >= num_maps` passed to `register_shuffle()`.
//!
//!   B4 (unregister path): `self.server_uris.get_mut(&shuffle_id).unwrap()…`
//!     Called from `unregister_map_output()` on an entry that was already removed
//!     or never registered.
//!
//! Fix: replace `.unwrap()` calls with `?` or `if let Some(...)` guards and
//! change the affected method signatures to return `Result<(), MapOutputError>`.

use atomic_data::shuffle::MapOutputTracker;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn tracker() -> MapOutputTracker {
    // `is_master = false` avoids spawning the background HTTP server.
    // Safe for unit tests — no tokio runtime required.
    MapOutputTracker::new(false, SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
}

// ── Bug B2 ────────────────────────────────────────────────────────────────────

/// Bug B2 (fixed): `register_map_output()` returns `Err` when `register_shuffle()` was never called.
#[test]
fn test_unregistered_shuffle() {
    let t = tracker();
    let result = t.register_map_output(99, 0, "http://127.0.0.1:9000".to_string());
    assert!(
        result.is_err(),
        "Expected Err(ShuffleIdNotFound) for unregistered shuffle_id 99, but got Ok"
    );
}

/// Complementary to B2: verify the correct path succeeds.
#[test]
fn test_register_map_ok() {
    let t = tracker();
    t.register_shuffle(1, 3);
    t.register_map_output(1, 0, "http://host1:9000".to_string())
        .unwrap();
    t.register_map_output(1, 1, "http://host2:9000".to_string())
        .unwrap();
    t.register_map_output(1, 2, "http://host3:9000".to_string())
        .unwrap();
    // All three slots filled.
    assert!(t.server_uris.contains_key(&1));
}

// ── Bug B3 ────────────────────────────────────────────────────────────────────

/// Bug B3 (fixed): `register_map_output()` returns `Err` when `map_id >= num_maps`.
#[test]
fn test_oob_map_id() {
    let t = tracker();
    t.register_shuffle(42, 2); // two map slots: index 0 and 1
    let result = t.register_map_output(42, 5, "http://127.0.0.1:9000".to_string());
    assert!(
        result.is_err(),
        "Expected Err(MapIdOutOfBounds) for map_id=5 (num_maps=2), but got Ok"
    );
}

/// Sanity: map_id within bounds succeeds.
#[test]
fn test_inbounds_map_id() {
    let t = tracker();
    t.register_shuffle(5, 3);
    t.register_map_output(5, 0, "http://a:9000".to_string())
        .unwrap();
    t.register_map_output(5, 2, "http://c:9000".to_string())
        .unwrap();
    // map_id 1 left as None — that is valid.
}

// ── Bug B4 ────────────────────────────────────────────────────────────────────

/// Bug B4 (partial): `unregister_map_output()` on an unregistered shuffle_id.
///
/// Looking at the code, `unregister_map_output()` uses `if let Some(arr)` so
/// it is already a no-op for unknown shuffle_ids. This test PASSES to document
/// that the safe path is in place.
///
/// However, the `get_mut` inside the `if let` block (line 302) can still panic
/// if called on an empty entry — this is tested by B2.
#[test]
fn test_unregister_unknown() {
    let t = tracker();
    // shuffle_id 999 was never registered — should be a silent no-op.
    t.unregister_map_output(999, 0, "http://127.0.0.1:9000".to_string());
    // No panic means the guard is working for this path.
}

/// Bug B4 (fixed): `unregister_map_output()` with an out-of-bounds `map_id` is a no-op.
#[test]
fn test_unregister_oob() {
    let t = tracker();
    t.register_shuffle(7, 2); // two slots
    // Out-of-bounds map_id must be a silent no-op, not a panic.
    t.unregister_map_output(7, 99, "http://127.0.0.1:9000".to_string());
    // State must be unchanged: both slots still None.
    let uris = t.server_uris.get(&7).unwrap();
    assert_eq!(uris.len(), 2, "slot count must be unchanged");
    assert!(uris[0].is_none());
    assert!(uris[1].is_none());
}

// ── Idempotency ───────────────────────────────────────────────────────────────

/// `register_shuffle()` called twice for the same id must be idempotent.
/// The current implementation returns early if the id already exists.
#[test]
fn test_register_shuffle_idempotent() {
    let t = tracker();
    t.register_shuffle(10, 4);
    t.register_shuffle(10, 4); // second call — should not overwrite
    assert!(t.server_uris.contains_key(&10));
    assert_eq!(t.server_uris.get(&10).unwrap().len(), 4);
}
