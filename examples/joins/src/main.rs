//! Pair-RDD joins — `join`, outer joins, and `cogroup`.
//!
//! Mirrors Spark's pair-RDD join examples. These joins are **shuffle-based**:
//! both sides are partitioned by key hash into two shuffle stages, so they run
//! in local *and* distributed mode without collecting either dataset to the
//! driver.
//!
//! Two relations keyed by user id:
//! - `users`:  id → name        (4, 5 have no orders)
//! - `orders`: user_id → amount  (user 9 placed an order but isn't in `users`)
//!
//! # Running locally
//!
//! ```bash
//! cargo run -p joins
//! ```
//!
//! # Running distributed (two terminals)
//!
//! Terminal 1 — start a worker:
//! ```bash
//! cargo build -p joins --release
//! ./target/release/joins --worker --port 10001
//! ```
//!
//! Terminal 2 — run the driver:
//! ```bash
//! ./target/release/joins --workers 127.0.0.1:10001
//! ```
use std::sync::Arc;

use atomic_compute::app::{AppRole, AtomicApp};
use atomic_compute::context::Context;
use atomic_compute::rdd::TypedRdd;

// Shuffle-based joins/cogroup partition each side by key hash, so the
// shuffle-map writer for every (K, V) that crosses the shuffle must be linked
// into the binary. Both relations are keyed by i32.
atomic_compute::register_shuffle_map!(i32, String);
atomic_compute::register_shuffle_map!(i32, u32);

fn users(ctx: &Arc<Context>) -> TypedRdd<(i32, String)> {
    ctx.parallelize_typed(
        vec![
            (1, "Alice".to_string()),
            (2, "Bob".to_string()),
            (3, "Carol".to_string()),
            (4, "Dave".to_string()),
            (5, "Eve".to_string()),
        ],
        2,
    )
}

fn orders(ctx: &Arc<Context>) -> TypedRdd<(i32, u32)> {
    ctx.parallelize_typed(vec![(1, 50), (1, 20), (2, 80), (3, 10), (9, 99)], 2)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = AtomicApp::build().await?;
    let ctx = match app.role {
        AppRole::Worker { .. } => app.run_worker(),
        AppRole::Driver => app.driver_context()?,
    };

    // --- inner join: only keys present in BOTH sides ---
    let mut inner = users(&ctx).join(orders(&ctx)).collect()?;
    inner.sort();
    println!("=== join (inner) — name ⨝ amount ===");
    for (id, (name, amount)) in &inner {
        println!("  {id}: {name:<6} ${amount}");
    }

    // --- left outer join: every user, orders optional ---
    let mut left = users(&ctx).left_outer_join(orders(&ctx)).collect()?;
    left.sort();
    println!("\n=== left_outer_join — every user, amount optional ===");
    for (id, (name, amount)) in &left {
        println!("  {id}: {name:<6} {amount:?}");
    }

    // --- right outer join: every order, user name optional (user 9 unknown) ---
    let mut right = users(&ctx).right_outer_join(orders(&ctx)).collect()?;
    right.sort();
    println!("\n=== right_outer_join — every order, name optional ===");
    for (id, (name, amount)) in &right {
        println!("  {id}: {name:?} ${amount}");
    }

    // --- full outer join: union of keys, both sides optional ---
    let mut full = users(&ctx).full_outer_join(orders(&ctx)).collect()?;
    full.sort();
    println!("\n=== full_outer_join — union of keys ===");
    for (id, (name, amount)) in &full {
        println!("  {id}: {name:?} {amount:?}");
    }

    // --- cogroup: all values per key grouped from both sides ---
    let mut grouped = users(&ctx).cogroup(orders(&ctx)).collect()?;
    grouped.sort_by_key(|(k, _, _)| *k);
    println!("\n=== cogroup — (names, amounts) per id ===");
    for (id, names, amounts) in &grouped {
        println!("  {id}: names={names:?} amounts={amounts:?}");
    }

    Ok(())
}
