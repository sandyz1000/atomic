/// Distributed sort — demonstrates per-partition sort + k-way merge on the driver.
///
/// Each partition sorts its own elements independently (narrow transformation).
/// The driver merges sorted partition outputs into a globally sorted sequence.
///
/// Run locally:
///   cargo run --manifest-path examples/sort/Cargo.toml
///
/// Run distributed (after `atomic deploy`):
///   ATOMIC_DEPLOYMENT_MODE=distributed ATOMIC_IS_DRIVER=true \
///   ATOMIC_SLAVES=host1@10.0.0.1 \
///   cargo run --manifest-path examples/sort/Cargo.toml
use atomic_compute::context::Context;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    let data: Vec<i32> = vec![
        42, 7, 19, 3, 88, 55, 1, 33, 72, 14,
        61, 28, 95, 6, 47, 83, 11, 39, 66, 24,
        50, 17, 78, 5, 92, 31, 44, 68, 22, 57,
    ];

    let rdd = ctx.parallelize_typed(data, 4);

    // Sort each partition independently on workers.
    let per_partition: Vec<Vec<i32>> = ctx.run_job(rdd.into_rdd(), |iter| {
        let mut v: Vec<i32> = iter.collect();
        v.sort();
        v
    })?;

    let merged = merge_sorted(per_partition);

    println!("Sorted output ({} elements):", merged.len());
    println!("{:?}", merged);
    assert!(merged.windows(2).all(|w| w[0] <= w[1]), "result is not sorted");
    println!("Sort verified.");

    Ok(())
}

/// K-way merge of sorted vectors using a min-heap.
fn merge_sorted(partitions: Vec<Vec<i32>>) -> Vec<i32> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut heap: BinaryHeap<Reverse<(i32, usize, usize)>> = BinaryHeap::new();
    for (pi, partition) in partitions.iter().enumerate() {
        if let Some(&first) = partition.first() {
            heap.push(Reverse((first, pi, 0)));
        }
    }

    let total: usize = partitions.iter().map(|p| p.len()).sum();
    let mut result = Vec::with_capacity(total);

    while let Some(Reverse((val, pi, ei))) = heap.pop() {
        result.push(val);
        let next_ei = ei + 1;
        if let Some(&next_val) = partitions[pi].get(next_ei) {
            heap.push(Reverse((next_val, pi, next_ei)));
        }
    }

    result
}
